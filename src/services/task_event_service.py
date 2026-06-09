"""Structured task event models and persistence services."""

from __future__ import annotations

import asyncio
import uuid
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from typing import Any

from pydantic import BaseModel, Field

from src.core.sensitive import redact_sensitive, redact_sensitive_text
from src.storage.base import BaseStorage, StorageRecord


class TaskEvent(BaseModel):
    """A redacted, queryable event emitted during task execution."""

    event_id: str = Field(default_factory=lambda: uuid.uuid4().hex)
    task_id: str
    seq: int
    type: str
    level: str = "info"
    message: str = ""
    payload: dict[str, Any] = Field(default_factory=dict)
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    def to_public_payload(self) -> dict[str, Any]:
        """Return the API/WebSocket-safe representation."""
        return redact_sensitive(self.model_dump(mode="json"))


class TaskEventService(ABC):
    """Persistence boundary for structured task events."""

    @abstractmethod
    async def append(
        self,
        task_id: str,
        event_type: str,
        *,
        level: str = "info",
        message: str = "",
        payload: dict[str, Any] | None = None,
    ) -> TaskEvent:
        """Append an event and return the stored model."""
        ...

    @abstractmethod
    async def list_events(
        self,
        task_id: str,
        *,
        limit: int = 200,
        offset: int = 0,
        order: str = "asc",
    ) -> list[TaskEvent]:
        """List events for a task ordered by sequence."""
        ...

    async def delete_events(self, task_id: str) -> None:
        """Delete events for a task when the backend supports it."""
        return None


class InMemoryTaskEventService(TaskEventService):
    """In-memory implementation for tests and lightweight local wiring."""

    def __init__(self) -> None:
        self._events: dict[str, list[TaskEvent]] = {}
        self._lock = asyncio.Lock()

    async def append(
        self,
        task_id: str,
        event_type: str,
        *,
        level: str = "info",
        message: str = "",
        payload: dict[str, Any] | None = None,
    ) -> TaskEvent:
        async with self._lock:
            events = self._events.setdefault(task_id, [])
            event = _build_event(
                task_id=task_id,
                seq=len(events) + 1,
                event_type=event_type,
                level=level,
                message=message,
                payload=payload,
            )
            events.append(event)
            return event

    async def list_events(
        self,
        task_id: str,
        *,
        limit: int = 200,
        offset: int = 0,
        order: str = "asc",
    ) -> list[TaskEvent]:
        async with self._lock:
            events = list(self._events.get(task_id, []))
        if _normalize_order(order) == "desc":
            events.reverse()
        return _slice_events(events, limit=limit, offset=offset)

    async def delete_events(self, task_id: str) -> None:
        async with self._lock:
            self._events.pop(task_id, None)


class StorageTaskEventService(TaskEventService):
    """Task event service backed by the existing scheduler storage namespace."""

    def __init__(self, storage: BaseStorage) -> None:
        self._storage = storage
        self._counters: dict[str, int] = {}
        self._lock = asyncio.Lock()

    async def append(
        self,
        task_id: str,
        event_type: str,
        *,
        level: str = "info",
        message: str = "",
        payload: dict[str, Any] | None = None,
    ) -> TaskEvent:
        async with self._lock:
            seq = await self._next_seq(task_id)
            event = _build_event(
                task_id=task_id,
                seq=seq,
                event_type=event_type,
                level=level,
                message=message,
                payload=payload,
            )
            await self._storage.save(
                StorageRecord(
                    key=_event_key(task_id, seq),
                    data=event.model_dump(mode="json"),
                    metadata={
                        "kind": "task_event",
                        "task_id": task_id,
                        "seq": seq,
                        "type": event.type,
                        "level": event.level,
                    },
                    source="scheduler",
                    tags=["task_event", event.type, task_id],
                )
            )
            return event

    async def list_events(
        self,
        task_id: str,
        *,
        limit: int = 200,
        offset: int = 0,
        order: str = "asc",
    ) -> list[TaskEvent]:
        result = await self._storage.query(f"key:{_event_prefix(task_id)}", limit=5000)
        events = [_event_from_record(record) for record in result.records]
        events = [event for event in events if event is not None]
        events.sort(key=lambda event: (event.seq, event.created_at, event.event_id))
        if _normalize_order(order) == "desc":
            events.reverse()
        return _slice_events(events, limit=limit, offset=offset)

    async def delete_events(self, task_id: str) -> None:
        keys = await self._storage.list_keys(prefix=_event_prefix(task_id), limit=5000)
        for key in keys:
            await self._storage.delete(key)
        self._counters.pop(task_id, None)

    async def _next_seq(self, task_id: str) -> int:
        current = self._counters.get(task_id)
        if current is None:
            existing = await self.list_events(task_id, limit=5000)
            current = max((event.seq for event in existing), default=0)
        seq = current + 1
        self._counters[task_id] = seq
        return seq


def _build_event(
    *,
    task_id: str,
    seq: int,
    event_type: str,
    level: str,
    message: str,
    payload: dict[str, Any] | None,
) -> TaskEvent:
    return TaskEvent(
        task_id=task_id,
        seq=seq,
        type=redact_sensitive_text(str(event_type or "log")),
        level=redact_sensitive_text(str(level or "info")),
        message=redact_sensitive_text(str(message or "")),
        payload=redact_sensitive(payload or {}),
    )


def _event_prefix(task_id: str) -> str:
    return f"task_event:{task_id}:"


def _event_key(task_id: str, seq: int) -> str:
    return f"{_event_prefix(task_id)}{seq:08d}"


def _event_from_record(record: StorageRecord) -> TaskEvent | None:
    try:
        if isinstance(record.data, TaskEvent):
            return record.data
        if isinstance(record.data, dict):
            return TaskEvent.model_validate(record.data)
    except Exception:
        return None
    return None


def _slice_events(events: list[TaskEvent], *, limit: int, offset: int) -> list[TaskEvent]:
    safe_offset = max(0, int(offset or 0))
    safe_limit = min(max(1, int(limit or 200)), 1000)
    return events[safe_offset : safe_offset + safe_limit]


def _normalize_order(value: str) -> str:
    order = str(value or "asc").strip().lower()
    return order if order in {"asc", "desc"} else "asc"
