"""Worker registry models and persistence services."""

from __future__ import annotations

import asyncio
import socket
import uuid
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from typing import Any

from pydantic import BaseModel, Field

from src.core.sensitive import redact_sensitive, redact_sensitive_text
from src.storage.base import BaseStorage, StorageRecord


class WorkerState(BaseModel):
    """Current registration snapshot for a worker."""

    worker_id: str = Field(default_factory=lambda: f"worker-{uuid.uuid4().hex[:12]}")
    hostname: str = Field(default_factory=socket.gethostname)
    status: str = "online"
    capabilities: list[str] = Field(default_factory=list)
    current_task_ids: list[str] = Field(default_factory=list)
    metadata: dict[str, Any] = Field(default_factory=dict)
    registered_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    last_heartbeat_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    def to_public_payload(self) -> dict[str, Any]:
        return redact_sensitive(self.model_dump(mode="json"))


class WorkerRegistry(ABC):
    """Persistence boundary for worker registration and heartbeats."""

    @abstractmethod
    async def register(
        self,
        *,
        worker_id: str | None = None,
        hostname: str = "",
        capabilities: list[str] | None = None,
        current_task_ids: list[str] | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> WorkerState:
        """Register or update a worker."""
        ...

    @abstractmethod
    async def heartbeat(
        self,
        worker_id: str,
        *,
        status: str = "online",
        capabilities: list[str] | None = None,
        current_task_ids: list[str] | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> WorkerState | None:
        """Update a worker heartbeat."""
        ...

    @abstractmethod
    async def update_worker_state(
        self,
        worker_id: str,
        *,
        status: str | None = None,
        current_task_ids: list[str] | None = None,
        metadata: dict[str, Any] | None = None,
        touch_heartbeat: bool = False,
    ) -> WorkerState | None:
        """Update worker state fields without requiring a heartbeat refresh."""
        ...

    @abstractmethod
    async def get_worker(self, worker_id: str) -> WorkerState | None:
        """Return a worker snapshot by id."""
        ...

    @abstractmethod
    async def list_workers(self, *, stale_after_seconds: int = 120) -> list[WorkerState]:
        """List worker snapshots."""
        ...


class InMemoryWorkerRegistry(WorkerRegistry):
    """In-memory worker registry for tests and local fallback."""

    def __init__(self) -> None:
        self._workers: dict[str, WorkerState] = {}
        self._lock = asyncio.Lock()

    async def register(
        self,
        *,
        worker_id: str | None = None,
        hostname: str = "",
        capabilities: list[str] | None = None,
        current_task_ids: list[str] | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> WorkerState:
        async with self._lock:
            now = datetime.now(timezone.utc)
            resolved_id = _safe_worker_id(worker_id)
            existing = self._workers.get(resolved_id) if resolved_id else None
            worker = _build_worker_state(
                worker_id=resolved_id,
                hostname=hostname,
                capabilities=capabilities,
                current_task_ids=current_task_ids,
                metadata=metadata,
                registered_at=existing.registered_at if existing else now,
                last_heartbeat_at=now,
            )
            self._workers[worker.worker_id] = worker
            return worker

    async def heartbeat(
        self,
        worker_id: str,
        *,
        status: str = "online",
        capabilities: list[str] | None = None,
        current_task_ids: list[str] | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> WorkerState | None:
        async with self._lock:
            worker = self._workers.get(worker_id)
            if worker is None:
                return None
            updated = worker.model_copy(
                update={
                    "status": _safe_status(status),
                    "capabilities": _safe_list(capabilities, fallback=worker.capabilities),
                    "current_task_ids": _safe_list(
                        current_task_ids,
                        fallback=worker.current_task_ids,
                    ),
                    "metadata": redact_sensitive(metadata)
                    if metadata is not None
                    else worker.metadata,
                    "last_heartbeat_at": datetime.now(timezone.utc),
                }
            )
            self._workers[worker_id] = updated
            return updated

    async def update_worker_state(
        self,
        worker_id: str,
        *,
        status: str | None = None,
        current_task_ids: list[str] | None = None,
        metadata: dict[str, Any] | None = None,
        touch_heartbeat: bool = False,
    ) -> WorkerState | None:
        async with self._lock:
            worker = self._workers.get(worker_id)
            if worker is None:
                return None
            update = {
                "status": _safe_status(status) if status is not None else worker.status,
                "current_task_ids": _safe_list(
                    current_task_ids,
                    fallback=worker.current_task_ids,
                ),
                "metadata": redact_sensitive(metadata)
                if metadata is not None
                else worker.metadata,
            }
            if touch_heartbeat:
                update["last_heartbeat_at"] = datetime.now(timezone.utc)
            updated = worker.model_copy(update=update)
            self._workers[worker_id] = updated
            return updated

    async def get_worker(self, worker_id: str) -> WorkerState | None:
        async with self._lock:
            return self._workers.get(worker_id)

    async def list_workers(self, *, stale_after_seconds: int = 120) -> list[WorkerState]:
        async with self._lock:
            workers = list(self._workers.values())
        return _with_stale_status(workers, stale_after_seconds=stale_after_seconds)


class StorageWorkerRegistry(WorkerRegistry):
    """Worker registry backed by scheduler_states."""

    def __init__(self, storage: BaseStorage) -> None:
        self._storage = storage
        self._lock = asyncio.Lock()

    async def register(
        self,
        *,
        worker_id: str | None = None,
        hostname: str = "",
        capabilities: list[str] | None = None,
        current_task_ids: list[str] | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> WorkerState:
        async with self._lock:
            now = datetime.now(timezone.utc)
            resolved_id = _safe_worker_id(worker_id)
            existing = await self.get_worker(resolved_id) if resolved_id else None
            worker = _build_worker_state(
                worker_id=resolved_id,
                hostname=hostname,
                capabilities=capabilities,
                current_task_ids=current_task_ids,
                metadata=metadata,
                registered_at=existing.registered_at if existing else now,
                last_heartbeat_at=now,
            )
            await self._save(worker)
            return worker

    async def heartbeat(
        self,
        worker_id: str,
        *,
        status: str = "online",
        capabilities: list[str] | None = None,
        current_task_ids: list[str] | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> WorkerState | None:
        async with self._lock:
            worker = await self.get_worker(worker_id)
            if worker is None:
                return None
            updated = worker.model_copy(
                update={
                    "status": _safe_status(status),
                    "capabilities": _safe_list(capabilities, fallback=worker.capabilities),
                    "current_task_ids": _safe_list(
                        current_task_ids,
                        fallback=worker.current_task_ids,
                    ),
                    "metadata": redact_sensitive(metadata)
                    if metadata is not None
                    else worker.metadata,
                    "last_heartbeat_at": datetime.now(timezone.utc),
                }
            )
            await self._save(updated)
            return updated

    async def update_worker_state(
        self,
        worker_id: str,
        *,
        status: str | None = None,
        current_task_ids: list[str] | None = None,
        metadata: dict[str, Any] | None = None,
        touch_heartbeat: bool = False,
    ) -> WorkerState | None:
        async with self._lock:
            worker = await self.get_worker(worker_id)
            if worker is None:
                return None
            update = {
                "status": _safe_status(status) if status is not None else worker.status,
                "current_task_ids": _safe_list(
                    current_task_ids,
                    fallback=worker.current_task_ids,
                ),
                "metadata": redact_sensitive(metadata)
                if metadata is not None
                else worker.metadata,
            }
            if touch_heartbeat:
                update["last_heartbeat_at"] = datetime.now(timezone.utc)
            updated = worker.model_copy(update=update)
            await self._save(updated)
            return updated

    async def get_worker(self, worker_id: str) -> WorkerState | None:
        record = await self._storage.load(_worker_key(worker_id))
        if record is None or not isinstance(record.data, dict):
            return None
        try:
            return WorkerState.model_validate(record.data)
        except Exception:
            return None

    async def list_workers(self, *, stale_after_seconds: int = 120) -> list[WorkerState]:
        result = await self._storage.query(f"key:{_worker_prefix()}", limit=5000)
        workers: list[WorkerState] = []
        for record in result.records:
            if isinstance(record.data, dict):
                try:
                    workers.append(WorkerState.model_validate(record.data))
                except Exception:
                    continue
        workers.sort(key=lambda worker: worker.last_heartbeat_at, reverse=True)
        return _with_stale_status(workers, stale_after_seconds=stale_after_seconds)

    async def _save(self, worker: WorkerState) -> None:
        await self._storage.save(
            StorageRecord(
                key=_worker_key(worker.worker_id),
                data=worker.model_dump(mode="json"),
                metadata={
                    "kind": "worker",
                    "worker_id": worker.worker_id,
                    "status": worker.status,
                    "hostname": worker.hostname,
                },
                source="scheduler",
                tags=["worker", worker.status, worker.worker_id],
            )
        )


def _build_worker_state(
    *,
    worker_id: str | None,
    hostname: str,
    capabilities: list[str] | None,
    current_task_ids: list[str] | None,
    metadata: dict[str, Any] | None,
    registered_at: datetime,
    last_heartbeat_at: datetime,
) -> WorkerState:
    return WorkerState(
        worker_id=worker_id or f"worker-{uuid.uuid4().hex[:12]}",
        hostname=redact_sensitive_text(hostname or socket.gethostname()),
        status="online",
        capabilities=_safe_list(capabilities),
        current_task_ids=_safe_list(current_task_ids),
        metadata=redact_sensitive(metadata or {}),
        registered_at=registered_at,
        last_heartbeat_at=last_heartbeat_at,
    )


def _with_stale_status(
    workers: list[WorkerState],
    *,
    stale_after_seconds: int,
) -> list[WorkerState]:
    threshold = max(1, int(stale_after_seconds or 120))
    now = datetime.now(timezone.utc)
    result: list[WorkerState] = []
    for worker in workers:
        last = _ensure_aware(worker.last_heartbeat_at)
        if (
            worker.status not in {"offline", "draining"}
            and (now - last).total_seconds() > threshold
        ):
            result.append(worker.model_copy(update={"status": "offline"}))
        else:
            result.append(worker)
    return result


def _ensure_aware(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value


def _safe_worker_id(worker_id: str | None) -> str | None:
    value = redact_sensitive_text(str(worker_id or "")).strip()
    return value or None


def _safe_status(status: str) -> str:
    value = str(status or "online").strip().lower()
    return value if value in {"online", "idle", "busy", "draining", "offline"} else "online"


def _safe_list(values: list[str] | None, *, fallback: list[str] | None = None) -> list[str]:
    if values is None:
        return list(fallback or [])
    return [redact_sensitive_text(str(value)) for value in values if str(value or "").strip()]


def _worker_prefix() -> str:
    return "worker:"


def _worker_key(worker_id: str) -> str:
    return f"{_worker_prefix()}{worker_id}"
