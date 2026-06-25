"""Task checkpoint models and persistence services."""

from __future__ import annotations

import asyncio
import uuid
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urlsplit

from pydantic import BaseModel, Field

from src.core.sensitive import redact_sensitive, redact_sensitive_text
from src.storage.base import BaseStorage, StorageRecord


class TaskCheckpoint(BaseModel):
    """A resumability marker emitted by a task or worker."""

    checkpoint_id: str = Field(default_factory=lambda: uuid.uuid4().hex)
    task_id: str
    seq: int
    pipeline_name: str = ""
    collector_name: str = ""
    worker_id: str = ""
    recovery_level: str = "L0"
    cursor: dict[str, Any] = Field(default_factory=dict)
    state: dict[str, Any] = Field(default_factory=dict)
    stats: dict[str, Any] = Field(default_factory=dict)
    artifacts: list[dict[str, Any]] = Field(default_factory=list)
    metadata: dict[str, Any] = Field(default_factory=dict)
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    def to_public_payload(self) -> dict[str, Any]:
        """Return the API/WebSocket-safe representation."""
        payload = redact_sensitive(self.model_dump(mode="json"))
        payload.pop("state", None)
        payload["artifacts"] = _public_checkpoint_artifacts(payload.get("artifacts", []))
        return payload

    def to_worker_payload(self) -> dict[str, Any]:
        """Return a worker-safe representation that retains internal resume state."""
        payload = redact_sensitive(self.model_dump(mode="json"))
        payload["artifacts"] = _public_checkpoint_artifacts(payload.get("artifacts", []))
        return payload


class TaskCheckpointService(ABC):
    """Persistence boundary for task checkpoints."""

    @abstractmethod
    async def append(
        self,
        task_id: str,
        *,
        pipeline_name: str = "",
        collector_name: str = "",
        worker_id: str = "",
        recovery_level: str = "L0",
        cursor: dict[str, Any] | None = None,
        state: dict[str, Any] | None = None,
        stats: dict[str, Any] | None = None,
        artifacts: list[dict[str, Any]] | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> TaskCheckpoint:
        """Append a checkpoint and return the stored model."""
        ...

    @abstractmethod
    async def list_checkpoints(
        self,
        task_id: str,
        *,
        limit: int = 200,
        offset: int = 0,
    ) -> list[TaskCheckpoint]:
        """List checkpoints for a task ordered by sequence."""
        ...

    async def latest_checkpoint(self, task_id: str) -> TaskCheckpoint | None:
        checkpoints = await self.list_checkpoints(task_id, limit=1)
        return checkpoints[0] if checkpoints else None

    async def delete_checkpoints(self, task_id: str) -> None:
        """Delete task checkpoints when the backend supports it."""
        return None


class InMemoryTaskCheckpointService(TaskCheckpointService):
    """In-memory implementation for tests."""

    def __init__(self) -> None:
        self._checkpoints: dict[str, list[TaskCheckpoint]] = {}
        self._lock = asyncio.Lock()

    async def append(
        self,
        task_id: str,
        *,
        pipeline_name: str = "",
        collector_name: str = "",
        worker_id: str = "",
        recovery_level: str = "L0",
        cursor: dict[str, Any] | None = None,
        state: dict[str, Any] | None = None,
        stats: dict[str, Any] | None = None,
        artifacts: list[dict[str, Any]] | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> TaskCheckpoint:
        async with self._lock:
            checkpoints = self._checkpoints.setdefault(task_id, [])
            checkpoint = _build_checkpoint(
                task_id=task_id,
                seq=len(checkpoints) + 1,
                pipeline_name=pipeline_name,
                collector_name=collector_name,
                worker_id=worker_id,
                recovery_level=recovery_level,
                cursor=cursor,
                state=state,
                stats=stats,
                artifacts=artifacts,
                metadata=metadata,
            )
            checkpoints.append(checkpoint)
            return checkpoint

    async def list_checkpoints(
        self,
        task_id: str,
        *,
        limit: int = 200,
        offset: int = 0,
    ) -> list[TaskCheckpoint]:
        async with self._lock:
            checkpoints = list(self._checkpoints.get(task_id, []))
        checkpoints.sort(key=lambda checkpoint: checkpoint.seq, reverse=True)
        return _slice_checkpoints(checkpoints, limit=limit, offset=offset)

    async def delete_checkpoints(self, task_id: str) -> None:
        async with self._lock:
            self._checkpoints.pop(task_id, None)


class StorageTaskCheckpointService(TaskCheckpointService):
    """Task checkpoint service backed by the scheduler storage namespace."""

    def __init__(self, storage: BaseStorage) -> None:
        self._storage = storage
        self._counters: dict[str, int] = {}
        self._lock = asyncio.Lock()

    async def append(
        self,
        task_id: str,
        *,
        pipeline_name: str = "",
        collector_name: str = "",
        worker_id: str = "",
        recovery_level: str = "L0",
        cursor: dict[str, Any] | None = None,
        state: dict[str, Any] | None = None,
        stats: dict[str, Any] | None = None,
        artifacts: list[dict[str, Any]] | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> TaskCheckpoint:
        async with self._lock:
            seq = await self._next_seq(task_id)
            checkpoint = _build_checkpoint(
                task_id=task_id,
                seq=seq,
                pipeline_name=pipeline_name,
                collector_name=collector_name,
                worker_id=worker_id,
                recovery_level=recovery_level,
                cursor=cursor,
                state=state,
                stats=stats,
                artifacts=artifacts,
                metadata=metadata,
            )
            await self._storage.save(
                StorageRecord(
                    key=_checkpoint_key(task_id, seq),
                    data=checkpoint.model_dump(mode="json"),
                    metadata={
                        "kind": "task_checkpoint",
                        "task_id": task_id,
                        "seq": seq,
                        "pipeline_name": checkpoint.pipeline_name,
                        "collector_name": checkpoint.collector_name,
                        "worker_id": checkpoint.worker_id,
                        "recovery_level": checkpoint.recovery_level,
                    },
                    source="scheduler",
                    tags=["task_checkpoint", checkpoint.recovery_level, task_id],
                )
            )
            return checkpoint

    async def list_checkpoints(
        self,
        task_id: str,
        *,
        limit: int = 200,
        offset: int = 0,
    ) -> list[TaskCheckpoint]:
        result = await self._storage.query(f"key:{_checkpoint_prefix(task_id)}", limit=5000)
        checkpoints = [_checkpoint_from_record(record) for record in result.records]
        checkpoints = [checkpoint for checkpoint in checkpoints if checkpoint is not None]
        checkpoints.sort(
            key=lambda checkpoint: (
                checkpoint.seq,
                checkpoint.created_at,
                checkpoint.checkpoint_id,
            ),
            reverse=True,
        )
        return _slice_checkpoints(checkpoints, limit=limit, offset=offset)

    async def delete_checkpoints(self, task_id: str) -> None:
        keys = await self._storage.list_keys(prefix=_checkpoint_prefix(task_id), limit=5000)
        for key in keys:
            await self._storage.delete(key)
        self._counters.pop(task_id, None)

    async def _next_seq(self, task_id: str) -> int:
        current = self._counters.get(task_id)
        if current is None:
            existing = await self.list_checkpoints(task_id, limit=5000)
            current = max((checkpoint.seq for checkpoint in existing), default=0)
        seq = current + 1
        self._counters[task_id] = seq
        return seq


def _build_checkpoint(
    *,
    task_id: str,
    seq: int,
    pipeline_name: str,
    collector_name: str,
    worker_id: str,
    recovery_level: str,
    cursor: dict[str, Any] | None,
    state: dict[str, Any] | None,
    stats: dict[str, Any] | None,
    artifacts: list[dict[str, Any]] | None,
    metadata: dict[str, Any] | None,
) -> TaskCheckpoint:
    return TaskCheckpoint(
        task_id=task_id,
        seq=seq,
        pipeline_name=redact_sensitive_text(str(pipeline_name or "")),
        collector_name=redact_sensitive_text(str(collector_name or "")),
        worker_id=redact_sensitive_text(str(worker_id or "")),
        recovery_level=_safe_recovery_level(recovery_level),
        cursor=redact_sensitive(cursor or {}),
        state=redact_sensitive(state or {}),
        stats=redact_sensitive(stats or {}),
        artifacts=redact_sensitive(artifacts or []),
        metadata=redact_sensitive(metadata or {}),
    )


def _safe_recovery_level(value: str) -> str:
    level = str(value or "L0").strip().upper()
    return level if level in {"L0", "L1", "L2", "L3"} else "L0"


def _public_checkpoint_artifacts(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    artifacts: list[dict[str, Any]] = []
    for item in value:
        if not isinstance(item, dict):
            continue
        artifact = dict(item)
        for path_key in ("path", "local_path", "file_path", "absolute_path"):
            if path_key in artifact:
                artifact[path_key] = ""
        if "download_url" in artifact:
            artifact["download_url"] = _safe_checkpoint_download_url(artifact["download_url"])
        artifacts.append(artifact)
    return artifacts


def _safe_checkpoint_download_url(value: Any) -> str:
    url = redact_sensitive_text(str(value or "")).strip()
    if not url:
        return ""
    if url.startswith("/api/"):
        return url
    parsed = urlsplit(url)
    if parsed.scheme in {"http", "https"} and parsed.netloc:
        return url
    return ""


def _checkpoint_prefix(task_id: str) -> str:
    return f"task_checkpoint:{task_id}:"


def _checkpoint_key(task_id: str, seq: int) -> str:
    return f"{_checkpoint_prefix(task_id)}{seq:08d}"


def _checkpoint_from_record(record: StorageRecord) -> TaskCheckpoint | None:
    try:
        if isinstance(record.data, TaskCheckpoint):
            return record.data
        if isinstance(record.data, dict):
            return TaskCheckpoint.model_validate(record.data)
    except Exception:
        return None
    return None


def _slice_checkpoints(
    checkpoints: list[TaskCheckpoint],
    *,
    limit: int,
    offset: int,
) -> list[TaskCheckpoint]:
    safe_offset = max(0, int(offset or 0))
    safe_limit = min(max(1, int(limit or 200)), 1000)
    return checkpoints[safe_offset : safe_offset + safe_limit]
