"""Task artifact models and persistence services."""

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


class TaskArtifact(BaseModel):
    """A file or generated object associated with a task."""

    artifact_id: str = Field(default_factory=lambda: uuid.uuid4().hex)
    task_id: str
    seq: int
    type: str
    name: str
    path: str = ""
    mime_type: str = ""
    size: int | None = None
    download_url: str = ""
    metadata: dict[str, Any] = Field(default_factory=dict)
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    def to_public_payload(self) -> dict[str, Any]:
        """Return the API/WebSocket-safe representation."""
        payload = redact_sensitive(self.model_dump(mode="json"))
        payload["path"] = ""
        return payload


class TaskArtifactService(ABC):
    """Persistence boundary for task artifacts."""

    @abstractmethod
    async def append(
        self,
        task_id: str,
        artifact_type: str,
        *,
        name: str,
        path: str = "",
        mime_type: str = "",
        size: int | None = None,
        download_url: str = "",
        metadata: dict[str, Any] | None = None,
    ) -> TaskArtifact:
        """Append an artifact and return the stored model."""
        ...

    @abstractmethod
    async def list_artifacts(
        self,
        task_id: str,
        *,
        limit: int = 200,
        offset: int = 0,
    ) -> list[TaskArtifact]:
        """List artifacts for a task ordered by sequence."""
        ...

    async def delete_artifacts(self, task_id: str) -> None:
        """Delete task artifacts when the backend supports it."""
        return None


class InMemoryTaskArtifactService(TaskArtifactService):
    """In-memory implementation for tests."""

    def __init__(self) -> None:
        self._artifacts: dict[str, list[TaskArtifact]] = {}
        self._lock = asyncio.Lock()

    async def append(
        self,
        task_id: str,
        artifact_type: str,
        *,
        name: str,
        path: str = "",
        mime_type: str = "",
        size: int | None = None,
        download_url: str = "",
        metadata: dict[str, Any] | None = None,
    ) -> TaskArtifact:
        async with self._lock:
            artifacts = self._artifacts.setdefault(task_id, [])
            artifact = _build_artifact(
                task_id=task_id,
                seq=len(artifacts) + 1,
                artifact_type=artifact_type,
                name=name,
                path=path,
                mime_type=mime_type,
                size=size,
                download_url=download_url,
                metadata=metadata,
            )
            artifacts.append(artifact)
            return artifact

    async def list_artifacts(
        self,
        task_id: str,
        *,
        limit: int = 200,
        offset: int = 0,
    ) -> list[TaskArtifact]:
        async with self._lock:
            artifacts = list(self._artifacts.get(task_id, []))
        return _slice_artifacts(artifacts, limit=limit, offset=offset)

    async def delete_artifacts(self, task_id: str) -> None:
        async with self._lock:
            self._artifacts.pop(task_id, None)


class StorageTaskArtifactService(TaskArtifactService):
    """Task artifact service backed by the scheduler storage namespace."""

    def __init__(self, storage: BaseStorage) -> None:
        self._storage = storage
        self._counters: dict[str, int] = {}
        self._lock = asyncio.Lock()

    async def append(
        self,
        task_id: str,
        artifact_type: str,
        *,
        name: str,
        path: str = "",
        mime_type: str = "",
        size: int | None = None,
        download_url: str = "",
        metadata: dict[str, Any] | None = None,
    ) -> TaskArtifact:
        async with self._lock:
            seq = await self._next_seq(task_id)
            artifact = _build_artifact(
                task_id=task_id,
                seq=seq,
                artifact_type=artifact_type,
                name=name,
                path=path,
                mime_type=mime_type,
                size=size,
                download_url=download_url,
                metadata=metadata,
            )
            await self._storage.save(
                StorageRecord(
                    key=_artifact_key(task_id, seq),
                    data=artifact.model_dump(mode="json"),
                    metadata={
                        "kind": "task_artifact",
                        "task_id": task_id,
                        "seq": seq,
                        "type": artifact.type,
                        "name": artifact.name,
                    },
                    source="scheduler",
                    tags=["task_artifact", artifact.type, task_id],
                )
            )
            return artifact

    async def list_artifacts(
        self,
        task_id: str,
        *,
        limit: int = 200,
        offset: int = 0,
    ) -> list[TaskArtifact]:
        result = await self._storage.query(f"key:{_artifact_prefix(task_id)}", limit=5000)
        artifacts = [_artifact_from_record(record) for record in result.records]
        artifacts = [artifact for artifact in artifacts if artifact is not None]
        artifacts.sort(
            key=lambda artifact: (artifact.seq, artifact.created_at, artifact.artifact_id)
        )
        return _slice_artifacts(artifacts, limit=limit, offset=offset)

    async def delete_artifacts(self, task_id: str) -> None:
        keys = await self._storage.list_keys(prefix=_artifact_prefix(task_id), limit=5000)
        for key in keys:
            await self._storage.delete(key)
        self._counters.pop(task_id, None)

    async def _next_seq(self, task_id: str) -> int:
        current = self._counters.get(task_id)
        if current is None:
            existing = await self.list_artifacts(task_id, limit=5000)
            current = max((artifact.seq for artifact in existing), default=0)
        seq = current + 1
        self._counters[task_id] = seq
        return seq


def _build_artifact(
    *,
    task_id: str,
    seq: int,
    artifact_type: str,
    name: str,
    path: str,
    mime_type: str,
    size: int | None,
    download_url: str,
    metadata: dict[str, Any] | None,
) -> TaskArtifact:
    safe_size = None
    if size is not None:
        try:
            parsed_size = int(size)
            safe_size = parsed_size if parsed_size >= 0 else None
        except (TypeError, ValueError):
            safe_size = None

    return TaskArtifact(
        task_id=task_id,
        seq=seq,
        type=redact_sensitive_text(str(artifact_type or "file")),
        name=redact_sensitive_text(str(name or artifact_type or "artifact")),
        path=redact_sensitive_text(str(path or "")),
        mime_type=redact_sensitive_text(str(mime_type or "")),
        size=safe_size,
        download_url=_safe_download_url(download_url),
        metadata=redact_sensitive(metadata or {}),
    )


def _safe_download_url(value: str) -> str:
    url = redact_sensitive_text(str(value or "")).strip()
    if not url:
        return ""
    if url.startswith("/api/"):
        return url
    parsed = urlsplit(url)
    if parsed.scheme in {"http", "https"} and parsed.netloc:
        return url
    return ""


def _artifact_prefix(task_id: str) -> str:
    return f"task_artifact:{task_id}:"


def _artifact_key(task_id: str, seq: int) -> str:
    return f"{_artifact_prefix(task_id)}{seq:08d}"


def _artifact_from_record(record: StorageRecord) -> TaskArtifact | None:
    try:
        if isinstance(record.data, TaskArtifact):
            return record.data
        if isinstance(record.data, dict):
            return TaskArtifact.model_validate(record.data)
    except Exception:
        return None
    return None


def _slice_artifacts(
    artifacts: list[TaskArtifact],
    *,
    limit: int,
    offset: int,
) -> list[TaskArtifact]:
    safe_offset = max(0, int(offset or 0))
    safe_limit = min(max(1, int(limit or 200)), 1000)
    return artifacts[safe_offset : safe_offset + safe_limit]
