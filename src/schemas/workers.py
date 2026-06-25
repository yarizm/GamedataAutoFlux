"""Worker API schemas."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field, field_validator


_WORKER_STATUSES = {"online", "idle", "busy", "draining", "offline"}
_EVENT_LEVELS = {"debug", "info", "warning", "error"}
_RECOVERY_LEVELS = {"L0", "L1", "L2", "L3"}


class WorkerRegisterRequest(BaseModel):
    worker_id: str | None = Field(
        default=None,
        min_length=1,
        max_length=128,
        description="Worker id; generated if omitted",
    )
    hostname: str = Field(default="", max_length=255, description="Worker host name")
    capabilities: list[str] = Field(default_factory=list, max_length=256)
    current_task_ids: list[str] = Field(default_factory=list, max_length=256)
    metadata: dict[str, Any] = Field(default_factory=dict)


class WorkerHeartbeatRequest(BaseModel):
    status: str = Field(default="online")
    capabilities: list[str] | None = Field(default=None, max_length=256)
    current_task_ids: list[str] | None = Field(default=None, max_length=256)
    metadata: dict[str, Any] | None = None

    @field_validator("status")
    @classmethod
    def normalize_status(cls, value: str) -> str:
        return _normalize_choice(value, choices=_WORKER_STATUSES, default="online")


class WorkerClaimTaskRequest(BaseModel):
    capabilities: list[str] | None = Field(default=None, max_length=256)


class WorkerClaimTaskResponse(BaseModel):
    worker_id: str
    task_id: str | None = None
    claim_status: str = "no_task"
    claim_reason: str = ""
    blocked_sessions: list[dict[str, Any]] = Field(default_factory=list)
    task: dict[str, Any] | None = None
    pipeline: dict[str, Any] | None = None
    latest_checkpoint: dict[str, Any] | None = None
    collector_metadata: dict[str, Any] = Field(default_factory=dict)
    session_diagnostics: dict[str, Any] = Field(default_factory=dict)
    recovery: dict[str, Any] = Field(default_factory=dict)


class WorkerReconcileStaleTasksResponse(BaseModel):
    offline_worker_ids: list[str] = Field(default_factory=list)
    updated_worker_ids: list[str] = Field(default_factory=list)
    interrupted_tasks: list[dict[str, Any]] = Field(default_factory=list)
    recovered_retry_tasks: list[dict[str, Any]] = Field(default_factory=list)


class WorkerTaskEventRequest(BaseModel):
    type: str = Field(default="log", min_length=1, max_length=64)
    level: str = Field(default="info")
    message: str = Field(default="", max_length=4000)
    payload: dict[str, Any] = Field(default_factory=dict)

    @field_validator("level")
    @classmethod
    def normalize_level(cls, value: str) -> str:
        return _normalize_choice(value, choices=_EVENT_LEVELS, default="info")


class WorkerTaskArtifactRequest(BaseModel):
    type: str = Field(default="file", min_length=1, max_length=64)
    name: str = Field(min_length=1, max_length=255)
    path: str = Field(default="", max_length=2000)
    mime_type: str = Field(default="", max_length=255)
    size: int | None = Field(default=None, ge=0)
    download_url: str = Field(default="", max_length=2000)
    metadata: dict[str, Any] = Field(default_factory=dict)


class WorkerTaskCheckpointRequest(BaseModel):
    recovery_level: str = Field(default="L0")
    cursor: dict[str, Any] = Field(default_factory=dict)
    state: dict[str, Any] = Field(default_factory=dict)
    stats: dict[str, Any] = Field(default_factory=dict)
    artifacts: list[dict[str, Any]] = Field(default_factory=list)
    metadata: dict[str, Any] = Field(default_factory=dict)

    @field_validator("recovery_level")
    @classmethod
    def normalize_recovery_level(cls, value: str) -> str:
        return _normalize_choice(
            value,
            choices=_RECOVERY_LEVELS,
            default="L0",
            uppercase=True,
        )


class WorkerTaskCompleteRequest(BaseModel):
    result: dict[str, Any] = Field(default_factory=dict)


class WorkerTaskFailRequest(BaseModel):
    error: str = Field(min_length=1, max_length=4000)
    result: dict[str, Any] = Field(default_factory=dict)


class WorkerResponse(BaseModel):
    worker_id: str
    hostname: str
    status: str
    capabilities: list[str] = Field(default_factory=list)
    current_task_ids: list[str] = Field(default_factory=list)
    metadata: dict[str, Any] = Field(default_factory=dict)
    registered_at: str
    last_heartbeat_at: str


def _normalize_choice(
    value: str,
    *,
    choices: set[str],
    default: str,
    uppercase: bool = False,
) -> str:
    raw = str(value or default).strip()
    normalized = raw.upper() if uppercase else raw.lower()
    if normalized not in choices:
        raise ValueError(f"must be one of: {', '.join(sorted(choices))}")
    return normalized
