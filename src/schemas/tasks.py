"""Task-related shared Pydantic schemas."""

from typing import Any

from pydantic import BaseModel, Field


class TaskPrecheckIssue(BaseModel):
    level: str
    code: str
    field: str
    message: str


class TaskPrecheckResponse(BaseModel):
    status: str
    can_submit: bool
    pipeline_name: str
    collector_name: str = ""
    required_fields: list[str] = Field(default_factory=list)
    issues: list[TaskPrecheckIssue] = Field(default_factory=list)
    credential_status: dict[str, str] = Field(default_factory=dict)
    data_source_status: dict[str, str] = Field(default_factory=dict)
    collector_metadata: dict[str, Any] = Field(default_factory=dict)
    session_diagnostics: dict[str, Any] = Field(default_factory=dict)
    session_readiness: dict[str, Any] = Field(default_factory=dict)
    recovery: dict[str, Any] = Field(default_factory=dict)


class TaskEventResponse(BaseModel):
    event_id: str
    task_id: str
    seq: int
    type: str
    level: str
    message: str
    payload: dict[str, Any] = Field(default_factory=dict)
    created_at: str


class TaskEventsResponse(BaseModel):
    task_id: str
    events: list[TaskEventResponse] = Field(default_factory=list)


class TaskArtifactResponse(BaseModel):
    artifact_id: str
    task_id: str
    seq: int
    type: str
    name: str
    path: str = ""
    mime_type: str = ""
    size: int | None = None
    download_url: str = ""
    metadata: dict[str, Any] = Field(default_factory=dict)
    created_at: str


class TaskArtifactsResponse(BaseModel):
    task_id: str
    artifacts: list[TaskArtifactResponse] = Field(default_factory=list)


class TaskCheckpointResponse(BaseModel):
    checkpoint_id: str
    task_id: str
    seq: int
    pipeline_name: str = ""
    collector_name: str = ""
    worker_id: str = ""
    recovery_level: str = "L0"
    cursor: dict[str, Any] = Field(default_factory=dict)
    stats: dict[str, Any] = Field(default_factory=dict)
    artifacts: list[dict[str, Any]] = Field(default_factory=list)
    metadata: dict[str, Any] = Field(default_factory=dict)
    created_at: str


class TaskCheckpointsResponse(BaseModel):
    task_id: str
    checkpoints: list[TaskCheckpointResponse] = Field(default_factory=list)
    latest: TaskCheckpointResponse | None = None
