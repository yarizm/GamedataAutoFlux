"""Task-related shared Pydantic schemas."""

from typing import Any

from pydantic import BaseModel, Field


class TaskPrecheckIssue(BaseModel):
    level: str
    code: str
    field: str
    message: str
    collector_id: str = ""
    category: str = ""  # config | target | credential | session | graph | probe | runtime
    suggested_action: str = ""


class CollectorReadiness(BaseModel):
    """Per-collector readiness summary for multi-collector pipelines."""

    collector_id: str
    status: str = "ok"  # ok | warning | error
    requires_session: bool = False
    session_precheck_status: str = "ok"
    is_root: bool = True  # False when targets come from from_upstream
    from_upstream: bool = False
    issue_count: int = 0
    error_count: int = 0
    warning_count: int = 0


class TaskPrecheckResponse(BaseModel):
    status: str
    can_submit: bool
    pipeline_name: str
    collector_name: str = ""
    collectors: list[str] = Field(default_factory=list)
    collectors_readiness: list[CollectorReadiness] = Field(default_factory=list)
    required_fields: list[str] = Field(default_factory=list)
    issues: list[TaskPrecheckIssue] = Field(default_factory=list)
    credential_status: dict[str, str] = Field(default_factory=dict)
    data_source_status: dict[str, str] = Field(default_factory=dict)
    collector_metadata: dict[str, Any] = Field(default_factory=dict)
    session_diagnostics: dict[str, Any] = Field(default_factory=dict)
    session_diagnostics_by_collector: dict[str, Any] = Field(default_factory=dict)
    session_readiness: dict[str, Any] = Field(default_factory=dict)
    session_readiness_by_collector: dict[str, Any] = Field(default_factory=dict)
    recovery: dict[str, Any] = Field(default_factory=dict)
    deep: bool = False
    probe_report: dict[str, Any] = Field(default_factory=dict)


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


class TaskResumeRequest(BaseModel):
    """Explicit resume from preferred or selected checkpoint."""

    checkpoint_id: str | None = Field(
        default=None, description="Optional checkpoint id to resume from"
    )
    reset_retry_count: bool = Field(
        default=False,
        description="When true, reset retry_count to 0 before requeue",
    )


class TaskRerunRequest(BaseModel):
    """Force full rerun ignoring recovery checkpoints."""

    reset_retry_count: bool = Field(
        default=False,
        description="When true, reset retry_count to 0 before requeue",
    )
