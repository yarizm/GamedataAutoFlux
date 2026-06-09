"""共享 Pydantic schema 模型，供 routes、services、agent 等层引用。"""

from .tasks import (
    TaskArtifactResponse,
    TaskArtifactsResponse,
    TaskCheckpointResponse,
    TaskCheckpointsResponse,
    TaskEventResponse,
    TaskEventsResponse,
    TaskPrecheckIssue,
    TaskPrecheckResponse,
)
from .workers import (
    WorkerClaimTaskRequest,
    WorkerClaimTaskResponse,
    WorkerHeartbeatRequest,
    WorkerRegisterRequest,
    WorkerReconcileStaleTasksResponse,
    WorkerResponse,
    WorkerTaskArtifactRequest,
    WorkerTaskCheckpointRequest,
    WorkerTaskCompleteRequest,
    WorkerTaskEventRequest,
    WorkerTaskFailRequest,
)

__all__ = [
    "TaskArtifactResponse",
    "TaskArtifactsResponse",
    "TaskCheckpointResponse",
    "TaskCheckpointsResponse",
    "TaskEventResponse",
    "TaskEventsResponse",
    "TaskPrecheckIssue",
    "TaskPrecheckResponse",
    "WorkerClaimTaskRequest",
    "WorkerClaimTaskResponse",
    "WorkerHeartbeatRequest",
    "WorkerRegisterRequest",
    "WorkerReconcileStaleTasksResponse",
    "WorkerResponse",
    "WorkerTaskArtifactRequest",
    "WorkerTaskCheckpointRequest",
    "WorkerTaskCompleteRequest",
    "WorkerTaskEventRequest",
    "WorkerTaskFailRequest",
]
