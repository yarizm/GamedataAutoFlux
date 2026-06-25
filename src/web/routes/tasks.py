"""
任务管理 API 路由
"""

from __future__ import annotations

from typing import Annotated, Any
from fastapi import APIRouter, HTTPException, Query, Path, Body
from pydantic import BaseModel, Field
from src.core.sensitive import redact_sensitive, redact_sensitive_text
from src.core.task import Task
from src.schemas.tasks import (
    TaskArtifactResponse,
    TaskArtifactsResponse,
    TaskCheckpointResponse,
    TaskCheckpointsResponse,
    TaskEventResponse,
    TaskEventsResponse,
    TaskPrecheckResponse,
)
from src.services.session_inventory_sync import sync_session_inventory_via_provider_best_effort
from src.web.safety import require_explicit_confirmation

router = APIRouter(tags=["tasks"])


# ==================== 请求/响应模型 ====================


class CreateTaskRequest(BaseModel):
    """创建任务请求"""

    name: str = Field(..., description="任务名称")
    description: str = Field(default="", description="任务描述")
    pipeline_name: str = Field(..., description="Pipeline 名称")
    collector_name: str = Field(default="", description="采集器名称")
    targets: list[dict[str, Any]] = Field(default_factory=list, description="采集目标")
    config: dict[str, Any] = Field(default_factory=dict, description="运行时配置")


class TaskResponse(BaseModel):
    """任务响应"""

    id: str
    name: str
    status: str
    progress: float
    pipeline_name: str
    collector_name: str
    targets_count: int
    created_at: str
    started_at: str | None
    completed_at: str | None
    duration: float | None
    error: str | None


class TaskLogResponse(BaseModel):
    step: str
    status: str
    message: str
    error: str | None
    started_at: str | None
    completed_at: str | None


class TaskDetailResponse(TaskResponse):
    description: str
    targets: list[dict[str, Any]]
    config: dict[str, Any]
    retry_count: int
    max_retries: int
    step_logs: list[TaskLogResponse]
    result_summary: dict[str, Any] | None
    collector_metadata: dict[str, Any] = Field(default_factory=dict)
    session_diagnostics: dict[str, Any] = Field(default_factory=dict)
    session_readiness: dict[str, Any] = Field(default_factory=dict)
    recovery: dict[str, Any] = Field(default_factory=dict)


# ==================== 路由 ====================


@router.get("/tasks", response_model=list[TaskResponse])
async def list_tasks(status: Annotated[str | None, Query(description="按状态过滤任务")] = None):
    """获取所有任务列表"""
    from src.web.app import get_task_service

    try:
        tasks = get_task_service().list_tasks(status)
    except ValueError:
        raise HTTPException(400, f"无效的状态: {status}")

    return [_task_to_response(t) for t in tasks]


@router.post("/tasks/precheck", response_model=TaskPrecheckResponse)
async def precheck_task(
    req: Annotated[CreateTaskRequest, Body(description="Task creation precheck")],
):
    """Validate task input before submitting it to the scheduler."""
    from src.web.app import get_task_service

    precheck = get_task_service().precheck(
        name=req.name,
        pipeline_name=req.pipeline_name,
        collector_name=req.collector_name,
        targets=req.targets,
        config=req.config,
    )
    if precheck.session_diagnostics:
        await _sync_session_inventory_best_effort(precheck.session_diagnostics)
    return precheck


# run_task_precheck and its helpers moved to TaskService in src.services.task_service


@router.post("/tasks", response_model=TaskResponse)
async def create_task(req: Annotated[CreateTaskRequest, Body(description="任务创建信息")]):
    """创建并提交新任务"""
    from src.web.app import get_task_service

    try:
        task = await get_task_service().create(
            name=req.name,
            pipeline_name=req.pipeline_name,
            collector_name=req.collector_name,
            targets=req.targets,
            config=req.config,
            description=req.description,
        )
    except ValueError as e:
        raise HTTPException(400, str(e))

    return _task_to_response(task)


@router.get("/tasks/{task_id}", response_model=TaskDetailResponse)
async def get_task(task_id: Annotated[str, Path(description="任务 ID")]):
    """获取单个任务详情"""
    from src.web.app import get_task_service

    task_service = get_task_service()
    task = task_service.get_task(task_id)
    if task is None:
        raise HTTPException(404, f"任务不存在: {task_id}")

    recovery = await task_service.get_task_recovery_info(task_id)
    collector_metadata = task_service.get_task_collector_metadata(task_id)
    session_diagnostics = task_service.get_task_session_diagnostics(task_id)
    session_readiness_getter = getattr(task_service, "get_task_session_readiness", None)
    session_readiness = (
        session_readiness_getter(task_id) if callable(session_readiness_getter) else {}
    )
    if session_diagnostics:
        await _sync_session_inventory_best_effort(session_diagnostics)
    return _task_to_detail_response(
        task,
        collector_metadata=collector_metadata or {},
        session_diagnostics=session_diagnostics or {},
        session_readiness=session_readiness or {},
        recovery=recovery or {},
    )


@router.get("/tasks/{task_id}/logs")
async def get_task_logs(task_id: Annotated[str, Path(description="任务 ID")]):
    """获取任务步骤日志"""
    from src.web.app import get_task_service

    ts = get_task_service()
    if ts.get_task(task_id) is None:
        raise HTTPException(404, f"任务不存在: {task_id}")

    logs = ts.get_task_logs(task_id)
    return {
        "task_id": task_id,
        "logs": logs,
    }


@router.get("/tasks/{task_id}/events", response_model=TaskEventsResponse)
async def get_task_events(
    task_id: Annotated[str, Path(description="任务 ID")],
    limit: Annotated[int, Query(ge=1, le=1000, description="最大返回事件数")] = 200,
    offset: Annotated[int, Query(ge=0, description="跳过事件数")] = 0,
    order: Annotated[str, Query(pattern="^(asc|desc)$", description="事件排序")] = "asc",
):
    """获取任务结构化事件流"""
    from src.web.app import get_task_service

    events = await get_task_service().get_task_events(
        task_id,
        limit=limit,
        offset=offset,
        order=order,
    )
    if events is None:
        raise HTTPException(404, f"任务不存在: {task_id}")

    return TaskEventsResponse(
        task_id=task_id,
        events=[_event_to_response(event) for event in events],
    )


@router.get("/tasks/{task_id}/artifacts", response_model=TaskArtifactsResponse)
async def get_task_artifacts(
    task_id: Annotated[str, Path(description="任务 ID")],
    limit: Annotated[int, Query(ge=1, le=1000, description="最大返回产物数")] = 200,
    offset: Annotated[int, Query(ge=0, description="跳过产物数")] = 0,
):
    """获取任务产物列表"""
    from src.web.app import get_task_service

    artifacts = await get_task_service().get_task_artifacts(
        task_id,
        limit=limit,
        offset=offset,
    )
    if artifacts is None:
        raise HTTPException(404, f"任务不存在: {task_id}")

    return TaskArtifactsResponse(
        task_id=task_id,
        artifacts=[_artifact_to_response(artifact) for artifact in artifacts],
    )


@router.get("/tasks/{task_id}/checkpoints", response_model=TaskCheckpointsResponse)
async def get_task_checkpoints(
    task_id: Annotated[str, Path(description="任务 ID")],
    limit: Annotated[int, Query(ge=1, le=1000, description="最大返回 checkpoint 数")] = 200,
    offset: Annotated[int, Query(ge=0, description="跳过 checkpoint 数")] = 0,
):
    """获取任务 checkpoint 列表"""
    from src.web.app import get_task_service

    result = await get_task_service().get_task_checkpoints(
        task_id,
        limit=limit,
        offset=offset,
    )
    if result is None:
        raise HTTPException(404, f"任务不存在: {task_id}")
    checkpoints, latest = result

    return TaskCheckpointsResponse(
        task_id=task_id,
        checkpoints=[_checkpoint_to_response(checkpoint) for checkpoint in checkpoints],
        latest=_checkpoint_to_response(latest) if latest is not None else None,
    )


@router.post("/tasks/{task_id}/cancel")
async def cancel_task(task_id: Annotated[str, Path(description="任务 ID")]):
    """取消任务"""
    from src.web.app import get_task_service

    success = await get_task_service().cancel(task_id)
    if not success:
        raise HTTPException(400, f"无法取消任务: {task_id}")

    return {"message": f"任务已取消: {task_id}"}


@router.delete("/tasks/{task_id}")
async def delete_task(
    task_id: Annotated[str, Path(description="任务 ID")],
    confirm: Annotated[bool, Query(description="Must be true for destructive delete")] = False,
):
    """删除任务"""
    from src.web.app import get_task_service

    require_explicit_confirmation(confirm, "task deletion")
    success = await get_task_service().delete(task_id)
    if not success:
        raise HTTPException(400, f"无法删除任务: {task_id}")

    return {"message": f"任务已删除: {task_id}"}


@router.get("/tasks/stats/summary")
async def get_task_stats():
    """获取任务统计信息"""
    from src.web.app import get_task_service

    return get_task_service().get_stats()


# ==================== 辅助函数 ====================

# All precheck/creation helpers moved to src.services.task_service.TaskService


def _task_to_response(task: Task) -> TaskResponse:
    return TaskResponse(
        id=task.id,
        name=redact_sensitive_text(task.name),
        status=task.status.value,
        progress=task.progress,
        pipeline_name=redact_sensitive_text(task.pipeline_name),
        collector_name=redact_sensitive_text(task.collector_name),
        targets_count=len(task.targets),
        created_at=task.created_at.isoformat(),
        started_at=task.started_at.isoformat() if task.started_at else None,
        completed_at=task.completed_at.isoformat() if task.completed_at else None,
        duration=task.duration_seconds,
        error=redact_sensitive_text(task.error) if task.error else None,
    )


def _task_to_detail_response(
    task: Task,
    *,
    collector_metadata: dict[str, Any] | None = None,
    session_diagnostics: dict[str, Any] | None = None,
    session_readiness: dict[str, Any] | None = None,
    recovery: dict[str, Any] | None = None,
) -> TaskDetailResponse:
    base = _task_to_response(task)
    return TaskDetailResponse(
        **base.model_dump(),
        description=redact_sensitive_text(task.description),
        targets=redact_sensitive([target.model_dump() for target in task.targets]),
        config=redact_sensitive(task.config),
        retry_count=task.retry_count,
        max_retries=task.max_retries,
        step_logs=[_log_to_response(log) for log in task.step_logs],
        result_summary=task.result_summary,
        collector_metadata=collector_metadata or {},
        session_diagnostics=session_diagnostics or {},
        session_readiness=session_readiness or {},
        recovery=recovery or {},
    )


def _log_to_response(log) -> TaskLogResponse:
    return TaskLogResponse(
        step=redact_sensitive_text(log.step_name),
        status=log.status.value,
        message=redact_sensitive_text(log.message),
        error=redact_sensitive_text(log.error) if log.error else None,
        started_at=log.started_at.isoformat() if log.started_at else None,
        completed_at=log.completed_at.isoformat() if log.completed_at else None,
    )


def _event_to_response(event) -> TaskEventResponse:
    payload = event.to_public_payload() if hasattr(event, "to_public_payload") else event
    return TaskEventResponse(
        event_id=payload["event_id"],
        task_id=payload["task_id"],
        seq=payload["seq"],
        type=payload["type"],
        level=payload["level"],
        message=payload["message"],
        payload=payload.get("payload", {}),
        created_at=payload["created_at"],
    )


def _artifact_to_response(artifact) -> TaskArtifactResponse:
    payload = artifact.to_public_payload() if hasattr(artifact, "to_public_payload") else artifact
    return TaskArtifactResponse(
        artifact_id=payload["artifact_id"],
        task_id=payload["task_id"],
        seq=payload["seq"],
        type=payload["type"],
        name=payload["name"],
        path=payload.get("path", ""),
        mime_type=payload.get("mime_type", ""),
        size=payload.get("size"),
        download_url=payload.get("download_url", ""),
        metadata=payload.get("metadata", {}),
        created_at=payload["created_at"],
    )


def _checkpoint_to_response(checkpoint) -> TaskCheckpointResponse:
    payload = (
        checkpoint.to_public_payload() if hasattr(checkpoint, "to_public_payload") else checkpoint
    )
    return TaskCheckpointResponse(
        checkpoint_id=payload["checkpoint_id"],
        task_id=payload["task_id"],
        seq=payload["seq"],
        pipeline_name=payload.get("pipeline_name", ""),
        collector_name=payload.get("collector_name", ""),
        worker_id=payload.get("worker_id", ""),
        recovery_level=payload.get("recovery_level", "L0"),
        cursor=payload.get("cursor", {}),
        stats=payload.get("stats", {}),
        artifacts=payload.get("artifacts", []),
        metadata=payload.get("metadata", {}),
        created_at=payload["created_at"],
    )


async def _sync_session_inventory_best_effort(session_diagnostics: dict[str, Any]) -> None:
    from src.web.app import get_session_registry

    await sync_session_inventory_via_provider_best_effort(
        get_session_registry,
        session_diagnostics,
        context="task_route",
    )
