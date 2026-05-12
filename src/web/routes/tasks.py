"""
任务管理 API 路由
"""

from __future__ import annotations

from typing import Annotated, Any
from fastapi import APIRouter, HTTPException, Query, Path, Body
from pydantic import BaseModel, Field

from src.core.sensitive import redact_sensitive
from src.core.task import Task
from src.schemas.tasks import TaskPrecheckResponse
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


# ==================== 路由 ====================


@router.get("/tasks", response_model=list[TaskResponse])
async def list_tasks(
    status: Annotated[str | None, Query(description="按状态过滤任务")] = None
):
    """获取所有任务列表"""
    from src.web.app import get_task_service

    try:
        tasks = get_task_service().list_tasks(status)
    except ValueError:
        raise HTTPException(400, f"无效的状态: {status}")

    return [_task_to_response(t) for t in tasks]


@router.post("/tasks/precheck", response_model=TaskPrecheckResponse)
async def precheck_task(
    req: Annotated[CreateTaskRequest, Body(description="Task creation precheck")]
):
    """Validate task input before submitting it to the scheduler."""
    from src.web.app import get_task_service
    return get_task_service().precheck(
        name=req.name,
        pipeline_name=req.pipeline_name,
        collector_name=req.collector_name,
        targets=req.targets,
    )


# run_task_precheck and its helpers moved to TaskService in src.services.task_service


@router.post("/tasks", response_model=TaskResponse)
async def create_task(
    req: Annotated[CreateTaskRequest, Body(description="任务创建信息")]
):
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
async def get_task(
    task_id: Annotated[str, Path(description="任务 ID")]
):
    """获取单个任务详情"""
    from src.web.app import get_task_service

    task = get_task_service().get_task(task_id)
    if task is None:
        raise HTTPException(404, f"任务不存在: {task_id}")

    return _task_to_detail_response(task)


@router.get("/tasks/{task_id}/logs")
async def get_task_logs(
    task_id: Annotated[str, Path(description="任务 ID")]
):
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


@router.post("/tasks/{task_id}/cancel")
async def cancel_task(
    task_id: Annotated[str, Path(description="任务 ID")]
):
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
        name=task.name,
        status=task.status.value,
        progress=task.progress,
        pipeline_name=task.pipeline_name,
        collector_name=task.collector_name,
        targets_count=len(task.targets),
        created_at=task.created_at.isoformat(),
        started_at=task.started_at.isoformat() if task.started_at else None,
        completed_at=task.completed_at.isoformat() if task.completed_at else None,
        duration=task.duration_seconds,
        error=task.error,
    )


def _task_to_detail_response(task: Task) -> TaskDetailResponse:
    base = _task_to_response(task)
    return TaskDetailResponse(
        **base.model_dump(),
        description=task.description,
        targets=redact_sensitive([target.model_dump() for target in task.targets]),
        config=redact_sensitive(task.config),
        retry_count=task.retry_count,
        max_retries=task.max_retries,
        step_logs=[_log_to_response(log) for log in task.step_logs],
        result_summary=task.result_summary,
    )


def _log_to_response(log) -> TaskLogResponse:
    return TaskLogResponse(
        step=log.step_name,
        status=log.status.value,
        message=log.message,
        error=log.error,
        started_at=log.started_at.isoformat() if log.started_at else None,
        completed_at=log.completed_at.isoformat() if log.completed_at else None,
    )
