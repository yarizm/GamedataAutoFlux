"""
任务管理 API 路由
"""

from __future__ import annotations

from typing import Annotated, Any
from fastapi import APIRouter, HTTPException, Query, Path, Body
from pydantic import BaseModel, Field

from src.core.sensitive import redact_sensitive
from src.core.task import Task, TaskTarget, TaskStatus

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
    from src.web.app import scheduler

    if status:
        try:
            task_status = TaskStatus(status)
            tasks = scheduler.get_tasks_by_status(task_status)
        except ValueError:
            raise HTTPException(400, f"无效的状态: {status}")
    else:
        tasks = scheduler.get_all_tasks()

    tasks = sorted(tasks, key=lambda task: task.created_at, reverse=True)
    return [_task_to_response(t) for t in tasks]


@router.post("/tasks", response_model=TaskResponse)
async def create_task(
    req: Annotated[CreateTaskRequest, Body(description="任务创建信息")]
):
    """创建并提交新任务"""
    from src.web.app import scheduler

    collector_name = req.collector_name
    pipeline = scheduler.get_pipeline(req.pipeline_name)
    if not collector_name and pipeline is not None:
        collector_step = next((step for step in pipeline.steps if step.step_type.value == "collector"), None)
        if collector_step is not None:
            collector_name = collector_step.component_name

    targets = [
        TaskTarget(
            name=t.get("name", ""),
            target_type=t.get("target_type", "default"),
            params=t.get("params", {}),
        )
        for t in req.targets
    ]

    task = Task(
        name=req.name,
        description=req.description,
        pipeline_name=req.pipeline_name,
        collector_name=collector_name,
        targets=targets,
        config=req.config,
    )

    try:
        await scheduler.submit(task, pipeline_name=req.pipeline_name)
    except (ValueError, RuntimeError) as e:
        raise HTTPException(400, str(e))

    return _task_to_response(task)


@router.get("/tasks/{task_id}", response_model=TaskDetailResponse)
async def get_task(
    task_id: Annotated[str, Path(description="任务 ID")]
):
    """获取单个任务详情"""
    from src.web.app import scheduler

    task = scheduler.get_task(task_id)
    if task is None:
        raise HTTPException(404, f"任务不存在: {task_id}")

    return _task_to_detail_response(task)


@router.get("/tasks/{task_id}/logs")
async def get_task_logs(
    task_id: Annotated[str, Path(description="任务 ID")]
):
    """获取任务步骤日志"""
    from src.web.app import scheduler

    task = scheduler.get_task(task_id)
    if task is None:
        raise HTTPException(404, f"任务不存在: {task_id}")

    return {
        "task_id": task.id,
        "logs": [_log_to_response(log) for log in task.step_logs],
    }


@router.post("/tasks/{task_id}/cancel")
async def cancel_task(
    task_id: Annotated[str, Path(description="任务 ID")]
):
    """取消任务"""
    from src.web.app import scheduler

    success = await scheduler.cancel(task_id)
    if not success:
        raise HTTPException(400, f"无法取消任务: {task_id}")

    return {"message": f"任务已取消: {task_id}"}


@router.delete("/tasks/{task_id}")
async def delete_task(
    task_id: Annotated[str, Path(description="任务 ID")]
):
    """鍒犻櫎浠诲姟"""
    from src.web.app import scheduler

    success = await scheduler.delete_task(task_id)
    if not success:
        raise HTTPException(400, f"鏃犳硶鍒犻櫎浠诲姟: {task_id}")

    return {"message": f"浠诲姟宸插垹闄? {task_id}"}


@router.get("/tasks/stats/summary")
async def get_task_stats():
    """获取任务统计信息"""
    from src.web.app import scheduler
    return scheduler.get_stats()


# ==================== 辅助函数 ====================

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
