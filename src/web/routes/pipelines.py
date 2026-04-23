"""
Pipeline 配置 API 路由
"""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from src.core.pipeline import Pipeline
from src.core.registry import registry

router = APIRouter(tags=["pipelines"])


# ==================== 请求/响应模型 ====================

class PipelineStepConfig(BaseModel):
    """Pipeline 步骤配置"""
    type: str = Field(..., description="步骤类型: collector/processor/storage")
    name: str = Field(..., description="组件名称")
    config: dict[str, Any] = Field(default_factory=dict, description="步骤配置")


class CreatePipelineRequest(BaseModel):
    """创建 Pipeline 请求"""
    name: str = Field(..., description="Pipeline 名称")
    steps: list[PipelineStepConfig] = Field(..., description="步骤列表")


class CronJobRequest(BaseModel):
    """创建定时任务请求"""
    name: str = Field(..., description="任务名称")
    pipeline_name: str = Field(..., description="Pipeline 名称")
    cron_expr: str = Field(..., description="Cron 表达式")
    task_template: dict[str, Any] = Field(default_factory=dict, description="任务模板")


PIPELINE_TEMPLATES = [
    {
        "id": "steam_basic",
        "name": "Steam 基础采集",
        "description": "Steam -> cleaner -> local，适合先保存原始清洗结果",
        "steps": [
            {"type": "collector", "name": "steam", "config": {"request_delay": 0.5}},
            {"type": "processor", "name": "cleaner", "config": {}},
            {"type": "storage", "name": "local", "config": {}},
        ],
    },
    {
        "id": "steam_vector_report",
        "name": "Steam 检索报告链路",
        "description": "Steam -> cleaner -> embedding -> vector，适合报告和语义检索",
        "steps": [
            {"type": "collector", "name": "steam", "config": {"request_delay": 0.5}},
            {"type": "processor", "name": "cleaner", "config": {}},
            {"type": "processor", "name": "embedding", "config": {}},
            {"type": "storage", "name": "vector", "config": {}},
        ],
    },
]


# ==================== 路由 ====================

@router.get("/components")
async def list_components():
    """获取所有可用组件（采集器、处理器、存储）"""
    return registry.list_components()


@router.get("/pipeline-templates")
async def list_pipeline_templates():
    """获取预设 Pipeline 模板。"""
    return PIPELINE_TEMPLATES


@router.get("/pipelines")
async def list_pipelines():
    """获取所有已注册的 Pipeline"""
    from src.web.app import scheduler
    pipelines = {}
    for pipeline in scheduler.get_all_pipelines():
        pipelines[pipeline.name] = pipeline.to_config()
    return pipelines


@router.post("/pipelines")
async def create_pipeline(req: CreatePipelineRequest):
    """创建并注册 Pipeline"""
    from src.web.app import scheduler

    pipeline = Pipeline(req.name)
    for step in req.steps:
        # 验证组件是否存在
        try:
            registry.get(step.type, step.name)
        except KeyError as e:
            raise HTTPException(400, str(e))

        if step.type == "collector":
            pipeline.add_collector(step.name, step.config)
        elif step.type == "processor":
            pipeline.add_processor(step.name, step.config)
        elif step.type == "storage":
            pipeline.add_storage(step.name, step.config)
        else:
            raise HTTPException(400, f"未知的步骤类型: {step.type}")

    await scheduler.save_pipeline(pipeline)
    return {"message": f"Pipeline 已创建: {req.name}", "config": pipeline.to_config()}


@router.delete("/pipelines/{name}")
async def delete_pipeline(name: str):
    """删除 Pipeline"""
    from src.web.app import scheduler

    if scheduler.get_pipeline(name) is None:
        raise HTTPException(404, f"Pipeline 不存在: {name}")

    await scheduler.delete_pipeline(name)
    return {"message": f"Pipeline 已删除: {name}"}


# ==================== 定时任务 ====================

@router.get("/cron-jobs")
async def list_cron_jobs():
    """获取所有定时任务"""
    from src.web.app import scheduler
    return scheduler.list_cron_jobs()


@router.post("/cron-jobs")
async def create_cron_job(req: CronJobRequest):
    """创建定时任务"""
    from src.web.app import scheduler

    try:
        job_id = scheduler.add_cron_job(
            name=req.name,
            pipeline_name=req.pipeline_name,
            cron_expr=req.cron_expr,
            task_template=req.task_template,
        )
        return {"message": f"定时任务已创建: {req.name}", "job_id": job_id}
    except ValueError as e:
        raise HTTPException(400, str(e))


@router.delete("/cron-jobs/{name}")
async def delete_cron_job(name: str):
    """删除定时任务"""
    from src.web.app import scheduler

    if not scheduler.remove_cron_job(name):
        raise HTTPException(404, f"定时任务不存在: {name}")

    return {"message": f"定时任务已删除: {name}"}
