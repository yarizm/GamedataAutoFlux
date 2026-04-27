"""
Pipeline configuration API routes.
"""

from __future__ import annotations

from typing import Annotated, Any
from fastapi import APIRouter, HTTPException, Path, Body
from pydantic import BaseModel, Field

from src.core.pipeline import Pipeline
from src.core.registry import registry

router = APIRouter(tags=["pipelines"])


class PipelineStepConfig(BaseModel):
    """Single pipeline step."""

    type: str = Field(..., description="Step type: collector/processor/storage")
    name: str = Field(..., description="Component name")
    config: dict[str, Any] = Field(default_factory=dict, description="Step config")


class CreatePipelineRequest(BaseModel):
    """Create pipeline request."""

    name: str = Field(..., description="Pipeline name")
    steps: list[PipelineStepConfig] = Field(..., description="Step list")


class CronJobRequest(BaseModel):
    """Create cron job request."""

    name: str = Field(..., description="Cron job name")
    pipeline_name: str = Field(..., description="Pipeline name")
    cron_expr: str = Field(..., description="Cron expression")
    task_template: dict[str, Any] = Field(default_factory=dict, description="Task template")


PIPELINE_TEMPLATES = [
    {
        "id": "steam_basic",
        "name": "Steam 基础采集",
        "description": "Steam -> cleaner -> local，适合保存清洗后的采集结果",
        "steps": [
            {"type": "collector", "name": "steam", "config": {"request_delay": 0.5}},
            {"type": "processor", "name": "cleaner", "config": {}},
            {"type": "storage", "name": "local", "config": {}},
        ],
    },
    {
        "id": "steam_vector_report",
        "name": "Steam 检索报告链路",
        "description": "Steam -> cleaner -> embedding -> vector，适合语义检索与报告",
        "steps": [
            {"type": "collector", "name": "steam", "config": {"request_delay": 0.5}},
            {"type": "processor", "name": "cleaner", "config": {}},
            {"type": "processor", "name": "embedding", "config": {}},
            {"type": "storage", "name": "vector", "config": {}},
        ],
    },
    {
        "id": "taptap_basic",
        "name": "TapTap 基础采集",
        "description": "TapTap -> cleaner -> local，适合公开页详情、评价、更新采集",
        "steps": [
            {"type": "collector", "name": "taptap", "config": {}},
            {"type": "processor", "name": "cleaner", "config": {}},
            {"type": "storage", "name": "local", "config": {}},
        ],
    },
    {
        "id": "taptap_report",
        "name": "TapTap 检索报告链路",
        "description": "TapTap -> cleaner -> embedding -> vector，适合移动端游戏检索与报告",
        "steps": [
            {"type": "collector", "name": "taptap", "config": {}},
            {"type": "processor", "name": "cleaner", "config": {}},
            {"type": "processor", "name": "embedding", "config": {}},
            {"type": "storage", "name": "vector", "config": {}},
        ],
    },
    {
        "id": "steam_full_report",
        "name": "Steam 一条龙报告",
        "description": "Steam -> cleaner -> embedding -> local -> vector，适合采集、落库和报告一条龙",
        "steps": [
            {"type": "collector", "name": "steam", "config": {"request_delay": 0.5}},
            {"type": "processor", "name": "cleaner", "config": {}},
            {"type": "processor", "name": "embedding", "config": {}},
            {"type": "storage", "name": "local", "config": {}},
            {"type": "storage", "name": "vector", "config": {}},
        ],
    },
    {
        "id": "steam_discussions_basic",
        "name": "Steam Community 讨论采集",
        "description": "steam_discussions -> cleaner -> local，适合按时间区间采集玩家讨论",
        "steps": [
            {"type": "collector", "name": "steam_discussions", "config": {}},
            {"type": "processor", "name": "cleaner", "config": {}},
            {"type": "storage", "name": "local", "config": {}},
        ],
    },
    {
        "id": "steam_discussions_report",
        "name": "Steam Community 讨论检索报告链路",
        "description": "steam_discussions -> cleaner -> embedding -> vector，适合讨论语义检索与报告",
        "steps": [
            {"type": "collector", "name": "steam_discussions", "config": {}},
            {"type": "processor", "name": "cleaner", "config": {}},
            {"type": "processor", "name": "embedding", "config": {}},
            {"type": "storage", "name": "vector", "config": {}},
        ],
    },
    {
        "id": "steam_discussions_full_report",
        "name": "Steam Community 讨论一条龙报告",
        "description": "steam_discussions -> cleaner -> embedding -> local -> vector，适合讨论采集、落库和自动报告",
        "steps": [
            {"type": "collector", "name": "steam_discussions", "config": {}},
            {"type": "processor", "name": "cleaner", "config": {}},
            {"type": "processor", "name": "embedding", "config": {}},
            {"type": "storage", "name": "local", "config": {}},
            {"type": "storage", "name": "vector", "config": {}},
        ],
    },
    {
        "id": "taptap_full_report",
        "name": "TapTap 一条龙报告",
        "description": "TapTap -> cleaner -> embedding -> local -> vector，适合公开页采集、落库和报告",
        "steps": [
            {"type": "collector", "name": "taptap", "config": {}},
            {"type": "processor", "name": "cleaner", "config": {}},
            {"type": "processor", "name": "embedding", "config": {}},
            {"type": "storage", "name": "local", "config": {}},
            {"type": "storage", "name": "vector", "config": {}},
        ],
    },
    {
        "id": "gtrends_basic",
        "name": "Google Trends 基础采集",
        "description": "gtrends -> cleaner -> local，适合获取游戏时序热度",
        "steps": [
            {"type": "collector", "name": "gtrends", "config": {}},
            {"type": "processor", "name": "cleaner", "config": {}},
            {"type": "storage", "name": "local", "config": {}},
        ],
    },
    {
        "id": "monitor_basic",
        "name": "Monitor 基础采集",
        "description": "monitor -> cleaner -> local，适合 Steam 外围趋势指标采集",
        "steps": [
            {"type": "collector", "name": "monitor", "config": {}},
            {"type": "processor", "name": "cleaner", "config": {}},
            {"type": "storage", "name": "local", "config": {}},
        ],
    },
    {
        "id": "monitor_report",
        "name": "Monitor 检索报告链路",
        "description": "monitor -> cleaner -> embedding -> vector，适合趋势检索与报告",
        "steps": [
            {"type": "collector", "name": "monitor", "config": {}},
            {"type": "processor", "name": "cleaner", "config": {}},
            {"type": "processor", "name": "embedding", "config": {}},
            {"type": "storage", "name": "vector", "config": {}},
        ],
    },
    {
        "id": "monitor_full_report",
        "name": "Monitor 一条龙报告",
        "description": "monitor -> cleaner -> embedding -> local -> vector，适合采集、落库和自动报告",
        "steps": [
            {"type": "collector", "name": "monitor", "config": {}},
            {"type": "processor", "name": "cleaner", "config": {}},
            {"type": "processor", "name": "embedding", "config": {}},
            {"type": "storage", "name": "local", "config": {}},
            {"type": "storage", "name": "vector", "config": {}},
        ],
    },
    {
        "id": "qimai_basic",
        "name": "Qimai(七麦) 基础采集",
        "description": "qimai -> cleaner -> local，适合 AppStore 排名评分获取",
        "steps": [
            {"type": "collector", "name": "qimai", "config": {}},
            {"type": "processor", "name": "cleaner", "config": {}},
            {"type": "storage", "name": "local", "config": {}},
        ],
    },
    {
        "id": "qimai_report",
        "name": "Qimai(七麦) 检索报告链路",
        "description": "qimai -> cleaner -> embedding -> vector，适合 AppStore 排名报告",
        "steps": [
            {"type": "collector", "name": "qimai", "config": {}},
            {"type": "processor", "name": "cleaner", "config": {}},
            {"type": "processor", "name": "embedding", "config": {}},
            {"type": "storage", "name": "vector", "config": {}},
        ],
    },
]


@router.get("/components")
async def list_components():
    return registry.list_components()


@router.get("/pipeline-templates")
async def list_pipeline_templates():
    return PIPELINE_TEMPLATES


@router.get("/pipelines")
async def list_pipelines():
    from src.web.app import scheduler

    pipelines = {}
    for pipeline in scheduler.get_all_pipelines():
        pipelines[pipeline.name] = pipeline.to_config()
    return pipelines


@router.post("/pipelines")
async def create_pipeline(
    req: Annotated[CreatePipelineRequest, Body(description="Pipeline configuration")]
):
    from src.web.app import scheduler

    pipeline = Pipeline(req.name)
    for step in req.steps:
        try:
            registry.get(step.type, step.name)
        except KeyError as exc:
            raise HTTPException(400, str(exc))

        if step.type == "collector":
            pipeline.add_collector(step.name, step.config)
        elif step.type == "processor":
            pipeline.add_processor(step.name, step.config)
        elif step.type == "storage":
            pipeline.add_storage(step.name, step.config)
        else:
            raise HTTPException(400, f"Unknown step type: {step.type}")

    await scheduler.save_pipeline(pipeline)
    return {"message": f"Pipeline created: {req.name}", "config": pipeline.to_config()}


@router.delete("/pipelines/{name}")
async def delete_pipeline(
    name: Annotated[str, Path(description="Pipeline name")]
):
    from src.web.app import scheduler

    if scheduler.get_pipeline(name) is None:
        raise HTTPException(404, f"Pipeline not found: {name}")

    await scheduler.delete_pipeline(name)
    return {"message": f"Pipeline deleted: {name}"}


@router.get("/cron-jobs")
async def list_cron_jobs():
    from src.web.app import scheduler

    return scheduler.list_cron_jobs()


@router.post("/cron-jobs")
async def create_cron_job(
    req: Annotated[CronJobRequest, Body(description="Cron job setup")]
):
    from src.web.app import scheduler

    try:
        job_id = scheduler.add_cron_job(
            name=req.name,
            pipeline_name=req.pipeline_name,
            cron_expr=req.cron_expr,
            task_template=req.task_template,
        )
        return {"message": f"Cron job created: {req.name}", "job_id": job_id}
    except ValueError as exc:
        raise HTTPException(400, str(exc))


@router.delete("/cron-jobs/{name}")
async def delete_cron_job(
    name: Annotated[str, Path(description="Cron job ID/Name")]
):
    from src.web.app import scheduler

    if not scheduler.remove_cron_job(name):
        raise HTTPException(404, f"Cron job not found: {name}")

    return {"message": f"Cron job deleted: {name}"}
