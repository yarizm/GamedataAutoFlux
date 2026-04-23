"""
Pipeline configuration API routes.
"""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException
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
async def create_pipeline(req: CreatePipelineRequest):
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
async def delete_pipeline(name: str):
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
async def create_cron_job(req: CronJobRequest):
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
async def delete_cron_job(name: str):
    from src.web.app import scheduler

    if not scheduler.remove_cron_job(name):
        raise HTTPException(404, f"Cron job not found: {name}")

    return {"message": f"Cron job deleted: {name}"}
