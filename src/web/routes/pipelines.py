"""
Pipeline configuration API routes.
"""

from __future__ import annotations

from typing import Annotated, Any
from fastapi import APIRouter, HTTPException, Query, Path, Body
from pydantic import BaseModel, Field

from src.core.collector_metadata import fallback_collector_metadata, get_collector_metadata
from src.core.pipeline import Pipeline
from src.core.registry import registry
from src.core.pipeline_templates import PIPELINE_TEMPLATES
from src.web.safety import require_explicit_confirmation, validate_dynamic_playwright_config

router = APIRouter(tags=["pipelines"])


def _get_scheduler():
    """Lazy import scheduler to avoid circular dependency."""
    from src.web.app import scheduler

    return scheduler


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


@router.get("/components")
async def list_components():
    return registry.list_components()


@router.get("/components/metadata")
async def list_component_metadata():
    components = registry.list_components()
    collector_metadata = {}
    for collector_id in components.get("collector", []):
        metadata = get_collector_metadata(collector_id) or fallback_collector_metadata(collector_id)
        collector_metadata[collector_id] = metadata.model_dump(mode="json")
    return {
        "components": components,
        "collectors": collector_metadata,
    }


@router.get("/pipeline-templates")
async def list_pipeline_templates():
    return PIPELINE_TEMPLATES


@router.get("/pipelines")
async def list_pipelines():
    scheduler = _get_scheduler()

    pipelines = {}
    for pipeline in scheduler.get_all_pipelines():
        pipelines[pipeline.name] = pipeline.to_config()
    return pipelines


@router.post("/pipelines")
async def create_pipeline(
    req: Annotated[CreatePipelineRequest, Body(description="Pipeline configuration")],
):
    scheduler = _get_scheduler()

    pipeline = Pipeline(req.name)
    for step in req.steps:
        try:
            registry.get(step.type, step.name)
        except KeyError as exc:
            raise HTTPException(400, str(exc))

        if step.type == "collector":
            if step.name == "dynamic_playwright":
                validate_dynamic_playwright_config(step.config)
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
    name: Annotated[str, Path(description="Pipeline name")],
    confirm: Annotated[bool, Query(description="Must be true for destructive delete")] = False,
):
    scheduler = _get_scheduler()

    require_explicit_confirmation(confirm, "pipeline deletion")
    if scheduler.get_pipeline(name) is None:
        raise HTTPException(404, f"Pipeline not found: {name}")

    await scheduler.delete_pipeline(name)
    return {"message": f"Pipeline deleted: {name}"}


@router.get("/cron-jobs")
async def list_cron_jobs():
    scheduler = _get_scheduler()

    return scheduler.list_cron_jobs()


@router.post("/cron-jobs")
async def create_cron_job(req: Annotated[CronJobRequest, Body(description="Cron job setup")]):
    scheduler = _get_scheduler()

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
    name: Annotated[str, Path(description="Cron job ID/Name")],
    confirm: Annotated[bool, Query(description="Must be true for destructive delete")] = False,
):
    scheduler = _get_scheduler()

    require_explicit_confirmation(confirm, "cron job deletion")
    if not scheduler.remove_cron_job(name):
        raise HTTPException(404, f"Cron job not found: {name}")

    return {"message": f"Cron job deleted: {name}"}
