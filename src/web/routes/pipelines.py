"""
Pipeline configuration API routes.
"""

from __future__ import annotations

from typing import Annotated, Any
from fastapi import APIRouter, HTTPException, Query, Path, Body
from pydantic import BaseModel, Field

from src.core.collector_metadata import (
    collector_metadata_payload,
    fallback_collector_metadata,
    get_collector_metadata,
)
from src.core.pipeline import Pipeline
from src.core.dag import DAG, Edge as DagEdge, NodeSpec, PortSpec, dag_to_pipeline
from src.core.registry import registry
from src.core.pipeline_templates import PIPELINE_TEMPLATES
from src.web.safety import require_explicit_confirmation, validate_dynamic_playwright_config

router = APIRouter(tags=["pipelines"])


def _get_scheduler():
    """Lazy import scheduler to avoid circular dependency."""
    from src.web.app import scheduler

    return scheduler


def _get_dag_repo():
    from src.web.app import get_dag_repository

    return get_dag_repository()


class PipelineStepConfig(BaseModel):
    """Single pipeline step."""

    type: str = Field(..., description="Step type: collector/processor/storage")
    name: str = Field(..., description="Component name")
    config: dict[str, Any] = Field(default_factory=dict, description="Step config")


class CreatePipelineRequest(BaseModel):
    """Create pipeline request."""

    name: str = Field(..., description="Pipeline name")
    steps: list[PipelineStepConfig] = Field(..., description="Step list")


class PortSpecConfig(BaseModel):
    name: str
    required: bool = True
    type_hint: str = ""


class NodeSpecConfig(BaseModel):
    id: str
    type: str
    component: str = ""
    config: dict[str, Any] = Field(default_factory=dict)
    ports_in: list[PortSpecConfig] = Field(default_factory=list)
    ports_out: list[PortSpecConfig] = Field(default_factory=list)
    is_param_port: list[str] = Field(default_factory=list)
    subgraph_name: str | None = None
    # Frontend layout metadata (x/y/label); ignored by executor
    ui: dict[str, Any] = Field(default_factory=dict)


class EdgeConfig(BaseModel):
    from_node: str = Field(..., alias="from")
    from_port: str = Field(..., alias="out")
    to_node: str = Field(..., alias="to")
    to_port: str = Field(..., alias="in")
    condition: str | None = None

    model_config = {"populate_by_name": True}


class CreateDagRequest(BaseModel):
    """Create DAG request."""

    name: str
    nodes: list[NodeSpecConfig]
    edges: list[EdgeConfig] = Field(default_factory=list)
    conditions: list[str] = Field(default_factory=list)
    # Graph-level UI metadata (zoom/pan); ignored by executor
    ui: dict[str, Any] = Field(default_factory=dict)


class CronJobRequest(BaseModel):
    """Create / update cron job request."""

    name: str = Field(..., description="Cron job name")
    pipeline_name: str = Field(..., description="Pipeline name")
    cron_expr: str = Field(default="", description="Five-field cron expression (optional if schedule provided)")
    schedule: dict[str, Any] = Field(
        default_factory=dict,
        description="Visual schedule: {mode: preset|cron, preset: {...}, cron_expr, timezone}",
    )
    task_template: dict[str, Any] = Field(
        default_factory=dict,
        description="Task template: targets, config, collector_name, description",
    )
    enabled: bool = Field(default=True, description="Whether the job is active")
    timezone: str = Field(default="", description="IANA timezone, e.g. Asia/Shanghai")
    description: str = Field(default="", description="Human description")


class CronSchedulePreviewRequest(BaseModel):
    cron_expr: str = Field(default="", description="Five-field cron expression")
    schedule: dict[str, Any] = Field(default_factory=dict)
    timezone: str = Field(default="")
    count: int = Field(default=5, ge=1, le=20)


class CronEnabledRequest(BaseModel):
    enabled: bool = True


@router.get("/components")
async def list_components():
    return registry.list_components()


@router.get("/components/metadata")
async def list_component_metadata():
    components = registry.list_components()
    collector_metadata = {}
    for collector_id in components.get("collector", []):
        metadata = get_collector_metadata(collector_id) or fallback_collector_metadata(collector_id)
        collector_metadata[collector_id] = collector_metadata_payload(metadata.collector_id)
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
    # 合并 DAG 图定义
    try:
        dag_repo = _get_dag_repo()
        for dag in await dag_repo.list_all():
            if dag.name not in pipelines:
                pipelines[dag.name] = dag.to_storage()
    except Exception:
        pass
    return pipelines


@router.get("/dags")
async def list_dags():
    """列出所有已保存的 DAG 图定义。"""
    dag_repo = _get_dag_repo()
    return {dag.name: dag.to_storage() for dag in await dag_repo.list_all()}


@router.get("/dags/{name}")
async def get_dag(
    name: Annotated[str, Path(description="DAG name")],
):
    dag_repo = _get_dag_repo()
    dag = await dag_repo.load(name)
    if dag is None:
        raise HTTPException(404, f"DAG not found: {name}")
    return dag.to_storage()


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


@router.post("/dags")
async def create_dag(
    req: Annotated[CreateDagRequest, Body(description="DAG configuration")],
):
    """保存 DAG 图，并投影注册为同名 Pipeline 供任务创建使用。"""
    dag = _build_dag_from_request(req)
    if not any(n.type == "collector" for n in dag.nodes):
        raise HTTPException(400, "DAG must contain at least one collector node")
    await _get_dag_repo().save(dag)

    # 双写：投影 Pipeline 注册进 Scheduler，任务下拉/ precheck / submit 可用
    pipeline = dag_to_pipeline(dag)
    if not pipeline.steps:
        raise HTTPException(400, "DAG has no executable collector/processor/storage nodes")
    await _get_scheduler().save_pipeline(pipeline)

    return {
        "message": f"DAG created: {req.name}",
        "config": dag.to_storage(),
        "pipeline": pipeline.to_config(),
    }


@router.delete("/dags/{name}")
async def delete_dag(
    name: Annotated[str, Path(description="DAG name")],
    confirm: Annotated[bool, Query(description="Must be true for destructive delete")] = False,
):
    require_explicit_confirmation(confirm, "DAG deletion")
    deleted = await _get_dag_repo().delete(name)
    if not deleted:
        raise HTTPException(404, f"DAG not found: {name}")
    # 同步删除同名 pipeline 投影（若存在）
    scheduler = _get_scheduler()
    if scheduler.get_pipeline(name) is not None:
        await scheduler.delete_pipeline(name)
    return {"message": f"DAG deleted: {name}"}


def _build_dag_from_request(req: CreateDagRequest) -> DAG:
    nodes = [
        NodeSpec(
            id=n.id,
            type=n.type,
            component=n.component,
            config=n.config,
            ports_in=[PortSpec(name=p.name, required=p.required, type_hint=p.type_hint) for p in n.ports_in],
            ports_out=[PortSpec(name=p.name, required=p.required, type_hint=p.type_hint) for p in n.ports_out],
            is_param_port=set(n.is_param_port),
            subgraph_name=n.subgraph_name,
            ui=dict(n.ui or {}),
        )
        for n in req.nodes
    ]
    edges = [
        DagEdge(
            from_node=e.from_node, from_port=e.from_port,
            to_node=e.to_node, to_port=e.to_port, condition=e.condition,
        )
        for e in req.edges
    ]
    return DAG(name=req.name, nodes=nodes, edges=edges, ui=dict(req.ui or {}))


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


@router.post("/cron-jobs/preview")
async def preview_cron_schedule(
    req: Annotated[CronSchedulePreviewRequest, Body(description="Schedule preview")],
):
    """Preview human label and next run times without creating a job."""
    from src.core.cron_schedule import resolve_schedule_input, next_run_times

    try:
        resolved = resolve_schedule_input(
            cron_expr=req.cron_expr or None,
            schedule=req.schedule,
            timezone=req.timezone or None,
        )
        runs = next_run_times(
            resolved["cron_expr"],
            count=req.count,
            timezone=resolved["timezone"],
        )
        return {
            **resolved,
            "next_runs": runs,
            "valid": True,
        }
    except ValueError as exc:
        raise HTTPException(400, str(exc))


@router.post("/cron-jobs")
async def create_cron_job(req: Annotated[CronJobRequest, Body(description="Cron job setup")]):
    scheduler = _get_scheduler()
    from src.core.cron_schedule import resolve_schedule_input

    try:
        resolved = resolve_schedule_input(
            cron_expr=req.cron_expr or None,
            schedule=req.schedule,
            timezone=req.timezone or None,
        )
        job_id = scheduler.add_cron_job(
            name=req.name,
            pipeline_name=req.pipeline_name,
            cron_expr=resolved["cron_expr"],
            task_template=req.task_template,
            enabled=req.enabled,
            timezone=resolved["timezone"],
            schedule_meta=resolved["schedule_meta"],
            description=req.description,
        )
        job = scheduler.get_cron_job(req.name)
        return {
            "message": f"Cron job created: {req.name}",
            "job_id": job_id,
            "job": job,
        }
    except ValueError as exc:
        raise HTTPException(400, str(exc))


@router.put("/cron-jobs/{name}")
async def update_cron_job(
    name: Annotated[str, Path(description="Cron job ID/Name")],
    req: Annotated[CronJobRequest, Body(description="Cron job update")],
):
    scheduler = _get_scheduler()
    from src.core.cron_schedule import resolve_schedule_input

    if scheduler.get_cron_job(name) is None:
        raise HTTPException(404, f"Cron job not found: {name}")
    try:
        resolved = resolve_schedule_input(
            cron_expr=req.cron_expr or None,
            schedule=req.schedule,
            timezone=req.timezone or None,
        )
        # Allow rename only if same name; path is authoritative
        job_id = scheduler.update_cron_job(
            name,
            pipeline_name=req.pipeline_name,
            cron_expr=resolved["cron_expr"],
            task_template=req.task_template,
            enabled=req.enabled,
            timezone=resolved["timezone"],
            schedule_meta=resolved["schedule_meta"],
            description=req.description,
        )
        return {
            "message": f"Cron job updated: {name}",
            "job_id": job_id,
            "job": scheduler.get_cron_job(name),
        }
    except ValueError as exc:
        raise HTTPException(400, str(exc))


@router.patch("/cron-jobs/{name}/enabled")
async def set_cron_job_enabled(
    name: Annotated[str, Path(description="Cron job ID/Name")],
    req: Annotated[CronEnabledRequest, Body(description="Enable/disable")],
):
    scheduler = _get_scheduler()
    if not scheduler.set_cron_job_enabled(name, req.enabled):
        raise HTTPException(404, f"Cron job not found: {name}")
    return {
        "message": f"Cron job {'enabled' if req.enabled else 'disabled'}: {name}",
        "job": scheduler.get_cron_job(name),
    }


@router.post("/cron-jobs/{name}/run")
async def run_cron_job_now(name: Annotated[str, Path(description="Cron job ID/Name")]):
    scheduler = _get_scheduler()
    job = scheduler.get_cron_job(name)
    if job is None:
        raise HTTPException(404, f"Cron job not found: {name}")
    # DAG-only / template pipelines: project into scheduler before submit
    pipeline_name = str(job.get("pipeline_name") or "")
    if pipeline_name and hasattr(scheduler, "resolve_pipeline"):
        try:
            await scheduler.resolve_pipeline(pipeline_name)
        except Exception:
            pass
    try:
        task_id = await scheduler.run_cron_job_now(name)
        return {"message": f"Cron job triggered: {name}", "task_id": task_id}
    except LookupError as exc:
        raise HTTPException(404, str(exc))
    except ValueError as exc:
        raise HTTPException(400, str(exc))
    except Exception as exc:
        raise HTTPException(400, str(exc))


@router.get("/cron-jobs/{name}")
async def get_cron_job(name: Annotated[str, Path(description="Cron job ID/Name")]):
    scheduler = _get_scheduler()
    job = scheduler.get_cron_job(name)
    if job is None:
        raise HTTPException(404, f"Cron job not found: {name}")
    return job


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
