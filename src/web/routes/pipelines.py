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
from src.core.collector_validators import validate_collector_config
from src.core.pipeline import Pipeline
from src.core.pipeline_availability import inspect_pipeline_availability
from src.core.dag import DAG, Edge as DagEdge, NodeSpec, PortSpec, dag_to_pipeline
from src.core.dag_executor import validate_dag_detailed
from src.core.dag_nodes import dag_node_catalog_payload, get_dag_node
from src.core.registry import registry
from src.core.pipeline_templates import PIPELINE_TEMPLATES
from src.web.safety import require_explicit_confirmation

router = APIRouter(tags=["pipelines"])


def _get_scheduler():
    """Lazy import scheduler to avoid circular dependency."""
    from src.bootstrap.container import scheduler

    return scheduler


def _get_dag_repo():
    from src.bootstrap.container import get_dag_repository

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
    cron_expr: str = Field(
        default="", description="Five-field cron expression (optional if schedule provided)"
    )
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


@router.get("/plugins")
async def list_plugins():
    """Return installed collector plugin activation diagnostics."""
    from src.core.plugin_system import plugin_manager

    return plugin_manager.payload()


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
        "dag_nodes": dag_node_catalog_payload(components),
    }


@router.get("/pipeline-templates")
async def list_pipeline_templates():
    return PIPELINE_TEMPLATES


@router.get("/pipelines")
async def list_pipelines(
    available_only: Annotated[
        bool,
        Query(description="Only return pipelines whose components are currently active"),
    ] = False,
):
    scheduler = _get_scheduler()

    pipelines = {}
    for pipeline in scheduler.get_all_pipelines():
        config = pipeline.to_config()
        if not available_only or inspect_pipeline_availability(config).available:
            pipelines[pipeline.name] = config
    # 合并 DAG 图定义
    try:
        dag_repo = _get_dag_repo()
        for dag in await dag_repo.list_all():
            config = dag.to_storage()
            if dag.name not in pipelines and (
                not available_only or inspect_pipeline_availability(config).available
            ):
                pipelines[dag.name] = config
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
            _validate_collector_config_or_400(step.name, step.config)
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

    # 保存前完整结构校验；composite 引用的子图预载后一并校验可解析性
    dag_repo = _get_dag_repo()
    subgraphs: dict[str, Any] = {}
    for node in dag.nodes:
        if node.type == "composite" and (node.subgraph_name or "").strip():
            subgraphs[node.subgraph_name] = await dag_repo.load(node.subgraph_name)
    issues = validate_dag_detailed(
        dag,
        subgraph_loader=(lambda name: subgraphs.get(name)) if subgraphs else None,
    )
    errors = [i.message for i in issues if i.severity == "error"]
    if errors:
        raise HTTPException(
            400,
            {"code": "dag_validation_failed", "issues": errors},
        )
    await dag_repo.save(dag)

    # 双写：投影 Pipeline 注册进 Scheduler，任务下拉/ precheck / submit 可用
    pipeline = dag_to_pipeline(dag)
    if not pipeline.steps:
        raise HTTPException(400, "DAG has no executable collector/processor/storage nodes")
    await _get_scheduler().save_pipeline(pipeline)

    return {
        "message": f"DAG created: {req.name}",
        "config": dag.to_storage(),
        "pipeline": pipeline.to_config(),
        "warnings": [i.message for i in issues if i.severity == "warning"],
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
    for node in req.nodes:
        if node.type in {"collector", "processor", "storage"}:
            if not node.component:
                raise HTTPException(400, f"DAG node '{node.id}' component is required")
            try:
                registry.get(node.type, node.component)
            except KeyError as exc:
                raise HTTPException(
                    400,
                    {
                        "code": "dag_component_unavailable",
                        "node_id": node.id,
                        "component_type": node.type,
                        "component": node.component,
                        "message": str(exc),
                    },
                ) from exc
            _validate_node_ports_or_400(node)
        elif node.type != "composite":
            raise HTTPException(400, f"Unsupported DAG node type: {node.type}")
        if node.type == "collector":
            _validate_collector_config_or_400(node.component, node.config)
    nodes = [
        NodeSpec(
            id=n.id,
            type=n.type,
            component=n.component,
            config=n.config,
            ports_in=_resolved_node_ports(n, direction="in"),
            ports_out=_resolved_node_ports(n, direction="out"),
            is_param_port=set(n.is_param_port),
            subgraph_name=n.subgraph_name,
            ui=dict(n.ui or {}),
        )
        for n in req.nodes
    ]
    edges = [
        DagEdge(
            from_node=e.from_node,
            from_port=e.from_port,
            to_node=e.to_node,
            to_port=e.to_port,
            condition=e.condition,
        )
        for e in req.edges
    ]
    return DAG(name=req.name, nodes=nodes, edges=edges, ui=dict(req.ui or {}))


def _validate_node_ports_or_400(node: NodeSpecConfig) -> None:
    definition = get_dag_node(node.type, node.component)
    if definition is None:
        return
    for direction, supplied, declared in (
        ("input", node.ports_in, definition.ports_in),
        ("output", node.ports_out, definition.ports_out),
    ):
        if not supplied:
            continue
        allowed = {item.name for item in declared}
        unknown = sorted({item.name for item in supplied} - allowed)
        if unknown:
            raise HTTPException(
                400,
                {
                    "code": "dag_port_undeclared",
                    "node_id": node.id,
                    "component": node.component,
                    "direction": direction,
                    "ports": unknown,
                    "message": (
                        f"DAG node '{node.id}' uses undeclared {direction} ports: "
                        + ", ".join(unknown)
                    ),
                },
            )


def _resolved_node_ports(
    node: NodeSpecConfig,
    *,
    direction: str,
) -> list[PortSpec]:
    supplied = node.ports_in if direction == "in" else node.ports_out
    if supplied:
        return [
            PortSpec(name=item.name, required=item.required, type_hint=item.type_hint)
            for item in supplied
        ]
    definition = get_dag_node(node.type, node.component)
    if definition is None:
        return []
    declared = definition.ports_in if direction == "in" else definition.ports_out
    return [
        PortSpec(name=item.name, required=item.required, type_hint=item.type_hint)
        for item in declared
    ]


def _validate_collector_config_or_400(
    collector_id: str,
    config: dict[str, Any],
) -> None:
    issues = validate_collector_config(collector_id, config)
    if not issues:
        return
    first = issues[0]
    raise HTTPException(
        400,
        {
            "code": first.code,
            "field": first.field,
            "message": first.message,
        },
    )


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
