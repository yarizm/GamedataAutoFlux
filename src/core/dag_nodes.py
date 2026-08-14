"""DAG execution adapters and plugin-owned editor node definitions.

The execution adapters wrap collector, processor, and storage components in a
uniform runtime interface.  The declarative catalog at the end of the module
describes the same components to the DAG editor.  Collector plugins receive a
conventional editor node automatically, while advanced plugins may override
ports and schemas through :class:`src.core.plugin_system.PluginSpec`.
"""

from __future__ import annotations

import asyncio
from copy import deepcopy
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable

from loguru import logger
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from src.collectors.base import BaseCollector, CollectResult, CollectTarget
from src.core.dag import NodeSpec, PortSpec
from src.core.registry import registry
from src.core.sensitive import redact_sensitive_text
from src.core.task import Task
from src.processors.base import BaseProcessor, ProcessInput, ProcessOutput
from src.storage.base import StorageRecord


@dataclass
class NodeContext:
    inputs: dict[str, Any]
    task: Task
    config: dict[str, Any]
    recovery_checkpoint: dict[str, Any] = field(default_factory=dict)
    emit_event: Callable[..., Awaitable[None]] | None = None
    register_artifact: Callable[..., Awaitable[None]] | None = None
    register_checkpoint: Callable[..., Awaitable[None]] | None = None

    def param(self, name: str, default: Any = None) -> Any:
        return self.inputs.get(name, default)


class NodeProtocol:
    node_id: str
    input_ports: list[PortSpec]
    output_ports: list[PortSpec]

    async def setup(self) -> None: ...

    async def run(self, ctx: NodeContext) -> dict[str, Any]: ...

    async def teardown(self) -> None: ...


def _build_collect_targets(task: Task) -> list[CollectTarget]:
    return [
        CollectTarget(name=target.name, target_type=target.target_type, params=target.params)
        for target in task.targets
    ]


def _flatten_records(value: Any) -> list:
    """Normalize a port value to a flat list after fan-in edges."""

    if value is None:
        return []
    if isinstance(value, list):
        flat: list = []
        for item in value:
            if isinstance(item, list):
                flat.extend(item)
            else:
                flat.append(item)
        return flat
    return [value]


def build_emit_checkpoint(
    *,
    task_id: str,
    component: str,
    emit_pipeline_event: Callable[..., Awaitable[None] | None] | None = None,
    emit_dag_event: Callable[..., Awaitable[None] | None] | None = None,
    node_spec: Any | None = None,
) -> Callable[..., Awaitable[None]]:
    """Build the checkpoint callback injected into collector configuration."""

    async def _emit_checkpoint(
        cursor: dict[str, Any],
        state: dict[str, Any] | None = None,
        stats: dict[str, Any] | None = None,
    ) -> None:
        if not isinstance(cursor, dict) or not cursor:
            return
        checkpoint_state = state if isinstance(state, dict) else {}
        checkpoint_stats = stats if isinstance(stats, dict) else {}
        try:
            if emit_pipeline_event is not None:
                result = emit_pipeline_event(
                    task_id,
                    "collect",
                    "info",
                    "collect progress checkpoint",
                    {
                        "status": "progress",
                        "component": component,
                        "checkpoint_cursor": cursor,
                        "checkpoint_state": checkpoint_state,
                        "stats": checkpoint_stats,
                    },
                )
                if asyncio.iscoroutine(result):
                    await result
                return
            if emit_dag_event is not None and node_spec is not None:
                result = emit_dag_event(
                    task_id,
                    node_spec,
                    "progress",
                    out={
                        "checkpoint_cursor": cursor,
                        "checkpoint_state": checkpoint_state,
                        "stats": checkpoint_stats,
                    },
                )
                if asyncio.iscoroutine(result):
                    await result
        except Exception as exc:  # noqa: BLE001 - checkpoint emission is best effort
            logger.warning(
                "collect progress checkpoint emit failed ({}): {}",
                component,
                redact_sensitive_text(str(exc)),
            )

    return _emit_checkpoint


class CollectorNode:
    """Runtime node wrapping one registered collector."""

    def __init__(
        self,
        spec: NodeSpec,
        *,
        task: Task,
        recovery_checkpoint: dict[str, Any],
    ) -> None:
        self.spec = spec
        self.node_id = spec.id
        self.input_ports = spec.ports_in
        self.output_ports = spec.ports_out
        self._task = task
        self._recovery_context = recovery_checkpoint
        self._collector: BaseCollector | None = None

    async def setup(self) -> None:
        collector_type = registry.get("collector", self.spec.component)
        config = dict(self.spec.config)
        if self._recovery_context:
            config["recovery_checkpoint"] = self._recovery_context
        self._collector = collector_type(config=config)

    async def run(self, ctx: NodeContext) -> dict[str, Any]:
        assert self._collector is not None
        await self._collector.setup(self.spec.config)

        from src.core.dag_upstream import resolve_collector_targets

        node_config = {**(self.spec.config or {}), **(ctx.config or {})}
        upstream_records = _flatten_records(ctx.inputs.get("records"))
        task_targets = _build_collect_targets(self._task)
        targets = resolve_collector_targets(
            task_targets=task_targets,
            upstream_records=upstream_records,
            node_config=node_config,
        )

        if not node_config.get("from_upstream"):
            collect_context = (self._recovery_context or {}).get("collect", {})
            if collect_context:
                from src.core.pipeline_recovery import apply_collect_resume_context

                targets = apply_collect_resume_context(targets, collect_context)

        if not targets:
            return {"records": []}

        self._collector.config["_emit_checkpoint"] = build_emit_checkpoint(
            task_id=self._task.id,
            component=self.spec.component,
            emit_dag_event=ctx.emit_event,
            node_spec=self.spec,
        )
        results = await self._collector.collect_batch(targets)
        return {"records": results}

    async def teardown(self) -> None:
        if self._collector is not None:
            await self._collector.teardown()
            self._collector = None


class ProcessorNode:
    """Runtime node wrapping one registered processor."""

    def __init__(
        self,
        spec: NodeSpec,
        *,
        task: Task,
        recovery_checkpoint: dict[str, Any],
    ) -> None:
        self.spec = spec
        self.node_id = spec.id
        self.input_ports = spec.ports_in
        self.output_ports = spec.ports_out
        self._task = task
        self._recovery_context = recovery_checkpoint
        self._processor: BaseProcessor | None = None

    async def setup(self) -> None:
        processor_type = registry.get("processor", self.spec.component)
        self._processor = processor_type(config=self.spec.config)

    async def run(self, ctx: NodeContext) -> dict[str, Any]:
        assert self._processor is not None
        collect_results = [
            result
            for result in _flatten_records(ctx.inputs.get("records"))
            if isinstance(result, CollectResult)
        ]
        inputs = [
            ProcessInput(
                data=result.data,
                metadata={
                    **result.metadata,
                    "target": result.target.name,
                    "collected_at": result.collected_at.isoformat(),
                },
                source=result.target.name,
            )
            for result in collect_results
            if result.success and result.data is not None
        ]
        await self._processor.setup()
        outputs = await self._processor.process_batch(inputs)
        return {"records": outputs}

    async def teardown(self) -> None:
        if self._processor is not None:
            await self._processor.teardown()
            self._processor = None


class StorageNode:
    """Runtime sink node wrapping the configured storage implementation."""

    def __init__(
        self,
        spec: NodeSpec,
        *,
        task: Task,
        recovery_checkpoint: dict[str, Any],
    ) -> None:
        self.spec = spec
        self.node_id = spec.id
        self.input_ports = spec.ports_in
        self.output_ports = spec.ports_out
        self._task = task
        self._recovery_context = recovery_checkpoint
        self._storage = None

    async def setup(self) -> None:
        from src.storage.factory import get_storage, normalize_storage_name

        raw = self.spec.component if self.spec.component != "storage" else None
        storage_name = normalize_storage_name(raw) if raw else None
        self._storage = get_storage(storage_name)
        await self._storage.initialize()

    async def run(self, ctx: NodeContext) -> dict[str, Any]:
        from src.core.pipeline import _build_storage_metadata
        from src.core.pipeline_recovery import (
            build_storage_record_key,
            resolve_storage_resume_context,
        )

        raw = _flatten_records(ctx.inputs.get("records"))
        collect_results = [item for item in raw if isinstance(item, CollectResult)]
        process_outputs = [item for item in raw if isinstance(item, ProcessOutput)]

        process_inputs: list[ProcessInput] = []
        if process_outputs:
            for output in process_outputs:
                if not output.success or output.data is None:
                    continue
                process_inputs.append(
                    ProcessInput(
                        data=output.data,
                        metadata=output.metadata,
                        source=output.processor_name or "unknown",
                    )
                )
        else:
            for result in collect_results:
                if not result.success or result.data is None:
                    continue
                process_inputs.append(
                    ProcessInput(
                        data=result.data,
                        metadata=result.metadata,
                        source=result.target.name,
                    )
                )

        storage_context = resolve_storage_resume_context(
            self._recovery_context,
            current_data=process_inputs,
        )
        records: list[StorageRecord] = [
            StorageRecord(
                key=build_storage_record_key(
                    self._task,
                    process_input,
                    index=index,
                    storage_context=storage_context,
                ),
                data=process_input.data,
                metadata=_build_storage_metadata(self._task, process_input.metadata),
                source=process_input.source,
            )
            for index, process_input in enumerate(process_inputs)
        ]
        await self._storage.save_batch(records)
        return {"_stored": len(records), "output_records": records}

    async def teardown(self) -> None:
        if self._storage is not None:
            await self._storage.close()
            self._storage = None


SUPPORTED_DAG_NODE_TYPES = ("collector", "processor", "storage")


class DagPortDefinition(BaseModel):
    """One typed input or output port exposed in the DAG editor."""

    name: str
    required: bool = True
    type_hint: str = "records"

    @field_validator("name")
    @classmethod
    def _validate_name(cls, value: str) -> str:
        normalized = str(value or "").strip()
        if not normalized:
            raise ValueError("DAG port name is required")
        return normalized


class DagOutputField(BaseModel):
    """One record field that downstream nodes may map from this node."""

    key: str
    label: str = ""
    type_hint: str = ""
    description: str = ""

    @field_validator("key")
    @classmethod
    def _validate_key(cls, value: str) -> str:
        normalized = str(value or "").strip()
        if not normalized:
            raise ValueError("DAG output field key is required")
        return normalized


class DagNodeDefinition(BaseModel):
    """Declarative node definition contributed by a plugin or the core."""

    model_config = ConfigDict(populate_by_name=True)

    node_type: str = Field(alias="type")
    component: str
    display_name: str = ""
    description: str = ""
    ports_in: list[DagPortDefinition] = Field(default_factory=list)
    ports_out: list[DagPortDefinition] = Field(default_factory=list)
    config_schema: dict[str, Any] = Field(default_factory=dict)
    output_fields: list[DagOutputField] = Field(default_factory=list)

    @field_validator("node_type")
    @classmethod
    def _validate_node_type(cls, value: str) -> str:
        normalized = str(value or "").strip()
        if normalized not in SUPPORTED_DAG_NODE_TYPES:
            raise ValueError(
                "DAG node type must be collector, processor, or storage"
            )
        return normalized

    @field_validator("component")
    @classmethod
    def _validate_component(cls, value: str) -> str:
        normalized = str(value or "").strip()
        if not normalized:
            raise ValueError("DAG node component is required")
        return normalized

    @model_validator(mode="after")
    def _validate_definition(self) -> DagNodeDefinition:
        _require_unique((item.name for item in self.ports_in), "input port")
        _require_unique((item.name for item in self.ports_out), "output port")
        _require_unique((item.key for item in self.output_fields), "output field")
        _validate_config_schema(self.config_schema)
        return self

    def to_payload(self, *, owner: str) -> dict[str, Any]:
        payload = self.model_dump(mode="json", by_alias=True)
        payload["owner"] = owner
        return payload


_DAG_NODES: dict[tuple[str, str], tuple[DagNodeDefinition, str]] = {}
_CORE_DAG_NODE_COPY: dict[tuple[str, str], tuple[str, str]] = {
    ("processor", "cleaner"): (
        "Data Cleaner",
        "Normalizes collected records, removes empty values, and prepares data for storage.",
    ),
    ("processor", "vectorizer"): (
        "Vectorizer",
        "Transforms collected text into vector representations for downstream semantic use.",
    ),
    ("storage", "sqlalchemy"): (
        "SQLAlchemy Storage",
        "Persists processed records through the configured SQLAlchemy database backend.",
    ),
    ("storage", "sqlalchemy_scheduler"): (
        "SQLAlchemy Scheduler Storage",
        "Persists scheduler execution records through the configured SQLAlchemy backend.",
    ),
}


def register_dag_node(definition: DagNodeDefinition, *, owner: str) -> None:
    """Register a validated node without allowing cross-plugin replacement."""

    normalized_owner = str(owner or "").strip()
    if not normalized_owner:
        raise ValueError("DAG node owner is required")
    normalized = definition.model_copy(deep=True)
    key = (normalized.node_type, normalized.component)
    current = _DAG_NODES.get(key)
    if current and current[1] != normalized_owner:
        raise ValueError(
            f"DAG node '{key[0]}:{key[1]}' already belongs to plugin "
            f"'{current[1]}'"
        )
    _DAG_NODES[key] = (normalized, normalized_owner)


def get_dag_node(node_type: str, component: str) -> DagNodeDefinition | None:
    entry = _DAG_NODES.get((str(node_type or "").strip(), str(component or "").strip()))
    return entry[0].model_copy(deep=True) if entry else None


def dag_node_catalog_payload(
    components: dict[str, list[str]] | None = None,
) -> list[dict[str, Any]]:
    """Return definitions for every currently executable DAG component."""

    if components is None:
        from src.core.registry import registry

        components = registry.list_components()

    payload: list[dict[str, Any]] = []
    for node_type in SUPPORTED_DAG_NODE_TYPES:
        for component in components.get(node_type, []):
            entry = _DAG_NODES.get((node_type, component))
            if entry:
                definition, owner = entry
            else:
                display_name, description = _CORE_DAG_NODE_COPY.get(
                    (node_type, component),
                    (component, ""),
                )
                definition = default_dag_node(
                    node_type,
                    component,
                    display_name=display_name,
                    description=description,
                )
                owner = "core"
            payload.append(definition.to_payload(owner=owner))
    return payload


def default_dag_node(
    node_type: str,
    component: str,
    *,
    display_name: str = "",
    description: str = "",
    config_schema: dict[str, Any] | None = None,
    output_fields: list[DagOutputField] | None = None,
) -> DagNodeDefinition:
    """Build the conventional node contract used by most components."""

    if node_type == "storage":
        ports_in = [DagPortDefinition(name="records", required=True)]
        ports_out: list[DagPortDefinition] = []
    elif node_type == "collector":
        ports_in = [DagPortDefinition(name="records", required=False)]
        ports_out = [DagPortDefinition(name="records", required=True)]
    else:
        ports_in = [DagPortDefinition(name="records", required=True)]
        ports_out = [DagPortDefinition(name="records", required=True)]
    return DagNodeDefinition(
        type=node_type,
        component=component,
        display_name=display_name or component,
        description=description,
        ports_in=ports_in,
        ports_out=ports_out,
        config_schema=deepcopy(config_schema or {}),
        output_fields=[item.model_copy(deep=True) for item in (output_fields or [])],
    )


def snapshot_dag_nodes() -> dict[tuple[str, str], tuple[DagNodeDefinition, str]]:
    return {
        key: (definition.model_copy(deep=True), owner)
        for key, (definition, owner) in _DAG_NODES.items()
    }


def restore_dag_nodes(
    snapshot: dict[tuple[str, str], tuple[DagNodeDefinition, str]],
) -> None:
    _DAG_NODES.clear()
    _DAG_NODES.update(
        {
            key: (definition.model_copy(deep=True), owner)
            for key, (definition, owner) in snapshot.items()
        }
    )


def _require_unique(values: Any, label: str) -> None:
    normalized = [str(value or "").strip() for value in values]
    duplicates = sorted({value for value in normalized if normalized.count(value) > 1})
    if duplicates:
        raise ValueError(f"duplicate DAG {label}s: {', '.join(duplicates)}")


def _validate_config_schema(schema: dict[str, Any]) -> None:
    if not schema:
        return
    if schema.get("type", "object") != "object":
        raise ValueError("DAG node config_schema must describe an object")
    properties = schema.get("properties", {})
    if not isinstance(properties, dict):
        raise ValueError("DAG node config_schema.properties must be an object")
    required = schema.get("required", [])
    if not isinstance(required, list) or any(not isinstance(item, str) for item in required):
        raise ValueError("DAG node config_schema.required must be a string array")
    unknown = sorted(set(required) - set(properties))
    if unknown:
        raise ValueError(
            "DAG node config_schema.required references unknown properties: "
            + ", ".join(unknown)
        )
