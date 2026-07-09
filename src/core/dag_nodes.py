"""DAG 节点包装层：把现有组件适配为统一节点接口。"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable

from src.collectors.base import BaseCollector, CollectResult, CollectTarget
from src.core.dag import NodeSpec, PortSpec
from src.core.registry import registry
from src.core.task import Task
from src.processors.base import BaseProcessor, ProcessInput, ProcessOutput
from src.storage.base import StorageRecord


@dataclass
class NodeContext:
    inputs: dict[str, Any]  # 端口名 -> 上游数据（多入边时为 list）
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
        CollectTarget(name=t.name, target_type=t.target_type, params=t.params)
        for t in task.targets
    ]


def _flatten_records(value: Any) -> list:
    """端口值规整为扁平 list（多入边汇合时可能是 list-of-lists）。"""
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


class CollectorNode:
    """组件级节点：包装采集器。"""

    def __init__(self, spec: NodeSpec, *, task: Task, recovery_checkpoint: dict[str, Any]) -> None:
        self.spec = spec
        self.node_id = spec.id
        self.input_ports = spec.ports_in
        self.output_ports = spec.ports_out
        self._task = task
        self._recovery_context = recovery_checkpoint
        self._collector: BaseCollector | None = None

    async def setup(self) -> None:
        cls_ = registry.get("collector", self.spec.component)
        config = dict(self.spec.config)
        if self._recovery_context:
            config["recovery_checkpoint"] = self._recovery_context
        self._collector = cls_(config=config)

    async def run(self, ctx: NodeContext) -> dict[str, Any]:
        assert self._collector is not None
        await self._collector.setup(self.spec.config)
        targets = _build_collect_targets(self._task)
        collect_ctx = (self._recovery_context or {}).get("collect", {})
        if collect_ctx:
            from src.core.pipeline_recovery import apply_collect_resume_context

            targets = apply_collect_resume_context(targets, collect_ctx)
        results = await self._collector.collect_batch(targets)
        return {"records": results}

    async def teardown(self) -> None:
        if self._collector is not None:
            await self._collector.teardown()
            self._collector = None


class ProcessorNode:
    """组件级节点：包装处理器。"""

    def __init__(self, spec: NodeSpec, *, task: Task, recovery_checkpoint: dict[str, Any]) -> None:
        self.spec = spec
        self.node_id = spec.id
        self.input_ports = spec.ports_in
        self.output_ports = spec.ports_out
        self._task = task
        self._recovery_context = recovery_checkpoint
        self._processor: BaseProcessor | None = None

    async def setup(self) -> None:
        cls_ = registry.get("processor", self.spec.component)
        self._processor = cls_(config=self.spec.config)

    async def run(self, ctx: NodeContext) -> dict[str, Any]:
        assert self._processor is not None
        collect_results = [
            r for r in _flatten_records(ctx.inputs.get("records"))
            if isinstance(r, CollectResult)
        ]
        inputs = [
            ProcessInput(
                data=r.data,
                metadata={**r.metadata, "target": r.target.name, "collected_at": r.collected_at.isoformat()},
                source=r.target.name,
            )
            for r in collect_results
            if r.success and r.data is not None
        ]
        await self._processor.setup()
        outputs = await self._processor.process_batch(inputs)
        return {"records": outputs}

    async def teardown(self) -> None:
        if self._processor is not None:
            await self._processor.teardown()
            self._processor = None


class StorageNode:
    """组件级节点：包装存储，sink 节点无输出。复用 pipeline 的 key/metadata helper。"""

    def __init__(self, spec: NodeSpec, *, task: Task, recovery_checkpoint: dict[str, Any]) -> None:
        self.spec = spec
        self.node_id = spec.id
        self.input_ports = spec.ports_in
        self.output_ports = spec.ports_out
        self._task = task
        self._recovery_context = recovery_checkpoint
        self._storage = None

    async def setup(self) -> None:
        from src.storage.factory import get_storage

        storage_name = self.spec.component if self.spec.component != "storage" else None
        self._storage = get_storage(storage_name)
        await self._storage.initialize()

    async def run(self, ctx: NodeContext) -> dict[str, Any]:
        from src.core.pipeline import _build_storage_metadata
        from src.core.pipeline_recovery import (
            build_storage_record_key,
            resolve_storage_resume_context,
        )

        raw = _flatten_records(ctx.inputs.get("records"))
        collect_results = [r for r in raw if isinstance(r, CollectResult)]
        process_outputs = [r for r in raw if isinstance(r, ProcessOutput)]

        # 构造 ProcessInput 列表以复用 build_storage_record_key/resolve_storage_resume_context
        process_inputs: list[ProcessInput] = []
        if process_outputs:
            for po in process_outputs:
                if not po.success or po.data is None:
                    continue
                process_inputs.append(ProcessInput(
                    data=po.data, metadata=po.metadata, source=po.processor_name or "unknown",
                ))
        else:
            for cr in collect_results:
                if not cr.success or cr.data is None:
                    continue
                process_inputs.append(ProcessInput(
                    data=cr.data, metadata=cr.metadata, source=cr.target.name,
                ))

        storage_context = resolve_storage_resume_context(
            self._recovery_context, current_data=process_inputs,
        )
        records: list[StorageRecord] = [
            StorageRecord(
                key=build_storage_record_key(
                    self._task, pi, index=idx, storage_context=storage_context,
                ),
                data=pi.data,
                metadata=_build_storage_metadata(self._task, pi.metadata),
                source=pi.source,
            )
            for idx, pi in enumerate(process_inputs)
        ]
        await self._storage.save_batch(records)
        return {"_stored": len(records), "output_records": records}

    async def teardown(self) -> None:
        if self._storage is not None:
            await self._storage.close()
            self._storage = None
