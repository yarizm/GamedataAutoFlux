"""DAG 执行引擎：校验、拓扑排序、并发调度、端口表、条件边、checkpoint。"""
from __future__ import annotations

import asyncio
from collections import defaultdict, deque
from datetime import datetime
from typing import Any, Awaitable, Callable

from loguru import logger

from src.core.dag import DAG, DAGResult, Edge, NodeSpec
from src.core.dag_conditions import resolve_condition
from src.core.dag_nodes import (
    CollectorNode,
    NodeContext,
    ProcessorNode,
    StorageNode,
)
from src.core.pipeline_recovery import (
    build_pipeline_recovery_context,
    build_pipeline_resume_state,
)
from src.core.sensitive import redact_sensitive_text
from src.core.task import Task


class DAGValidationError(Exception):
    pass


def validate_dag(dag: DAG) -> list[str]:
    issues: list[str] = []
    node_ids = {n.id for n in dag.nodes}
    for e in dag.edges:
        if e.from_node not in node_ids:
            issues.append(f"edge references missing node: {e.from_node}")
        if e.to_node not in node_ids:
            issues.append(f"edge references missing node: {e.to_node}")
    # 循环检测（Kahn）
    indeg: dict[str, int] = {n.id: 0 for n in dag.nodes}
    adj: dict[str, list[str]] = defaultdict(list)
    for e in dag.edges:
        adj[e.from_node].append(e.to_node)
        indeg[e.to_node] += 1
    q = deque([nid for nid, d in indeg.items() if d == 0])
    seen = 0
    while q:
        nid = q.popleft()
        seen += 1
        for nxt in adj[nid]:
            indeg[nxt] -= 1
            if indeg[nxt] == 0:
                q.append(nxt)
    if seen != len(dag.nodes):
        issues.append("cycle detected in DAG")
    # 悬空必需输入端口
    incoming_by_node: dict[str, set[str]] = defaultdict(set)
    for e in dag.edges:
        incoming_by_node[e.to_node].add(e.to_port)
    for n in dag.nodes:
        for p in n.ports_in:
            if p.required and p.name not in incoming_by_node.get(n.id, set()):
                issues.append(f"dangling required input port: {n.id}.{p.name}")
    return issues


def topological_layers(dag: DAG) -> list[list[str]]:
    issues = validate_dag(dag)
    if issues:
        raise DAGValidationError("; ".join(issues))
    indeg: dict[str, int] = {n.id: 0 for n in dag.nodes}
    adj: dict[str, list[str]] = defaultdict(list)
    for e in dag.edges:
        adj[e.from_node].append(e.to_node)
        indeg[e.to_node] += 1
    layers: list[list[str]] = []
    current = sorted([nid for nid, d in indeg.items() if d == 0])
    while current:
        layers.append(current)
        nxt: list[str] = []
        for nid in current:
            for m in adj[nid]:
                indeg[m] -= 1
                if indeg[m] == 0:
                    nxt.append(m)
        current = sorted(nxt)
    return layers


def _instantiate_node(node_spec: NodeSpec, *, task: Task, recovery_checkpoint: dict) -> Any:
    if node_spec.type == "collector":
        return CollectorNode(node_spec, task=task, recovery_checkpoint=recovery_checkpoint)
    if node_spec.type == "processor":
        return ProcessorNode(node_spec, task=task, recovery_checkpoint=recovery_checkpoint)
    if node_spec.type == "storage":
        return StorageNode(node_spec, task=task, recovery_checkpoint=recovery_checkpoint)
    raise ValueError(f"unknown node type: {node_spec.type}")


def _has_successful_collects(collect_results: list) -> bool:
    return any(r.success and r.data is not None for r in collect_results)


def _flatten_records(value: Any) -> list:
    """把端口值规整为扁平 list：标量单值包成 list；list-of-lists 展平；None → []。"""
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


class DAGExecutor:
    def __init__(self, *, subgraph_loader: Callable[[str], DAG | None] | None = None) -> None:
        self._port_table: dict[str, dict[str, Any]] = {}
        self._node_success: dict[str, bool] = {}
        self._suppressed_edges: set[tuple[str, str, str]] = set()
        self._subgraph_loader = subgraph_loader

    async def execute(
        self,
        task: Task,
        dag: DAG,
        *,
        recovery_checkpoint: dict[str, Any] | None = None,
        semaphore: asyncio.Semaphore | None = None,
        on_progress: Callable[[str, float, str], Awaitable[None] | None] | None = None,
        on_event: Callable[..., Awaitable[None] | None] | None = None,
    ) -> DAGResult:
        # 展开 composite 节点（子图内联）
        dag = self._expand_composite_nodes(dag)
        layers = topological_layers(dag)
        result = DAGResult(pipeline_name=dag.name, task_id=task.id)

        # 用 pipeline 标准恢复上下文（含 collect 键），节点直接消费
        recovery_context = build_pipeline_recovery_context(task, recovery_checkpoint)

        # 每次执行重置实例状态，避免跨调用泄漏
        self._port_table = {}
        self._node_success = {}
        self._suppressed_edges = set()

        total_layers = len(layers)
        aborted = False
        for layer_idx, layer in enumerate(layers):
            if aborted:
                break
            runnable = [nid for nid in layer]

            async def _run_one(node_id: str) -> None:
                node_spec = dag.node_by_id(node_id)
                if node_spec is None:
                    return
                # 构造 inputs 视图：按端口累加，跳过被抑制的边
                accum: dict[str, list[Any]] = defaultdict(list)
                for e in dag.edges:
                    if e.to_node != node_id:
                        continue
                    if (e.from_node, e.to_node, e.to_port) in self._suppressed_edges:
                        continue
                    upstream_out = self._port_table.get(e.from_node, {})
                    accum[e.to_port].append(upstream_out.get(e.from_port))
                inputs: dict[str, Any] = {}
                for port, vals in accum.items():
                    inputs[port] = vals[0] if len(vals) == 1 else vals
                ctx = NodeContext(
                    inputs=inputs, task=task, config=node_spec.config,
                    recovery_checkpoint=recovery_context, emit_event=on_event,
                )
                if semaphore is not None:
                    await semaphore.acquire()
                try:
                    # setup/instantiate 也归入节点错误处理，不逃逸到 gather
                    try:
                        await self._notify(on_event, task.id, node_spec, "start")
                        node = _instantiate_node(node_spec, task=task, recovery_checkpoint=recovery_context)
                        await node.setup()
                        out = await node.run(ctx)
                        self._port_table[node_id] = out
                        self._node_success[node_id] = True
                        if node_spec.type == "collector":
                            result.collect_results.extend(out.get("records", []))
                        elif node_spec.type == "processor":
                            result.process_results.extend(out.get("records", []))
                        elif node_spec.type == "storage":
                            result.storage_count += out.get("_stored", 0)
                            result.output_records.extend(out.get("output_records", []))
                        await self._notify(on_event, task.id, node_spec, "complete", out=out)
                    except Exception as exc:
                        safe = redact_sensitive_text(str(exc))
                        self._node_success[node_id] = False
                        result.errors.append(f"{node_id}: {safe}")
                        logger.error("DAG node {} failed: {}", node_id, safe)
                        await self._notify(on_event, task.id, node_spec, "error", error=safe)
                finally:
                    try:
                        await node.teardown()
                    except Exception as te:
                        logger.warning("DAG node {} teardown error: {}", node_id, redact_sensitive_text(str(te)))
                    if semaphore is not None:
                        semaphore.release()

            await asyncio.gather(*[_run_one(nid) for nid in runnable])
            self._evaluate_outgoing_conditions(dag, layer)
            # 清掉该层节点的抑制记录（抑制只影响紧邻下一层 inputs 构造）
            self._suppressed_edges = {
                (fn, tn, tp) for (fn, tn, tp) in self._suppressed_edges if fn not in layer
            }

            # collector 层全失败 → 早终止（复刻 Pipeline._has_successful_collects）
            if layer_idx == 0 and not _has_successful_collects(result.collect_results):
                aborted = True
                result.success = False
                if not result.errors:
                    result.errors.append(f"{dag.name}: all collect targets failed")
                break

            if on_progress is not None:
                progress = (layer_idx + 1) / total_layers * 0.9
                r = on_progress(task.id, progress, f"layer {layer_idx + 1}/{total_layers} done")
                if asyncio.iscoroutine(r):
                    await r

        failed = [nid for nid, ok in self._node_success.items() if not ok]
        if failed:
            result.success = False
        result.completed_at = datetime.now()
        result.resume_state = build_pipeline_resume_state(
            task,
            recovery_context=recovery_context,
            collect_results=result.collect_results,
            output_records=result.output_records,
        )
        return result

    async def _notify(
        self,
        on_event: Callable[..., Awaitable[None] | None] | None,
        task_id: str,
        node_spec: NodeSpec,
        phase: str,
        *,
        out: dict[str, Any] | None = None,
        error: str | None = None,
    ) -> None:
        """向调用方透传节点生命周期事件（phase: start/complete/error）。

        调用方负责翻译成具体事件契约（如 pipeline 的 collect/process/storage 事件）。
        """
        if on_event is None:
            return
        try:
            r = on_event(task_id, node_spec, phase, out=out, error=error)
            if asyncio.iscoroutine(r):
                await r
        except Exception as exc:
            logger.warning("DAG node event emit failed: {}", redact_sensitive_text(str(exc)))

    def _evaluate_outgoing_conditions(self, dag: DAG, layer: list[str]) -> None:
        """对带条件的边求值；条件为假的边标记为抑制（边级，不影响同端口其它边）。"""
        for node_id in layer:
            success = self._node_success.get(node_id, False)
            out = self._port_table.get(node_id, {})
            for e in dag.edges:
                if e.from_node != node_id or e.condition is None:
                    continue
                pred = resolve_condition(e.condition)
                if pred is None:
                    continue
                ctx = NodeContext(inputs={}, task=Task(name=""), config={})
                activated = pred(out, success, ctx)
                if not activated:
                    self._suppressed_edges.add((e.from_node, e.to_node, e.to_port))

    def _expand_composite_nodes(self, dag: DAG) -> DAG:
        """把 type=composite 节点按 subgraph_name 内联展开。无 composite 则原样返回。"""
        composite_nodes = [n for n in dag.nodes if n.type == "composite"]
        if not composite_nodes:
            return dag
        if self._subgraph_loader is None:
            raise DAGValidationError("composite node present but no subgraph_loader configured")

        new_nodes: list[NodeSpec] = []
        new_edges: list[Edge] = []
        # composite_id -> (source_node_ids, sink_node_ids) 子图入口/出口
        for n in dag.nodes:
            if n.type != "composite":
                new_nodes.append(n)
                continue
            sub = self._subgraph_loader(n.subgraph_name or "")
            if sub is None:
                raise DAGValidationError(f"composite node {n.id}: subgraph '{n.subgraph_name}' not found")
            prefix = f"{n.id}/"
            sub_source_ids: list[str] = []
            sub_sink_ids: list[str] = []
            has_incoming: set[str] = set()
            has_outgoing: set[str] = set()
            for e in sub.edges:
                has_incoming.add(e.to_node)
                has_outgoing.add(e.from_node)
            for sn in sub.nodes:
                prefixed_id = prefix + sn.id
                new_nodes.append(NodeSpec(
                    id=prefixed_id, type=sn.type, component=sn.component,
                    config=sn.config, ports_in=sn.ports_in, ports_out=sn.ports_out,
                    is_param_port=sn.is_param_port, subgraph_name=None,
                ))
                if sn.id not in has_incoming:
                    sub_source_ids.append(prefixed_id)
                if sn.id not in has_outgoing:
                    sub_sink_ids.append(prefixed_id)
            for se in sub.edges:
                new_edges.append(Edge(
                    from_node=prefix + se.from_node, from_port=se.from_port,
                    to_node=prefix + se.to_node, to_port=se.to_port,
                    condition=se.condition,
                ))
            # 记录 composite 的入出边映射，下面重写父图边
            n._sub_sources = sub_source_ids  # type: ignore[attr-defined]
            n._sub_sinks = sub_sink_ids  # type: ignore[attr-defined]

        # 重写父图边：原指向/出自 composite 的边改指向子图入口/出口
        composite_by_id = {n.id: n for n in dag.nodes if n.type == "composite"}
        for e in dag.edges:
            if e.to_node in composite_by_id and e.from_node in composite_by_id:
                # composite → composite：从源子图每个 sink 连到目标子图每个 source
                src_sinks = getattr(composite_by_id[e.from_node], "_sub_sinks", [])
                dst_sources = getattr(composite_by_id[e.to_node], "_sub_sources", [])
                for s in src_sinks:
                    for d in dst_sources:
                        new_edges.append(Edge(s, e.from_port, d, e.to_port, e.condition))
            elif e.to_node in composite_by_id:
                dst_sources = getattr(composite_by_id[e.to_node], "_sub_sources", [])
                for d in dst_sources:
                    new_edges.append(Edge(e.from_node, e.from_port, d, e.to_port, e.condition))
            elif e.from_node in composite_by_id:
                src_sinks = getattr(composite_by_id[e.from_node], "_sub_sinks", [])
                for s in src_sinks:
                    new_edges.append(Edge(s, e.from_port, e.to_node, e.to_port, e.condition))
            else:
                new_edges.append(e)

        # ID 冲突校验
        all_ids = [n.id for n in new_nodes]
        if len(set(all_ids)) != len(all_ids):
            raise DAGValidationError("composite expansion produced duplicate node ids")

        return DAG(name=dag.name, nodes=new_nodes, edges=new_edges, conditions=dag.conditions)
