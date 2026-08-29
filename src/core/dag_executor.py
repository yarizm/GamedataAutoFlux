"""DAG 执行引擎：校验、拓扑排序、并发调度、端口表、条件边、checkpoint。"""
from __future__ import annotations

import asyncio
import time
from collections import defaultdict, deque
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable

from loguru import logger

from src.core.dag import DAG, DAGResult, Edge, NodeSpec
from src.core.dag_conditions import CONDITION_PREDICATES, resolve_condition
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
from src.core.metrics import metrics
from src.core.sensitive import redact_sensitive_text
from src.core.task import Task


class DAGValidationError(Exception):
    pass


_VALID_NODE_TYPES = frozenset({"collector", "processor", "storage", "composite"})


@dataclass(frozen=True)
class DAGValidationIssue:
    """结构校验结果；severity=warning 不阻断保存/执行，仅提示。"""

    code: str
    message: str
    severity: str = "error"


def validate_dag_detailed(
    dag: DAG,
    *,
    subgraph_loader: Callable[[str], Any] | None = None,
) -> list[DAGValidationIssue]:
    """保存/执行前的完整结构校验。

    subgraph_loader 提供时校验 composite 子图可解析（如 Web 保存路径预载
    子图后的 dict 查询）；不提供则跳过该检查（执行期展开时仍会兜底报错）。
    """
    issues: list[DAGValidationIssue] = []

    if not dag.nodes:
        issues.append(DAGValidationIssue("empty_dag", "DAG has no nodes"))
        return issues

    # 节点：ID 唯一且非空、类型合法、composite 子图声明完整
    seen_ids: set[str] = set()
    for n in dag.nodes:
        if not n.id or not str(n.id).strip():
            issues.append(DAGValidationIssue("empty_node_id", "node id must be non-empty"))
            continue
        if n.id in seen_ids:
            issues.append(DAGValidationIssue(
                "duplicate_node_id", f"duplicate node id: {n.id}"
            ))
        seen_ids.add(n.id)
        if n.type not in _VALID_NODE_TYPES:
            issues.append(DAGValidationIssue(
                "invalid_node_type", f"node {n.id} has unknown type '{n.type}'"
            ))
        if n.type == "composite" and not (n.subgraph_name or "").strip():
            issues.append(DAGValidationIssue(
                "composite_missing_subgraph", f"composite node {n.id} lacks subgraph_name"
            ))

    node_by_id = {n.id: n for n in dag.nodes}
    ports_out_by_node = {
        n.id: {p.name for p in n.ports_out} for n in dag.nodes if n.ports_out
    }
    ports_in_by_node = {
        n.id: {p.name for p in n.ports_in} for n in dag.nodes if n.ports_in
    }
    type_hint_out = {
        (n.id, p.name): p.type_hint for n in dag.nodes for p in n.ports_out
    }
    type_hint_in = {
        (n.id, p.name): p.type_hint for n in dag.nodes for p in n.ports_in
    }

    # 边：端点存在、无自环、无重复边、端口存在（仅当节点声明了端口）、类型兼容
    seen_edges: set[tuple[str, str, str, str]] = set()
    for e in dag.edges:
        if e.from_node not in node_by_id or e.to_node not in node_by_id:
            issues.append(DAGValidationIssue(
                "unknown_edge_endpoint",
                f"edge references missing node: {e.from_node} -> {e.to_node}",
            ))
            continue
        key = (e.from_node, e.from_port, e.to_node, e.to_port)
        if key in seen_edges:
            issues.append(DAGValidationIssue(
                "duplicate_edge",
                f"duplicate edge: {e.from_node}.{e.from_port} -> {e.to_node}.{e.to_port}",
            ))
        seen_edges.add(key)
        if e.from_node == e.to_node:
            issues.append(DAGValidationIssue("self_loop", f"self loop on node {e.from_node}"))
        declared_out = ports_out_by_node.get(e.from_node)
        if declared_out is not None and e.from_port not in declared_out:
            issues.append(DAGValidationIssue(
                "missing_output_port",
                f"edge uses undeclared output port: {e.from_node}.{e.from_port}",
            ))
        declared_in = ports_in_by_node.get(e.to_node)
        if declared_in is not None and e.to_port not in declared_in:
            issues.append(DAGValidationIssue(
                "missing_input_port",
                f"edge uses undeclared input port: {e.to_node}.{e.to_port}",
            ))
        out_hint = type_hint_out.get((e.from_node, e.from_port), "")
        in_hint = type_hint_in.get((e.to_node, e.to_port), "")
        if out_hint and in_hint and out_hint != in_hint:
            issues.append(DAGValidationIssue(
                "port_type_mismatch",
                f"port type mismatch: {e.from_node}.{e.from_port}({out_hint}) "
                f"-> {e.to_node}.{e.to_port}({in_hint})",
            ))
        if e.condition is not None and e.condition not in CONDITION_PREDICATES:
            issues.append(DAGValidationIssue(
                "unknown_condition",
                f"edge {e.from_node} -> {e.to_node} has unknown condition '{e.condition}'",
            ))

    # 环检测（Kahn）
    indeg: dict[str, int] = {n.id: 0 for n in dag.nodes}
    adj: dict[str, list[str]] = defaultdict(list)
    for e in dag.edges:
        if e.from_node in indeg and e.to_node in indeg:
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
        issues.append(DAGValidationIssue("cycle_detected", "cycle detected in DAG"))

    # source 合理性：至少一个 collector 或 composite（子图内可含 collector）
    if not any(n.type in ("collector", "composite") for n in dag.nodes):
        issues.append(DAGValidationIssue(
            "no_source",
            "DAG must contain at least one collector or composite node",
        ))

    # composite 子图可解析（loader 提供时）
    if subgraph_loader is not None:
        for n in dag.nodes:
            if n.type != "composite" or not (n.subgraph_name or "").strip():
                continue
            try:
                resolved = subgraph_loader(n.subgraph_name)
            except Exception as exc:
                issues.append(DAGValidationIssue(
                    "composite_subgraph_unresolvable",
                    f"composite node {n.id}: subgraph '{n.subgraph_name}' failed to load: {exc}",
                ))
                continue
            if resolved is None:
                issues.append(DAGValidationIssue(
                    "composite_subgraph_unresolvable",
                    f"composite node {n.id}: subgraph '{n.subgraph_name}' not found",
                ))

    # 悬空必需输入端口；全部入边带条件 → 仅告警（故障转移等合法模式）
    incoming_by_node: dict[str, set[str]] = defaultdict(set)
    conditional_by_node: dict[str, set[str]] = defaultdict(set)
    for e in dag.edges:
        incoming_by_node[e.to_node].add(e.to_port)
        if e.condition is not None:
            conditional_by_node[e.to_node].add(e.to_port)
    for n in dag.nodes:
        for p in n.ports_in:
            if not p.required:
                continue
            providers = incoming_by_node.get(n.id, set())
            if p.name not in providers:
                issues.append(DAGValidationIssue(
                    "dangling_required_input",
                    f"dangling required input port: {n.id}.{p.name}",
                ))
            elif p.name in conditional_by_node.get(n.id, set()):
                issues.append(DAGValidationIssue(
                    "conditional_required_input",
                    f"required input {n.id}.{p.name} only fed by conditional edge(s)",
                    severity="warning",
                ))

    # 不可达节点（warning）：从有效源（collector/composite，或必需输入
    # 全为 param 端口的配置驱动节点）出发无法到达 —— 这些节点永远收不到数据
    sources = [n.id for n in dag.nodes if _is_effective_source(n, incoming_by_node)]
    reachable: set[str] = set()
    stack = list(sources)
    while stack:
        cur = stack.pop()
        if cur in reachable:
            continue
        reachable.add(cur)
        stack.extend(adj.get(cur, []))
    for n in dag.nodes:
        if n.id not in reachable:
            issues.append(DAGValidationIssue(
                "unreachable_node", f"node {n.id} is unreachable from any source",
                severity="warning",
            ))

    return issues


def _is_effective_source(node: NodeSpec, incoming_by_node: dict[str, set[str]]) -> bool:
    if node.id in incoming_by_node:
        return False
    if node.type in ("collector", "composite"):
        return True
    # 无入边的 processor/storage：必需端口全部是 param 端口（任务配置注入）
    # 才算配置驱动的有效源；否则永远收不到数据
    required = [p.name for p in node.ports_in if p.required]
    if not required:
        return True
    return all(name in node.is_param_port for name in required)


def validate_dag(dag: DAG) -> list[str]:
    """向后兼容视图：仅返回 error 级问题消息（warning 见 validate_dag_detailed）。"""
    return [i.message for i in validate_dag_detailed(dag) if i.severity == "error"]


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
        self._skipped_nodes: set[str] = set()
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
        self._skipped_nodes = set()

        collector_ids = {n.id for n in dag.nodes if n.type == "collector"}
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
                incoming = [e for e in dag.edges if e.to_node == node_id]
                active = [
                    e for e in incoming
                    if (e.from_node, e.to_node, e.to_port) not in self._suppressed_edges
                ]
                if incoming and not active:
                    # 入边全被条件抑制 → 该分支未激活，节点不执行，跳过继续向下传播
                    self._skip_node(dag, node_id)
                    metrics.inc("dag_node_total", type=node_spec.type, result="skipped")
                    return
                accum: dict[str, list[Any]] = defaultdict(list)
                for e in active:
                    upstream_out = self._port_table.get(e.from_node, {})
                    accum[e.to_port].append(upstream_out.get(e.from_port))
                inputs: dict[str, Any] = {}
                for port, vals in accum.items():
                    inputs[port] = vals[0] if len(vals) == 1 else vals
                ctx = NodeContext(
                    inputs=inputs, task=task, config=node_spec.config,
                    recovery_checkpoint=recovery_context, emit_event=on_event,
                )
                node = None
                node_started = time.perf_counter()
                node_result = "error"
                if semaphore is not None:
                    await semaphore.acquire()
                try:
                    # setup/instantiate 也归入节点错误处理，不逃逸到 gather
                    try:
                        await self._notify(on_event, task.id, node_spec, "start")
                        node = _instantiate_node(node_spec, task=task, recovery_checkpoint=recovery_context)
                        await node.setup()
                        if node_spec.type == "collector":
                            with metrics.timer("collector_duration_seconds"):
                                out = await node.run(ctx)
                        else:
                            out = await node.run(ctx)
                        self._port_table[node_id] = out
                        self._node_success[node_id] = True
                        node_result = "ok"
                        metrics.inc("dag_node_total", type=node_spec.type, result="ok")
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
                        metrics.inc("dag_node_total", type=node_spec.type, result="error")
                        result.errors.append(f"{node_id}: {safe}")
                        logger.error("DAG node {} failed: {}", node_id, safe)
                        await self._notify(on_event, task.id, node_spec, "error", error=safe)
                finally:
                    # node 为 None 说明实例化本身就失败了，此时没有可 teardown 的对象；
                    # 硬调只会抛 UnboundLocalError，把真正的初始化异常淹没在告警里
                    if node is not None:
                        try:
                            await node.teardown()
                        except Exception as te:
                            logger.warning("DAG node {} teardown error: {}", node_id, redact_sensitive_text(str(te)))
                    metrics.observe(
                        "dag_node_duration_seconds",
                        time.perf_counter() - node_started,
                        type=node_spec.type,
                        result=node_result,
                    )
                    if semaphore is not None:
                        semaphore.release()

            await asyncio.gather(*[_run_one(nid) for nid in runnable])
            self._evaluate_outgoing_conditions(dag, layer)
            # 抑制记录不清理：一条边的条件只在其 from_node 完成时求值一次，
            # 而 to_node 可能落在若干层之后，提前清掉等于条件边完全失效。

            # collector 全失败 → 早终止（复刻 Pipeline._has_successful_collects）。
            # 必须等所有 collector 节点都已执行或被跳过再判，否则条件分支里的
            # 兜底 collector 还没轮到就被砍掉，故障转移永远走不通。
            settled = set(self._node_success) | self._skipped_nodes
            if (
                collector_ids
                and collector_ids <= settled
                and not _has_successful_collects(result.collect_results)
            ):
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
        result.completed_at = datetime.now(timezone.utc)
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

    def _skip_node(self, dag: DAG, node_id: str) -> None:
        """标记节点未激活，并抑制其全部出边，让跳过沿 DAG 继续传播。

        不写 `_node_success`：跳过是正常控制流，不是失败，不该影响 result.success。
        """
        self._skipped_nodes.add(node_id)
        for e in dag.edges:
            if e.from_node == node_id:
                self._suppressed_edges.add((e.from_node, e.to_node, e.to_port))

    def _evaluate_outgoing_conditions(self, dag: DAG, layer: list[str]) -> None:
        """对带条件的边求值；条件为假的边标记为抑制（边级，不影响同端口其它边）。"""
        for node_id in layer:
            if node_id in self._skipped_nodes:
                continue  # 出边已在 _skip_node 里全部抑制
            ran_ok = self._node_success.get(node_id, False)
            out = self._port_table.get(node_id, {})
            for e in dag.edges:
                if e.from_node != node_id or e.condition is None:
                    continue
                pred = resolve_condition(e.condition)
                if pred is None:
                    continue
                ctx = NodeContext(inputs={}, task=Task(name=""), config={})
                activated = pred(out, ran_ok, ctx)
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
