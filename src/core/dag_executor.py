"""DAG 执行引擎：校验、就绪队列调度、端口表、条件边、节点策略、checkpoint。"""
from __future__ import annotations

import asyncio
import inspect
import time
from collections import defaultdict, deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable

from loguru import logger

from src.core.dag import DAG, DAGResult, Edge, NodeSpec
from src.core.dag_conditions import CONDITION_PREDICATES, resolve_condition
from src.core.dag_logic import (
    ELSE_PORT,
    LOGIC_CONDITION,
    LOGIC_NODE_TYPE,
    instantiate_logic_node,
    logic_output_port_names,
    validate_logic_config,
)
from src.core.dag_nodes import (
    CollectorNode,
    InputNode,
    NodeContext,
    ProcessorNode,
    StorageNode,
)
from src.core.dag_policy import (
    ERROR_PORT,
    ON_ERROR_BRANCH,
    ON_ERROR_CONTINUE,
    ON_ERROR_STOP,
    NodePolicy,
    parse_node_policy,
    uses_error_port,
    validate_node_policy,
)
from src.core.errors import ErrorCode, classify_exception

# 结构校验异常升级为类型化异常（携带 dag_validation 错误码）；
# 此处 re-export 保持既有导入路径兼容
from src.core.exceptions import DAGNodeTimeoutError, DAGValidationError
from src.core.metrics import metrics
from src.core.pipeline_recovery import (
    build_pipeline_recovery_context,
    build_pipeline_resume_state,
)
from src.core.sensitive import redact_sensitive_text
from src.core.task import Task

_VALID_NODE_TYPES = frozenset(
    {"collector", "processor", "storage", "composite", "input", LOGIC_NODE_TYPE}
)

EdgeKey = tuple[str, str, str, str]


@dataclass(frozen=True)
class DAGValidationIssue:
    """结构校验结果；severity=warning 不阻断保存/执行，仅提示。"""

    code: str
    message: str
    severity: str = "error"
    node_id: str | None = None


def _effective_out_ports(node: NodeSpec) -> set[str] | None:
    """节点可用的出端口；None 表示节点没声明端口（不做端口校验）。

    logic 节点的端口由配置推导；error_branch 策略额外开放 error 端口。
    """
    if node.type == LOGIC_NODE_TYPE:
        names = set(logic_output_port_names(node.component, node.config))
    elif node.ports_out:
        names = {p.name for p in node.ports_out}
    else:
        names = None
    if uses_error_port(node.policy):
        names = (names or set()) | {ERROR_PORT}
    return names


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

    # 节点：ID 唯一且非空、类型合法、composite 子图声明完整、策略与逻辑配置合法
    seen_ids: set[str] = set()
    for n in dag.nodes:
        if not n.id or not str(n.id).strip():
            issues.append(DAGValidationIssue("empty_node_id", "node id must be non-empty"))
            continue
        if n.id in seen_ids:
            issues.append(DAGValidationIssue(
                "duplicate_node_id", f"duplicate node id: {n.id}", node_id=n.id
            ))
        seen_ids.add(n.id)
        if n.type not in _VALID_NODE_TYPES:
            issues.append(DAGValidationIssue(
                "invalid_node_type", f"node {n.id} has unknown type '{n.type}'", node_id=n.id
            ))
        if n.type == "composite" and not (n.subgraph_name or "").strip():
            issues.append(DAGValidationIssue(
                "composite_missing_subgraph",
                f"composite node {n.id} lacks subgraph_name",
                node_id=n.id,
            ))
        for problem in validate_node_policy(n.policy):
            issues.append(DAGValidationIssue(
                "invalid_node_policy", f"node {n.id} policy: {problem}", node_id=n.id
            ))
        if n.type == LOGIC_NODE_TYPE:
            for problem in validate_logic_config(n.component, n.config):
                issues.append(DAGValidationIssue(
                    "invalid_logic_config", f"node {n.id}: {problem}", node_id=n.id
                ))

    node_by_id = {n.id: n for n in dag.nodes}
    ports_out_by_node = {
        n.id: ports for n in dag.nodes if (ports := _effective_out_ports(n)) is not None
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
    seen_edges: set[EdgeKey] = set()
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
            issues.append(DAGValidationIssue(
                "self_loop", f"self loop on node {e.from_node}", node_id=e.from_node
            ))
        declared_out = ports_out_by_node.get(e.from_node)
        if declared_out is not None and e.from_port not in declared_out:
            issues.append(DAGValidationIssue(
                "missing_output_port",
                f"edge uses undeclared output port: {e.from_node}.{e.from_port}",
                node_id=e.from_node,
            ))
        declared_in = ports_in_by_node.get(e.to_node)
        if declared_in is not None and e.to_port not in declared_in:
            issues.append(DAGValidationIssue(
                "missing_input_port",
                f"edge uses undeclared input port: {e.to_node}.{e.to_port}",
                node_id=e.to_node,
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
                    node_id=n.id,
                ))
                continue
            if resolved is None:
                issues.append(DAGValidationIssue(
                    "composite_subgraph_unresolvable",
                    f"composite node {n.id}: subgraph '{n.subgraph_name}' not found",
                    node_id=n.id,
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
                    node_id=n.id,
                ))
            elif p.name in conditional_by_node.get(n.id, set()):
                issues.append(DAGValidationIssue(
                    "conditional_required_input",
                    f"required input {n.id}.{p.name} only fed by conditional edge(s)",
                    severity="warning",
                    node_id=n.id,
                ))

    # error_branch 没接 error 边：失败会被静默吸收（warning）
    error_edges = {e.from_node for e in dag.edges if e.from_port == ERROR_PORT}
    for n in dag.nodes:
        if uses_error_port(n.policy) and n.id not in error_edges:
            issues.append(DAGValidationIssue(
                "error_branch_unconnected",
                f"node {n.id} uses on_error=error_branch but its error port is not connected",
                severity="warning",
                node_id=n.id,
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
                node_id=n.id,
            ))

    return issues


def _is_effective_source(node: NodeSpec, incoming_by_node: dict[str, set[str]]) -> bool:
    if node.id in incoming_by_node:
        return False
    if node.type in ("collector", "composite", "input"):
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
    """Kahn 分层（只用于展示与测试；执行走就绪队列，不按层同步）。"""
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
    if node_spec.type == "input":
        return InputNode(node_spec, task=task, recovery_checkpoint=recovery_checkpoint)
    if node_spec.type == "collector":
        return CollectorNode(node_spec, task=task, recovery_checkpoint=recovery_checkpoint)
    if node_spec.type == "processor":
        return ProcessorNode(node_spec, task=task, recovery_checkpoint=recovery_checkpoint)
    if node_spec.type == "storage":
        return StorageNode(node_spec, task=task, recovery_checkpoint=recovery_checkpoint)
    if node_spec.type == LOGIC_NODE_TYPE:
        return instantiate_logic_node(node_spec, task=task, recovery_checkpoint=recovery_checkpoint)
    raise ValueError(f"unknown node type: {node_spec.type}")


def _has_successful_collects(collect_results: list) -> bool:
    return any(r.success and r.data is not None for r in collect_results)


def _collect_failure_message(dag_name: str, recovery_context: dict[str, Any]) -> str:
    """采集阶段零结果的如实归因：区分"续跑跳过"与"采集全失败"。

    续跑按 checkpoint 的 next_target_index 跳过已完成 target；当它覆盖全部 target 时
    ``apply_collect_resume_context`` 返回空列表，采集节点会"成功"产出零结果。此时
    collector 并没有失败，沿用 "all collect targets failed" 会掩盖真实原因。
    """
    if isinstance(recovery_context, dict):
        collect_context = recovery_context.get("collect")
    else:
        collect_context = None
    if isinstance(collect_context, dict) and collect_context.get("enabled"):
        target_order = collect_context.get("target_order") or []
        try:
            next_index = int(collect_context.get("next_target_index") or 0)
        except (TypeError, ValueError):
            next_index = 0
        if target_order and next_index >= len(target_order):
            return f"{dag_name}: collect skipped by resume checkpoint; nothing to store"
    return f"{dag_name}: all collect targets failed"


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


def _summarize_outputs(node_spec: NodeSpec, out: dict[str, Any]) -> dict[str, int]:
    if node_spec.type == "storage":
        return {"stored": int(out.get("_stored", 0) or 0)}
    return {
        port: len(_flatten_records(value))
        for port, value in out.items()
        if not port.startswith("_")
    }


def _utc_iso(value: datetime | None) -> str | None:
    return value.isoformat() if value is not None else None


@dataclass
class NodeRun:
    """单个节点在一次运行中的执行记录（对标 Dify workflow_node_executions）。"""

    node_id: str
    node_type: str
    component: str
    status: str = "pending"  # pending|running|retrying|succeeded|failed|exception|skipped|not_run
    attempts: int = 0
    max_attempts: int = 1
    started_at: datetime | None = None
    finished_at: datetime | None = None
    input_count: int = 0
    output_counts: dict[str, int] = field(default_factory=dict)
    error: str | None = None
    error_code: str | None = None
    branch: str | None = None
    on_error: str | None = None

    @property
    def elapsed_ms(self) -> int | None:
        if self.started_at is None or self.finished_at is None:
            return None
        return int((self.finished_at - self.started_at).total_seconds() * 1000)

    def to_dict(self) -> dict[str, Any]:
        return {
            "node_id": self.node_id,
            "node_type": self.node_type,
            "component": self.component,
            "status": self.status,
            "attempts": self.attempts,
            "max_attempts": self.max_attempts,
            "started_at": _utc_iso(self.started_at),
            "finished_at": _utc_iso(self.finished_at),
            "elapsed_ms": self.elapsed_ms,
            "input_count": self.input_count,
            "output_counts": dict(self.output_counts),
            "error": self.error,
            "error_code": self.error_code,
            "branch": self.branch,
            "on_error": self.on_error,
        }


def _edge_key(edge: Edge) -> EdgeKey:
    return (edge.from_node, edge.from_port, edge.to_node, edge.to_port)


def _accepts_trace_kwarg(callback: Callable[..., Any]) -> bool:
    """旧回调签名只有 out/error；按签名决定是否传 trace，避免 TypeError 吞掉事件。"""
    try:
        params = inspect.signature(callback).parameters.values()
    except (TypeError, ValueError):
        return False
    return any(p.kind is inspect.Parameter.VAR_KEYWORD or p.name == "trace" for p in params)


class DAGExecutor:
    """就绪队列调度：节点的入边全部有结论（上游完成或被跳过）即启动。

    与按层同步（Kahn 分层 + gather）不同，慢分支不会拖住无关分支的下游。
    """

    def __init__(self, *, subgraph_loader: Callable[[str], DAG | None] | None = None) -> None:
        self._port_table: dict[str, dict[str, Any]] = {}
        # 执行过的节点：True=成功或失败已被策略吸收；False=失败。跳过的节点不写
        self._node_success: dict[str, bool] = {}
        # 边键 = 完整 4 元组；缺 from_port 会让同节点不同出端口的边互相误伤
        self._suppressed_edges: set[EdgeKey] = set()
        self._skipped_nodes: set[str] = set()
        # 节点声明的激活出端口；None/缺省 = 全部普通端口
        self._active_ports: dict[str, set[str] | None] = {}
        # 走了 error_branch 的节点
        self._error_branch_nodes: set[str] = set()
        self._runs: dict[str, NodeRun] = {}
        self._stop_requested: str | None = None
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
        problems = validate_dag(dag)
        if problems:
            raise DAGValidationError("; ".join(problems))
        result = DAGResult(pipeline_name=dag.name, task_id=task.id)

        # 用 pipeline 标准恢复上下文（含 collect 键），节点直接消费
        recovery_context = build_pipeline_recovery_context(task, recovery_checkpoint)

        # 每次执行重置实例状态，避免跨调用泄漏
        self._port_table = {}
        self._node_success = {}
        self._suppressed_edges = set()
        self._skipped_nodes = set()
        self._active_ports = {}
        self._error_branch_nodes = set()
        self._stop_requested = None
        self._runs = {
            n.id: NodeRun(
                node_id=n.id,
                node_type=n.type,
                component=n.component,
                max_attempts=parse_node_policy(n.policy).max_attempts,
                on_error=parse_node_policy(n.policy).on_error,
            )
            for n in dag.nodes
        }

        node_by_id = {n.id: n for n in dag.nodes}
        incoming: dict[str, list[Edge]] = defaultdict(list)
        outgoing: dict[str, list[Edge]] = defaultdict(list)
        for e in dag.edges:
            incoming[e.to_node].append(e)
            outgoing[e.from_node].append(e)
        unresolved = {nid: len(incoming[nid]) for nid in node_by_id}
        ready: deque[str] = deque(sorted(nid for nid, count in unresolved.items() if count == 0))
        running: dict[asyncio.Task[None], str] = {}

        collector_ids = {n.id for n in dag.nodes if n.type == "collector"}
        total_nodes = len(dag.nodes)
        settled = 0
        aborted = False

        def _settle(node_id: str) -> None:
            nonlocal settled
            settled += 1
            for e in outgoing[node_id]:
                unresolved[e.to_node] -= 1
                if unresolved[e.to_node] == 0:
                    ready.append(e.to_node)

        try:
            while ready or running:
                # 启动所有就绪节点；入边全被抑制的节点就地跳过并级联
                while ready:
                    node_id = ready.popleft()
                    node_spec = node_by_id[node_id]
                    if aborted:
                        self._runs[node_id].status = "not_run"
                        continue
                    inbound = incoming[node_id]
                    if inbound and all(_edge_key(e) in self._suppressed_edges for e in inbound):
                        # 入边全被条件抑制 → 该分支未激活，节点不执行，跳过继续向下传播
                        await self._skip_node(node_spec, task.id, on_event, outgoing[node_id])
                        _settle(node_id)
                        continue
                    job = asyncio.create_task(
                        self._run_node(
                            node_spec,
                            inbound,
                            task=task,
                            result=result,
                            recovery_context=recovery_context,
                            semaphore=semaphore,
                            on_event=on_event,
                        )
                    )
                    running[job] = node_id

                if not running:
                    break
                done, _ = await asyncio.wait(running, return_when=asyncio.FIRST_COMPLETED)
                for job in sorted(done, key=lambda item: running[item]):
                    node_id = running.pop(job)
                    job.result()  # _run_node 自己吞掉节点异常；这里只会冒出取消
                    self._resolve_outgoing_edges(node_by_id[node_id], outgoing[node_id])
                    _settle(node_id)

                if self._stop_requested and not aborted:
                    aborted = True
                    result.success = False
                    result.errors.append(self._stop_requested)

                # collector 全失败 → 早终止（复刻 Pipeline._has_successful_collects）。
                # 必须等所有 collector 节点都已执行或被跳过再判，否则条件分支里的
                # 兜底 collector 还没轮到就被砍掉，故障转移永远走不通。
                done_ids = set(self._node_success) | self._skipped_nodes
                if (
                    not aborted
                    and collector_ids
                    and collector_ids <= done_ids
                    and not _has_successful_collects(result.collect_results)
                ):
                    aborted = True
                    result.success = False
                    if not result.errors:
                        result.errors.append(_collect_failure_message(dag.name, recovery_context))

                if on_progress is not None and total_nodes:
                    progress = settled / total_nodes * 0.9
                    r = on_progress(task.id, progress, f"{settled}/{total_nodes} nodes done")
                    if asyncio.iscoroutine(r):
                        await r

        finally:
            # 异常或外部取消（如任务被取消）时连带取消仍在跑的节点；
            # create_task 起的子任务不会随父协程自动取消
            for job in running:
                job.cancel()
            if running:
                await asyncio.gather(*running, return_exceptions=True)

        for run in self._runs.values():
            if run.status == "pending":
                run.status = "not_run"
        failed = [nid for nid, ok in self._node_success.items() if not ok]
        if failed:
            result.success = False
        result.node_runs = [self._runs[n.id].to_dict() for n in dag.nodes]
        result.completed_at = datetime.now(timezone.utc)
        result.resume_state = build_pipeline_resume_state(
            task,
            recovery_context=recovery_context,
            collect_results=result.collect_results,
            output_records=result.output_records,
            run_succeeded=result.success,
        )
        return result

    def _gather_inputs(self, inbound: list[Edge]) -> dict[str, Any]:
        """按端口累加激活边的上游输出；同端口多入边聚合为 list。"""
        accum: dict[str, list[Any]] = defaultdict(list)
        for e in inbound:
            if _edge_key(e) in self._suppressed_edges:
                continue
            upstream_out = self._port_table.get(e.from_node, {})
            accum[e.to_port].append(upstream_out.get(e.from_port))
        return {port: vals[0] if len(vals) == 1 else vals for port, vals in accum.items()}

    async def _run_node(
        self,
        node_spec: NodeSpec,
        inbound: list[Edge],
        *,
        task: Task,
        result: DAGResult,
        recovery_context: dict[str, Any],
        semaphore: asyncio.Semaphore | None,
        on_event: Callable[..., Awaitable[None] | None] | None,
    ) -> None:
        node_id = node_spec.id
        run = self._runs[node_id]
        policy = parse_node_policy(node_spec.policy)
        inputs = self._gather_inputs(inbound)
        run.input_count = sum(len(_flatten_records(value)) for value in inputs.values())
        ctx = NodeContext(
            inputs=inputs, task=task, config=node_spec.config,
            recovery_checkpoint=recovery_context, emit_event=on_event,
        )
        node_started = time.perf_counter()
        last_error: BaseException | None = None
        out: dict[str, Any] | None = None
        active_ports: set[str] | None = None
        branch: str | None = None
        run.status = "running"
        run.started_at = datetime.now(timezone.utc)
        await self._notify(on_event, task.id, node_spec, "start", trace=run)
        for attempt in range(1, policy.max_attempts + 1):
            run.attempts = attempt
            # 并发名额按"单次尝试"占用：重试等待期间释放，不拖住别的节点
            if semaphore is not None:
                await semaphore.acquire()
            try:
                out, active_ports, branch = await self._attempt_node(
                    node_spec,
                    ctx,
                    task=task,
                    recovery_context=recovery_context,
                    timeout=policy.timeout_seconds,
                )
                last_error = None
                break
            except Exception as exc:  # noqa: BLE001 - 节点异常统一走策略处理
                last_error = exc
            finally:
                if semaphore is not None:
                    semaphore.release()
            if attempt < policy.max_attempts:
                safe = redact_sensitive_text(str(last_error))
                run.status = "retrying"
                run.error = safe
                logger.warning(
                    "DAG node {} attempt {}/{} failed, retrying: {}",
                    node_id, attempt, policy.max_attempts, safe,
                )
                await self._notify(on_event, task.id, node_spec, "retry", error=safe, trace=run)
                if policy.retry_interval_seconds:
                    await asyncio.sleep(policy.retry_interval_seconds)
                run.status = "running"

        if last_error is None and out is not None:
            self._record_success(node_spec, out, active_ports, branch, result)
            metrics.inc("dag_node_total", type=node_spec.type, result="ok")
            outcome = "ok"
            await self._notify(on_event, task.id, node_spec, "complete", out=out, trace=run)
        else:
            assert last_error is not None
            self._record_failure(node_spec, policy, last_error, result)
            metrics.inc("dag_node_total", type=node_spec.type, result="error")
            outcome = "error"
            await self._notify(on_event, task.id, node_spec, "error", error=run.error, trace=run)
        metrics.observe(
            "dag_node_duration_seconds",
            time.perf_counter() - node_started,
            type=node_spec.type,
            result=outcome,
        )

    async def _attempt_node(
        self,
        node_spec: NodeSpec,
        ctx: NodeContext,
        *,
        task: Task,
        recovery_context: dict[str, Any],
        timeout: float | None,
    ) -> tuple[dict[str, Any], set[str] | None, str | None]:
        """一次完整尝试：实例化 → setup → run → teardown；超时整体计时。"""
        node: Any = None

        async def _go() -> dict[str, Any]:
            nonlocal node
            # setup/instantiate 也归入节点错误处理，不逃逸到调度循环
            node = _instantiate_node(node_spec, task=task, recovery_checkpoint=recovery_context)
            await node.setup()
            if node_spec.type == "collector":
                with metrics.timer("collector_duration_seconds"):
                    return await node.run(ctx)
            return await node.run(ctx)

        try:
            if timeout:
                try:
                    out = await asyncio.wait_for(_go(), timeout)
                except asyncio.TimeoutError as exc:
                    raise DAGNodeTimeoutError(
                        f"node {node_spec.id} timed out after {timeout:g}s"
                    ) from exc
            else:
                out = await _go()
            active = getattr(node, "active_ports", None)
            branch = getattr(node, "selected_branch", None)
            return out, (set(active) if active is not None else None), branch
        finally:
            # node 为 None 说明实例化本身就失败了，此时没有可 teardown 的对象；
            # 硬调只会抛 UnboundLocalError，把真正的初始化异常淹没在告警里
            if node is not None:
                try:
                    await node.teardown()
                except Exception as te:
                    logger.warning(
                        "DAG node {} teardown error: {}",
                        node_spec.id,
                        redact_sensitive_text(str(te)),
                    )

    def _record_success(
        self,
        node_spec: NodeSpec,
        out: dict[str, Any],
        active_ports: set[str] | None,
        branch: str | None,
        result: DAGResult,
    ) -> None:
        node_id = node_spec.id
        run = self._runs[node_id]
        self._port_table[node_id] = out
        self._node_success[node_id] = True
        self._active_ports[node_id] = active_ports
        run.status = "succeeded"
        run.error = None
        run.branch = branch
        run.finished_at = datetime.now(timezone.utc)
        run.output_counts = _summarize_outputs(node_spec, out)
        if node_spec.type == "collector":
            result.collect_results.extend(out.get("records", []))
        elif node_spec.type == "processor":
            result.process_results.extend(out.get("records", []))
        elif node_spec.type == "storage":
            result.storage_count += out.get("_stored", 0)
            result.output_records.extend(out.get("output_records", []))

    def _record_failure(
        self,
        node_spec: NodeSpec,
        policy: NodePolicy,
        exc: BaseException,
        result: DAGResult,
    ) -> None:
        node_id = node_spec.id
        run = self._runs[node_id]
        safe = redact_sensitive_text(str(exc)) or exc.__class__.__name__
        typed_code = classify_exception(exc) if isinstance(exc, Exception) else ErrorCode.unknown
        run.error = safe
        run.error_code = typed_code.value
        run.finished_at = datetime.now(timezone.utc)

        if policy.handles_errors:
            # 失败被策略吸收：节点视为"带异常完成"，不计入任务失败
            run.status = "exception"
            self._node_success[node_id] = True
            if policy.on_error == ON_ERROR_BRANCH:
                self._error_branch_nodes.add(node_id)
                self._active_ports[node_id] = {ERROR_PORT}
                self._port_table[node_id] = {
                    ERROR_PORT: [{
                        "success": False,
                        "node_id": node_id,
                        "component": node_spec.component,
                        "error": safe,
                        "error_code": typed_code.value,
                        "attempts": run.attempts,
                    }]
                }
                run.branch = ERROR_PORT
            else:
                assert policy.on_error == ON_ERROR_CONTINUE
                out_ports = [p.name for p in node_spec.ports_out if p.name != ERROR_PORT]
                if node_spec.type == LOGIC_NODE_TYPE:
                    out_ports = logic_output_port_names(node_spec.component, node_spec.config)
                self._port_table[node_id] = {port: [] for port in out_ports}
                if node_spec.type == LOGIC_NODE_TYPE and node_spec.component == LOGIC_CONDITION:
                    self._active_ports[node_id] = {ELSE_PORT}
                    run.branch = ELSE_PORT
                else:
                    self._active_ports[node_id] = None
            run.output_counts = _summarize_outputs(node_spec, self._port_table[node_id])
            logger.warning(
                "DAG node {} failed, handled by on_error={}: {}", node_id, policy.on_error, safe
            )
            return

        run.status = "failed"
        self._node_success[node_id] = False
        result.errors.append(f"{node_id}: {safe}")
        # 类型化异常的错误码直达结果面（消息启发式仅兜底）
        if result.error_code is None and typed_code is not ErrorCode.unknown:
            result.error_code = typed_code.value
        logger.error("DAG node {} failed: {}", node_id, safe)
        if policy.on_error == ON_ERROR_STOP and self._stop_requested is None:
            self._stop_requested = f"run stopped after node {node_id} failed (on_error=stop)"

    def _resolve_outgoing_edges(self, node_spec: NodeSpec, outgoing: list[Edge]) -> None:
        """节点结束后给每条出边下结论：激活或抑制（边级，不影响同端口其它边）。"""
        node_id = node_spec.id
        ran_ok = self._node_success.get(node_id, False)
        failed = self._node_success.get(node_id) is False
        active_ports = self._active_ports.get(node_id)
        took_error_branch = node_id in self._error_branch_nodes
        out = self._port_table.get(node_id, {})
        for e in outgoing:
            key = _edge_key(e)
            if e.from_port == ERROR_PORT and not took_error_branch:
                self._suppressed_edges.add(key)
                continue
            if active_ports is not None and e.from_port not in active_ports:
                self._suppressed_edges.add(key)
                continue
            if e.condition is None:
                if failed:
                    # 失败节点不再把空数据推给下游；故障转移用 on_failure 条件边
                    self._suppressed_edges.add(key)
                continue
            pred = resolve_condition(e.condition)
            if pred is None:
                continue
            # 谓词看的是这条边出端口上的数据，而不是节点固定的 records 端口
            port_view = {"records": out[e.from_port]} if e.from_port in out else {}
            ctx = NodeContext(inputs={}, task=Task(name=""), config={})
            if not pred(port_view, ran_ok, ctx):
                self._suppressed_edges.add(key)

    async def _skip_node(
        self,
        node_spec: NodeSpec,
        task_id: str,
        on_event: Callable[..., Awaitable[None] | None] | None,
        outgoing: list[Edge],
    ) -> None:
        """标记节点未激活，并抑制其全部出边，让跳过沿 DAG 继续传播。

        不写 `_node_success`：跳过是正常控制流，不是失败，不该影响 result.success。
        """
        node_id = node_spec.id
        self._skipped_nodes.add(node_id)
        for e in outgoing:
            self._suppressed_edges.add(_edge_key(e))
        run = self._runs[node_id]
        run.status = "skipped"
        metrics.inc("dag_node_total", type=node_spec.type, result="skipped")
        await self._notify(on_event, task_id, node_spec, "skipped", trace=run)

    async def _notify(
        self,
        on_event: Callable[..., Awaitable[None] | None] | None,
        task_id: str,
        node_spec: NodeSpec,
        phase: str,
        *,
        out: dict[str, Any] | None = None,
        error: str | None = None,
        trace: NodeRun | None = None,
    ) -> None:
        """向调用方透传节点生命周期事件。

        phase: start / retry / complete / error / skipped；``trace`` 是该节点当前的
        运行记录快照（status 区分 failed 与被策略吸收的 exception）。调用方负责
        翻译成具体事件契约（如 pipeline 的 collect/process/storage 事件）。
        """
        if on_event is None:
            return
        if node_spec.type == "input":
            # input 节点只声明参数入口，不是执行阶段；透传会让任务事件流里
            # 多出一个假的 input 阶段（前端会当成一个采集阶段显示）
            return
        try:
            kwargs: dict[str, Any] = {"out": out, "error": error}
            if trace is not None and _accepts_trace_kwarg(on_event):
                kwargs["trace"] = trace.to_dict()
            r = on_event(task_id, node_spec, phase, **kwargs)
            if asyncio.iscoroutine(r):
                await r
        except Exception as exc:
            logger.warning("DAG node event emit failed: {}", redact_sensitive_text(str(exc)))

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
                    policy=dict(sn.policy or {}),
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
