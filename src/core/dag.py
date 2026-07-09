# src/core/dag.py
"""DAG 数据结构：节点、边、端口、条件、结果。"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from src.collectors.base import CollectResult
from src.processors.base import ProcessOutput
from src.storage.base import StorageRecord


@dataclass
class PortSpec:
    name: str
    required: bool = True
    type_hint: str = ""


@dataclass
class NodeSpec:
    id: str
    type: str  # collector | processor | storage | composite
    component: str
    config: dict[str, Any] = field(default_factory=dict)
    ports_in: list[PortSpec] = field(default_factory=list)
    ports_out: list[PortSpec] = field(default_factory=list)
    is_param_port: set[str] = field(default_factory=set)
    # 复合节点专属：内部子图名
    subgraph_name: str | None = None
    # 前端布局元数据（x/y/label 等）；执行路径忽略
    ui: dict[str, Any] = field(default_factory=dict)


@dataclass
class Edge:
    from_node: str
    from_port: str
    to_node: str
    to_port: str
    condition: str | None = None  # 预置谓词名


@dataclass
class Condition:
    name: str
    predicate: Any  # Callable[[NodeRunResult, NodeContext], bool]，运行时注入


@dataclass
class DAG:
    name: str
    nodes: list[NodeSpec]
    edges: list[Edge]
    conditions: dict[str, Condition] = field(default_factory=dict)
    # 前端图级 UI 元数据（zoom/pan 等）；执行路径忽略
    ui: dict[str, Any] = field(default_factory=dict)

    def node_by_id(self, node_id: str) -> NodeSpec | None:
        return next((n for n in self.nodes if n.id == node_id), None)

    def to_storage(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "kind": "dag",
            "nodes": [
                {
                    "id": n.id,
                    "type": n.type,
                    "component": n.component,
                    "config": n.config,
                    "ports_in": [{"name": p.name, "required": p.required, "type_hint": p.type_hint} for p in n.ports_in],
                    "ports_out": [{"name": p.name, "required": p.required, "type_hint": p.type_hint} for p in n.ports_out],
                    "is_param_port": sorted(n.is_param_port),
                    "subgraph_name": n.subgraph_name,
                    "ui": dict(n.ui or {}),
                }
                for n in self.nodes
            ],
            "edges": [
                {
                    "from": e.from_node,
                    "out": e.from_port,
                    "to": e.to_node,
                    "in": e.to_port,
                    "condition": e.condition,
                }
                for e in self.edges
            ],
            "conditions": list(self.conditions.keys()),
            "ui": dict(self.ui or {}),
        }

    @classmethod
    def from_storage(cls, payload: dict[str, Any]) -> "DAG":
        from src.storage.factory import normalize_storage_name

        nodes = []
        for n in payload.get("nodes", []):
            component = n.get("component", "")
            if n.get("type") == "storage":
                component = normalize_storage_name(component)
            nodes.append(
                NodeSpec(
                    id=n["id"],
                    type=n["type"],
                    component=component,
                    config=n.get("config", {}),
                    ports_in=[PortSpec(**p) for p in n.get("ports_in", [])],
                    ports_out=[PortSpec(**p) for p in n.get("ports_out", [])],
                    is_param_port=set(n.get("is_param_port", [])),
                    subgraph_name=n.get("subgraph_name"),
                    ui=dict(n.get("ui") or {}),
                )
            )
        edges = [
            Edge(
                from_node=e["from"],
                from_port=e["out"],
                to_node=e["to"],
                to_port=e["in"],
                condition=e.get("condition"),
            )
            for e in payload.get("edges", [])
        ]
        return cls(
            name=payload["name"],
            nodes=nodes,
            edges=edges,
            conditions={name: Condition(name, None) for name in payload.get("conditions", [])},
            ui=dict(payload.get("ui") or {}),
        )


@dataclass
class DAGResult:
    pipeline_name: str
    task_id: str
    success: bool = True
    collect_results: list[CollectResult] = field(default_factory=list)
    process_results: list[ProcessOutput] = field(default_factory=list)
    output_records: list[StorageRecord] = field(default_factory=list)
    storage_count: int = 0
    resume_state: dict[str, Any] = field(default_factory=dict)
    errors: list[str] = field(default_factory=list)
    generated_report_id: str | None = None
    generated_report_title: str | None = None
    generated_report_matched_records: int = 0
    started_at: datetime = field(default_factory=datetime.now)
    completed_at: datetime | None = None

    @property
    def duration_seconds(self) -> float | None:
        if self.completed_at is None:
            return None
        return (self.completed_at - self.started_at).total_seconds()

    @property
    def collection_summary(self) -> dict[str, Any]:
        from src.core.pipeline import _build_collection_summary

        return _build_collection_summary(self.collect_results)


def pipeline_to_dag(pipeline: Any) -> DAG:
    """把三段式 Pipeline 转成等价 DAG：collectors 并行汇合到 processor 链，链尾连 storage。"""
    from src.core.pipeline import StepType

    collectors = [s for s in pipeline.steps if s.step_type == StepType.COLLECTOR]
    processors = [s for s in pipeline.steps if s.step_type == StepType.PROCESSOR]
    storages = [s for s in pipeline.steps if s.step_type == StepType.STORAGE]

    nodes: list[NodeSpec] = []
    edges: list[Edge] = []
    collector_ids = []
    for i, s in enumerate(collectors):
        nid = f"collect_{i}_{s.component_name}"
        nodes.append(NodeSpec(nid, "collector", s.component_name, s.config, [], [PortSpec("records")], set()))
        collector_ids.append(nid)
    prev_ids = collector_ids
    for i, s in enumerate(processors):
        nid = f"process_{i}_{s.component_name}"
        nodes.append(NodeSpec(nid, "processor", s.component_name, s.config, [PortSpec("records")], [PortSpec("records")], set()))
        for src_id in prev_ids:
            edges.append(Edge(src_id, "records", nid, "records"))
        prev_ids = [nid]
    sink_inputs = collector_ids if not processors else prev_ids
    from src.storage.factory import normalize_storage_name

    for i, s in enumerate(storages):
        storage_name = normalize_storage_name(s.component_name)
        nid = f"storage_{i}_{storage_name}"
        nodes.append(NodeSpec(nid, "storage", storage_name, s.config, [PortSpec("records")], [], set()))
        for src_id in sink_inputs:
            edges.append(Edge(src_id, "records", nid, "records"))
    return DAG(name=pipeline.name, nodes=nodes, edges=edges)


def dag_to_pipeline(dag: DAG) -> Any:
    """把 DAG 投影为任务系统可用的 Pipeline（按 collector→processor→storage 顺序）。

    用于任务选择器/precheck/调度注册；真正执行应优先加载 state_type=graph 真图。
    条件边、composite 等拓扑细节不在投影中保留。
    """
    from src.core.pipeline import Pipeline
    from src.storage.factory import normalize_storage_name

    pipeline = Pipeline(dag.name)
    order = {"collector": 0, "processor": 1, "storage": 2}
    nodes = sorted(
        [n for n in dag.nodes if n.type in order],
        key=lambda n: (order[n.type], n.id),
    )
    for n in nodes:
        config = dict(n.config or {})
        if n.type == "collector":
            pipeline.add_collector(n.component, config)
        elif n.type == "processor":
            pipeline.add_processor(n.component, config)
        elif n.type == "storage":
            pipeline.add_storage(normalize_storage_name(n.component), config)
    return pipeline
