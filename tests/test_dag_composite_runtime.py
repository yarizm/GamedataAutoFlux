"""composite 节点的真实执行路径：存储的图经 Pipeline / Worker 下发都要能展开子图。

回归：Pipeline 和 Worker 都用 `DAGExecutor()`（没有 subgraph_loader），保存时校验通过的
composite 图一运行就报 "no subgraph_loader configured"，可复用子图功能实际不可用。
"""

from __future__ import annotations

import pytest

from src.collectors.base import BaseCollector, CollectResult, CollectTarget
from src.core.dag import DAG, Edge, NodeSpec, PortSpec
from src.core.dag_executor import (
    expand_composite_graph,
    expand_stored_composites,
    validate_dag_detailed,
)
from src.core.exceptions import DAGValidationError
from src.core.pipeline import Pipeline
from src.core.registry import registry
from src.core.task import Task, TaskTarget
from src.processors.base import BaseProcessor, ProcessInput, ProcessOutput
from src.services.sqlalchemy_dag_repository import SQLAlchemyDAGRepository


class _SubCollector(BaseCollector):
    async def collect(self, target: CollectTarget) -> CollectResult:
        return CollectResult(target=target, data={"name": target.name}, success=True)


class _SubProcessor(BaseProcessor):
    async def process(self, input_data: ProcessInput) -> ProcessOutput:
        return ProcessOutput(
            success=True,
            data=input_data.data,
            metadata=input_data.metadata,
            processor_name="sub_proc",
        )


@pytest.fixture
def components(isolated_db_config):
    snapshot = registry.snapshot()
    registry.register("collector", "_comp_src")(_SubCollector)
    registry.register("processor", "_comp_proc")(_SubProcessor)
    try:
        yield
    finally:
        registry.restore(snapshot)


def _collector(node_id: str = "src") -> NodeSpec:
    return NodeSpec(node_id, "collector", "_comp_src", {}, [], [PortSpec("records")], set())


def _processor(node_id: str) -> NodeSpec:
    return NodeSpec(
        node_id,
        "processor",
        "_comp_proc",
        {},
        [PortSpec("records")],
        [PortSpec("records")],
        set(),
    )


def _composite(node_id: str, subgraph: str) -> NodeSpec:
    return NodeSpec(
        node_id, "composite", "", {}, [], [PortSpec("records")], set(), subgraph_name=subgraph
    )


def _sub_graph(name: str = "sub") -> DAG:
    return DAG(
        name=name,
        nodes=[_collector(), _processor("proc")],
        edges=[Edge("src", "records", "proc", "records")],
    )


def _parent_graph(name: str = "parent", subgraph: str = "sub") -> DAG:
    return DAG(
        name=name,
        nodes=[_composite("comp", subgraph), _processor("after")],
        edges=[Edge("comp", "records", "after", "records")],
    )


def _task() -> Task:
    return Task(name="t", targets=[TaskTarget(name="gameA")])


async def _repo() -> SQLAlchemyDAGRepository:
    from src.storage.session_factory import init_shared_session_factory

    return SQLAlchemyDAGRepository(await init_shared_session_factory())


async def _close() -> None:
    from src.storage.session_factory import close_shared_session_factory

    await close_shared_session_factory()


async def test_stored_composite_graph_runs_through_pipeline(components) -> None:
    repo = await _repo()
    try:
        await repo.save(_sub_graph())
        await repo.save(_parent_graph())

        result = await Pipeline("parent").execute(_task())
    finally:
        await _close()

    assert result.success, result.errors
    assert len(result.collect_results) == 1
    # 子图处理器 + 父图处理器
    assert len(result.process_results) == 2


async def test_nested_composite_graph_runs_through_pipeline(components) -> None:
    repo = await _repo()
    try:
        await repo.save(_sub_graph("leaf"))
        await repo.save(
            DAG(
                name="middle",
                nodes=[_composite("inner", "leaf"), _processor("mid")],
                edges=[Edge("inner", "records", "mid", "records")],
            )
        )
        await repo.save(_parent_graph(subgraph="middle"))

        result = await Pipeline("parent").execute(_task())
    finally:
        await _close()

    assert result.success, result.errors
    assert len(result.process_results) == 3


async def test_missing_subgraph_makes_task_fail_visibly(components) -> None:
    repo = await _repo()
    try:
        await repo.save(_parent_graph(subgraph="deleted"))

        with pytest.raises(DAGValidationError, match="deleted"):
            await Pipeline("parent").execute(_task())
    finally:
        await _close()


def test_subgraph_cycle_is_rejected() -> None:
    graphs = {
        "a": DAG(name="a", nodes=[_composite("to_b", "b")], edges=[]),
        "b": DAG(name="b", nodes=[_composite("to_a", "a")], edges=[]),
    }

    with pytest.raises(DAGValidationError, match="a -> b -> a"):
        expand_composite_graph(graphs["a"], graphs.get)


def test_composite_can_be_used_twice_with_unique_node_ids() -> None:
    parent = DAG(
        name="twice",
        nodes=[_composite("x", "sub"), _composite("y", "sub"), _processor("join")],
        edges=[
            Edge("x", "records", "join", "records"),
            Edge("y", "records", "join", "records"),
        ],
    )

    expanded = expand_composite_graph(parent, {"sub": _sub_graph()}.get)

    assert {n.id for n in expanded.nodes} == {"x/src", "x/proc", "y/src", "y/proc", "join"}
    assert {(e.from_node, e.to_node) for e in expanded.edges} == {
        ("x/src", "x/proc"),
        ("y/src", "y/proc"),
        ("x/proc", "join"),
        ("y/proc", "join"),
    }


def test_composite_to_composite_edge_connects_sink_to_source() -> None:
    parent = DAG(
        name="chain",
        nodes=[
            _composite("first", "sub"),
            NodeSpec(
                "second",
                "composite",
                "",
                {},
                [PortSpec("records")],
                [PortSpec("records")],
                set(),
                subgraph_name="downstream",
            ),
        ],
        edges=[Edge("first", "records", "second", "records")],
    )
    downstream = DAG(name="downstream", nodes=[_processor("p")], edges=[])

    expanded = expand_composite_graph(parent, {"sub": _sub_graph(), "downstream": downstream}.get)

    assert ("first/proc", "second/p") in {(e.from_node, e.to_node) for e in expanded.edges}


@pytest.mark.parametrize(
    ("graphs", "message"),
    [
        (
            {"sub": DAG(name="sub", nodes=[_composite("nested", "gone")], edges=[])},
            "gone",
        ),
        (
            {"sub": DAG(name="sub", nodes=[_composite("back", "parent")], edges=[])},
            "parent -> sub -> parent",
        ),
    ],
)
def test_save_validation_reports_nested_and_cyclic_subgraphs(graphs, message) -> None:
    issues = validate_dag_detailed(_parent_graph(), subgraph_loader=graphs.get)

    errors = [i for i in issues if i.severity == "error"]
    assert [i.code for i in errors] == ["composite_expansion_invalid"]
    assert message in errors[0].message


def test_save_validation_reports_wiring_that_only_breaks_after_expansion() -> None:
    # 子图的出口节点没有 records 出端口，父图接它的 records 只有展开后才暴露问题
    sub = DAG(
        name="sub",
        nodes=[NodeSpec("src", "collector", "_comp_src", {}, [], [PortSpec("other")], set())],
        edges=[],
    )

    issues = validate_dag_detailed(_parent_graph(), subgraph_loader={"sub": sub}.get)

    assert any(
        i.code == "composite_expansion_invalid" and "comp/src.records" in i.message for i in issues
    )


async def test_expand_stored_composites_leaves_plain_graph_untouched() -> None:
    async def load(name: str) -> DAG | None:
        raise AssertionError("plain graph must not hit storage")

    graph = _sub_graph()

    assert await expand_stored_composites(graph, load) is graph
