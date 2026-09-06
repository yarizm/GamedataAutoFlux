# tests/test_dag_conditional_edges.py
"""条件边语义：抑制的生命周期、分支跳过、失败分支、多条件互斥。

这些用例针对的是 `test_dag_scenarios.py` / `test_dag_executor.py` 里两个
"故障转移"测试断言过弱、抑制彻底失效也照样通过的盲区。核心断言都是
"被抑制的那一支**没有**留下痕迹"，而不是"另一支有痕迹"。
"""
from __future__ import annotations

import pytest

from src.collectors.base import BaseCollector, CollectResult, CollectTarget
from src.core.dag import DAG, Edge, NodeSpec, PortSpec
from src.core.dag_executor import DAGExecutor
from src.core.registry import registry
from src.core.task import Task, TaskTarget
from src.processors.base import BaseProcessor, ProcessInput, ProcessOutput
from src.storage.base import BaseStorage, QueryResult

# 节点实际执行痕迹（processor 只有真正被调度才会写入）
EXECUTED: list[str] = []


class _OkCollector(BaseCollector):
    """每个 target 都成功，data.src 取自 config 以便追踪来源。"""

    async def collect(self, target: CollectTarget) -> CollectResult:
        return CollectResult(
            target=target,
            data={"src": self.config.get("src", "ok"), "name": target.name},
            success=True,
        )


class _FailCollector(BaseCollector):
    """每个 target 都失败：不抛异常，只返回 success=False。"""

    async def collect(self, target: CollectTarget) -> CollectResult:
        return CollectResult(target=target, success=False, error="boom", data=None)


class _SpyProcessor(BaseProcessor):
    async def setup(self) -> None:
        EXECUTED.append(self.config.get("tag", "proc"))

    async def process(self, input_data: ProcessInput) -> ProcessOutput:
        return ProcessOutput(
            success=True,
            data=input_data.data,
            metadata=input_data.metadata,
            processor_name=self.config.get("tag", "proc"),
        )


class _MemStorage(BaseStorage):
    saved: list = []

    async def save(self, record):
        _MemStorage.saved.append(record)

    async def save_batch(self, records):
        _MemStorage.saved.extend(records)

    async def load(self, key):
        return None

    async def query(self, query, limit=10, **kwargs):
        return QueryResult(records=[], total=0, query=query)


@pytest.fixture
def components():
    snap = registry.snapshot()
    EXECUTED.clear()
    _MemStorage.saved = []
    registry.register("collector", "_ce_ok")(_OkCollector)
    registry.register("collector", "_ce_fail")(_FailCollector)
    registry.register("processor", "_ce_spy")(_SpyProcessor)
    registry.register("storage", "_ce_mem")(_MemStorage)
    try:
        yield
    finally:
        registry.restore(snap)


def _collector(node_id, src, *, optional_input=False):
    ports_in = [PortSpec("records", required=False)] if optional_input else []
    return NodeSpec(
        node_id, "collector", "_ce_ok", {"src": src},
        ports_in, [PortSpec("records")], set(),
    )


def _failing_collector(node_id):
    return NodeSpec(
        node_id, "collector", "_ce_fail", {},
        [], [PortSpec("records")], set(),
    )


def _processor(node_id, tag=None):
    return NodeSpec(
        node_id, "processor", "_ce_spy", {"tag": tag or node_id},
        [PortSpec("records")], [PortSpec("records")], set(),
    )


def _storage(node_id):
    return NodeSpec(node_id, "storage", "_ce_mem", {}, [PortSpec("records")], [], set())


def _task():
    return Task(name="t", targets=[TaskTarget(name="gameA")])


def _sources(result) -> set[str]:
    return {
        r.data.get("src")
        for r in result.process_results
        if isinstance(r.data, dict)
    }


@pytest.mark.asyncio
async def test_suppressed_edge_records_do_not_reach_downstream(components):
    """on_failure 边在上游成功时必须抑制，下游拿不到它的记录。"""
    dag = DAG(
        name="suppress_adjacent",
        nodes=[_collector("early", "early"), _collector("seed", "seed"), _processor("proc")],
        edges=[
            Edge("early", "records", "proc", "records", condition="on_failure"),
            Edge("seed", "records", "proc", "records"),
        ],
    )
    result = await DAGExecutor().execute(_task(), dag)

    assert _sources(result) == {"seed"}


@pytest.mark.asyncio
async def test_suppression_survives_across_intermediate_layers(components):
    """抑制不能只活一层：layer0 → layer2 的条件边同样要生效。"""
    dag = DAG(
        name="suppress_deep",
        nodes=[
            _collector("early", "early"),
            _collector("seed", "seed"),
            _collector("relay", "relay", optional_input=True),
            _processor("merge"),
        ],
        edges=[
            # early 在 layer0，merge 在 layer2，跨越了中间的 relay 层
            Edge("early", "records", "merge", "records", condition="on_failure"),
            Edge("seed", "records", "relay", "records"),
            Edge("relay", "records", "merge", "records"),
        ],
    )
    result = await DAGExecutor().execute(_task(), dag)

    assert _sources(result) == {"relay"}


@pytest.mark.asyncio
async def test_node_with_all_inputs_suppressed_is_not_executed(components):
    """入边全被抑制的节点不应被调度（而不是拿空输入空跑一趟）。"""
    dag = DAG(
        name="skip_branch",
        nodes=[_collector("ok", "ok"), _processor("rescue")],
        edges=[Edge("ok", "records", "rescue", "records", condition="on_failure")],
    )
    result = await DAGExecutor().execute(_task(), dag)

    assert "rescue" not in EXECUTED


@pytest.mark.asyncio
async def test_skipped_branch_does_not_mark_dag_failed(components):
    """跳过的条件分支是正常控制流，不是失败。

    这条是护栏而非 bug 复现：修复前后都该绿。它锁住的是"跳过节点不能被
    记成失败节点"，防止实现跳过逻辑时顺手把它塞进 _node_success。
    """
    dag = DAG(
        name="skip_ok",
        nodes=[_collector("ok", "ok"), _processor("rescue")],
        edges=[Edge("ok", "records", "rescue", "records", condition="on_failure")],
    )
    result = await DAGExecutor().execute(_task(), dag)

    assert result.success is True
    assert result.errors == []


@pytest.mark.asyncio
async def test_skip_cascades_to_downstream_of_skipped_node(components):
    """被跳过节点的出边同样要抑制，否则下游会当它"没有输出"照跑。"""
    dag = DAG(
        name="skip_cascade",
        nodes=[_collector("ok", "ok"), _processor("rescue"), _processor("after")],
        edges=[
            Edge("ok", "records", "rescue", "records", condition="on_failure"),
            Edge("rescue", "records", "after", "records"),
        ],
    )
    result = await DAGExecutor().execute(_task(), dag)

    assert "rescue" not in EXECUTED
    assert "after" not in EXECUTED


@pytest.mark.asyncio
async def test_failure_branch_runs_when_primary_collector_yields_nothing(components):
    """primary 全 target 失败 → on_failure 兜底分支必须真的跑起来。"""
    dag = DAG(
        name="failover",
        nodes=[
            _failing_collector("primary"),
            _collector("backup", "backup", optional_input=True),
            _processor("proc"),
        ],
        edges=[
            Edge("primary", "records", "backup", "records", condition="on_failure"),
            Edge("backup", "records", "proc", "records"),
        ],
    )
    result = await DAGExecutor().execute(_task(), dag)

    assert _sources(result) == {"backup"}


@pytest.mark.asyncio
async def test_on_success_edge_is_suppressed_when_collector_yields_nothing(components):
    """collector 不抛异常但所有 target 都失败时，on_success 不成立。

    断言下游"没被调度"而不是"没拿到数据"：ProcessorNode 本来就会丢弃
    success=False 的记录，只看数据的话这个语义 bug 是观测不到的。
    `seed` 存在是为了避开 collector 层全失败的早终止。
    """
    dag = DAG(
        name="empty_success",
        nodes=[
            _failing_collector("primary"),
            _collector("seed", "seed"),
            _processor("proc"),
            _processor("other"),
        ],
        edges=[
            Edge("primary", "records", "proc", "records", condition="on_success"),
            Edge("seed", "records", "other", "records"),
        ],
    )
    result = await DAGExecutor().execute(_task(), dag)

    assert "proc" not in EXECUTED
    assert "other" in EXECUTED


@pytest.mark.asyncio
async def test_mutually_exclusive_conditions_activate_exactly_one_branch(components):
    """同一节点的 on_nonempty / on_empty 两条出边只能激活一条。"""
    dag = DAG(
        name="two_conditions",
        nodes=[_collector("probe", "probe"), _processor("hot"), _processor("cold")],
        edges=[
            Edge("probe", "records", "hot", "records", condition="on_nonempty"),
            Edge("probe", "records", "cold", "records", condition="on_empty"),
        ],
    )
    result = await DAGExecutor().execute(_task(), dag)

    assert "hot" in EXECUTED
    assert "cold" not in EXECUTED


@pytest.mark.asyncio
async def test_suppression_scopes_to_full_edge_including_from_port(components):
    """A.records2 的无条件边不得被 A.records 条件边的抑制误伤。

    回归背景：suppression 曾以 (from, to, to_port) 三元组为键，同一节点
    经不同出端口汇入同一目标端口的两条合法边会互相误伤。
    """
    from src.core.dag import DAG, Edge, NodeSpec, PortSpec
    from src.core.dag_executor import DAGExecutor
    from src.core.task import Task, TaskTarget

    a = NodeSpec(
        "a", "collector", "_ce_ok", {}, [],
        [PortSpec("records"), PortSpec("records2")], set(),
    )
    b = NodeSpec(
        "b", "processor", "_ce_spy", {"tag": "via_port2"},
        [PortSpec("records")], [PortSpec("records")], set(),
    )
    dag = DAG(
        name="edge_key_scope",
        nodes=[a, b],
        edges=[
            Edge("a", "records", "b", "records", condition="on_empty"),
            Edge("a", "records2", "b", "records"),  # 无条件，必须存活
        ],
    )
    await DAGExecutor().execute(Task(name="t", targets=[TaskTarget(name="g")]), dag)

    assert EXECUTED == ["via_port2"]  # b 经 records2 边执行了
