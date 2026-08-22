# tests/test_dag_node_lifecycle.py
"""DAG 节点生命周期：实例化失败时不得把真实异常淹没在 teardown 告警里。"""
from __future__ import annotations

import pytest
from loguru import logger

from src.collectors.base import BaseCollector, CollectResult, CollectTarget
from src.core.dag import DAG, NodeSpec, PortSpec
from src.core.dag_executor import DAGExecutor, DAGValidationError
from src.core.registry import registry
from src.core.task import Task, TaskTarget

LIFECYCLE: list[str] = []


class _SetupFailsCollector(BaseCollector):
    """实例化成功但 setup 抛异常——此时节点对象存在，teardown 必须照常调用。"""

    async def setup(self, config=None) -> None:
        raise RuntimeError("setup exploded")

    async def teardown(self) -> None:
        LIFECYCLE.append("teardown")

    async def collect(self, target: CollectTarget) -> CollectResult:
        return CollectResult(target=target, success=True, data={"ok": True})


@pytest.fixture
def components():
    snap = registry.snapshot()
    LIFECYCLE.clear()
    registry.register("collector", "_lc_setup_fails")(_SetupFailsCollector)
    try:
        yield
    finally:
        registry.restore(snap)


def _task():
    return Task(name="t", targets=[TaskTarget(name="gameA")])


@pytest.mark.asyncio
async def test_node_instantiation_failure_rejected_before_execution():
    """未知节点类型在执行前即被结构校验拒绝，不再进入节点运行（也就不会
    产生误导性的 "teardown error" 告警——实例化失败本就没有可 teardown 的对象）。
    """
    warnings: list[str] = []
    sink_id = logger.add(lambda m: warnings.append(str(m)), level="WARNING")
    try:
        dag = DAG(
            name="bad_node",
            nodes=[NodeSpec("bogus", "bogus_type", "x", {}, [], [PortSpec("records")], set())],
            edges=[],
        )
        with pytest.raises(DAGValidationError, match="unknown type"):
            await DAGExecutor().execute(_task(), dag)
    finally:
        logger.remove(sink_id)

    assert not any("teardown error" in message for message in warnings)


@pytest.mark.asyncio
async def test_node_teardown_still_runs_when_setup_fails(components):
    """护栏：节点已实例化时，即使 setup 失败也必须 teardown，不能被跳过。"""
    dag = DAG(
        name="setup_fail",
        nodes=[
            NodeSpec("src", "collector", "_lc_setup_fails", {}, [], [PortSpec("records")], set())
        ],
        edges=[],
    )
    result = await DAGExecutor().execute(_task(), dag)

    assert LIFECYCLE == ["teardown"]
    assert any("setup exploded" in err for err in result.errors)
