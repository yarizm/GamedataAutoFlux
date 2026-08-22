"""运行时指标测试：注册表语义 + 执行链路埋点。"""

import time

import pytest

from src.collectors.base import BaseCollector, CollectResult, CollectTarget
from src.core.dag import DAG, Edge, NodeSpec, PortSpec
from src.core.dag_executor import DAGExecutor
from src.core.metrics import MetricsRegistry, metrics
from src.core.registry import registry
from src.core.task import Task, TaskTarget


def test_counter_accumulates_with_sorted_labels():
    reg = MetricsRegistry()
    reg.inc("task_completed_total", status="success")
    reg.inc("task_completed_total", status="success")
    reg.inc("task_completed_total", status="failed")

    snap = reg.snapshot()
    assert snap["counters"]["task_completed_total{status=success}"] == 2
    assert snap["counters"]["task_completed_total{status=failed}"] == 1


def test_label_key_ordering_is_stable():
    reg = MetricsRegistry()
    reg.inc("m", b="2", a="1")
    reg.inc("m", a="1", b="2")
    assert list(reg.snapshot()["counters"]) == ["m{a=1,b=2}"]


def test_observe_aggregates_count_sum_avg_max():
    reg = MetricsRegistry()
    for value in (1.0, 2.0, 5.0):
        reg.observe("collector_duration_seconds", value)

    timer = reg.snapshot()["timers"]["collector_duration_seconds"]
    assert timer["count"] == 3
    assert timer["sum"] == 8.0
    assert timer["avg"] == pytest.approx(2.666666, abs=1e-4)
    assert timer["max"] == 5.0


def test_timer_context_observes_elapsed():
    metrics.reset()
    with metrics.timer("_test_elapsed"):
        time.sleep(0.01)
    timer = metrics.snapshot()["timers"]["_test_elapsed"]
    assert timer["count"] == 1
    assert timer["sum"] >= 0.01
    metrics.reset()


def test_reset_clears_everything():
    reg = MetricsRegistry()
    reg.inc("a")
    reg.observe("b", 1.0)
    reg.reset()
    assert reg.snapshot() == {"counters": {}, "timers": {}}


class _OkCollector(BaseCollector):
    async def collect(self, target: CollectTarget) -> CollectResult:
        return CollectResult(target=target, success=True, data={"ok": True})


@pytest.fixture
def ok_collector():
    snap = registry.snapshot()
    metrics.reset()
    registry.register("collector", "_metrics_ok")(_OkCollector)
    try:
        yield
    finally:
        registry.restore(snap)
        metrics.reset()


@pytest.mark.asyncio
async def test_dag_executor_records_node_counters(ok_collector):
    """成功节点 + collector 耗时 + 条件抑制的跳过节点都会计入指标。"""
    dag = DAG(
        name="metrics_probe",
        nodes=[
            NodeSpec("c1", "collector", "_metrics_ok", {}, [], [PortSpec("records")], set()),
            NodeSpec(
                "p1", "processor", "_metrics_ok", {},
                [PortSpec("records")], [PortSpec("records")], set(),
            ),
        ],
        edges=[
            # 返回非空数据 → on_empty 为假 → p1 被抑制跳过
            Edge("c1", "records", "p1", "records", condition="on_empty"),
        ],
    )
    await DAGExecutor().execute(Task(name="t", targets=[TaskTarget(name="g")]), dag)

    counters = metrics.snapshot()["counters"]
    assert counters.get('dag_node_total{result=ok,type=collector}') == 1
    assert counters.get('dag_node_total{result=skipped,type=processor}') == 1
    assert "collector_duration_seconds" in metrics.snapshot()["timers"]
