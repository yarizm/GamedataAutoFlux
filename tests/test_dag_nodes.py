# tests/test_dag_nodes.py
import pytest
from src.core.dag import NodeSpec, PortSpec
from src.core.dag_nodes import CollectorNode, NodeContext
from src.core.task import Task, TaskTarget
from src.collectors.base import BaseCollector, CollectResult, CollectTarget
from src.core.registry import registry


class _DummyCollector(BaseCollector):
    async def collect(self, target: CollectTarget) -> CollectResult:
        return CollectResult(target=target, data={"name": target.name}, success=True)


@pytest.mark.asyncio
async def test_collector_node_outputs_records_port():
    snap = registry.snapshot()
    registry.register("collector", "_dummy_for_node_test")(_DummyCollector)
    try:
        spec = NodeSpec(
            id="src", type="collector", component="_dummy_for_node_test",
            config={}, ports_in=[], ports_out=[PortSpec("records")], is_param_port=set(),
        )
        task = Task(name="t", targets=[TaskTarget(name="gameA")])
        node = CollectorNode(spec, task=task, recovery_checkpoint={})
        await node.setup()
        ctx = NodeContext(inputs={}, task=task, config={}, recovery_checkpoint={})
        out = await node.run(ctx)
        assert "records" in out
        assert len(out["records"]) == 1
        assert out["records"][0].data["name"] == "gameA"
        await node.teardown()
    finally:
        registry.restore(snap)


@pytest.mark.asyncio
async def test_collector_node_respects_recovery():
    snap = registry.snapshot()
    registry.register("collector", "_dummy_rec")(_DummyCollector)
    try:
        spec = NodeSpec(
            id="src", type="collector", component="_dummy_rec",
            config={}, ports_in=[], ports_out=[PortSpec("records")], is_param_port=set(),
        )
        task = Task(name="t", targets=[TaskTarget(name="gameB")])
        recovery = {"collect": {"next_target_index": 0}}
        node = CollectorNode(spec, task=task, recovery_checkpoint=recovery)
        await node.setup()
        ctx = NodeContext(inputs={}, task=task, config={}, recovery_checkpoint=recovery)
        out = await node.run(ctx)
        assert len(out["records"]) == 1
        await node.teardown()
    finally:
        registry.restore(snap)


@pytest.mark.asyncio
async def test_node_context_param_fallback():
    ctx = NodeContext(inputs={"app_id": "123"}, task=Task(name="t"), config={})
    assert ctx.param("app_id") == "123"
    assert ctx.param("missing", "fallback") == "fallback"
    assert ctx.param("nope") is None
