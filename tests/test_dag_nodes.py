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


class _CaptureCollector(BaseCollector):
    """记录收到的 targets，便于断言上游映射。"""

    last_targets: list = []

    async def collect(self, target: CollectTarget) -> CollectResult:
        _CaptureCollector.last_targets.append(target)
        return CollectResult(
            target=target,
            data={"profile": target.name, "params": dict(target.params)},
            success=True,
        )


@pytest.mark.asyncio
async def test_collector_node_maps_upstream_records_to_targets():
    """youtube 视频采集结果 → 频道主页采集：from_upstream.auto。"""
    snap = registry.snapshot()
    registry.register("collector", "_capture_profiles")(_CaptureCollector)
    _CaptureCollector.last_targets = []
    try:
        spec = NodeSpec(
            id="profiles",
            type="collector",
            component="_capture_profiles",
            config={"from_upstream": {"auto": True}},
            ports_in=[PortSpec("records", required=False)],
            ports_out=[PortSpec("records")],
            is_param_port=set(),
        )
        # 任务 targets 是视频；下游 collector 应忽略它们，改用上游 channel
        task = Task(name="t", targets=[TaskTarget(name="video1", params={"video_url": "https://youtu.be/xxx"})])
        upstream = [
            CollectResult(
                target=CollectTarget(name="v1"),
                success=True,
                data={
                    "channel_id": "UCabc",
                    "channel_url": "https://www.youtube.com/channel/UCabc",
                    "channel_name": "CreatorA",
                },
            ),
            CollectResult(
                target=CollectTarget(name="v2"),
                success=True,
                data={
                    "channel_id": "UCabc",  # 去重
                    "channel_url": "https://www.youtube.com/channel/UCabc",
                    "channel_name": "CreatorA",
                },
            ),
            CollectResult(
                target=CollectTarget(name="v3"),
                success=True,
                data={
                    "channel_id": "UCxyz",
                    "channel_url": "https://www.youtube.com/channel/UCxyz",
                    "channel_name": "CreatorB",
                },
            ),
        ]
        node = CollectorNode(spec, task=task, recovery_checkpoint={})
        await node.setup()
        out = await node.run(NodeContext(
            inputs={"records": upstream},
            task=task,
            config=spec.config,
            recovery_checkpoint={},
        ))
        await node.teardown()

        assert len(_CaptureCollector.last_targets) == 2
        ids = {t.params.get("channel_id") for t in _CaptureCollector.last_targets}
        assert ids == {"UCabc", "UCxyz"}
        assert len(out["records"]) == 2
    finally:
        registry.restore(snap)


@pytest.mark.asyncio
async def test_collector_node_from_upstream_empty_does_not_fallback_to_task():
    snap = registry.snapshot()
    registry.register("collector", "_capture_empty")(_CaptureCollector)
    _CaptureCollector.last_targets = []
    try:
        spec = NodeSpec(
            id="profiles",
            type="collector",
            component="_capture_empty",
            config={"from_upstream": True},
            ports_in=[PortSpec("records", required=False)],
            ports_out=[PortSpec("records")],
            is_param_port=set(),
        )
        task = Task(name="t", targets=[TaskTarget(name="should_not_run")])
        node = CollectorNode(spec, task=task, recovery_checkpoint={})
        await node.setup()
        out = await node.run(NodeContext(inputs={"records": []}, task=task, config=spec.config))
        await node.teardown()
        assert out["records"] == []
        assert _CaptureCollector.last_targets == []
    finally:
        registry.restore(snap)
