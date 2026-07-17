"""S0: mid-progress collect checkpoints via injected _emit_checkpoint."""

from __future__ import annotations

import pytest

from src.collectors.base import BaseCollector, CollectResult, CollectTarget
from src.core.collector_resume import build_collector_cursor
from src.core.dag import NodeSpec, PortSpec
from src.core.dag_nodes import CollectorNode, NodeContext
from src.core.pipeline import Pipeline
from src.core.registry import registry
from src.core.scheduler import Scheduler
from src.core.task import Task, TaskTarget
from src.services.task_checkpoint_service import InMemoryTaskCheckpointService
from src.services.task_event_service import InMemoryTaskEventService


def _deep_cursor(**payload_extra: object) -> dict:
    return build_collector_cursor(
        collector_id="steam",
        target_key="app:1",
        stage="api_reviews",
        payload={"review_cursor": "CURSOR_ABC", **payload_extra},
    )


@pytest.mark.asyncio
async def test_observability_records_collect_progress_deep_cursor() -> None:
    checkpoint_service = InMemoryTaskCheckpointService()
    event_service = InMemoryTaskEventService()
    scheduler = Scheduler(
        task_event_service=event_service,
        task_checkpoint_service=checkpoint_service,
    )
    task = Task(
        id="progress-cp-task",
        name="Progress CP",
        pipeline_name="steam_basic",
        collector_name="steam",
        targets=[TaskTarget(name="GameA")],
    )
    scheduler._tasks[task.id] = task

    cursor = _deep_cursor()
    state = {
        "target_order": ["GameA"],
        "next_target_index": 0,
        "completed_targets": [],
        "successful_targets": [],
        "failed_targets": [],
    }
    await scheduler._on_task_event(
        task.id,
        "collect",
        "info",
        "collect progress checkpoint",
        {
            "status": "progress",
            "component": "steam",
            "checkpoint_cursor": cursor,
            "checkpoint_state": state,
            "stats": {"pages": 3},
        },
    )

    checkpoints = await checkpoint_service.list_checkpoints(task.id)
    events = await event_service.list_events(task.id)

    assert len(checkpoints) == 1
    cp = checkpoints[0]
    assert cp.recovery_level == "L1"
    assert cp.cursor == cursor
    assert cp.cursor["payload"]["review_cursor"] == "CURSOR_ABC"
    assert cp.state == state
    assert cp.stats.get("pages") == 3
    assert cp.metadata.get("source") == "collect_progress"
    assert "collect" in [e.type for e in events]
    assert "checkpoint" in [e.type for e in events]


@pytest.mark.asyncio
async def test_observability_skips_progress_without_checkpoint_cursor() -> None:
    checkpoint_service = InMemoryTaskCheckpointService()
    scheduler = Scheduler(
        task_event_service=InMemoryTaskEventService(),
        task_checkpoint_service=checkpoint_service,
    )
    task = Task(
        id="progress-no-cursor",
        name="No Cursor",
        pipeline_name="p",
        collector_name="steam",
    )
    scheduler._tasks[task.id] = task

    await scheduler._on_task_event(
        task.id,
        "collect",
        "info",
        "collect progress without cursor",
        {"status": "progress", "component": "steam", "stats": {"pages": 1}},
    )

    assert await checkpoint_service.list_checkpoints(task.id) == []


@pytest.mark.asyncio
async def test_observability_skips_progress_for_l0_collector() -> None:
    checkpoint_service = InMemoryTaskCheckpointService()
    scheduler = Scheduler(
        task_event_service=InMemoryTaskEventService(),
        task_checkpoint_service=checkpoint_service,
    )
    task = Task(
        id="progress-l0",
        name="L0",
        pipeline_name="p",
        collector_name="qimai",
    )
    scheduler._tasks[task.id] = task

    await scheduler._on_task_event(
        task.id,
        "collect",
        "info",
        "collect progress checkpoint",
        {
            "status": "progress",
            "component": "qimai",
            "checkpoint_cursor": _deep_cursor(),
        },
    )

    assert await checkpoint_service.list_checkpoints(task.id) == []


@pytest.mark.asyncio
async def test_collector_node_injects_emit_checkpoint() -> None:
    captured: dict = {}

    class _SpyCollector(BaseCollector):
        async def collect(self, target: CollectTarget) -> CollectResult:
            captured["emit"] = self.config.get("_emit_checkpoint")
            return CollectResult(target=target, data={"ok": True})

        async def teardown(self) -> None:
            return None

    name = "_test_emit_inject_collector"
    registry.register("collector", name)(_SpyCollector)
    try:
        task = Task(
            id="inject-node",
            name="Inject Node",
            pipeline_name="p",
            collector_name=name,
            targets=[TaskTarget(name="T1")],
        )
        spec = NodeSpec(
            id="c1",
            type="collector",
            component=name,
            config={},
            ports_in=[],
            ports_out=[PortSpec(name="records")],
        )
        node = CollectorNode(spec, task=task, recovery_checkpoint={})
        events: list[tuple] = []

        async def on_event(task_id, node_spec, phase, *, out=None, error=None):
            events.append((task_id, getattr(node_spec, "component", None), phase, out))

        await node.setup()
        await node.run(
            NodeContext(
                inputs={},
                task=task,
                config={},
                emit_event=on_event,
            )
        )
        await node.teardown()

        assert callable(captured.get("emit"))
        cursor = _deep_cursor()
        await captured["emit"](cursor, state={"target_order": ["T1"]}, stats={"n": 1})
        assert any(e[2] == "progress" for e in events)
        progress_events = [e for e in events if e[2] == "progress"]
        assert progress_events
        out = progress_events[0][3] or {}
        assert out.get("checkpoint_cursor") == cursor
        assert out.get("checkpoint_state") == {"target_order": ["T1"]}
        assert out.get("stats") == {"n": 1}
    finally:
        registry._registry.get("collector", {}).pop(name, None)


@pytest.mark.asyncio
async def test_pipeline_legacy_injects_and_records_progress_checkpoint(monkeypatch) -> None:
    """Legacy collect path injects _emit_checkpoint; calling it records via event callback."""
    checkpoint_service = InMemoryTaskCheckpointService()
    event_service = InMemoryTaskEventService()
    scheduler = Scheduler(
        task_event_service=event_service,
        task_checkpoint_service=checkpoint_service,
    )

    class _SpyCollector(BaseCollector):
        async def collect(self, target: CollectTarget) -> CollectResult:
            emit = self.config.get("_emit_checkpoint")
            assert callable(emit)
            await emit(
                _deep_cursor(),
                state={
                    "target_order": ["GameA"],
                    "next_target_index": 0,
                    "completed_targets": [],
                    "successful_targets": [],
                    "failed_targets": [],
                },
                stats={"pages": 1},
            )
            return CollectResult(target=target, data={"ok": True})

        async def teardown(self) -> None:
            return None

    name = "_test_legacy_emit_collector"
    registry.register("collector", name)(_SpyCollector)
    # Treat spy as steam for metadata checkpoint support via component override in events
    try:
        monkeypatch.setattr(
            "src.core.config.get",
            lambda key, default=None: False if key == "pipeline.use_dag_execution" else default,
        )
        pipeline = Pipeline("legacy_emit_pipe").add_collector(name)
        pipeline.on_event(scheduler._on_task_event)

        task = Task(
            id="legacy-emit-task",
            name="Legacy Emit",
            pipeline_name="legacy_emit_pipe",
            collector_name="steam",  # L1 metadata
            targets=[TaskTarget(name="GameA")],
        )
        scheduler._tasks[task.id] = task

        # Component on events will be spy name; force metadata path by using steam in payload
        # via monkeypatch of get_collector_metadata for spy name → steam-like L1
        from src.core.collector_metadata import (
            CollectorMetadata,
            get_collector_metadata as real_get,
        )

        def _meta(cid: str):
            if cid == name:
                return CollectorMetadata(
                    collector_id=name,
                    display_name="Test Legacy Emit",
                    supports_checkpoint=True,
                    recovery_level="L1",
                )
            return real_get(cid)

        monkeypatch.setattr(
            "src.core.task_observability_service.get_collector_metadata",
            _meta,
        )

        result = await pipeline.execute(task)
        assert result.success, result.errors

        checkpoints = await checkpoint_service.list_checkpoints(task.id)
        progress_cps = [
            c for c in checkpoints if (c.metadata or {}).get("source") == "collect_progress"
        ]
        assert progress_cps, (
            f"expected collect_progress checkpoint, got {[c.metadata for c in checkpoints]}"
        )
        assert progress_cps[0].cursor["payload"]["review_cursor"] == "CURSOR_ABC"
    finally:
        registry._registry.get("collector", {}).pop(name, None)
