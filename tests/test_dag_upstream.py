"""Unit tests for upstream record → CollectTarget mapping (shipped dag_upstream path)."""

from src.collectors.base import CollectResult, CollectTarget
from src.core.dag_upstream import resolve_collector_targets, targets_from_upstream_records
from src.core.task import Task, TaskTarget


def test_manual_map_produces_targets_with_mapped_params():
    """Non-auto from_upstream.map must map source keys into target.params."""
    records = [
        CollectResult(
            target=CollectTarget(name="v1"),
            success=True,
            data={
                "title": "hello",
                "channel_id": "UCabc",
                "channel_url": "https://www.youtube.com/channel/UCabc",
                "channel_name": "CreatorA",
            },
        ),
        CollectResult(
            target=CollectTarget(name="v2"),
            success=True,
            data={
                "channel_id": "UCxyz",
                "channel_url": "https://www.youtube.com/channel/UCxyz",
                "channel_name": "CreatorB",
            },
        ),
    ]
    targets = targets_from_upstream_records(
        records,
        {
            "auto": False,
            "map": {
                "channel_url": "channel_url",
                "channel_id": "channel_id",
            },
            "name_from": "channel_name",
            "dedupe_by": ["channel_id"],
        },
    )
    assert len(targets) == 2
    by_id = {t.params["channel_id"]: t for t in targets}
    assert by_id["UCabc"].params["channel_url"].endswith("/UCabc")
    assert by_id["UCabc"].name == "CreatorA"
    assert by_id["UCxyz"].params["channel_url"].endswith("/UCxyz")
    # unmapped keys must not appear
    assert "title" not in by_id["UCabc"].params


def test_manual_map_skips_missing_source_fields():
    records = [
        CollectResult(
            target=CollectTarget(name="v1"),
            success=True,
            data={"title": "only title"},
        ),
    ]
    targets = targets_from_upstream_records(
        records,
        {"auto": False, "map": {"channel_url": "channel_url"}},
    )
    assert targets == []


def test_resolve_with_map_ignores_task_targets():
    task_targets = [
        CollectTarget(name="video", params={"video_url": "https://youtu.be/xxx"}),
    ]
    records = [
        CollectResult(
            target=CollectTarget(name="v1"),
            success=True,
            data={"channel_url": "https://www.youtube.com/channel/UConly"},
        ),
    ]
    out = resolve_collector_targets(
        task_targets=task_targets,
        upstream_records=records,
        node_config={
            "from_upstream": {
                "auto": False,
                "map": {"channel_url": "channel_url"},
            }
        },
    )
    assert len(out) == 1
    assert out[0].params["channel_url"].endswith("/UConly")
    assert "video_url" not in out[0].params


def test_resolve_without_from_upstream_uses_task_targets():
    task_targets = [CollectTarget(name="t1", params={"video_url": "u"})]
    out = resolve_collector_targets(
        task_targets=task_targets,
        upstream_records=[],
        node_config={},
    )
    assert out == task_targets
