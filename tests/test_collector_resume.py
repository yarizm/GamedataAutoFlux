from src.collectors.base import CollectResult, CollectTarget
from src.core.collector_resume import (
    build_collector_cursor,
    cap_partial_list,
    compose_recovery_checkpoint,
    cursor_has_deep_payload,
    merge_checkpoint_state,
    parse_recovery_cursor,
    select_preferred_checkpoint,
    PARTIAL_ITEM_CAP,
)
from src.core.pipeline_recovery import apply_collect_resume_context


def test_build_and_parse_cursor_roundtrip() -> None:
    cursor = build_collector_cursor(
        collector_id="steam",
        target_key="app:570",
        stage="api_reviews",
        payload={"review_cursor": "ABC", "collected_count": 100},
    )
    recovery = {"cursor": cursor, "state": {"target_order": ["G"]}}
    parsed = parse_recovery_cursor(recovery, collector_id="steam", target_key="app:570")
    assert parsed is not None
    assert parsed["payload"]["review_cursor"] == "ABC"


def test_parse_rejects_wrong_collector_or_target_or_version() -> None:
    cursor = build_collector_cursor(
        collector_id="steam", target_key="app:1", stage="x", payload={"a": 1}
    )
    cursor["schema_version"] = 999
    assert (
        parse_recovery_cursor({"cursor": cursor}, collector_id="steam", target_key="app:1") is None
    )
    cursor = build_collector_cursor(
        collector_id="steam", target_key="app:1", stage="x", payload={"a": 1}
    )
    assert (
        parse_recovery_cursor(
            {"cursor": cursor}, collector_id="youtube_comments", target_key="app:1"
        )
        is None
    )
    assert (
        parse_recovery_cursor({"cursor": cursor}, collector_id="steam", target_key="app:2") is None
    )


def test_cap_partial_list_truncates() -> None:
    items = list(range(PARTIAL_ITEM_CAP + 50))
    kept, truncated = cap_partial_list(items)
    assert truncated is True
    assert len(kept) == PARTIAL_ITEM_CAP


def test_select_preferred_checkpoint_prefers_deep_cursor() -> None:
    class CP:
        def __init__(self, seq, cursor, state=None):
            self.seq = seq
            self.cursor = cursor
            self.state = state or {}

    shallow = CP(3, {"stage": "collect", "status": "failed"}, {"target_order": ["A"]})
    deep = CP(
        2,
        build_collector_cursor(
            collector_id="steam",
            target_key="app:1",
            stage="api_reviews",
            payload={"review_cursor": "X"},
        ),
    )
    older_deep = CP(
        1,
        build_collector_cursor(
            collector_id="steam",
            target_key="app:1",
            stage="api_reviews",
            payload={"review_cursor": "OLD"},
        ),
    )
    # list ordered newest-first as service does
    preferred = select_preferred_checkpoint([shallow, deep, older_deep])
    assert preferred is deep
    assert cursor_has_deep_payload(preferred.cursor)


def test_merge_checkpoint_state_uses_real_success_fail_names() -> None:
    results = [
        CollectResult(target=CollectTarget(name="A"), success=True),
        CollectResult(target=CollectTarget(name="B"), success=False, error="boom"),
        CollectResult(target=CollectTarget(name="C"), success=True),
    ]
    state = merge_checkpoint_state(
        target_order=["A", "B", "C"],
        previous=None,
        collect_results=results,
    )
    assert state["successful_targets"] == ["A", "C"]
    assert state["failed_targets"] == ["B"]
    # failed B must be re-run: next index points at B; completed is only successful prefix
    assert state["next_target_index"] == 1
    assert state["completed_targets"] == ["A"]
    assert "B" not in state["completed_targets"]


def test_compose_recovery_merges_deep_cursor_with_target_order_state() -> None:
    """Deep mid-progress (empty state) + older collect-complete state → both kept."""

    class CP:
        def __init__(self, seq, cursor, state=None):
            self.seq = seq
            self.cursor = cursor
            self.state = state or {}

    deep_mid = CP(
        5,
        build_collector_cursor(
            collector_id="steam",
            target_key="app:1",
            stage="api_reviews",
            payload={"review_cursor": "DEEP"},
        ),
        {},  # mid-progress often emits empty state
    )
    collect_complete = CP(
        3,
        {"stage": "collect", "status": "failed"},
        {
            "target_order": ["A", "B"],
            "next_target_index": 1,
            "completed_targets": ["A"],
            "successful_targets": ["A"],
            "failed_targets": ["B"],
        },
    )
    # newest-first
    composed = compose_recovery_checkpoint([deep_mid, collect_complete])
    assert composed is not None
    assert cursor_has_deep_payload(composed.cursor)
    assert composed.cursor["payload"]["review_cursor"] == "DEEP"
    assert composed.state["target_order"] == ["A", "B"]
    assert composed.state["next_target_index"] == 1
    # original preferred object must not be mutated
    assert deep_mid.state == {}


def test_compose_recovery_skips_completed_target_via_apply() -> None:
    """Composed state.next_target_index=1 → multi-target resume skips A."""

    class CP:
        def __init__(self, seq, cursor, state=None):
            self.seq = seq
            self.cursor = cursor
            self.state = state or {}

    deep_mid = CP(
        2,
        build_collector_cursor(
            collector_id="steam",
            target_key="app:1",
            stage="api_reviews",
            payload={"review_cursor": "X"},
        ),
        {},
    )
    collect_complete = CP(
        1,
        {"stage": "collect", "status": "failed"},
        {
            "target_order": ["A", "B"],
            "next_target_index": 1,
            "completed_targets": ["A"],
        },
    )
    composed = compose_recovery_checkpoint([deep_mid, collect_complete])
    assert composed is not None
    remaining = apply_collect_resume_context(
        [CollectTarget(name="A"), CollectTarget(name="B")],
        {
            "enabled": True,
            "next_target_index": composed.state["next_target_index"],
        },
    )
    assert [t.name for t in remaining] == ["B"]
