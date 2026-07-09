"""Static DAG / from_upstream validation tests."""

from src.core.dag_validate import (
    collector_uses_from_upstream,
    validate_from_upstream_config,
    validate_pipeline_collector_upstream,
)


def test_collector_uses_from_upstream_true_false() -> None:
    assert collector_uses_from_upstream({"from_upstream": True}) is True
    assert collector_uses_from_upstream({"from_upstream": {"auto": True}}) is True
    assert collector_uses_from_upstream({"from_upstream": False}) is False
    assert collector_uses_from_upstream({}) is False
    assert collector_uses_from_upstream(None) is False


def test_empty_map_without_auto_is_error() -> None:
    issues = validate_from_upstream_config(
        {"map": {}, "auto": False},
        collector_id="youtube_profiles",
    )
    assert any(i["code"] == "empty_from_upstream_map" and i["level"] == "error" for i in issues)


def test_empty_map_with_auto_is_warning() -> None:
    issues = validate_from_upstream_config(
        {"map": {}, "auto": True},
        collector_id="youtube_profiles",
    )
    assert any(i["code"] == "empty_from_upstream_map_auto" for i in issues)


def test_valid_map_passes() -> None:
    issues = validate_from_upstream_config(
        {"map": {"channel_id": "channel_id"}, "auto": False},
        collector_id="youtube_profiles",
    )
    assert issues == []


def test_pipeline_upstream_validation_aggregates() -> None:
    collectors = [
        ("youtube_comments", {}),
        (
            "youtube_profiles",
            {"from_upstream": {"map": {}, "auto": False}},
        ),
    ]
    issues = validate_pipeline_collector_upstream(collectors)
    assert len(issues) == 1
    assert issues[0]["collector_id"] == "youtube_profiles"
