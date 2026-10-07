"""Collector target_schema is the single source of task targets.

Covers the shared construction helpers added so that Web forms, Agent
workflows and the create_task tool all derive parameters from the collector's
declared contract instead of hardcoded per-collector tables.
"""

from __future__ import annotations

from src.core.collector_metadata import (
    build_task_targets,
    list_collector_metadata,
    merge_collector_defaults,
)
from src.core.pipeline_templates import (
    find_pipeline_template_for_collector,
    find_template_entry_collector,
)


def test_pipeline_template_lookup_round_trips() -> None:
    assert find_pipeline_template_for_collector("steam") == "steam_basic"
    assert find_template_entry_collector("steam_basic") == "steam"
    assert find_pipeline_template_for_collector("no_such_collector") is None
    assert find_template_entry_collector("no_such_template") is None


def test_youtube_uses_its_real_template_id() -> None:
    """The plugin registers *_pipeline, not the *_basic name a table once assumed."""

    assert find_pipeline_template_for_collector("youtube_profiles") == "youtube_profiles_pipeline"
    assert find_pipeline_template_for_collector("youtube_comments") == "youtube_comments_pipeline"


def test_every_installed_collector_resolves_to_a_template() -> None:
    for collector_id in list_collector_metadata():
        assert find_pipeline_template_for_collector(collector_id), collector_id


def test_build_task_targets_applies_schema_defaults() -> None:
    targets = build_task_targets("steam", name="原神")

    assert len(targets) == 1
    assert targets[0]["name"] == "原神"
    assert targets[0]["target_type"] == "game"
    assert targets[0]["params"]["skip_steamdb"] is True


def test_build_task_targets_honours_explicit_values() -> None:
    targets = build_task_targets("steam", {"app_id": 730, "skip_steamdb": False}, name="原神")

    assert targets[0]["params"]["app_id"] == 730
    assert targets[0]["params"]["skip_steamdb"] is False


def test_build_task_targets_expands_multiple_fields() -> None:
    targets = build_task_targets(
        "youtube_comments",
        {"video_url": "https://youtu.be/a\nhttps://youtu.be/b"},
    )

    assert [target["name"] for target in targets] == ["https://youtu.be/a", "https://youtu.be/b"]
    assert all(target["target_type"] == "youtube_video" for target in targets)
    assert all(target["params"]["video_url"].startswith("https://") for target in targets)


def test_build_task_targets_requires_an_identifying_name() -> None:
    # steam_discussions declares a name field, so schema defaults alone must not
    # invent a target named after a parameter value.
    assert build_task_targets("steam_discussions") == []


def test_merge_collector_defaults_keeps_explicit_params() -> None:
    merged = merge_collector_defaults(
        "steam_discussions",
        [{"name": "原神", "params": {"max_pages": 5}}],
    )

    assert merged[0]["params"]["max_pages"] == 5
    assert merged[0]["params"]["max_topics"] == 1000
    assert merged[0]["params"]["include_replies"] is True


def test_merge_collector_defaults_is_a_noop_without_a_contract() -> None:
    targets = [{"name": "x", "target_type": "game", "params": {"a": 1}}]

    assert merge_collector_defaults("no_such_collector", targets) == targets
