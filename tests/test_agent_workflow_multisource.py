"""P4 multi-source workflow: matcher, draft, confirm gate, card, meta."""

from __future__ import annotations

import asyncio

from src.agent.workflow_events import ENTRY_NODE_TO_WORKFLOW, WORKFLOW_META
from src.agent.workflow_matchers import (
    _match_multisource_workflow,
    _match_pipeline_workflow,
    _match_readiness_workflow,
    _match_report_workflow,
)
from src.agent.workflow_multisource_parse import build_multisource_draft
from src.agent.workflow_result_cards import (
    build_multisource_result_card,
    build_pipeline_result_card,
    build_report_result_card,
    build_task_review_result_card,
)
from src.agent.workflow_runtime_nodes import (
    apply_multisource_action_node,
    build_multisource_draft_node,
    resolve_multisource_intent_node,
)
from src.agent.workflows import _workflow_graph_definitions


def test_multisource_meta_registered() -> None:
    assert "multisource_workflow" in WORKFLOW_META
    meta = WORKFLOW_META["multisource_workflow"]
    assert meta.entry_node == "resolve_multisource_intent"
    assert ENTRY_NODE_TO_WORKFLOW[meta.entry_node] == "multisource_workflow"
    assert len(meta.steps) == 4


def test_match_multisource_game_and_collectors() -> None:
    state = _match_multisource_workflow("多源采集《原神》 steam 七麦")
    assert state is not None
    assert state["route"] == "multisource_workflow"
    assert state["workflow_multisource_game"] == "原神"
    assert "steam" in state["workflow_multisource_collectors"]
    assert "qimai" in state["workflow_multisource_collectors"]
    assert state["workflow_multisource_confirm"] is False


def test_match_two_collectors_with_collect_intent() -> None:
    state = _match_multisource_workflow("同时采 steam 和 taptap 的黑神话")
    assert state is not None
    assert "steam" in state["workflow_multisource_collectors"]
    assert "taptap" in state["workflow_multisource_collectors"]


def test_match_multisource_confirm() -> None:
    state = _match_multisource_workflow("确认创建 多源采集《原神》 steam 七麦")
    assert state is not None
    assert state["workflow_multisource_confirm"] is True


def test_match_multisource_does_not_steal_report() -> None:
    text = "对任务 task:abc 生成报告"
    assert _match_report_workflow(text) is not None
    assert _match_multisource_workflow(text) is None


def test_match_multisource_does_not_steal_pipeline() -> None:
    text = "采集这个页面 https://example.com/game"
    assert _match_pipeline_workflow(text) is not None
    assert _match_multisource_workflow(text) is None


def test_match_multisource_does_not_steal_readiness() -> None:
    assert _match_readiness_workflow("七麦能不能采") is not None
    assert _match_multisource_workflow("七麦能不能采") is None


def test_build_draft_has_task_templates() -> None:
    draft = build_multisource_draft("多源采集《原神》 steam 七麦")
    assert draft["game_name"] == "原神"
    assert len(draft["task_drafts"]) >= 2
    pipelines = {d["pipeline_name"] for d in draft["task_drafts"]}
    assert "steam_basic" in pipelines
    assert "qimai_basic" in pipelines
    assert draft["status"] in {"draft", "incomplete"}


def test_apply_needs_confirm_no_create(monkeypatch) -> None:
    created: list[str] = []

    class _Pre:
        can_submit = True
        issues = []

    class _TS:
        def precheck(self, **kwargs):
            return _Pre()

        async def create(self, **kwargs):
            created.append(kwargs.get("name") or "")
            return type("T", (), {"id": "t1"})()

    import src.web.app as app_mod

    monkeypatch.setattr(app_mod, "get_task_service", lambda: _TS(), raising=False)

    matched = _match_multisource_workflow("多源采集《原神》 steam 七麦")
    assert matched is not None
    state = {**matched, **resolve_multisource_intent_node(matched)}  # type: ignore[arg-type]
    state.update(build_multisource_draft_node(state))  # type: ignore[arg-type]
    out = asyncio.run(apply_multisource_action_node(state))  # type: ignore[arg-type]
    assert out["multisource_result"]["status"] == "needs_confirm"
    assert created == []


def test_apply_confirm_creates(monkeypatch) -> None:
    created: list[dict] = []

    class _Pre:
        can_submit = True
        issues = []

    class _TS:
        def precheck(self, **kwargs):
            return _Pre()

        async def create(self, **kwargs):
            created.append(kwargs)
            return type("T", (), {"id": f"id-{len(created)}"})()

    import src.web.app as app_mod

    monkeypatch.setattr(app_mod, "get_task_service", lambda: _TS(), raising=False)

    matched = _match_multisource_workflow("确认创建 多源采集《原神》 steam 七麦")
    assert matched is not None
    state = {**matched, **resolve_multisource_intent_node(matched)}  # type: ignore[arg-type]
    state.update(build_multisource_draft_node(state))  # type: ignore[arg-type]
    out = asyncio.run(apply_multisource_action_node(state))  # type: ignore[arg-type]
    assert out["multisource_result"]["status"] == "success"
    assert len(created) >= 2
    assert all(c.get("pipeline_name") for c in created)


def test_multisource_card_needs_confirm() -> None:
    card = build_multisource_result_card(
        {
            "workflow_multisource_game": "原神",
            "workflow_multisource_collectors": ["steam", "qimai"],
            "multisource_draft": {
                "game_name": "原神",
                "collectors": ["steam", "qimai"],
                "task_drafts": [
                    {"name": "原神_steam", "pipeline_name": "steam_basic", "collector_id": "steam"},
                ],
            },
            "multisource_result": {
                "status": "needs_confirm",
                "summary": "待确认",
                "task_drafts": [],
                "created_tasks": [],
            },
        }
    )
    assert card["card_type"] == "multisource"
    assert card["payload"]["status"] == "needs_confirm"
    assert any(a.get("href") == "tasks" for a in card["actions"])


def test_pipeline_card_has_run_next_step() -> None:
    card = build_pipeline_result_card(
        {
            "workflow_url": "https://example.com/g",
            "workflow_pipeline_name": "example_com_page",
            "dynamic_pipeline_result": {
                "status": "ok",
                "summary": "created",
                "data": {"pipeline_name": "example_com_page"},
            },
        }
    )
    assert card["payload"]["status"] == "success"
    assert any(a.get("id") == "copy_create_task" for a in card["actions"])
    assert "create_task_phrase" in card["payload"]


def test_task_review_card_retry_copy() -> None:
    card = build_task_review_result_card(
        {
            "workflow_task_id": "task-1",
            "workflow_auto_retry": False,
            "task_detail": {"status": "ok"},
            "collection_review": {
                "completeness": "partial",
                "record_count": 1,
                "issues": [{"level": "error", "message": "failed target"}],
            },
        }
    )
    assert any(a.get("id") == "copy_retry_phrase" for a in card["actions"])


def test_report_card_collect_draft_on_block() -> None:
    card = build_report_result_card(
        {
            "workflow_task_id": "task-1",
            "task_detail": {"status": "ok"},
            "collection_review": {"record_count": 1},
            "report_precheck": {
                "can_generate": False,
                "status": "empty",
                "missing_collectors": ["steam", "qimai"],
                "next_best_action": {
                    "collector": "steam",
                    "collector_label": "Steam",
                    "pipeline_name": "steam_basic",
                    "create_task_draft": {
                        "name": "Collect steam",
                        "pipeline_name": "steam_basic",
                        "targets": [{"name": "原神", "target_type": "game", "params": {}}],
                    },
                },
            },
        }
    )
    assert any(a.get("id") == "copy_collect_draft" for a in card["actions"])


def test_workflow_definitions_include_multisource() -> None:
    _workflow_graph_definitions.cache_clear()
    defs = _workflow_graph_definitions()
    routes = {d.route for d in defs}
    assert "multisource_workflow" in routes
    ms = next(d for d in defs if d.route == "multisource_workflow")
    assert ms.entry_node == "resolve_multisource_intent"
    assert ms.resolve("多源采集《原神》 steam 七麦") is not None
