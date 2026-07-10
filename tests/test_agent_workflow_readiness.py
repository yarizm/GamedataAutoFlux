"""P2 readiness workflow: matcher, card, meta registration."""

from __future__ import annotations

from src.agent.workflow_events import ENTRY_NODE_TO_WORKFLOW, WORKFLOW_META
from src.agent.workflow_matchers import _match_pipeline_workflow, _match_readiness_workflow
from src.agent.workflow_result_cards import build_readiness_result_card
from src.agent.workflow_runtime_nodes import (
    check_readiness_config_node,
    resolve_readiness_target_node,
)
from src.agent.workflows import _workflow_graph_definitions


def test_readiness_meta_registered() -> None:
    assert "readiness_workflow" in WORKFLOW_META
    meta = WORKFLOW_META["readiness_workflow"]
    assert meta.entry_node == "resolve_readiness_target"
    assert ENTRY_NODE_TO_WORKFLOW[meta.entry_node] == "readiness_workflow"
    assert len(meta.steps) == 4


def test_match_readiness_qimai() -> None:
    state = _match_readiness_workflow("七麦能不能采")
    assert state is not None
    assert state["route"] == "readiness_workflow"
    assert state["workflow_collector_id"] == "qimai"
    assert state["workflow_readiness_scope"] == "collector"


def test_match_readiness_system_only() -> None:
    state = _match_readiness_workflow("系统检查一下")
    assert state is not None
    assert state["workflow_readiness_scope"] == "system"
    assert state["workflow_collector_id"] == ""


def test_match_readiness_does_not_steal_pipeline() -> None:
    text = "采集这个页面 https://example.com/game"
    assert _match_pipeline_workflow(text) is not None
    # readiness intent alone without URL keywords
    assert _match_readiness_workflow(text) is None


def test_match_readiness_does_not_steal_report() -> None:
    assert _match_readiness_workflow("对任务 task:abc 生成报告") is None


def test_resolve_and_config_nodes() -> None:
    state = {
        "workflow_collector_id": "qimai",
        "workflow_readiness_scope": "collector",
    }
    resolved = resolve_readiness_target_node(state)  # type: ignore[arg-type]
    assert resolved["workflow_collector_id"] == "qimai"
    cfg = check_readiness_config_node(state)  # type: ignore[arg-type]
    assert "readiness_config" in cfg
    assert isinstance(cfg["readiness_config"], dict)


def test_readiness_result_card_health() -> None:
    card = build_readiness_result_card(
        {
            "workflow_readiness_scope": "collector",
            "workflow_collector_id": "steam",
            "readiness_config": {
                "status": "ok",
                "checks": [{"status": "ok", "message": "ok"}],
            },
            "readiness_session": {
                "status": "warning",
                "checks": [{"status": "warning", "message": "cookie 可能过期"}],
            },
        }
    )
    assert card["type"] == "result_card"
    assert card["card_type"] == "readiness"
    assert card["payload"]["health"] == "warning"
    assert any(a.get("href") == "system" for a in card["actions"])


def test_workflow_definitions_include_readiness() -> None:
    _workflow_graph_definitions.cache_clear()
    defs = _workflow_graph_definitions()
    routes = {d.route for d in defs}
    assert "readiness_workflow" in routes
    ready = next(d for d in defs if d.route == "readiness_workflow")
    assert ready.entry_node == "resolve_readiness_target"
    assert ready.resolve("系统检查一下") is not None
