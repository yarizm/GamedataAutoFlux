"""P3 cron workflow: matcher, schedule parse, apply gates, card, meta."""

from __future__ import annotations

from src.agent.workflow_cron_parse import parse_schedule
from src.agent.workflow_events import ENTRY_NODE_TO_WORKFLOW, WORKFLOW_META
from src.agent.workflow_matchers import (
    _match_cron_workflow,
    _match_pipeline_workflow,
    _match_readiness_workflow,
    _match_report_workflow,
)
from src.agent.workflow_result_cards import build_cron_result_card
from src.agent.workflow_runtime_nodes import (
    apply_cron_action_node,
    resolve_cron_intent_node,
    resolve_cron_schedule_node,
)
from src.agent.workflows import _workflow_graph_definitions


def test_cron_meta_registered() -> None:
    assert "cron_workflow" in WORKFLOW_META
    meta = WORKFLOW_META["cron_workflow"]
    assert meta.entry_node == "resolve_cron_intent"
    assert ENTRY_NODE_TO_WORKFLOW[meta.entry_node] == "cron_workflow"
    assert len(meta.steps) == 4


def test_match_cron_list() -> None:
    state = _match_cron_workflow("有哪些定时任务")
    assert state is not None
    assert state["route"] == "cron_workflow"
    assert state["workflow_cron_action"] == "list"
    assert state["workflow_cron_confirm"] is False


def test_match_cron_create_daily_without_confirm() -> None:
    state = _match_cron_workflow("每天 8 点跑 pipeline:steam_full")
    assert state is not None
    assert state["workflow_cron_action"] == "create"
    assert state["workflow_pipeline_name"] == "steam_full"
    assert state["workflow_cron_expr"] == "0 8 * * *"
    assert state["workflow_cron_confirm"] is False
    assert state["workflow_cron_name"]


def test_match_cron_create_every_minutes() -> None:
    state = _match_cron_workflow("每 15 分钟跑 pipeline:monitor")
    assert state is not None
    assert state["workflow_cron_action"] == "create"
    assert state["workflow_pipeline_name"] == "monitor"
    assert state["workflow_cron_expr"] == "*/15 * * * *"


def test_match_cron_create_with_confirm() -> None:
    state = _match_cron_workflow("确认创建 每天 8 点跑 pipeline:steam_full")
    assert state is not None
    assert state["workflow_cron_confirm"] is True
    assert state["workflow_cron_expr"] == "0 8 * * *"


def test_match_cron_delete() -> None:
    state = _match_cron_workflow("确认删除定时任务 daily_steam")
    assert state is not None
    assert state["workflow_cron_action"] == "delete"
    assert state["workflow_cron_name"] == "daily_steam"
    assert state["workflow_cron_confirm"] is True


def test_match_cron_does_not_steal_report() -> None:
    assert _match_report_workflow("对任务 task:abc 生成报告") is not None
    assert _match_cron_workflow("对任务 task:abc 生成报告") is None


def test_match_cron_does_not_steal_pipeline() -> None:
    text = "采集这个页面 https://example.com/game"
    assert _match_pipeline_workflow(text) is not None
    assert _match_cron_workflow(text) is None


def test_match_cron_does_not_steal_readiness() -> None:
    assert _match_readiness_workflow("七麦能不能采") is not None
    assert _match_cron_workflow("七麦能不能采") is None


def test_parse_schedule_daily() -> None:
    result = parse_schedule("每天 8 点跑 pipeline:steam_full")
    assert result["cron_expr"] == "0 8 * * *"
    assert result["human_schedule"]
    assert not result["issues"]


def test_parse_schedule_raw() -> None:
    result = parse_schedule("用 30 9 * * 1-5 调度")
    assert result["cron_expr"] == "30 9 * * 1-5"


def test_parse_schedule_unsupported() -> None:
    result = parse_schedule("隔一天跑一次 pipeline:steam_full")
    # may still match domain but schedule incomplete
    assert result["cron_expr"] == "" or result["issues"]


def test_resolve_and_apply_needs_confirm(monkeypatch) -> None:
    calls: list[str] = []

    class _Sched:
        def add_cron_job(self, **kwargs):
            calls.append("add")
            return "job-1"

        def list_cron_jobs(self):
            return []

        def remove_cron_job(self, name: str):
            calls.append(f"del:{name}")
            return True

    import src.web.app as app_mod

    monkeypatch.setattr(app_mod, "scheduler", _Sched(), raising=False)

    matched = _match_cron_workflow("每天 8 点跑 pipeline:steam_full")
    assert matched is not None
    state = {**matched, **resolve_cron_intent_node(matched)}  # type: ignore[arg-type]
    state.update(resolve_cron_schedule_node(state))  # type: ignore[arg-type]
    out = apply_cron_action_node(state)  # type: ignore[arg-type]
    result = out["cron_result"]
    assert result["status"] == "needs_confirm"
    assert "add" not in calls


def test_apply_create_with_confirm(monkeypatch) -> None:
    created: dict = {}

    class _Sched:
        def add_cron_job(self, **kwargs):
            created.update(kwargs)
            return "job-42"

        def list_cron_jobs(self):
            return []

        def remove_cron_job(self, name: str):
            return True

    import src.web.app as app_mod

    monkeypatch.setattr(app_mod, "scheduler", _Sched(), raising=False)

    matched = _match_cron_workflow("确认创建 每天 8 点跑 pipeline:steam_full 名称 steam_daily")
    assert matched is not None
    state = {**matched, **resolve_cron_intent_node(matched)}  # type: ignore[arg-type]
    state.update(resolve_cron_schedule_node(state))  # type: ignore[arg-type]
    out = apply_cron_action_node(state)  # type: ignore[arg-type]
    assert out["cron_result"]["status"] == "success"
    assert created.get("pipeline_name") == "steam_full"
    assert created.get("cron_expr") == "0 8 * * *"
    assert created.get("name") == "steam_daily"


def test_apply_list(monkeypatch) -> None:
    class _Sched:
        def list_cron_jobs(self):
            return [
                {
                    "name": "j1",
                    "pipeline_name": "steam_full",
                    "cron_expr": "0 8 * * *",
                    "enabled": True,
                }
            ]

        def add_cron_job(self, **kwargs):
            raise AssertionError("should not create")

        def remove_cron_job(self, name: str):
            raise AssertionError("should not delete")

    import src.web.app as app_mod

    monkeypatch.setattr(app_mod, "scheduler", _Sched(), raising=False)

    state = {
        "workflow_cron_action": "list",
        "workflow_cron_confirm": False,
        "cron_draft": {},
    }
    out = apply_cron_action_node(state)  # type: ignore[arg-type]
    assert out["cron_result"]["status"] == "success"
    assert len(out["cron_result"]["jobs"]) == 1


def test_cron_result_card_needs_confirm() -> None:
    card = build_cron_result_card(
        {
            "workflow_cron_action": "create",
            "workflow_cron_name": "steam_full_daily_0800",
            "workflow_pipeline_name": "steam_full",
            "workflow_cron_expr": "0 8 * * *",
            "cron_draft": {
                "human_schedule": "每天 08:00",
                "next_runs": ["2026-07-12T08:00:00+08:00"],
            },
            "cron_result": {
                "status": "needs_confirm",
                "action": "create",
                "summary": "待确认",
                "job_name": "steam_full_daily_0800",
                "pipeline_name": "steam_full",
                "cron_expr": "0 8 * * *",
            },
        }
    )
    assert card["type"] == "result_card"
    assert card["card_type"] == "cron"
    assert card["payload"]["status"] == "needs_confirm"
    assert any(a.get("href") == "cron" for a in card["actions"])
    assert any(a.get("id") == "copy_cron_expr" for a in card["actions"])


def test_copy_confirm_create_rematches_every_minutes_schedule() -> None:
    """needs_confirm card phrase must rematch matcher with confirm=True (non-daily)."""
    # Drive real draft path for every-N-minutes (no domain word like "daily" in slug).
    matched = _match_cron_workflow("每 15 分钟跑 pipeline:monitor")
    assert matched is not None
    assert matched["workflow_cron_expr"] == "*/15 * * * *"
    assert matched["workflow_cron_confirm"] is False

    state = {
        **matched,
        "workflow_cron_action": "create",
        "workflow_cron_name": "monitor_every_15m",
        "workflow_pipeline_name": "monitor",
        "workflow_cron_expr": "*/15 * * * *",
        "cron_result": {
            "status": "needs_confirm",
            "action": "create",
            "summary": "待确认",
            "job_name": "monitor_every_15m",
            "pipeline_name": "monitor",
            "cron_expr": "*/15 * * * *",
        },
    }
    card = build_cron_result_card(state)
    confirm_action = next(a for a in card["actions"] if a.get("id") == "copy_confirm_create")
    phrase = str(confirm_action["payload"]["text"])
    assert "确认创建" in phrase
    assert "pipeline:monitor" in phrase
    assert "*/15 * * * *" in phrase

    rematch = _match_cron_workflow(phrase)
    assert rematch is not None, f"confirm phrase did not rematch: {phrase!r}"
    assert rematch["route"] == "cron_workflow"
    assert rematch["workflow_cron_action"] == "create"
    assert rematch["workflow_cron_confirm"] is True
    assert rematch["workflow_pipeline_name"] == "monitor"
    assert rematch["workflow_cron_expr"] == "*/15 * * * *"
    assert rematch["workflow_cron_name"] == "monitor_every_15m"


def test_copy_confirm_create_rematches_custom_name() -> None:
    matched = _match_cron_workflow("每天 9 点跑 pipeline:steam_full 名称 custom_job_x")
    assert matched is not None
    state = {
        **matched,
        "cron_result": {
            "status": "needs_confirm",
            "action": "create",
            "job_name": matched["workflow_cron_name"],
            "pipeline_name": matched["workflow_pipeline_name"],
            "cron_expr": matched["workflow_cron_expr"],
        },
    }
    card = build_cron_result_card(state)
    phrase = next(a for a in card["actions"] if a.get("id") == "copy_confirm_create")[
        "payload"
    ]["text"]
    rematch = _match_cron_workflow(str(phrase))
    assert rematch is not None
    assert rematch["workflow_cron_confirm"] is True
    assert rematch["workflow_cron_name"] == "custom_job_x"
    assert rematch["workflow_cron_expr"] == "0 9 * * *"


def test_workflow_definitions_include_cron() -> None:
    _workflow_graph_definitions.cache_clear()
    defs = _workflow_graph_definitions()
    routes = {d.route for d in defs}
    assert "cron_workflow" in routes
    cron = next(d for d in defs if d.route == "cron_workflow")
    assert cron.entry_node == "resolve_cron_intent"
    assert cron.resolve("有哪些定时任务") is not None
