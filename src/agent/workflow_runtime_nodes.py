"""Runtime node helpers for Agent workflows."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Any

from src.agent.workflow_matchers import (
    _build_dynamic_pipeline_draft,
    _derive_pipeline_name,
    _last_user_text,
)
from src.agent.workflow_support import (
    _parse_tool_payload,
    _resolved_report_prompt,
    _resolved_template_name,
    _review_record_keys,
    _task_detail_data,
)
from src.agent.workflow_types import AgentWorkflowState

ToolInvoker = Callable[[dict[str, Any]], Awaitable[Any]]


def resolve_readiness_target_node(state: AgentWorkflowState) -> dict[str, Any]:
    """Normalize readiness scope / collector id from matcher state."""
    scope = str(state.get("workflow_readiness_scope") or "system").strip().lower()
    collector_id = str(state.get("workflow_collector_id") or "").strip()
    if collector_id and scope != "collector":
        scope = "collector"
    if not collector_id:
        scope = "system"
    return {
        "workflow_readiness_scope": scope,
        "workflow_collector_id": collector_id,
    }


def check_readiness_config_node(state: AgentWorkflowState) -> dict[str, Any]:
    """Run config diagnostics (no deep probe)."""
    from src.core.diagnostics import build_config_diagnostics

    try:
        payload = build_config_diagnostics()
        return {"readiness_config": {"status": "ok", **payload}}
    except Exception as exc:
        return {
            "readiness_config": {
                "status": "error",
                "summary": f"配置检查失败: {exc}",
                "checks": [],
                "error": str(exc),
            }
        }


def check_readiness_session_node(state: AgentWorkflowState) -> dict[str, Any]:
    """Run session diagnostics for one collector or session-sensitive overview."""
    from src.core.diagnostics import (
        build_collector_session_diagnostics,
        build_session_diagnostics_overview,
    )

    scope = str(state.get("workflow_readiness_scope") or "system").strip().lower()
    collector_id = str(state.get("workflow_collector_id") or "").strip()
    try:
        if scope == "collector" and collector_id:
            payload = build_collector_session_diagnostics(collector_id)
            return {"readiness_session": {"status": payload.get("status") or "ok", **payload}}
        payload = build_session_diagnostics_overview()
        return {"readiness_session": {"status": payload.get("status") or "ok", **payload}}
    except Exception as exc:
        return {
            "readiness_session": {
                "status": "error",
                "summary": f"会话检查失败: {exc}",
                "checks": [],
                "error": str(exc),
            }
        }


def resolve_cron_intent_node(state: AgentWorkflowState) -> dict[str, Any]:
    """Normalize cron action / names from matcher state."""
    action = str(state.get("workflow_cron_action") or "").strip().lower()
    if action not in {"list", "create", "delete"}:
        action = "list"
    return {
        "workflow_cron_action": action,
        "workflow_cron_name": str(state.get("workflow_cron_name") or "").strip(),
        "workflow_pipeline_name": str(state.get("workflow_pipeline_name") or "").strip(),
        "workflow_cron_confirm": bool(state.get("workflow_cron_confirm")),
        "workflow_cron_timezone": str(state.get("workflow_cron_timezone") or "").strip(),
    }


def resolve_cron_schedule_node(state: AgentWorkflowState) -> dict[str, Any]:
    """Resolve NL/raw schedule into validated expr + draft fields."""
    from src.agent.workflow_cron_parse import extract_job_name, parse_schedule
    from src.core.cron_schedule import default_timezone

    action = str(state.get("workflow_cron_action") or "").strip().lower()
    prompt = str(state.get("workflow_prompt") or "").strip()
    tz = str(state.get("workflow_cron_timezone") or "").strip() or default_timezone()

    if action != "create":
        draft = dict(state.get("cron_draft") or {}) if isinstance(state.get("cron_draft"), dict) else {}
        draft.setdefault("skipped", True)
        return {
            "cron_draft": draft,
            "workflow_cron_expr": str(state.get("workflow_cron_expr") or ""),
            "workflow_cron_timezone": tz,
        }

    schedule = parse_schedule(prompt, timezone=tz)
    expr = str(schedule.get("cron_expr") or state.get("workflow_cron_expr") or "").strip()
    pipeline = str(state.get("workflow_pipeline_name") or "").strip()
    job_name = str(state.get("workflow_cron_name") or "").strip()
    if not job_name and pipeline:
        job_name = extract_job_name(prompt, pipeline_name=pipeline, cron_expr=expr)

    issues = list(schedule.get("issues") or [])
    if not pipeline:
        issues.append("缺少 Pipeline 名称（可用 pipeline:xxx）")
    if not expr:
        if "未能解析调度时间" not in " ".join(issues):
            issues.append("缺少可解析的调度时间或 cron 表达式")

    draft = {
        "job_name": job_name,
        "pipeline_name": pipeline,
        "cron_expr": expr,
        "human_schedule": schedule.get("human_schedule") or "",
        "next_runs": list(schedule.get("next_runs") or []),
        "issues": issues,
        "timezone": schedule.get("timezone") or tz,
        "schedule_meta": schedule.get("schedule_meta") or state.get("workflow_cron_schedule_meta") or {},
    }
    return {
        "workflow_cron_expr": expr,
        "workflow_cron_name": job_name,
        "workflow_cron_timezone": str(schedule.get("timezone") or tz),
        "workflow_cron_schedule_meta": draft.get("schedule_meta") or {},
        "cron_draft": draft,
    }


def apply_cron_action_node(state: AgentWorkflowState) -> dict[str, Any]:
    """List always; create/delete only when confirm + complete."""
    action = str(state.get("workflow_cron_action") or "").strip().lower()
    confirm = bool(state.get("workflow_cron_confirm"))
    draft = state.get("cron_draft") if isinstance(state.get("cron_draft"), dict) else {}
    job_name = str(state.get("workflow_cron_name") or draft.get("job_name") or "").strip()
    pipeline = str(state.get("workflow_pipeline_name") or draft.get("pipeline_name") or "").strip()
    expr = str(state.get("workflow_cron_expr") or draft.get("cron_expr") or "").strip()
    tz = str(state.get("workflow_cron_timezone") or draft.get("timezone") or "").strip()
    meta = state.get("workflow_cron_schedule_meta")
    if not isinstance(meta, dict):
        meta = draft.get("schedule_meta") if isinstance(draft.get("schedule_meta"), dict) else {}

    try:
        if action == "list":
            return {"cron_result": _cron_list_result()}

        if action == "delete":
            if not job_name:
                return {
                    "cron_result": {
                        "status": "incomplete",
                        "action": "delete",
                        "summary": "删除定时任务需要任务名称。",
                        "issues": ["缺少定时任务名称"],
                    }
                }
            if not confirm:
                return {
                    "cron_result": {
                        "status": "needs_confirm",
                        "action": "delete",
                        "job_name": job_name,
                        "summary": f"将删除定时任务 `{job_name}`。请在同句加入「确认删除」后重试。",
                    }
                }
            return {"cron_result": _cron_delete_result(job_name)}

        # create
        issues = list(draft.get("issues") or [])
        if not pipeline or not expr or not job_name:
            return {
                "cron_result": {
                    "status": "incomplete",
                    "action": "create",
                    "job_name": job_name,
                    "pipeline_name": pipeline,
                    "cron_expr": expr,
                    "human_schedule": draft.get("human_schedule") or "",
                    "next_runs": list(draft.get("next_runs") or []),
                    "issues": issues
                    or ["创建定时任务需要名称、Pipeline 与有效调度表达式"],
                    "summary": "定时草案不完整，未创建任务。",
                }
            }
        if not confirm:
            return {
                "cron_result": {
                    "status": "needs_confirm",
                    "action": "create",
                    "job_name": job_name,
                    "pipeline_name": pipeline,
                    "cron_expr": expr,
                    "human_schedule": draft.get("human_schedule") or "",
                    "next_runs": list(draft.get("next_runs") or []),
                    "timezone": tz,
                    "summary": (
                        f"将创建定时任务 `{job_name}`：pipeline `{pipeline}`，"
                        f"调度 `{expr}`。请在同句加入「确认创建」后重试。"
                    ),
                }
            }
        return {
            "cron_result": _cron_create_result(
                name=job_name,
                pipeline_name=pipeline,
                cron_expr=expr,
                timezone=tz,
                schedule_meta=meta if isinstance(meta, dict) else {},
            )
        }
    except Exception as exc:
        return {
            "cron_result": {
                "status": "error",
                "action": action,
                "summary": f"定时操作失败: {exc}",
                "error": str(exc),
            }
        }


def _cron_list_result() -> dict[str, Any]:
    from src.web.app import scheduler

    jobs = scheduler.list_cron_jobs()
    slim = []
    for job in jobs or []:
        if not isinstance(job, dict):
            continue
        slim.append(
            {
                "name": job.get("name") or job.get("id") or "",
                "pipeline_name": job.get("pipeline_name") or "",
                "cron_expr": job.get("cron_expr") or "",
                "enabled": job.get("enabled", True),
                "human_schedule": job.get("human_schedule") or job.get("description") or "",
            }
        )
    return {
        "status": "success",
        "action": "list",
        "jobs": slim,
        "summary": f"当前共有 {len(slim)} 个定时任务。",
    }


def _cron_create_result(
    *,
    name: str,
    pipeline_name: str,
    cron_expr: str,
    timezone: str,
    schedule_meta: dict[str, Any],
) -> dict[str, Any]:
    from src.web.app import scheduler

    job_id = scheduler.add_cron_job(
        name=name,
        pipeline_name=pipeline_name,
        cron_expr=cron_expr,
        task_template={},
        timezone=timezone or None,
        schedule_meta=schedule_meta or {},
    )
    return {
        "status": "success",
        "action": "create",
        "job_id": job_id,
        "job_name": name,
        "pipeline_name": pipeline_name,
        "cron_expr": cron_expr,
        "timezone": timezone,
        "summary": f"定时任务 `{name}` 已创建。",
    }


def _cron_delete_result(name: str) -> dict[str, Any]:
    from src.web.app import scheduler

    ok = scheduler.remove_cron_job(name)
    if ok:
        return {
            "status": "success",
            "action": "delete",
            "job_name": name,
            "summary": f"定时任务 `{name}` 已删除。",
        }
    return {
        "status": "error",
        "action": "delete",
        "job_name": name,
        "summary": f"删除失败，定时任务 `{name}` 不存在。",
    }


def resolve_multisource_intent_node(state: AgentWorkflowState) -> dict[str, Any]:
    """Normalize multi-source game / collectors / confirm flags."""
    game = str(state.get("workflow_multisource_game") or "").strip()
    collectors = state.get("workflow_multisource_collectors") or []
    if not isinstance(collectors, list):
        collectors = []
    collectors = [str(c).strip() for c in collectors if str(c).strip()]
    return {
        "workflow_multisource_game": game,
        "workflow_multisource_collectors": collectors,
        "workflow_multisource_confirm": bool(state.get("workflow_multisource_confirm")),
    }


def build_multisource_draft_node(state: AgentWorkflowState) -> dict[str, Any]:
    """(Re)build pure multi-source task drafts from prompt + state."""
    from src.agent.workflow_multisource_parse import build_multisource_draft

    prompt = str(state.get("workflow_prompt") or "").strip()
    game = str(state.get("workflow_multisource_game") or "").strip() or None
    collectors = state.get("workflow_multisource_collectors")
    if not isinstance(collectors, list):
        collectors = None
    draft = build_multisource_draft(
        prompt,
        game_name=game,
        collectors=list(collectors) if collectors is not None else None,
    )
    return {
        "multisource_draft": draft,
        "workflow_multisource_game": str(draft.get("game_name") or game or ""),
        "workflow_multisource_collectors": list(draft.get("collectors") or collectors or []),
    }


async def apply_multisource_action_node(state: AgentWorkflowState) -> dict[str, Any]:
    """Create tasks only when same-turn confirm + complete drafts."""
    draft = state.get("multisource_draft") if isinstance(state.get("multisource_draft"), dict) else {}
    confirm = bool(state.get("workflow_multisource_confirm"))
    issues = list(draft.get("issues") or [])
    task_drafts = draft.get("task_drafts") if isinstance(draft.get("task_drafts"), list) else []

    if not task_drafts or issues:
        return {
            "multisource_result": {
                "status": "incomplete",
                "summary": str(draft.get("summary") or "多源采集草案不完整。"),
                "issues": issues or ["无可用任务草案"],
                "task_drafts": task_drafts,
                "created_tasks": [],
            }
        }

    if not confirm:
        return {
            "multisource_result": {
                "status": "needs_confirm",
                "summary": (
                    str(draft.get("summary") or "多源采集草案已就绪。")
                    + " 请在同句加入「确认创建」后才会提交任务。"
                ),
                "task_drafts": task_drafts,
                "created_tasks": [],
                "game_name": draft.get("game_name") or "",
                "collectors": draft.get("collectors") or [],
            }
        }

    created: list[dict[str, Any]] = []
    errors: list[str] = []
    try:
        from src.web.app import get_task_service

        ts = get_task_service()
        for item in task_drafts:
            if not isinstance(item, dict):
                continue
            name = str(item.get("name") or "").strip()
            pipeline_name = str(item.get("pipeline_name") or "").strip()
            targets = item.get("targets") if isinstance(item.get("targets"), list) else []
            collector_name = str(item.get("collector_name") or "").strip()
            config = item.get("config") if isinstance(item.get("config"), dict) else {}
            try:
                precheck = ts.precheck(
                    name=name,
                    pipeline_name=pipeline_name,
                    collector_name=collector_name,
                    targets=targets,
                )
                if not getattr(precheck, "can_submit", False):
                    issue_msgs = []
                    for issue in getattr(precheck, "issues", []) or []:
                        issue_msgs.append(getattr(issue, "message", str(issue)))
                    errors.append(
                        f"{pipeline_name}: 预检失败 "
                        + ("; ".join(issue_msgs) if issue_msgs else "不可提交")
                    )
                    continue
                task = await ts.create(
                    name=name,
                    pipeline_name=pipeline_name,
                    collector_name=collector_name,
                    targets=targets,
                    config=config,
                )
                created.append(
                    {
                        "task_id": getattr(task, "id", ""),
                        "name": name,
                        "pipeline_name": pipeline_name,
                        "collector_id": item.get("collector_id") or collector_name,
                    }
                )
            except Exception as exc:
                errors.append(f"{pipeline_name or name}: {exc}")
    except Exception as exc:
        return {
            "multisource_result": {
                "status": "error",
                "summary": f"多源任务提交失败: {exc}",
                "error": str(exc),
                "task_drafts": task_drafts,
                "created_tasks": created,
            }
        }

    if created and not errors:
        status = "success"
        summary = f"已提交 {len(created)} 个采集任务。"
    elif created and errors:
        status = "partial"
        summary = f"已提交 {len(created)} 个任务，{len(errors)} 路失败。"
    else:
        status = "error"
        summary = "多源任务均未提交成功。" + (" " + "；".join(errors[:3]) if errors else "")

    return {
        "multisource_result": {
            "status": status,
            "summary": summary,
            "created_tasks": created,
            "errors": errors,
            "task_drafts": task_drafts,
            "game_name": draft.get("game_name") or "",
            "collectors": draft.get("collectors") or [],
        }
    }


async def load_task_detail_node(
    state: AgentWorkflowState,
    *,
    invoke_task_detail_tool: ToolInvoker,
) -> dict[str, Any]:
    task_id = str(state.get("workflow_task_id") or "").strip()
    result = await invoke_task_detail_tool({"task_id": task_id})
    return {"task_detail": _parse_tool_payload(result)}


async def review_collection_results_node(
    state: AgentWorkflowState,
    *,
    invoke_review_collection_results_tool: ToolInvoker,
) -> dict[str, Any]:
    task_id = str(state.get("workflow_task_id") or "").strip()
    result = await invoke_review_collection_results_tool(
        {"task_id": task_id, "auto_retry": bool(state.get("workflow_auto_retry"))}
    )
    return {"collection_review": _parse_tool_payload(result)}


async def precheck_report_node(
    state: AgentWorkflowState,
    *,
    invoke_precheck_report_tool: ToolInvoker,
) -> dict[str, Any]:
    task_detail = _task_detail_data(state)
    review = state.get("collection_review") or {}
    template = _resolved_template_name(state, task_detail)
    prompt = _resolved_report_prompt(state, task_detail)
    result = await invoke_precheck_report_tool(
        {
            "prompt": prompt,
            "template": template,
            "record_keys": _review_record_keys(review),
        }
    )
    return {
        "workflow_template": template,
        "workflow_prompt": prompt,
        "report_precheck": _parse_tool_payload(result),
    }


async def generate_report_node(
    state: AgentWorkflowState,
    *,
    invoke_generate_report_tool: ToolInvoker,
) -> dict[str, Any]:
    review = state.get("collection_review") or {}
    template = str(state.get("workflow_template") or "general_game")
    prompt = str(state.get("workflow_prompt") or _last_user_text(state.get("messages", [])))
    result = await invoke_generate_report_tool(
        {
            "prompt": prompt,
            "template": template,
            "record_keys": _review_record_keys(review),
        }
    )
    return {"generated_report": _parse_tool_payload(result)}


async def prepare_dynamic_pipeline_node(state: AgentWorkflowState) -> dict[str, Any]:
    url = str(state.get("workflow_url") or "").strip()
    pipeline_name = str(state.get("workflow_pipeline_name") or "").strip()
    if not url:
        return {}
    prepared = _build_dynamic_pipeline_draft(url, pipeline_name or _derive_pipeline_name(url, ""))
    return {
        "workflow_url": url,
        "workflow_pipeline_name": prepared["pipeline_name"],
        "workflow_wait_strategy_type": prepared["wait_strategy_type"],
        "workflow_wait_strategy_selector": prepared["wait_strategy_selector"],
        "workflow_js_script": prepared["js_script"],
    }


async def create_dynamic_pipeline_node(
    state: AgentWorkflowState,
    *,
    invoke_create_dynamic_pipeline_tool: ToolInvoker,
) -> dict[str, Any]:
    result = await invoke_create_dynamic_pipeline_tool(
        {
            "pipeline_name": str(state.get("workflow_pipeline_name") or "").strip(),
            "url": str(state.get("workflow_url") or "").strip(),
            "wait_strategy_type": str(state.get("workflow_wait_strategy_type") or "networkidle"),
            "wait_strategy_selector": state.get("workflow_wait_strategy_selector"),
            "js_script": str(state.get("workflow_js_script") or "").strip(),
        }
    )
    return {"dynamic_pipeline_result": _parse_tool_payload(result)}
