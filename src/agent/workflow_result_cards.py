"""Structured result_card builders for Agent workflows."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from src.agent.workflow_events import result_card_event


def build_report_result_card(state: Mapping[str, Any]) -> dict[str, Any]:
    """Build a result_card event for the report workflow."""
    task_id = str(state.get("workflow_task_id") or "").strip()
    task_detail = state.get("task_detail") or {}
    review = state.get("collection_review") or {}
    precheck = state.get("report_precheck") or {}
    generated = state.get("generated_report") or {}

    if str(task_detail.get("status") or "").lower() != "ok":
        summary = str(
            task_detail.get("summary") or task_detail.get("error") or "任务详情读取失败"
        )
        return result_card_event(
            "report",
            "任务详情不可用",
            summary,
            actions=_task_navigate_actions(task_id),
            payload=_base_payload(task_id, status="error"),
        )

    if state.get("workflow_action") == "generate" and bool(generated.get("success")):
        title = str(generated.get("title") or generated.get("report_id") or "报告").strip()
        report_id = str(generated.get("report_id") or "").strip()
        download_url = str(generated.get("download_url") or "").strip()
        summary = f"已基于任务 {task_id or '当前任务'} 生成报告《{title}》。"
        actions = [
            _navigate_action("open_reports", "打开报告", "reports"),
            *_task_navigate_actions(task_id),
        ]
        if download_url:
            actions.append(
                {
                    "id": "copy_download_url",
                    "label": "复制下载链接",
                    "kind": "copy",
                    "payload": {"text": download_url},
                }
            )
        return result_card_event(
            "report",
            "报告已生成",
            summary,
            actions=actions,
            payload=_compact_payload(
                {
                    **_base_payload(task_id, status="success"),
                    "report_id": report_id,
                    "download_url": download_url,
                    "title": title,
                }
            ),
        )

    if precheck:
        can_generate = bool(precheck.get("can_generate"))
        status = str(precheck.get("status") or "").strip() or (
            "ready" if can_generate else "blocked"
        )
        if can_generate:
            title = "报告预检通过"
            summary = f"任务 {task_id or '当前任务'} 可生成报告。"
        else:
            title = "报告预检未通过"
            summary = f"任务 {task_id or '当前任务'} 暂不建议直接生成报告。"
        actions = [
            *_task_navigate_actions(task_id),
            _navigate_action("open_reports", "查看报告", "reports"),
        ]
        draft_phrase = _report_collect_draft_phrase(precheck)
        if draft_phrase:
            actions.append(
                {
                    "id": "copy_collect_draft",
                    "label": "复制补采指令",
                    "kind": "copy",
                    "payload": {"text": draft_phrase},
                }
            )
            if not can_generate:
                summary += " 可复制补采指令继续采集缺口数据源。"
        next_action = precheck.get("next_best_action")
        if isinstance(next_action, dict) and next_action.get("collector_label"):
            summary += f" 优先处理：{next_action.get('collector_label')}。"
        return result_card_event(
            "report",
            title,
            summary,
            actions=actions,
            payload=_compact_payload(
                {
                    **_base_payload(task_id, status=status),
                    "can_generate": can_generate,
                    "missing_collectors": precheck.get("missing_collectors"),
                    "collect_draft_phrase": draft_phrase or None,
                }
            ),
        )

    record_count = int(review.get("record_count") or 0) if isinstance(review, dict) else 0
    completeness = (
        str(review.get("completeness") or "unknown") if isinstance(review, dict) else "unknown"
    )
    summary = (
        f"任务 {task_id or '当前任务'} 已复查采集结果"
        + (f"，{record_count} 条记录" if record_count else "")
        + f"（完整度：{completeness}）。"
    )
    return result_card_event(
        "report",
        "采集复查完成",
        summary,
        actions=_task_navigate_actions(task_id),
        payload={
            **_base_payload(task_id, status=completeness),
            "record_count": record_count,
        },
    )


def build_task_review_result_card(state: Mapping[str, Any]) -> dict[str, Any]:
    """Build a result_card event for the task review workflow."""
    task_id = str(state.get("workflow_task_id") or "").strip()
    task_detail = state.get("task_detail") or {}
    review = state.get("collection_review") or {}
    auto_retry = bool(state.get("workflow_auto_retry"))

    if str(task_detail.get("status") or "").lower() != "ok":
        summary = str(
            task_detail.get("summary") or task_detail.get("error") or "任务详情读取失败"
        )
        return result_card_event(
            "task_review",
            "任务详情不可用",
            summary,
            actions=_task_navigate_actions(task_id),
            payload=_base_payload(task_id, status="error"),
        )

    if not review:
        return result_card_event(
            "task_review",
            "任务详情已检查",
            f"任务 {task_id or '当前任务'} 已完成详情检查。",
            actions=_task_navigate_actions(task_id),
            payload=_base_payload(task_id, status="checked"),
        )

    completeness = str(review.get("completeness") or "unknown")
    record_count = int(review.get("record_count") or 0)
    issues = review.get("issues") if isinstance(review.get("issues"), list) else []
    summary = (
        f"任务 {task_id or '当前任务'} 采集复查：{completeness}"
        + (f"，{record_count} 条记录" if record_count else "")
        + "。"
    )
    payload: dict[str, Any] = {
        **_base_payload(task_id, status=completeness),
        "record_count": record_count,
        "issues": issues,
        "auto_retry": auto_retry,
    }
    actions = list(_task_navigate_actions(task_id))
    retry_task_id = str(review.get("retry_task_id") or "").strip()
    retry_error = str(review.get("retry_error") or "").strip()
    if retry_task_id:
        payload["retry_task_id"] = retry_task_id
        summary += f" 已创建重试任务 {retry_task_id}。"
        actions.append(
            {
                "id": "copy_retry_task_id",
                "label": "复制重试任务 ID",
                "kind": "copy",
                "payload": {"text": retry_task_id},
            }
        )
    elif auto_retry and retry_error:
        summary += f" 自动重试失败：{retry_error}"
        retry_phrase = f"对任务 task:{task_id} 自动重试" if task_id else "请带 task_id 重试诊断"
        actions.append(
            {
                "id": "copy_retry_phrase",
                "label": "复制重试指令",
                "kind": "copy",
                "payload": {"text": retry_phrase},
            }
        )
        payload["retry_error"] = retry_error
    elif not auto_retry and completeness in {"partial", "empty", "failed", "unknown"}:
        retry_phrase = f"对任务 task:{task_id} 自动重试" if task_id else ""
        if retry_phrase:
            actions.append(
                {
                    "id": "copy_retry_phrase",
                    "label": "复制重试指令",
                    "kind": "copy",
                    "payload": {"text": retry_phrase},
                }
            )
            payload["retry_phrase"] = retry_phrase

    return result_card_event(
        "task_review",
        "任务复查完成",
        summary,
        actions=actions,
        payload=payload,
    )


def build_pipeline_result_card(state: Mapping[str, Any]) -> dict[str, Any]:
    """Build a result_card event for the dynamic pipeline workflow."""
    url = str(state.get("workflow_url") or "").strip()
    pipeline_name = str(state.get("workflow_pipeline_name") or "").strip()
    result = state.get("dynamic_pipeline_result") or {}

    if not url:
        return result_card_event(
            "dynamic_pipeline",
            "未识别到 URL",
            "未识别到可用于创建动态采集 Pipeline 的网页地址。",
            actions=[_navigate_action("open_pipelines", "打开 Pipeline", "pipelines")],
            payload={"status": "error"},
        )

    data = result.get("data") if isinstance(result, dict) else None
    data = data if isinstance(data, dict) else {}
    resolved_name = str(data.get("pipeline_name") or pipeline_name or "").strip()

    run_phrase = _create_task_phrase(
        pipeline_name=resolved_name or pipeline_name,
        task_name=f"run_{(resolved_name or pipeline_name or 'dynamic')[:40]}",
        targets_hint=url,
    )

    if result:
        status = str(result.get("status") or "").lower()
        summary = str(result.get("summary") or "").strip()
        if status == "ok":
            actions = [
                _navigate_action("open_pipelines", "打开 Pipeline", "pipelines"),
                _navigate_action("open_tasks", "打开任务", "tasks"),
            ]
            if run_phrase:
                actions.append(
                    {
                        "id": "copy_create_task",
                        "label": "复制创建任务指令",
                        "kind": "copy",
                        "payload": {"text": run_phrase},
                    }
                )
                summary = (
                    summary
                    or f"已为 {url} 创建动态采集 Pipeline {resolved_name}。"
                ) + " 可复制创建任务指令立即开跑。"
            return result_card_event(
                "dynamic_pipeline",
                "Pipeline 已创建",
                summary or f"已为 {url} 创建动态采集 Pipeline {resolved_name}。",
                actions=actions,
                payload=_compact_payload(
                    {
                        "status": "success",
                        "pipeline_name": resolved_name,
                        "url": url,
                        "create_task_phrase": run_phrase or None,
                    }
                ),
            )
        return result_card_event(
            "dynamic_pipeline",
            "Pipeline 创建失败",
            summary or f"为 {url} 创建动态采集 Pipeline 失败。",
            actions=[_navigate_action("open_pipelines", "打开 Pipeline", "pipelines")],
            payload=_compact_payload(
                {
                    "status": "error",
                    "pipeline_name": resolved_name,
                    "url": url,
                }
            ),
        )

    draft_actions = [_navigate_action("open_pipelines", "打开 Pipeline", "pipelines")]
    if run_phrase:
        draft_actions.append(
            {
                "id": "copy_create_task",
                "label": "复制创建任务指令",
                "kind": "copy",
                "payload": {"text": run_phrase},
            }
        )
    return result_card_event(
        "dynamic_pipeline",
        "Pipeline 草案已准备",
        f"已为 {url} 准备动态采集 Pipeline 草案：{resolved_name or pipeline_name or '未命名'}。",
        actions=draft_actions,
        payload=_compact_payload(
            {
                "status": "draft",
                "pipeline_name": resolved_name or pipeline_name,
                "url": url,
                "create_task_phrase": run_phrase or None,
            }
        ),
    )


def build_multisource_result_card(state: Mapping[str, Any]) -> dict[str, Any]:
    """Build result_card for multisource_workflow."""
    draft = state.get("multisource_draft") if isinstance(state.get("multisource_draft"), dict) else {}
    result = (
        state.get("multisource_result") if isinstance(state.get("multisource_result"), dict) else {}
    )
    status = str(result.get("status") or draft.get("status") or "error").strip().lower()
    game = str(
        result.get("game_name")
        or state.get("workflow_multisource_game")
        or draft.get("game_name")
        or ""
    ).strip()
    collectors = (
        result.get("collectors")
        or state.get("workflow_multisource_collectors")
        or draft.get("collectors")
        or []
    )
    if not isinstance(collectors, list):
        collectors = []
    task_drafts = result.get("task_drafts") or draft.get("task_drafts") or []
    if not isinstance(task_drafts, list):
        task_drafts = []
    created = result.get("created_tasks") if isinstance(result.get("created_tasks"), list) else []
    issues = result.get("issues") or draft.get("issues") or []
    if not isinstance(issues, list):
        issues = []

    title_map = {
        "success": "多源任务已提交",
        "partial": "多源任务部分提交",
        "needs_confirm": "待确认多源采集",
        "incomplete": "多源草案不完整",
        "draft": "多源采集草案",
        "error": "多源采集失败",
    }
    title = title_map.get(status, "多源采集")
    summary = str(result.get("summary") or draft.get("summary") or "").strip() or title

    actions = [
        _navigate_action("open_tasks", "打开任务", "tasks"),
        _navigate_action("open_pipelines", "打开 Pipeline", "pipelines"),
    ]
    from src.agent.workflow_multisource_parse import draft_to_confirm_phrase

    if status in {"needs_confirm", "draft", "incomplete"}:
        phrase = draft_to_confirm_phrase(
            {
                "game_name": game,
                "collectors": collectors,
            }
        )
        if phrase:
            actions.append(
                {
                    "id": "copy_confirm_multisource",
                    "label": "复制确认句",
                    "kind": "copy",
                    "payload": {"text": phrase},
                }
            )
    if created:
        ids = ", ".join(
            str(t.get("task_id") or "") for t in created if isinstance(t, dict) and t.get("task_id")
        )
        if ids:
            actions.append(
                {
                    "id": "copy_task_ids",
                    "label": "复制任务 ID",
                    "kind": "copy",
                    "payload": {"text": ids},
                }
            )

    slim_drafts = []
    for item in task_drafts[:12]:
        if not isinstance(item, dict):
            continue
        slim_drafts.append(
            {
                "collector_id": item.get("collector_id") or "",
                "pipeline_name": item.get("pipeline_name") or "",
                "name": item.get("name") or "",
            }
        )

    return result_card_event(
        "multisource",
        title,
        summary,
        actions=actions,
        payload=_compact_payload(
            {
                "status": status,
                "game_name": game or None,
                "collectors": collectors or None,
                "task_drafts": slim_drafts or None,
                "created_tasks": created or None,
                "issues": [str(i) for i in issues[:12]] or None,
            }
        ),
    )


def build_cron_result_card(state: Mapping[str, Any]) -> dict[str, Any]:
    """Build result_card for cron_workflow."""
    action = str(state.get("workflow_cron_action") or "").strip().lower() or "list"
    result = state.get("cron_result") if isinstance(state.get("cron_result"), dict) else {}
    draft = state.get("cron_draft") if isinstance(state.get("cron_draft"), dict) else {}
    status = str(result.get("status") or "").strip().lower() or "error"

    job_name = str(
        result.get("job_name") or state.get("workflow_cron_name") or draft.get("job_name") or ""
    ).strip()
    pipeline_name = str(
        result.get("pipeline_name")
        or state.get("workflow_pipeline_name")
        or draft.get("pipeline_name")
        or ""
    ).strip()
    cron_expr = str(
        result.get("cron_expr") or state.get("workflow_cron_expr") or draft.get("cron_expr") or ""
    ).strip()
    human = str(result.get("human_schedule") or draft.get("human_schedule") or "").strip()
    next_runs = result.get("next_runs") or draft.get("next_runs") or []
    if not isinstance(next_runs, list):
        next_runs = []
    issues = result.get("issues") or draft.get("issues") or []
    if not isinstance(issues, list):
        issues = []
    jobs = result.get("jobs") if isinstance(result.get("jobs"), list) else []

    title_map = {
        ("list", "success"): "定时任务列表",
        ("create", "success"): "定时任务已创建",
        ("create", "needs_confirm"): "待确认创建",
        ("create", "incomplete"): "定时草案不完整",
        ("delete", "success"): "定时任务已删除",
        ("delete", "needs_confirm"): "待确认删除",
        ("delete", "incomplete"): "删除信息不完整",
        ("create", "error"): "定时操作失败",
        ("delete", "error"): "定时操作失败",
        ("list", "error"): "定时操作失败",
    }
    title = title_map.get((action, status), "定时任务")
    if status == "error" and (action, status) not in title_map:
        title = "定时操作失败"

    summary = str(result.get("summary") or "").strip()
    if not summary:
        if action == "list":
            summary = f"当前共有 {len(jobs)} 个定时任务。"
        elif status == "needs_confirm" and action == "create":
            summary = f"已解析调度草案，等待同句「确认创建」：`{job_name or '未命名'}`。"
        elif status == "incomplete":
            summary = "定时草案不完整，请补充 Pipeline 与调度时间。"
        else:
            summary = "定时任务处理完成。"

    actions = [_navigate_action("open_cron", "打开定时任务", "cron")]
    if cron_expr:
        actions.append(
            {
                "id": "copy_cron_expr",
                "label": "复制 Cron",
                "kind": "copy",
                "payload": {"text": cron_expr},
            }
        )
    if status == "needs_confirm" and action == "create" and pipeline_name and cron_expr:
        # Phrase must rematch _match_cron_workflow (confirm create + pipeline + raw expr).
        confirm_text = (
            f"确认创建 pipeline:{pipeline_name} {cron_expr}"
            + (f" 名称 {job_name}" if job_name else "")
        )
        actions.append(
            {
                "id": "copy_confirm_create",
                "label": "复制确认句",
                "kind": "copy",
                "payload": {"text": confirm_text},
            }
        )
    if status == "needs_confirm" and action == "delete" and job_name:
        actions.append(
            {
                "id": "copy_confirm_delete",
                "label": "复制确认句",
                "kind": "copy",
                "payload": {"text": f"确认删除定时任务 {job_name}"},
            }
        )

    return result_card_event(
        "cron",
        title,
        summary,
        actions=actions,
        payload=_compact_payload(
            {
                "action": action,
                "status": status,
                "job_name": job_name or None,
                "pipeline_name": pipeline_name or None,
                "cron_expr": cron_expr or None,
                "human_schedule": human or None,
                "next_runs": next_runs[:5] or None,
                "jobs": jobs[:30] or None,
                "issues": [str(i) for i in issues[:12]] or None,
            }
        ),
    )


def build_readiness_result_card(state: Mapping[str, Any]) -> dict[str, Any]:
    """Build result_card for readiness_workflow."""
    scope = str(state.get("workflow_readiness_scope") or "system").strip().lower()
    collector_id = str(state.get("workflow_collector_id") or "").strip()
    config = state.get("readiness_config") or {}
    session = state.get("readiness_session") or {}
    note = str(state.get("workflow_readiness_note") or "").strip()

    checks: list[dict[str, Any]] = []
    if isinstance(config.get("checks"), list):
        checks.extend([c for c in config["checks"] if isinstance(c, dict)])
    if isinstance(session.get("checks"), list):
        checks.extend([c for c in session["checks"] if isinstance(c, dict)])

    health = _merge_health_statuses(
        str(config.get("status") or ""),
        str(session.get("status") or ""),
        checks=checks,
    )
    blocking = _collect_check_messages(checks, levels=("error",))
    warnings = _collect_check_messages(checks, levels=("warning", "warn"))

    if scope == "collector" and collector_id:
        title = f"{collector_id} 采集就绪"
        if health == "ok":
            summary = f"采集器 `{collector_id}` 配置与会话检查通过，可尝试提交采集任务。"
        elif health == "warning":
            summary = f"采集器 `{collector_id}` 可运行但存在需关注项（共 {len(warnings)} 条）。"
        else:
            summary = f"采集器 `{collector_id}` 当前存在阻塞问题（共 {len(blocking)} 条），不建议直接采集。"
    else:
        title = "系统就绪摘要"
        if health == "ok":
            summary = "系统配置与会话敏感采集源检查通过。"
        elif health == "warning":
            summary = f"系统总体可用，但有 {len(warnings)} 项需关注。"
        else:
            summary = f"系统检查发现 {len(blocking)} 项阻塞问题。"

    if note:
        summary = f"{summary} {note}"

    summary += " 深度探测不会在此自动执行；如需 live probe 请到「系统检查」页开启。"

    actions = [
        _navigate_action("open_system", "系统检查", "system"),
    ]
    if blocking:
        actions.append(
            {
                "id": "copy_blocking",
                "label": "复制阻塞项",
                "kind": "copy",
                "payload": {"text": "\n".join(blocking[:8])},
            }
        )

    slim_checks = []
    for c in checks[:12]:
        slim_checks.append(
            {
                "id": c.get("id") or c.get("name") or "",
                "status": c.get("status") or "",
                "message": c.get("message") or c.get("summary") or "",
            }
        )

    return result_card_event(
        "readiness",
        title,
        summary,
        actions=actions,
        payload=_compact_payload(
            {
                "scope": scope if scope in ("collector", "system") else "system",
                "collector_id": collector_id or None,
                "health": health,
                "blocking": blocking[:12],
                "warnings": warnings[:12],
                "checks": slim_checks,
            }
        ),
    )


def _merge_health_statuses(
    *statuses: str,
    checks: list[dict[str, Any]] | None = None,
) -> str:
    levels = {str(s).lower() for s in statuses if s}
    if checks:
        for c in checks:
            levels.add(str(c.get("status") or "").lower())
    if "error" in levels or "failed" in levels:
        return "error"
    if "warning" in levels or "warn" in levels:
        return "warning"
    return "ok"


def _collect_check_messages(
    checks: list[dict[str, Any]],
    *,
    levels: tuple[str, ...],
) -> list[str]:
    want = {x.lower() for x in levels}
    out: list[str] = []
    for c in checks:
        st = str(c.get("status") or "").lower()
        if st not in want:
            continue
        msg = str(c.get("message") or c.get("summary") or "").strip()
        if msg:
            out.append(msg)
    return out


def _navigate_action(action_id: str, label: str, href: str) -> dict[str, Any]:
    return {
        "id": action_id,
        "label": label,
        "kind": "navigate",
        "href": href,
    }


def _report_collect_draft_phrase(precheck: Mapping[str, Any]) -> str:
    """Build a copyable multi-source / create-task guidance phrase from precheck."""
    next_action = precheck.get("next_best_action")
    if not isinstance(next_action, dict):
        # fall back to first recommended action with draft
        actions = precheck.get("recommended_actions") or precheck.get("source_actions") or []
        if isinstance(actions, list):
            for item in actions:
                if isinstance(item, dict) and item.get("create_task_draft"):
                    next_action = item
                    break
    if not isinstance(next_action, dict):
        missing = precheck.get("missing_collectors") or []
        if isinstance(missing, list) and missing:
            labels = ", ".join(str(m) for m in missing[:4] if str(m).strip())
            if labels:
                return f"多源采集 补齐数据源 {labels}"
        return ""

    draft = next_action.get("create_task_draft")
    if isinstance(draft, dict) and draft.get("pipeline_name"):
        name = str(draft.get("name") or "补采任务").strip()
        pipeline = str(draft.get("pipeline_name") or "").strip()
        targets = draft.get("targets") if isinstance(draft.get("targets"), list) else []
        game = ""
        if targets and isinstance(targets[0], dict):
            game = str(targets[0].get("name") or "").strip()
        parts = [f"创建任务 pipeline:{pipeline}"]
        if game:
            parts.append(f"游戏 {game}")
        if name:
            parts.append(f"名称 {name}")
        return " ".join(parts)

    collector = str(
        next_action.get("collector") or next_action.get("collector_label") or ""
    ).strip()
    pipeline = str(next_action.get("pipeline_name") or "").strip()
    if collector and pipeline:
        return f"多源采集 {collector} pipeline:{pipeline}"
    if collector:
        return f"多源采集 {collector}"
    return ""


def _create_task_phrase(
    *,
    pipeline_name: str,
    task_name: str = "",
    targets_hint: str = "",
) -> str:
    pipeline = str(pipeline_name or "").strip()
    if not pipeline:
        return ""
    parts = [f"创建任务 pipeline:{pipeline}"]
    if task_name:
        parts.append(f"名称 {task_name}")
    if targets_hint:
        parts.append(f"目标 {targets_hint}")
    parts.append("（可在任务页提交或让 Agent 执行 create_task）")
    return " ".join(parts)


def _task_navigate_actions(task_id: str) -> list[dict[str, Any]]:
    # Always offer tasks navigation when relevant; task_id is optional for UI routing.
    _ = task_id
    return [_navigate_action("open_task", "查看任务", "tasks")]


def _base_payload(task_id: str, *, status: str) -> dict[str, Any]:
    payload: dict[str, Any] = {"status": status}
    if task_id:
        payload["task_id"] = task_id
    return payload


def _compact_payload(payload: dict[str, Any]) -> dict[str, Any]:
    return {key: value for key, value in payload.items() if value is not None and value != ""}
