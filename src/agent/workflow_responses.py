"""Response rendering helpers for Agent workflows."""

from __future__ import annotations

from typing import Any

from src.agent.workflow_result_cards import (
    build_pipeline_result_card,
    build_report_result_card,
    build_task_review_result_card,
)
from src.agent.workflow_support import _task_detail_data
from src.agent.workflow_types import AgentWorkflowState


def build_report_response_with_card(
    state: AgentWorkflowState,
) -> tuple[str, dict[str, Any] | None]:
    text = _build_report_response(state)
    return text, build_report_result_card(state)


def build_task_review_response_with_card(
    state: AgentWorkflowState,
) -> tuple[str, dict[str, Any] | None]:
    text = _build_task_review_response(state)
    return text, build_task_review_result_card(state)


def build_pipeline_response_with_card(
    state: AgentWorkflowState,
) -> tuple[str, dict[str, Any] | None]:
    text = _build_pipeline_response(state)
    return text, build_pipeline_result_card(state)


def _build_report_response(state: AgentWorkflowState) -> str:
    task_id = str(state.get("workflow_task_id") or "").strip()
    task_detail = state.get("task_detail") or {}
    review = state.get("collection_review") or {}
    precheck = state.get("report_precheck") or {}
    generated = state.get("generated_report") or {}

    if str(task_detail.get("status") or "").lower() != "ok":
        return _task_detail_failure_response(task_id, task_detail)

    if state.get("workflow_action") == "generate" and bool(generated.get("success")):
        title = str(generated.get("title") or generated.get("report_id") or "报告").strip()
        lines = [f"已基于任务 `{task_id}` 的采集结果生成报告《{title}》。"]
        report_id = str(generated.get("report_id") or "").strip()
        if report_id:
            lines.append(f"报告 ID：`{report_id}`")
        download_url = str(generated.get("download_url") or "").strip()
        if download_url:
            lines.append(f"下载地址：`{download_url}`")
        quality_status = _report_status_label(str(generated.get("quality_status") or ""))
        if quality_status:
            lines.append(f"质量状态：{quality_status}")
        missing_collectors = _join_collectors(generated.get("missing_collectors"))
        if missing_collectors:
            lines.append(f"仍有缺失数据源：{missing_collectors}")
        next_action = _format_next_action(generated.get("next_best_action"))
        _append_prefixed_line(lines, "后续建议：", next_action)
        return "\n".join(lines)

    if precheck:
        lines = [_format_precheck_summary(task_id, precheck)]
        next_action = _format_next_action(precheck.get("next_best_action"))
        _append_prefixed_line(lines, "下一步建议：", next_action)
        if state.get("workflow_action") == "generate" and not bool(precheck.get("can_generate")):
            lines.append("当前不建议直接生成报告，建议先补齐关键采集源后再重试。")
        _append_line(lines, _review_summary_line(review))
        return "\n".join(line for line in lines if line)

    lines = [_review_only_summary(task_id, review, _task_detail_data(state))]
    _append_line(lines, _task_recommended_action_text(_task_detail_data(state)))
    return "\n".join(line for line in lines if line)


def _build_task_review_response(state: AgentWorkflowState) -> str:
    task_id = str(state.get("workflow_task_id") or "").strip()
    task_detail = state.get("task_detail") or {}
    review = state.get("collection_review") or {}
    auto_retry = bool(state.get("workflow_auto_retry"))

    if str(task_detail.get("status") or "").lower() != "ok":
        return _task_detail_failure_response(task_id, task_detail)

    if not review:
        return _task_detail_checked_response(task_id, _task_detail_data(state))

    lines = [_format_task_review_summary(task_id, review)]
    _append_line(lines, _task_review_issue_line(review))

    if auto_retry:
        lines.append(_auto_retry_summary_line(review))

    _append_line(lines, _task_review_suggestion_line(review))

    if not auto_retry:
        _append_line(lines, _task_recommended_action_text(_task_detail_data(state)))

    return "\n".join(line for line in lines if line)


def _build_pipeline_response(state: AgentWorkflowState) -> str:
    url = str(state.get("workflow_url") or "").strip()
    pipeline_name = str(state.get("workflow_pipeline_name") or "").strip()
    result = state.get("dynamic_pipeline_result") or {}

    if not url:
        return "未识别到可用于创建动态采集 Pipeline 的网页地址。"

    if result:
        status = str(result.get("status") or "").lower()
        summary = str(result.get("summary") or "").strip()
        data = result.get("data")
        data = data if isinstance(data, dict) else {}
        if status == "ok":
            lines = [
                summary or f"已为 `{url}` 创建动态采集 Pipeline `{pipeline_name}`。",
                f"Pipeline 名称：`{data.get('pipeline_name') or pipeline_name}`",
                f"目标 URL：`{url}`",
                f"等待策略：`{state.get('workflow_wait_strategy_type') or 'networkidle'}`",
                "可直接继续使用 `create_task` 配合该 pipeline 发起采集任务。",
            ]
            return "\n".join(lines)
        if summary:
            return f"为 `{url}` 创建动态采集 Pipeline 失败：{summary}"

    lines = [
        f"已为 `{url}` 准备动态采集 Pipeline 草案：`{pipeline_name}`。",
        f"等待策略：`{state.get('workflow_wait_strategy_type') or 'networkidle'}`",
        "当前使用的是通用页面提取脚本草案，可按目标站点结构继续细化。",
    ]
    return "\n".join(lines)


def _format_precheck_summary(task_id: str, payload: dict[str, Any]) -> str:
    status_label = _report_status_label(str(payload.get("status") or ""))
    selected_records = int(payload.get("selected_records") or 0)
    usable_records = int(payload.get("usable_records") or 0)
    can_generate = bool(payload.get("can_generate"))
    should_collect_more = bool(payload.get("should_collect_more"))
    missing_collectors = _join_collectors(payload.get("missing_collectors"))

    if can_generate and should_collect_more:
        readiness = "可以先生成，但建议继续补采以提升覆盖度"
    elif can_generate:
        readiness = "可以直接生成报告"
    else:
        readiness = "暂不建议生成报告"

    summary = (
        f"任务 `{task_id}` 的报告预检结果：{status_label}。"
        f" 已选 {selected_records} 条源记录，可用 {usable_records} 条，当前{readiness}。"
    )
    if missing_collectors:
        summary += f" 缺失数据源：{missing_collectors}。"
    return summary


def _format_task_review_summary(task_id: str, review: dict[str, Any]) -> str:
    completeness = _collection_status_label(str(review.get("completeness") or "unknown"))
    record_count = int(review.get("record_count") or 0)
    source_coverage = review.get("source_coverage")
    source_count = len(source_coverage) if isinstance(source_coverage, dict) else 0
    summary = f"任务 `{task_id}` 的采集复查结果：{completeness}。"
    if record_count > 0:
        summary += f" 当前找到 {record_count} 条源记录"
        if source_count > 0:
            summary += f"，覆盖 {source_count} 个数据源"
        summary += "。"
    return summary


def _append_line(lines: list[str], value: str) -> None:
    if value:
        lines.append(value)


def _append_prefixed_line(lines: list[str], prefix: str, value: str) -> None:
    if value:
        lines.append(f"{prefix}{value}")


def _task_detail_failure_response(task_id: str, task_detail: dict[str, Any]) -> str:
    summary = str(task_detail.get("summary") or task_detail.get("error") or "任务详情读取失败")
    return f"未能读取任务 `{task_id}` 的详情：{summary}"


def _task_detail_checked_response(task_id: str, task_detail_data: dict[str, Any]) -> str:
    guidance = str(task_detail_data.get("agent_guidance") or "").strip()
    if guidance:
        return f"任务 `{task_id}` 已完成详情检查。{guidance}"
    return f"任务 `{task_id}` 已完成详情检查，但暂时没有更多复查信息。"


def _auto_retry_summary_line(review: dict[str, Any]) -> str:
    retry_created = review.get("retry_created")
    retry_task_id = str(review.get("retry_task_id") or "").strip()
    retry_task_name = str(review.get("retry_task_name") or "").strip()
    retry_error = str(review.get("retry_error") or "").strip()
    if retry_created and retry_task_id:
        return (
            f"已自动创建重试任务 `{retry_task_id}`"
            + (f"（{retry_task_name}）" if retry_task_name else "")
            + "。"
        )
    if retry_error:
        return f"自动重试未能创建成功：{retry_error}"
    return "复查未发现需要自动重试的问题，因此没有创建新的重试任务。"


def _review_only_summary(
    task_id: str,
    review: dict[str, Any],
    task_detail_data: dict[str, Any],
) -> str:
    record_count = int(review.get("record_count") or 0)
    completeness = _collection_status_label(str(review.get("completeness") or "unknown"))
    if record_count > 0:
        return (
            f"任务 `{task_id}` 当前已找到 {record_count} 条源记录，"
            f"采集完整度为{completeness}，可以继续做报告预检。"
        )

    suggestions = [str(item).strip() for item in review.get("suggestions", []) if str(item).strip()]
    if suggestions:
        return f"任务 `{task_id}` 目前还没有可用于报告的源记录。建议先处理：{suggestions[0]}"

    guidance = str(task_detail_data.get("agent_guidance") or "").strip()
    if guidance:
        return f"任务 `{task_id}` 目前还没有可用于报告的源记录。{guidance}"
    return f"任务 `{task_id}` 目前还没有可用于报告的源记录。"


def _review_summary_line(review: dict[str, Any]) -> str:
    suggestions = [str(item).strip() for item in review.get("suggestions", []) if str(item).strip()]
    if suggestions:
        return f"复查建议：{suggestions[0]}"
    return ""


def _task_review_issue_line(review: dict[str, Any]) -> str:
    issues = review.get("issues")
    if not isinstance(issues, list) or not issues:
        return ""
    prioritized = sorted(
        [item for item in issues if isinstance(item, dict)],
        key=lambda item: {"error": 0, "warning": 1, "info": 2}.get(str(item.get("level")), 3),
    )
    if not prioritized:
        return ""
    first = prioritized[0]
    message = str(first.get("message") or "").strip()
    if not message:
        return ""
    return f"主要发现：{message}"


def _task_review_suggestion_line(review: dict[str, Any]) -> str:
    suggestions = review.get("suggestions")
    if not isinstance(suggestions, list):
        return ""
    items = [str(item).strip() for item in suggestions if str(item).strip()]
    if not items:
        return ""
    return f"建议：{items[0]}"


def _task_recommended_action_text(task_detail_data: dict[str, Any]) -> str:
    actions = task_detail_data.get("recommended_actions")
    if not isinstance(actions, list) or not actions:
        return ""
    ordered = [
        str(action.get("recommended_tool") or "").strip()
        for action in actions
        if isinstance(action, dict)
    ]
    ordered = [tool_name for tool_name in ordered if tool_name]
    if not ordered:
        return ""
    return "建议顺序：" + " -> ".join(ordered)


def _format_next_action(action: Any) -> str:
    if not isinstance(action, dict):
        return ""
    collector = str(
        action.get("collector_label") or action.get("collector") or action.get("type") or ""
    ).strip()
    sequence = action.get("recommended_sequence")
    if not isinstance(sequence, list):
        sequence = []
    steps = " -> ".join(str(item).strip() for item in sequence if str(item).strip())
    if collector and steps:
        return f"优先处理 {collector}，推荐顺序：{steps}"
    if collector:
        return f"优先处理 {collector}"
    return steps


def _report_status_label(status: str) -> str:
    normalized = str(status or "").strip().lower()
    mapping = {
        "complete": "完整可用",
        "partial": "部分可用",
        "empty": "暂无有效数据",
        "unknown": "状态未知",
    }
    return mapping.get(normalized, normalized or "状态未知")


def _collection_status_label(status: str) -> str:
    normalized = str(status or "").strip().lower()
    mapping = {
        "full": "完整",
        "partial": "部分可用",
        "empty": "缺少有效数据",
        "unknown": "未知",
    }
    return mapping.get(normalized, normalized or "未知")


def _join_collectors(values: Any) -> str:
    if not isinstance(values, list):
        return ""
    items = [str(item).strip() for item in values if str(item).strip()]
    return ", ".join(items)
