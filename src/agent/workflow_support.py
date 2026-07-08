"""Shared payload and state helpers for Agent workflows."""

from __future__ import annotations

import json
from typing import Any

from src.agent.workflow_types import AgentWorkflowState

_TEMPLATE_NAMES = ("general_game", "steam_game", "taptap_game")


def _parse_tool_payload(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except (TypeError, ValueError):
            return {"success": False, "error": str(value)}
        if isinstance(parsed, dict):
            return parsed
        return {"success": True, "data": parsed}
    return {"success": False, "error": str(value)}


def _task_detail_data(state: AgentWorkflowState) -> dict[str, Any]:
    payload = state.get("task_detail") or {}
    data = payload.get("data")
    return data if isinstance(data, dict) else {}


def _review_record_keys(review_payload: Any) -> list[str]:
    if not isinstance(review_payload, dict):
        return []
    record_summaries = review_payload.get("record_summaries")
    if not isinstance(record_summaries, list):
        return []
    keys = []
    for item in record_summaries:
        if not isinstance(item, dict):
            continue
        key = str(item.get("key") or "").strip()
        if key:
            keys.append(key)
    return keys


def _resolved_template_name(
    state: AgentWorkflowState,
    task_detail_data: dict[str, Any],
) -> str:
    template = str(state.get("workflow_template") or "").strip().lower()
    if template in _TEMPLATE_NAMES:
        return template
    collector_name = str(task_detail_data.get("collector_name") or "").strip().lower()
    if collector_name in {"steam", "steam_discussions"}:
        return "steam_game"
    if collector_name == "taptap":
        return "taptap_game"
    return "general_game"


def _resolved_report_prompt(
    state: AgentWorkflowState,
    task_detail_data: dict[str, Any],
) -> str:
    task_name = str(
        task_detail_data.get("name") or state.get("workflow_task_id") or "当前任务"
    ).strip()
    return (
        f"请基于任务 {task_name} 的采集结果生成一份数据分析报告，"
        "总结核心指标、趋势变化、用户反馈和潜在风险。"
    )
