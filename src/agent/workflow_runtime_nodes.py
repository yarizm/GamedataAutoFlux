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
