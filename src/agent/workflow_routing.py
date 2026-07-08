"""Routing helpers for Agent workflows."""

from __future__ import annotations

from typing import Any, Literal, cast

from src.agent.workflow_matchers import _last_user_text
from src.agent.workflow_support import _review_record_keys
from src.agent.workflow_types import AgentWorkflowState, WorkflowGraphDefinition, WorkflowRoute


def resolve_workflow_request(
    user_text: str,
    definitions: tuple[WorkflowGraphDefinition, ...],
    *,
    fallback_state: dict[str, Any],
) -> dict[str, Any]:
    for definition in definitions:
        matched_state = definition.resolve(user_text)
        if matched_state is not None:
            return matched_state
    return fallback_state


def route_request(
    state: AgentWorkflowState,
    *,
    resolve_request: Any,
) -> dict[str, Any]:
    user_text = _last_user_text(state.get("messages", []))
    return resolve_request(user_text)


def route_branch(
    state: AgentWorkflowState,
    *,
    entry_nodes: dict[WorkflowRoute, str],
) -> WorkflowRoute:
    route = str(state.get("route") or "general_agent")
    if route in entry_nodes:
        return cast(WorkflowRoute, route)
    return "general_agent"


def report_task_detail_branch(state: AgentWorkflowState) -> Literal["review", "respond"]:
    payload = state.get("task_detail") or {}
    if str(payload.get("status") or "").lower() == "ok":
        return "review"
    return "respond"


def task_review_detail_branch(state: AgentWorkflowState) -> Literal["review", "respond"]:
    return report_task_detail_branch(state)


def review_branch(state: AgentWorkflowState) -> Literal["precheck", "respond"]:
    review = state.get("collection_review") or {}
    return "precheck" if _review_record_keys(review) else "respond"


def precheck_branch(state: AgentWorkflowState) -> Literal["generate", "respond"]:
    if state.get("workflow_action") != "generate":
        return "respond"
    payload = state.get("report_precheck") or {}
    if bool(payload.get("success")) and bool(payload.get("can_generate")):
        return "generate"
    return "respond"


def pipeline_prepare_branch(state: AgentWorkflowState) -> Literal["create", "respond"]:
    return "create" if str(state.get("workflow_url") or "").strip() else "respond"
