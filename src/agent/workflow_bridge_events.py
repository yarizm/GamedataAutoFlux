"""Helpers for translating workflow graph events into SSE payloads."""

from __future__ import annotations

import json
from collections.abc import Callable, Mapping
from typing import Any

from langchain_core.messages import AIMessage, ToolMessage

from src.agent.workflow_events import (
    STEP_DONE,
    STEP_FAILED,
    STEP_RUNNING,
    workflow_step_event,
)
from src.agent.workflow_types import WorkflowToolBridgeDefinition


def build_workflow_chain_start_events(
    event: dict[str, Any],
    *,
    bridge_map: Mapping[str, WorkflowToolBridgeDefinition],
    redact_value: Callable[[Any], Any],
    describe_tool_action: Callable[[str, dict[str, Any]], str],
    workflow_id: str | None = None,
) -> list[dict[str, Any]]:
    workflow_bridge = bridge_map.get(str(event.get("name") or ""))
    if workflow_bridge is None:
        return []

    node_input = event.get("data", {}).get("input", {})
    args = workflow_tool_args(workflow_bridge, node_input)
    safe_args = redact_value(args)
    thinking_desc = describe_tool_action(
        workflow_bridge.tool_name,
        safe_args if isinstance(safe_args, dict) else {},
    )

    events: list[dict[str, Any]] = []
    if thinking_desc:
        events.append({"type": "thinking", "content": thinking_desc})
    events.append(
        {
            "type": "tool_call",
            "name": workflow_bridge.tool_name,
            "args": safe_args,
        }
    )
    if workflow_id:
        events.append(
            _bridge_workflow_step_event(workflow_bridge, workflow_id, STEP_RUNNING)
        )
    return events


def build_workflow_chain_end_result_event(
    event: dict[str, Any],
    *,
    bridge_map: Mapping[str, WorkflowToolBridgeDefinition],
    redact_value: Callable[[Any], Any],
    max_output_length: int = 4000,
    workflow_id: str | None = None,
) -> list[dict[str, Any]]:
    """Build tool_result (and optional workflow_step) events for a chain end.

    Returns an empty list when the event name is not bridged.
    """
    workflow_bridge = bridge_map.get(str(event.get("name") or ""))
    if workflow_bridge is None:
        return []

    workflow_output = event.get("data", {}).get("output", {})
    payload = workflow_tool_output_payload(workflow_bridge, workflow_output)
    safe_output = redact_value(payload)
    events: list[dict[str, Any]] = [
        {
            "type": "tool_result",
            "name": workflow_bridge.tool_name,
            "content": serialize_stream_output(
                safe_output, max_length=max_output_length
            ),
        }
    ]
    if workflow_id:
        events.append(
            _bridge_workflow_step_event(
                workflow_bridge,
                workflow_id,
                _step_status_from_output(payload),
            )
        )
    return events


def _bridge_workflow_step_event(
    workflow_bridge: WorkflowToolBridgeDefinition,
    workflow_id: str,
    status: str,
) -> dict[str, Any]:
    step_id = workflow_bridge.step_id or workflow_bridge.tool_name
    label = workflow_bridge.step_label or workflow_bridge.tool_name
    return workflow_step_event(workflow_id, step_id, label, status)


def _step_status_from_output(output: Any) -> str:
    """Heuristic: treat explicit error markers as failed, otherwise done."""
    if not isinstance(output, dict):
        return STEP_DONE

    status = str(output.get("status") or "").strip().lower()
    if status in {"error", "failed", "failure"}:
        return STEP_FAILED
    if output.get("success") is False:
        return STEP_FAILED
    if output.get("error"):
        return STEP_FAILED
    return STEP_DONE


def serialize_stream_output(value: Any, *, max_length: int = 4000) -> str:
    output = (
        value
        if isinstance(value, str)
        else json.dumps(value, ensure_ascii=False, default=str)
    )
    if len(output) > max_length:
        return output[:max_length] + "...(已截断)"
    return output


def extract_graph_final_text(output: Any) -> str:
    """Extract the last assistant-facing text from a LangGraph agent result."""
    if not isinstance(output, dict):
        return ""

    messages = output.get("messages")
    if not isinstance(messages, list):
        return ""

    for message in reversed(messages):
        if isinstance(message, ToolMessage):
            continue
        if isinstance(message, AIMessage):
            content = getattr(message, "content", "")
            if isinstance(content, str):
                return content
            if isinstance(content, list):
                text_parts = []
                for item in content:
                    if isinstance(item, dict) and item.get("type") == "text":
                        text_parts.append(str(item.get("text", "")))
                    elif isinstance(item, str):
                        text_parts.append(item)
                return "".join(text_parts)
            return str(content or "")
        if isinstance(message, tuple) and len(message) >= 2 and message[0] == "ai":
            return str(message[1] or "")

    return ""


def workflow_tool_args(
    workflow_bridge: WorkflowToolBridgeDefinition,
    node_input: Any,
) -> dict[str, Any]:
    if not isinstance(node_input, dict):
        return {}
    return workflow_bridge.build_args(node_input)


def workflow_tool_output_payload(
    workflow_bridge: WorkflowToolBridgeDefinition,
    node_output: Any,
) -> Any:
    if not isinstance(node_output, dict):
        return node_output
    if workflow_bridge.output_state_key:
        return node_output.get(workflow_bridge.output_state_key, node_output)
    return node_output
