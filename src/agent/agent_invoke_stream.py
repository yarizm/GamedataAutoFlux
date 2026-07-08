"""Async orchestration helpers for Agent invocation event streams."""

from __future__ import annotations

from collections.abc import AsyncIterator, Callable, Mapping
from dataclasses import dataclass
from typing import Any

from src.agent.agent_invoke_lifecycle import AgentInvokeState
from src.agent.agent_stream_events import (
    build_tool_end_result_event,
    build_tool_start_events,
    handle_chain_end_event,
    handle_chat_model_start_event,
    handle_chat_model_stream_event,
)
from src.agent.stream_parser import StreamState


@dataclass
class AgentInvokeStreamContext:
    stream_state: StreamState
    invoke_state: AgentInvokeState
    suppress_final_stream: bool
    workflow_bridges: Mapping[str, Any]
    runtime_input_mode: str


def build_agent_event_stream_config(session_id: str) -> dict[str, Any]:
    return {
        "configurable": {
            "session_id": session_id,
            "thread_id": session_id,
        }
    }


async def stream_agent_executor_events(
    executor: Any,
    *,
    invoke_payload: dict[str, Any],
    session_id: str,
    context: AgentInvokeStreamContext,
    build_workflow_chain_start_events: Callable[..., list[dict[str, Any]]],
    redact_value: Callable[[Any], Any],
    redact_stream_event: Callable[[dict[str, Any]], dict[str, Any]],
    redact_text: Callable[[str], str],
    describe_tool_action: Callable[[str, dict[str, Any]], str],
) -> AsyncIterator[dict[str, Any]]:
    context.invoke_state.stream_started = True

    async for event in executor.astream_events(
        invoke_payload,
        config=build_agent_event_stream_config(session_id),
        version="v2",
    ):
        kind = event.get("event")

        if kind == "on_chat_model_start":
            rendered_events, context.stream_state = handle_chat_model_start_event(
                context.stream_state,
                suppress_final_stream=context.suppress_final_stream,
            )
            for rendered_event in rendered_events:
                yield rendered_event
            continue

        if kind == "on_chain_start":
            workflow_events = build_workflow_chain_start_events(
                event,
                bridge_map=context.workflow_bridges,
                redact_value=redact_value,
                describe_tool_action=describe_tool_action,
            )
            for workflow_event in workflow_events:
                yield workflow_event
            continue

        if kind == "on_chat_model_stream":
            rendered_events, context.stream_state = handle_chat_model_stream_event(
                event,
                context.stream_state,
                suppress_final_stream=context.suppress_final_stream,
                redact_stream_event=redact_stream_event,
            )
            for rendered_event in rendered_events:
                yield rendered_event
            continue

        if kind == "on_tool_start":
            rendered_events = build_tool_start_events(
                event,
                redact_value=redact_value,
                describe_tool_action=describe_tool_action,
            )
            for rendered_event in rendered_events:
                yield rendered_event
            continue

        if kind == "on_tool_end":
            yield build_tool_end_result_event(
                event,
                redact_value=redact_value,
            )
            continue

        if kind != "on_chain_end":
            continue

        chain_end_result = handle_chain_end_event(
            event,
            bridge_map=dict(context.workflow_bridges),
            redact_value=redact_value,
            redact_text=redact_text,
            suppress_final_stream=context.suppress_final_stream,
            has_state_final_output=bool(context.stream_state.final_output),
            runtime_input_mode=context.runtime_input_mode,
        )
        for rendered_event in chain_end_result.events:
            yield rendered_event
        context.invoke_state.final_output += chain_end_result.final_output
        if chain_end_result.run_completed:
            context.invoke_state.run_completed = True
            break
