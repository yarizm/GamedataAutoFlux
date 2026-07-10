"""Async orchestration helpers for Agent invocation event streams."""

from __future__ import annotations

from collections.abc import AsyncIterator, Callable, Mapping
from dataclasses import dataclass, field
from typing import Any

from src.agent.agent_invoke_lifecycle import AgentInvokeState
from src.agent.agent_stream_events import (
    build_tool_end_result_event,
    build_tool_start_events,
    handle_chain_end_event,
    handle_chat_model_start_event,
    handle_chat_model_stream_event,
    maybe_workflow_end_events,
    maybe_workflow_start_events,
)
from src.agent.stream_parser import StreamState
from src.agent.workflow_events import END_FAILED, workflow_end_event


@dataclass
class AgentInvokeStreamContext:
    stream_state: StreamState
    invoke_state: AgentInvokeState
    suppress_final_stream: bool
    workflow_bridges: Mapping[str, Any]
    runtime_input_mode: str
    active_workflow_id: str | None = None
    workflow_label: str | None = None
    workflow_steps: list[dict[str, str]] = field(default_factory=list)


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

    try:
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
                for start_event in maybe_workflow_start_events(event, context):
                    yield start_event
                workflow_events = build_workflow_chain_start_events(
                    event,
                    bridge_map=context.workflow_bridges,
                    redact_value=redact_value,
                    describe_tool_action=describe_tool_action,
                    workflow_id=context.active_workflow_id,
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
                workflow_id=context.active_workflow_id,
            )
            # workflow_end before result_card/final when compose or root completes
            end_events = maybe_workflow_end_events(event, context, chain_end_result)
            # Prefer: bridge tool_result/step first, then workflow_end, then card/final
            bridge_or_other = [
                e
                for e in chain_end_result.events
                if e.get("type") not in {"result_card", "final"}
            ]
            terminal = [
                e
                for e in chain_end_result.events
                if e.get("type") in {"result_card", "final"}
            ]
            for rendered_event in bridge_or_other:
                yield rendered_event
            for rendered_event in end_events:
                yield rendered_event
            for rendered_event in terminal:
                yield rendered_event
            context.invoke_state.final_output += chain_end_result.final_output
            if chain_end_result.run_completed:
                context.invoke_state.run_completed = True
                break
    except Exception as exc:
        if (
            context.active_workflow_id
            and not context.stream_state.workflow_meta_ended
        ):
            context.stream_state.workflow_meta_ended = True
            reason = str(exc).strip() or exc.__class__.__name__
            yield workflow_end_event(
                context.active_workflow_id,
                END_FAILED,
                reason=reason[:200],
            )
        raise
