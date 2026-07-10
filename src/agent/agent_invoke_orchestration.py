"""Preparation and recovery helpers for Agent invocation orchestration."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Awaitable, Callable, Mapping
from dataclasses import dataclass
from typing import Any

from loguru import logger

from src.agent.agent_invoke_lifecycle import AgentInvokeState, finalize_stream_tail
from src.agent.agent_invoke_stream import (
    AgentInvokeStreamContext,
    stream_agent_executor_events,
)
from src.agent.stream_parser import StreamState


@dataclass(frozen=True)
class PreparedAgentInvoke:
    history: list[Any]
    stream_state: StreamState
    suppress_final_stream: bool
    workflow_bridges: Mapping[str, Any]


async def prepare_agent_invoke(
    *,
    session_id: str,
    histories_loaded: bool,
    mark_pending_history_recovery: Callable[[str], Awaitable[None]],
    cleanup_stale_sessions: Callable[[], Awaitable[None]],
    get_history: Callable[[str], Awaitable[list[Any]]],
    uses_legacy_react_parser: Callable[[], bool],
    workflow_node_bridge_map: Callable[[], Mapping[str, Any]],
) -> PreparedAgentInvoke:
    if not histories_loaded:
        await mark_pending_history_recovery(session_id)

    await cleanup_stale_sessions()
    history = await get_history(session_id)
    return PreparedAgentInvoke(
        history=history,
        stream_state=StreamState(),
        suppress_final_stream=uses_legacy_react_parser(),
        workflow_bridges=workflow_node_bridge_map(),
    )


async def recover_agent_invoke(
    *,
    session_id: str,
    user_input: str,
    invoke_state: AgentInvokeState,
    discard_partial_runtime_state: Callable[..., Awaitable[None]],
    save_invoke_history: Callable[..., Awaitable[None]],
) -> None:
    await discard_partial_runtime_state(
        session_id=session_id,
        invoke_state=invoke_state,
    )
    await save_invoke_history(
        session_id=session_id,
        user_input=user_input,
        invoke_state=invoke_state,
    )


async def run_prepared_agent_invoke(
    *,
    executor: Any,
    session_id: str,
    user_input: str,
    invoke_state: AgentInvokeState,
    prepared_invoke: PreparedAgentInvoke,
    runtime_input_mode: str,
    build_invoke_payload: Callable[..., dict[str, Any]],
    build_workflow_chain_start_events: Callable[..., list[dict[str, Any]]],
    redact_value: Callable[[Any], Any],
    redact_stream_event: Callable[[dict[str, Any]], dict[str, Any]],
    redact_stream_text: Callable[[str], str],
    describe_tool_action: Callable[[str, dict[str, Any]], str],
    save_invoke_history: Callable[..., Awaitable[None]],
    discard_partial_runtime_state: Callable[..., Awaitable[None]],
) -> AsyncIterator[dict[str, Any]]:
    try:
        async for rendered_event in execute_prepared_agent_invoke(
            executor=executor,
            session_id=session_id,
            user_input=user_input,
            invoke_state=invoke_state,
            prepared_invoke=prepared_invoke,
            runtime_input_mode=runtime_input_mode,
            build_invoke_payload=build_invoke_payload,
            build_workflow_chain_start_events=build_workflow_chain_start_events,
            redact_value=redact_value,
            redact_stream_event=redact_stream_event,
            redact_stream_text=redact_stream_text,
            describe_tool_action=describe_tool_action,
            save_invoke_history=save_invoke_history,
        ):
            yield rendered_event
    except asyncio.CancelledError:
        logger.info("Agent stream cancelled for session {}.", session_id)
        await recover_agent_invoke(
            session_id=session_id,
            user_input=user_input,
            invoke_state=invoke_state,
            discard_partial_runtime_state=discard_partial_runtime_state,
            save_invoke_history=save_invoke_history,
        )
        raise
    except Exception:
        await recover_agent_invoke(
            session_id=session_id,
            user_input=user_input,
            invoke_state=invoke_state,
            discard_partial_runtime_state=discard_partial_runtime_state,
            save_invoke_history=save_invoke_history,
        )
        raise


async def execute_prepared_agent_invoke(
    *,
    executor: Any,
    session_id: str,
    user_input: str,
    invoke_state: AgentInvokeState,
    prepared_invoke: PreparedAgentInvoke,
    runtime_input_mode: str,
    build_invoke_payload: Callable[..., dict[str, Any]],
    build_workflow_chain_start_events: Callable[..., list[dict[str, Any]]],
    redact_value: Callable[[Any], Any],
    redact_stream_event: Callable[[dict[str, Any]], dict[str, Any]],
    redact_stream_text: Callable[[str], str],
    describe_tool_action: Callable[[str, dict[str, Any]], str],
    save_invoke_history: Callable[..., Awaitable[None]],
) -> AsyncIterator[dict[str, Any]]:
    if not executor:
        yield {"type": "error", "content": "Agent was re-initialized during request."}
        return

    invoke_payload = build_invoke_payload(
        user_input=user_input,
        history=prepared_invoke.history,
        thread_id=session_id,
    )

    stream_context = AgentInvokeStreamContext(
        stream_state=prepared_invoke.stream_state,
        invoke_state=invoke_state,
        suppress_final_stream=prepared_invoke.suppress_final_stream,
        workflow_bridges=prepared_invoke.workflow_bridges,
        runtime_input_mode=runtime_input_mode,
    )
    async for rendered_event in stream_agent_executor_events(
        executor,
        invoke_payload=invoke_payload,
        session_id=session_id,
        context=stream_context,
        build_workflow_chain_start_events=build_workflow_chain_start_events,
        redact_value=redact_value,
        redact_stream_event=redact_stream_event,
        redact_text=redact_stream_text,
        describe_tool_action=describe_tool_action,
    ):
        yield rendered_event

    rendered_events, rendered_final_output, _ = finalize_stream_tail(
        stream_context.stream_state,
        suppress_final_stream=prepared_invoke.suppress_final_stream,
        redact_stream_event=redact_stream_event,
        redact_stream_text=redact_stream_text,
    )
    for rendered_event in rendered_events:
        yield rendered_event
    invoke_state.final_output += rendered_final_output

    await save_invoke_history(
        session_id=session_id,
        user_input=user_input,
        invoke_state=invoke_state,
    )
