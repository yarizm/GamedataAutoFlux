"""Helpers for Agent request lifecycle and tail finalization."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from langchain_core.messages import AIMessage, HumanMessage

from src.agent.stream_parser import StreamState, flush_buffer

_STOPPED_MESSAGE = "\u5df2\u505c\u6b62"


@dataclass
class AgentInvokeState:
    final_output: str = ""
    saved: bool = False
    stream_started: bool = False
    run_completed: bool = False


def append_turn_history(
    histories: dict[str, list[Any]],
    pending_history_recovery_threads: set[str],
    *,
    session_id: str,
    user_input: str,
    final_output: str,
    redact_message_content: Callable[[Any], Any],
) -> None:
    if session_id not in histories:
        histories[session_id] = []
    histories[session_id].append(HumanMessage(content=redact_message_content(user_input)))
    histories[session_id].append(
        AIMessage(content=redact_message_content(final_output or _STOPPED_MESSAGE))
    )
    if (
        session_id not in pending_history_recovery_threads
        and len(histories[session_id]) > 40
    ):
        histories[session_id] = histories[session_id][-20:]


def should_discard_partial_runtime_state(state: AgentInvokeState) -> bool:
    return state.stream_started and not state.run_completed


def finalize_stream_tail(
    stream_state: StreamState,
    *,
    suppress_final_stream: bool,
    redact_stream_event: Callable[[dict[str, Any]], dict[str, Any]],
    redact_stream_text: Callable[[str], str],
) -> tuple[list[dict[str, Any]], str, StreamState]:
    events, stream_state = flush_buffer(stream_state, suppress_final_stream)
    rendered_events = [redact_stream_event(event) for event in events]
    rendered_final_output = redact_stream_text(stream_state.final_output)
    return rendered_events, rendered_final_output, stream_state
