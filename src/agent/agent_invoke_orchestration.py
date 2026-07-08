"""Preparation and recovery helpers for Agent invocation orchestration."""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from typing import Any

from src.agent.agent_invoke_lifecycle import AgentInvokeState
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
