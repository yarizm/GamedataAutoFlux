import pytest

from src.agent.agent_invoke_lifecycle import AgentInvokeState
from src.agent.agent_invoke_orchestration import (
    prepare_agent_invoke,
    recover_agent_invoke,
)


@pytest.mark.asyncio
async def test_prepare_agent_invoke_marks_pending_recovery_before_loading_history() -> None:
    calls: list[str] = []

    async def mark_pending_history_recovery(session_id: str) -> None:
        calls.append(f"mark:{session_id}")

    async def cleanup_stale_sessions() -> None:
        calls.append("cleanup")

    async def get_history(session_id: str) -> list[str]:
        calls.append(f"history:{session_id}")
        return ["persisted"]

    result = await prepare_agent_invoke(
        session_id="thread-a",
        histories_loaded=False,
        mark_pending_history_recovery=mark_pending_history_recovery,
        cleanup_stale_sessions=cleanup_stale_sessions,
        get_history=get_history,
        uses_legacy_react_parser=lambda: True,
        workflow_node_bridge_map=lambda: {"node": "bridge"},
    )

    assert calls == ["mark:thread-a", "cleanup", "history:thread-a"]
    assert result.history == ["persisted"]
    assert result.suppress_final_stream is True
    assert result.workflow_bridges == {"node": "bridge"}


@pytest.mark.asyncio
async def test_prepare_agent_invoke_skips_recovery_marker_when_histories_loaded() -> None:
    calls: list[str] = []

    async def mark_pending_history_recovery(session_id: str) -> None:
        calls.append(f"mark:{session_id}")

    async def cleanup_stale_sessions() -> None:
        calls.append("cleanup")

    async def get_history(session_id: str) -> list[str]:
        calls.append(f"history:{session_id}")
        return []

    await prepare_agent_invoke(
        session_id="thread-b",
        histories_loaded=True,
        mark_pending_history_recovery=mark_pending_history_recovery,
        cleanup_stale_sessions=cleanup_stale_sessions,
        get_history=get_history,
        uses_legacy_react_parser=lambda: False,
        workflow_node_bridge_map=dict,
    )

    assert calls == ["cleanup", "history:thread-b"]


@pytest.mark.asyncio
async def test_recover_agent_invoke_discards_and_saves_in_order() -> None:
    calls: list[str] = []
    state = AgentInvokeState(final_output="done", stream_started=True)

    async def discard_partial_runtime_state(*, session_id: str, invoke_state: AgentInvokeState) -> None:
        calls.append(f"discard:{session_id}:{invoke_state.final_output}")

    async def save_invoke_history(*, session_id: str, user_input: str, invoke_state: AgentInvokeState) -> None:
        calls.append(f"save:{session_id}:{user_input}:{invoke_state.final_output}")

    await recover_agent_invoke(
        session_id="thread-c",
        user_input="hello",
        invoke_state=state,
        discard_partial_runtime_state=discard_partial_runtime_state,
        save_invoke_history=save_invoke_history,
    )

    assert calls == [
        "discard:thread-c:done",
        "save:thread-c:hello:done",
    ]
