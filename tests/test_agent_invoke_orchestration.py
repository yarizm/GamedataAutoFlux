import asyncio

import pytest

from src.agent.agent_invoke_lifecycle import AgentInvokeState
from src.agent.agent_invoke_orchestration import (
    PreparedAgentInvoke,
    execute_prepared_agent_invoke,
    prepare_agent_invoke,
    recover_agent_invoke,
    run_prepared_agent_invoke,
)
from src.agent.stream_parser import StreamState


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
        workflow_node_bridge_map=lambda: {"node": "bridge"},
    )

    assert calls == ["mark:thread-a", "cleanup", "history:thread-a"]
    assert result.history == ["persisted"]
    assert result.suppress_final_stream is False
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


@pytest.mark.asyncio
async def test_run_prepared_agent_invoke_recovers_after_cancellation() -> None:
    calls: list[str] = []
    state = AgentInvokeState(stream_started=True)

    class CancelledExecutor:
        async def astream_events(self, payload, config, version):
            yield {"event": "on_chat_model_start"}
            raise asyncio.CancelledError()

    async def save_invoke_history(*, session_id: str, user_input: str, invoke_state: AgentInvokeState) -> None:
        calls.append(f"save:{session_id}:{user_input}:{invoke_state.final_output}")

    async def discard_partial_runtime_state(
        *,
        session_id: str,
        invoke_state: AgentInvokeState,
    ) -> None:
        calls.append(f"discard:{session_id}:{invoke_state.final_output}")

    with pytest.raises(asyncio.CancelledError):
        _ = [
            event
            async for event in run_prepared_agent_invoke(
                executor=CancelledExecutor(),
                session_id="thread-d",
                user_input="hello",
                invoke_state=state,
                prepared_invoke=PreparedAgentInvoke(
                    history=[],
                    stream_state=StreamState(),
                    suppress_final_stream=False,
                    workflow_bridges={},
                ),
                runtime_input_mode="messages_graph",
                build_invoke_payload=lambda **kwargs: {"messages": [("human", kwargs["user_input"])]},
                build_workflow_chain_start_events=lambda *args, **kwargs: [],
                redact_value=lambda value: value,
                redact_stream_event=lambda event: event,
                redact_stream_text=lambda text: text,
                describe_tool_action=lambda name, args: name,
                save_invoke_history=save_invoke_history,
                discard_partial_runtime_state=discard_partial_runtime_state,
            )
        ]

    assert calls == [
        "discard:thread-d:",
        "save:thread-d:hello:",
    ]


@pytest.mark.asyncio
async def test_run_prepared_agent_invoke_recover_after_runtime_error() -> None:
    calls: list[str] = []
    state = AgentInvokeState(final_output="partial", stream_started=True)

    class FailingExecutor:
        async def astream_events(self, payload, config, version):
            raise RuntimeError("boom")
            yield {}

    async def save_invoke_history(*, session_id: str, user_input: str, invoke_state: AgentInvokeState) -> None:
        calls.append(f"save:{session_id}:{user_input}:{invoke_state.final_output}")

    async def discard_partial_runtime_state(
        *,
        session_id: str,
        invoke_state: AgentInvokeState,
    ) -> None:
        calls.append(f"discard:{session_id}:{invoke_state.final_output}")

    with pytest.raises(RuntimeError, match="boom"):
        _ = [
            event
            async for event in run_prepared_agent_invoke(
                executor=FailingExecutor(),
                session_id="thread-e",
                user_input="retry",
                invoke_state=state,
                prepared_invoke=PreparedAgentInvoke(
                    history=[],
                    stream_state=StreamState(),
                    suppress_final_stream=False,
                    workflow_bridges={},
                ),
                runtime_input_mode="messages_graph",
                build_invoke_payload=lambda **kwargs: {"messages": [("human", kwargs["user_input"])]},
                build_workflow_chain_start_events=lambda *args, **kwargs: [],
                redact_value=lambda value: value,
                redact_stream_event=lambda event: event,
                redact_stream_text=lambda text: text,
                describe_tool_action=lambda name, args: name,
                save_invoke_history=save_invoke_history,
                discard_partial_runtime_state=discard_partial_runtime_state,
            )
        ]

    assert calls == [
        "discard:thread-e:partial",
        "save:thread-e:retry:partial",
    ]


@pytest.mark.asyncio
async def test_run_prepared_agent_invoke_surfaces_reinitialized_executor_error() -> None:
    events = [
        event
        async for event in run_prepared_agent_invoke(
            executor=None,
            session_id="thread-f",
            user_input="hello",
            invoke_state=AgentInvokeState(),
            prepared_invoke=PreparedAgentInvoke(
                history=[],
                stream_state=StreamState(),
                suppress_final_stream=False,
                workflow_bridges={},
            ),
            runtime_input_mode="messages_graph",
            build_invoke_payload=lambda **kwargs: {"messages": [("human", kwargs["user_input"])]},
            build_workflow_chain_start_events=lambda *args, **kwargs: [],
            redact_value=lambda value: value,
            redact_stream_event=lambda event: event,
            redact_stream_text=lambda text: text,
            describe_tool_action=lambda name, args: name,
            save_invoke_history=lambda **kwargs: None,
            discard_partial_runtime_state=lambda **kwargs: None,
        )
    ]

    assert events == [
        {"type": "error", "content": "Agent was re-initialized during request."}
    ]
