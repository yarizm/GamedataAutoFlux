from langchain_core.messages import AIMessage, HumanMessage

from src.agent.agent_invoke_lifecycle import (
    AgentInvokeState,
    append_turn_history,
    finalize_stream_tail,
    should_discard_partial_runtime_state,
)
from src.agent.stream_parser import StreamState


def test_append_turn_history_adds_messages_and_caps_non_recovery_session() -> None:
    histories = {
        "session-1": [
            HumanMessage(content=f"user-{index}") if index % 2 == 0 else AIMessage(content=f"ai-{index}")
            for index in range(40)
        ]
    }

    append_turn_history(
        histories,
        set(),
        session_id="session-1",
        user_input="new question",
        final_output="new answer",
        redact_message_content=lambda value: value,
    )

    assert len(histories["session-1"]) == 20
    assert histories["session-1"][-2].content == "new question"
    assert histories["session-1"][-1].content == "new answer"


def test_append_turn_history_keeps_full_history_for_pending_recovery_session() -> None:
    histories = {"session-2": []}

    append_turn_history(
        histories,
        {"session-2"},
        session_id="session-2",
        user_input="question",
        final_output="",
        redact_message_content=lambda value: value,
    )

    assert [message.content for message in histories["session-2"]] == ["question", "已停止"]


def test_should_discard_partial_runtime_state_checks_completion_flags() -> None:
    assert should_discard_partial_runtime_state(
        AgentInvokeState(stream_started=True, run_completed=False)
    )
    assert not should_discard_partial_runtime_state(
        AgentInvokeState(stream_started=False, run_completed=False)
    )
    assert not should_discard_partial_runtime_state(
        AgentInvokeState(stream_started=True, run_completed=True)
    )


def test_finalize_stream_tail_returns_redacted_events_and_final_output() -> None:
    stream_state = StreamState(content_buffer="tail", final_output="prefix")

    events, final_output, updated_state = finalize_stream_tail(
        stream_state,
        suppress_final_stream=False,
        redact_stream_event=lambda payload: {
            **payload,
            "content": f"redacted:{payload['content']}",
        },
        redact_stream_text=lambda text: f"safe:{text}",
    )

    assert events == [{"type": "final", "content": "redacted:tail"}]
    assert final_output == "safe:prefixtail"
    assert updated_state.content_buffer == ""
