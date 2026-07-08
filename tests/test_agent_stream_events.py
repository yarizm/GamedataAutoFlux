from src.agent.agent_stream_events import (
    build_tool_end_result_event,
    build_tool_start_events,
    handle_chain_end_event,
    handle_chat_model_start_event,
    handle_chat_model_stream_event,
)
from src.agent.stream_parser import StreamState
from src.agent.workflow_types import WorkflowToolBridgeDefinition


class _FakeChunk:
    def __init__(self, content, additional_kwargs=None) -> None:
        self.content = content
        self.additional_kwargs = additional_kwargs or {}


def test_handle_chat_model_start_event_resets_state_and_emits_thinking() -> None:
    state = StreamState(
        in_thinking_block=True,
        content_buffer="buffer",
        in_react_action=True,
        react_emitted_len=5,
    )

    events, updated_state = handle_chat_model_start_event(
        state,
        suppress_final_stream=False,
    )

    assert events == [{"type": "thinking", "content": "正在分析您的请求..."}]
    assert updated_state.in_thinking_block is False
    assert updated_state.in_react_action is False
    assert updated_state.react_emitted_len == 0
    assert updated_state.content_buffer == ""


def test_handle_chat_model_stream_event_emits_reasoning_and_final_text() -> None:
    event = {
        "data": {
            "chunk": _FakeChunk(
                "Final answer",
                {"reasoning_content": "Need to inspect"},
            )
        }
    }

    events, state = handle_chat_model_stream_event(
        event,
        StreamState(),
        suppress_final_stream=False,
        redact_stream_event=lambda payload: payload,
    )

    assert events == [
        {"type": "thinking", "content": "Need to inspect"},
        {"type": "final", "content": "Final answer"},
    ]
    assert state.final_output == "Final answer"


def test_handle_chat_model_stream_event_handles_list_content() -> None:
    event = {
        "data": {
            "chunk": _FakeChunk(
                [
                    {"type": "reasoning", "text": "step 1"},
                    {"type": "text", "text": "done"},
                ]
            )
        }
    }

    events, state = handle_chat_model_stream_event(
        event,
        StreamState(),
        suppress_final_stream=False,
        redact_stream_event=lambda payload: payload,
    )

    assert events == [
        {"type": "thinking", "content": "step 1"},
        {"type": "final", "content": "done"},
    ]
    assert state.final_output == "done"


def test_build_tool_start_events_emits_thinking_and_tool_call() -> None:
    events = build_tool_start_events(
        {
            "name": "create_task",
            "data": {"input": {"name": "collect-cs2"}},
        },
        redact_value=lambda value: value,
        describe_tool_action=lambda name, args: f"{name}:{args['name']}",
    )

    assert events == [
        {"type": "thinking", "content": "create_task:collect-cs2"},
        {
            "type": "tool_call",
            "name": "create_task",
            "args": {"name": "collect-cs2"},
        },
    ]


def test_build_tool_end_result_event_serializes_output() -> None:
    event = {
        "name": "echo",
        "data": {"output": {"value": "ok"}},
    }

    rendered = build_tool_end_result_event(
        event,
        redact_value=lambda value: value,
    )

    assert rendered == {
        "type": "tool_result",
        "name": "echo",
        "content": '{"value": "ok"}',
    }


def test_handle_chain_end_event_emits_workflow_result_without_finishing_run() -> None:
    bridge = WorkflowToolBridgeDefinition(
        tool_name="get_task_detail",
        build_args=lambda state: {"task_id": state.get("workflow_task_id")},
        output_state_key="task_detail",
    )
    event = {
        "name": "load_task_detail_report",
        "data": {"output": {"task_detail": {"status": "ok"}}},
    }

    result = handle_chain_end_event(
        event,
        bridge_map={"load_task_detail_report": bridge},
        redact_value=lambda value: value,
        redact_text=lambda text: text,
        suppress_final_stream=False,
        has_state_final_output=False,
        runtime_input_mode="messages_graph",
    )

    assert result.events == [
        {
            "type": "tool_result",
            "name": "get_task_detail",
            "content": '{"status": "ok"}',
        }
    ]
    assert result.final_output == ""
    assert result.run_completed is False
    assert result.handled is True


def test_handle_chain_end_event_emits_agent_executor_final_output() -> None:
    result = handle_chain_end_event(
        {
            "name": "AgentExecutor",
            "data": {"output": {"output": "done"}},
        },
        bridge_map={},
        redact_value=lambda value: value,
        redact_text=lambda text: f"safe:{text}",
        suppress_final_stream=False,
        has_state_final_output=False,
        runtime_input_mode="legacy_executor",
    )

    assert result.events == [{"type": "final", "content": "safe:done"}]
    assert result.final_output == "safe:done"
    assert result.run_completed is True
    assert result.handled is True


def test_handle_chain_end_event_emits_graph_final_output() -> None:
    result = handle_chain_end_event(
        {
            "name": "LangGraph",
            "data": {"output": {"messages": [("ai", "graph final")]}},
        },
        bridge_map={},
        redact_value=lambda value: value,
        redact_text=lambda text: f"safe:{text}",
        suppress_final_stream=False,
        has_state_final_output=False,
        runtime_input_mode="messages_graph",
    )

    assert result.events == [{"type": "final", "content": "safe:graph final"}]
    assert result.final_output == "safe:graph final"
    assert result.run_completed is True
    assert result.handled is True
