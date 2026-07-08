import pytest

from src.agent.agent_invoke_lifecycle import AgentInvokeState
from src.agent.agent_invoke_stream import (
    AgentInvokeStreamContext,
    build_agent_event_stream_config,
    stream_agent_executor_events,
)
from src.agent.stream_parser import StreamState


class _FakeExecutor:
    def __init__(self, events):
        self._events = list(events)
        self.calls = []

    async def astream_events(self, payload, config, version):
        self.calls.append(
            {
                "payload": payload,
                "config": config,
                "version": version,
            }
        )
        for event in self._events:
            yield event


@pytest.mark.asyncio
async def test_stream_agent_executor_events_builds_thread_aware_config() -> None:
    executor = _FakeExecutor([])
    context = AgentInvokeStreamContext(
        stream_state=StreamState(),
        invoke_state=AgentInvokeState(),
        suppress_final_stream=False,
        workflow_bridges={},
        runtime_input_mode="legacy_executor",
    )

    events = [
        event
        async for event in stream_agent_executor_events(
            executor,
            invoke_payload={"input": "hello"},
            session_id="thread-1",
            context=context,
            build_workflow_chain_start_events=lambda *args, **kwargs: [],
            redact_value=lambda value: value,
            redact_stream_event=lambda payload: payload,
            redact_text=lambda text: text,
            describe_tool_action=lambda name, args: "",
        )
    ]

    assert events == []
    assert executor.calls == [
        {
            "payload": {"input": "hello"},
            "config": build_agent_event_stream_config("thread-1"),
            "version": "v2",
        }
    ]
    assert context.invoke_state.stream_started is True


@pytest.mark.asyncio
async def test_stream_agent_executor_events_updates_state_and_stops_on_completion() -> None:
    executor = _FakeExecutor(
        [
            {"event": "on_chat_model_start"},
            {
                "event": "on_chat_model_stream",
                "data": {
                    "chunk": type(
                        "Chunk",
                        (),
                        {"content": "done", "additional_kwargs": {}},
                    )()
                },
            },
            {
                "event": "on_chain_end",
                "name": "AgentExecutor",
                "data": {"output": {"output": "done"}},
            },
        ]
    )
    context = AgentInvokeStreamContext(
        stream_state=StreamState(),
        invoke_state=AgentInvokeState(),
        suppress_final_stream=False,
        workflow_bridges={},
        runtime_input_mode="legacy_executor",
    )

    events = [
        event
        async for event in stream_agent_executor_events(
            executor,
            invoke_payload={"input": "hello"},
            session_id="thread-2",
            context=context,
            build_workflow_chain_start_events=lambda *args, **kwargs: [],
            redact_value=lambda value: value,
            redact_stream_event=lambda payload: payload,
            redact_text=lambda text: f"safe:{text}",
            describe_tool_action=lambda name, args: "",
        )
    ]

    assert events == [
        {"type": "thinking", "content": "正在分析您的请求..."},
        {"type": "final", "content": "done"},
    ]
    assert context.stream_state.final_output == "done"
    assert context.invoke_state.final_output == ""
    assert context.invoke_state.run_completed is True


@pytest.mark.asyncio
async def test_stream_agent_executor_events_emits_tool_and_workflow_events() -> None:
    executor = _FakeExecutor(
        [
            {"event": "on_chain_start", "name": "workflow-node", "data": {"input": {"task_id": "t-1"}}},
            {"event": "on_tool_start", "name": "create_task", "data": {"input": {"name": "collect"}}},
            {"event": "on_tool_end", "name": "create_task", "data": {"output": {"status": "ok"}}},
        ]
    )
    context = AgentInvokeStreamContext(
        stream_state=StreamState(),
        invoke_state=AgentInvokeState(),
        suppress_final_stream=False,
        workflow_bridges={},
        runtime_input_mode="legacy_executor",
    )

    events = [
        event
        async for event in stream_agent_executor_events(
            executor,
            invoke_payload={"input": "hello"},
            session_id="thread-3",
            context=context,
            build_workflow_chain_start_events=lambda event, **kwargs: [
                {"type": "tool_call", "name": f"workflow:{event['name']}", "args": {}}
            ],
            redact_value=lambda value: value,
            redact_stream_event=lambda payload: payload,
            redact_text=lambda text: text,
            describe_tool_action=lambda name, args: f"{name}:{args.get('name', '')}",
        )
    ]

    assert events == [
        {"type": "tool_call", "name": "workflow:workflow-node", "args": {}},
        {"type": "thinking", "content": "create_task:collect"},
        {"type": "tool_call", "name": "create_task", "args": {"name": "collect"}},
        {"type": "tool_result", "name": "create_task", "content": '{"status": "ok"}'},
    ]
