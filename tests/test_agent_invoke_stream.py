import pytest

from src.agent.agent_invoke_lifecycle import AgentInvokeState
from src.agent.agent_invoke_stream import (
    AgentInvokeStreamContext,
    build_agent_event_stream_config,
    stream_agent_executor_events,
)
from src.agent.stream_parser import StreamState
from src.agent.workflow_bridge_events import build_workflow_chain_start_events
from src.agent.workflow_types import WorkflowToolBridgeDefinition


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


class _RaisingExecutor:
    async def astream_events(self, payload, config, version):
        yield {
            "event": "on_chain_start",
            "name": "load_task_detail_report",
            "data": {"input": {}},
        }
        raise RuntimeError("boom mid workflow")


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
    assert context.active_workflow_id is None
    assert all(e.get("type") != "workflow_start" for e in events)


@pytest.mark.asyncio
async def test_stream_emits_workflow_start_end_for_report_route() -> None:
    bridge = WorkflowToolBridgeDefinition(
        tool_name="get_task_detail",
        build_args=lambda state: {"task_id": state.get("workflow_task_id")},
        output_state_key="task_detail",
        step_id="load_task",
        step_label="加载任务",
    )
    card = {
        "type": "result_card",
        "card_type": "report",
        "title": "报告已生成",
        "summary": "ok",
        "actions": [],
        "payload": {},
    }
    executor = _FakeExecutor(
        [
            {
                "event": "on_chain_start",
                "name": "load_task_detail_report",
                "data": {"input": {"workflow_task_id": "t-1"}},
            },
            {
                "event": "on_chain_end",
                "name": "load_task_detail_report",
                "parent_ids": ["root"],
                "data": {"output": {"task_detail": {"status": "ok"}}},
            },
            {
                "event": "on_chain_end",
                "name": "compose_report_response",
                "parent_ids": ["root"],
                "data": {"output": {"messages": [], "result_card": card}},
            },
            {
                "event": "on_chain_end",
                "name": "LangGraph",
                "parent_ids": [],
                "data": {
                    "output": {
                        "messages": [("ai", "done")],
                        "result_card": card,
                    }
                },
            },
        ]
    )
    context = AgentInvokeStreamContext(
        stream_state=StreamState(),
        invoke_state=AgentInvokeState(),
        suppress_final_stream=False,
        workflow_bridges={"load_task_detail_report": bridge},
        runtime_input_mode="messages_graph",
    )

    events = [
        event
        async for event in stream_agent_executor_events(
            executor,
            invoke_payload={"input": "生成报告"},
            session_id="thread-report",
            context=context,
            build_workflow_chain_start_events=build_workflow_chain_start_events,
            redact_value=lambda value: value,
            redact_stream_event=lambda payload: payload,
            redact_text=lambda text: text,
            describe_tool_action=lambda name, args: f"{name}",
        )
    ]

    types = [e["type"] for e in events]
    assert types[0] == "workflow_start"
    assert events[0]["workflow_id"] == "report_workflow"
    assert events[0]["label"] == "报告链路"
    assert {s["id"] for s in events[0]["steps"]} >= {
        "load_task",
        "review",
        "precheck",
        "generate",
        "respond",
    }
    assert "workflow_step" in types
    assert types.count("workflow_start") == 1
    assert types.count("workflow_end") == 1
    end_idx = types.index("workflow_end")
    card_idx = types.index("result_card")
    final_idx = types.index("final")
    assert end_idx < card_idx < final_idx
    assert events[end_idx]["status"] == "success"
    assert context.active_workflow_id == "report_workflow"
    assert context.stream_state.workflow_meta_started is True
    assert context.stream_state.workflow_meta_ended is True


@pytest.mark.asyncio
async def test_stream_general_agent_has_no_workflow_start() -> None:
    executor = _FakeExecutor(
        [
            {"event": "on_chain_start", "name": "general_agent", "data": {"input": {}}},
            {
                "event": "on_chain_end",
                "name": "LangGraph",
                "parent_ids": [],
                "data": {"output": {"messages": [("ai", "hi")]}},
            },
        ]
    )
    context = AgentInvokeStreamContext(
        stream_state=StreamState(),
        invoke_state=AgentInvokeState(),
        suppress_final_stream=False,
        workflow_bridges={},
        runtime_input_mode="messages_graph",
    )

    events = [
        event
        async for event in stream_agent_executor_events(
            executor,
            invoke_payload={"input": "hi"},
            session_id="thread-general",
            context=context,
            build_workflow_chain_start_events=lambda *args, **kwargs: [],
            redact_value=lambda value: value,
            redact_stream_event=lambda payload: payload,
            redact_text=lambda text: text,
            describe_tool_action=lambda name, args: "",
        )
    ]

    assert all(e.get("type") != "workflow_start" for e in events)
    assert all(e.get("type") != "workflow_end" for e in events)
    assert context.active_workflow_id is None


@pytest.mark.asyncio
async def test_stream_emits_workflow_end_failed_on_exception() -> None:
    context = AgentInvokeStreamContext(
        stream_state=StreamState(),
        invoke_state=AgentInvokeState(),
        suppress_final_stream=False,
        workflow_bridges={},
        runtime_input_mode="messages_graph",
    )

    events: list[dict] = []
    with pytest.raises(RuntimeError, match="boom"):
        async for event in stream_agent_executor_events(
            _RaisingExecutor(),
            invoke_payload={"input": "x"},
            session_id="thread-fail",
            context=context,
            build_workflow_chain_start_events=lambda *args, **kwargs: [],
            redact_value=lambda value: value,
            redact_stream_event=lambda payload: payload,
            redact_text=lambda text: text,
            describe_tool_action=lambda name, args: "",
        ):
            events.append(event)

    assert events[0]["type"] == "workflow_start"
    assert events[-1] == {
        "type": "workflow_end",
        "workflow_id": "report_workflow",
        "status": "failed",
        "reason": "boom mid workflow",
    }
    assert context.stream_state.workflow_meta_ended is True
