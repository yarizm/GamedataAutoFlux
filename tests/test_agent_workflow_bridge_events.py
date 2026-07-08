import json

from langchain_core.messages import AIMessage, ToolMessage

from src.agent.workflow_bridge_events import (
    build_workflow_chain_end_result_event,
    build_workflow_chain_start_events,
    extract_graph_final_text,
    serialize_stream_output,
)
from src.agent.workflow_types import WorkflowToolBridgeDefinition


def test_build_workflow_chain_start_events_emits_thinking_and_tool_call() -> None:
    bridge = WorkflowToolBridgeDefinition(
        tool_name="get_task_detail",
        build_args=lambda state: {"task_id": state.get("workflow_task_id")},
        output_state_key="task_detail",
    )
    event = {
        "name": "load_task_detail_report",
        "data": {"input": {"workflow_task_id": "task-001"}},
    }

    rendered = build_workflow_chain_start_events(
        event,
        bridge_map={"load_task_detail_report": bridge},
        redact_value=lambda value: value,
        describe_tool_action=lambda tool_name, args: f"{tool_name}:{args['task_id']}",
    )

    assert rendered == [
        {"type": "thinking", "content": "get_task_detail:task-001"},
        {
            "type": "tool_call",
            "name": "get_task_detail",
            "args": {"task_id": "task-001"},
        },
    ]


def test_build_workflow_chain_start_events_ignores_unknown_nodes() -> None:
    rendered = build_workflow_chain_start_events(
        {"name": "unknown", "data": {"input": {"workflow_task_id": "task-001"}}},
        bridge_map={},
        redact_value=lambda value: value,
        describe_tool_action=lambda tool_name, args: "",
    )

    assert rendered == []


def test_build_workflow_chain_end_result_event_extracts_named_output_payload() -> None:
    bridge = WorkflowToolBridgeDefinition(
        tool_name="precheck_report",
        build_args=lambda state: {},
        output_state_key="report_precheck",
    )
    event = {
        "name": "precheck_report",
        "data": {
            "output": {
                "workflow_template": "steam_game",
                "report_precheck": {"success": True, "status": "partial"},
            }
        },
    }

    rendered = build_workflow_chain_end_result_event(
        event,
        bridge_map={"precheck_report": bridge},
        redact_value=lambda value: value,
    )

    assert rendered == {
        "type": "tool_result",
        "name": "precheck_report",
        "content": json.dumps({"success": True, "status": "partial"}, ensure_ascii=False),
    }


def test_serialize_stream_output_truncates_long_payloads() -> None:
    rendered = serialize_stream_output("abcdef", max_length=4)

    assert rendered == "abcd...(已截断)"


def test_extract_graph_final_text_prefers_last_ai_message_and_skips_tool_messages() -> None:
    output = {
        "messages": [
            AIMessage(content="first"),
            ToolMessage(content="tool output", tool_call_id="call_1"),
            AIMessage(
                content=[
                    {"type": "text", "text": "second"},
                    {"type": "text", "text": " final"},
                ]
            ),
        ]
    }

    assert extract_graph_final_text(output) == "second final"
