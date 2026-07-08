"""Helpers for translating runtime stream events into SSE payloads."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from src.agent.stream_parser import (
    StreamState,
    parse_react_final_answer,
    process_react_chunk,
    process_text_chunk,
)
from src.agent.workflow_bridge_events import (
    build_workflow_chain_end_result_event,
    extract_graph_final_text,
    serialize_stream_output,
)


@dataclass(frozen=True)
class ChainEndHandlingResult:
    events: list[dict[str, Any]]
    final_output: str = ""
    run_completed: bool = False
    handled: bool = False


def handle_chat_model_start_event(
    state: StreamState,
    *,
    suppress_final_stream: bool,
) -> tuple[list[dict[str, Any]], StreamState]:
    state.in_react_action = False
    state.react_emitted_len = 0
    state.in_thinking_block = False
    state.content_buffer = ""

    events: list[dict[str, Any]] = []
    if not suppress_final_stream:
        events.append({"type": "thinking", "content": "正在分析您的请求..."})
    return events, state


def handle_chat_model_stream_event(
    event: dict[str, Any],
    state: StreamState,
    *,
    suppress_final_stream: bool,
    redact_stream_event: Callable[[dict[str, Any]], dict[str, Any]],
) -> tuple[list[dict[str, Any]], StreamState]:
    chunk = event.get("data", {}).get("chunk")
    if chunk is None:
        return [], state

    events: list[dict[str, Any]] = []

    reasoning = _chunk_reasoning(chunk)
    if reasoning:
        events.append(
            redact_stream_event({"type": "thinking", "content": str(reasoning)})
        )

    chunk_content = getattr(chunk, "content", None)
    if not chunk_content:
        return events, state

    if isinstance(chunk_content, str):
        rendered, state = _chunk_text_events(
            chunk_content,
            state,
            suppress_final_stream=suppress_final_stream,
            redact_stream_event=redact_stream_event,
        )
        events.extend(rendered)
        return events, state

    if isinstance(chunk_content, list):
        for item in chunk_content:
            if not isinstance(item, dict):
                continue
            if item.get("type") == "text":
                rendered, state = _chunk_text_events(
                    str(item.get("text", "")),
                    state,
                    suppress_final_stream=suppress_final_stream,
                    redact_stream_event=redact_stream_event,
                )
                events.extend(rendered)
            elif item.get("type") in {"reasoning", "thinking"}:
                events.append(
                    redact_stream_event(
                        {"type": "thinking", "content": item.get("text", "")}
                    )
                )

    return events, state


def build_tool_start_events(
    event: dict[str, Any],
    *,
    redact_value: Callable[[Any], Any],
    describe_tool_action: Callable[[str, dict[str, Any]], str],
) -> list[dict[str, Any]]:
    tool_name = str(event.get("name") or "unknown")
    tool_input = event.get("data", {}).get("input", {})
    args = tool_input if isinstance(tool_input, dict) else {}
    safe_args = redact_value(args)
    safe_args_dict = safe_args if isinstance(safe_args, dict) else {}
    thinking_desc = describe_tool_action(tool_name, safe_args_dict)

    events: list[dict[str, Any]] = []
    if thinking_desc:
        events.append({"type": "thinking", "content": thinking_desc})
    events.append(
        {
            "type": "tool_call",
            "name": tool_name,
            "args": safe_args,
        }
    )
    return events


def build_tool_end_result_event(
    event: dict[str, Any],
    *,
    redact_value: Callable[[Any], Any],
) -> dict[str, Any]:
    tool_name = str(event.get("name") or "unknown")
    tool_output = event.get("data", {}).get("output", "")
    safe_output = redact_value(tool_output)
    return {
        "type": "tool_result",
        "name": tool_name,
        "content": serialize_stream_output(safe_output),
    }


def handle_chain_end_event(
    event: dict[str, Any],
    *,
    bridge_map: dict[str, Any],
    redact_value: Callable[[Any], Any],
    redact_text: Callable[[str], str],
    suppress_final_stream: bool,
    has_state_final_output: bool,
    runtime_input_mode: str,
) -> ChainEndHandlingResult:
    workflow_result_event = build_workflow_chain_end_result_event(
        event,
        bridge_map=bridge_map,
        redact_value=redact_value,
    )
    if workflow_result_event:
        return ChainEndHandlingResult(events=[workflow_result_event], handled=True)

    name = event.get("name")
    if name == "AgentExecutor":
        out = event.get("data", {}).get("output", {})
        events: list[dict[str, Any]] = []
        final_output = ""
        if isinstance(out, dict) and "output" in out:
            if suppress_final_stream:
                final_ans = parse_react_final_answer(str(out["output"] or ""))
                if not final_ans:
                    final_ans = str(out["output"] or "")
                if not final_ans:
                    final_ans = "(无文本输出)"
                safe_final_ans = redact_text(final_ans)
                events.append({"type": "final", "content": safe_final_ans})
                final_output = safe_final_ans
            elif not has_state_final_output:
                final_ans = str(out["output"] or "")
                if final_ans:
                    safe_final_ans = redact_text(final_ans)
                    events.append({"type": "final", "content": safe_final_ans})
                    final_output = safe_final_ans
        return ChainEndHandlingResult(
            events=events,
            final_output=final_output,
            run_completed=True,
            handled=True,
        )

    if runtime_input_mode == "messages_graph" and not event.get("parent_ids"):
        graph_output = event.get("data", {}).get("output", {})
        events: list[dict[str, Any]] = []
        final_output = ""
        final_text = extract_graph_final_text(graph_output)
        if final_text and not has_state_final_output:
            safe_final_text = redact_text(final_text)
            events.append({"type": "final", "content": safe_final_text})
            final_output = safe_final_text
        return ChainEndHandlingResult(
            events=events,
            final_output=final_output,
            run_completed=True,
            handled=True,
        )

    return ChainEndHandlingResult(events=[], handled=False)


def _chunk_reasoning(chunk: Any) -> Any:
    if not hasattr(chunk, "additional_kwargs"):
        return None
    additional_kwargs = chunk.additional_kwargs
    if not isinstance(additional_kwargs, dict):
        return None
    return (
        additional_kwargs.get("reasoning_content")
        or additional_kwargs.get("thinking")
        or additional_kwargs.get("thoughts")
    )


def _chunk_text_events(
    text: str,
    state: StreamState,
    *,
    suppress_final_stream: bool,
    redact_stream_event: Callable[[dict[str, Any]], dict[str, Any]],
) -> tuple[list[dict[str, Any]], StreamState]:
    if suppress_final_stream:
        events, state = process_react_chunk(text, state)
    else:
        events, state = process_text_chunk(text, state, suppress_final_stream)
    return [redact_stream_event(event) for event in events], state
