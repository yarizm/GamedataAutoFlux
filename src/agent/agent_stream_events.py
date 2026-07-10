"""Helpers for translating runtime stream events into SSE payloads."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

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
from src.agent.workflow_events import (
    COMPOSE_NODE_TO_WORKFLOW,
    END_SUCCESS,
    ENTRY_NODE_TO_WORKFLOW,
    WORKFLOW_META,
    workflow_end_event,
    workflow_start_event,
)

if TYPE_CHECKING:
    from src.agent.agent_invoke_stream import AgentInvokeStreamContext


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


def maybe_workflow_start_events(
    event: dict[str, Any],
    context: AgentInvokeStreamContext,
) -> list[dict[str, Any]]:
    """Emit workflow_start once when a routed graph entry node starts.

    general_agent is intentionally excluded (not in ENTRY_NODE_TO_WORKFLOW).
    """
    if context.stream_state.workflow_meta_started:
        return []

    event_name = str(event.get("name") or "")
    workflow_id = ENTRY_NODE_TO_WORKFLOW.get(event_name)
    if workflow_id is None:
        return []

    meta = WORKFLOW_META[workflow_id]
    context.active_workflow_id = workflow_id
    context.workflow_label = meta.label
    context.workflow_steps = [dict(step) for step in meta.steps]
    context.stream_state.workflow_meta_started = True
    return [
        workflow_start_event(
            workflow_id,
            meta.label,
            list(context.workflow_steps),
        )
    ]


def maybe_workflow_end_events(
    event: dict[str, Any],
    context: AgentInvokeStreamContext,
    chain_end_result: ChainEndHandlingResult,
) -> list[dict[str, Any]]:
    """Emit workflow_end once on compose node end or root graph completion."""
    if not context.active_workflow_id or context.stream_state.workflow_meta_ended:
        return []

    event_name = str(event.get("name") or "")
    is_compose = event_name in COMPOSE_NODE_TO_WORKFLOW
    is_graph_root_complete = bool(
        chain_end_result.run_completed
        and context.runtime_input_mode == "messages_graph"
        and not event.get("parent_ids")
    )
    if not is_compose and not is_graph_root_complete:
        return []

    context.stream_state.workflow_meta_ended = True
    return [workflow_end_event(context.active_workflow_id, END_SUCCESS)]


def handle_chain_end_event(
    event: dict[str, Any],
    *,
    bridge_map: dict[str, Any],
    redact_value: Callable[[Any], Any],
    redact_text: Callable[[str], str],
    suppress_final_stream: bool,
    has_state_final_output: bool,
    runtime_input_mode: str,
    workflow_id: str | None = None,
) -> ChainEndHandlingResult:
    workflow_result_events = build_workflow_chain_end_result_event(
        event,
        bridge_map=bridge_map,
        redact_value=redact_value,
        workflow_id=workflow_id,
    )
    if workflow_result_events:
        return ChainEndHandlingResult(events=list(workflow_result_events), handled=True)

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

    # Intermediate workflow nodes (compose / entry without bridge) must not be
    # treated as graph root just because parent_ids is missing from the payload.
    node_name = str(name or "")
    if node_name in COMPOSE_NODE_TO_WORKFLOW or node_name in ENTRY_NODE_TO_WORKFLOW:
        return ChainEndHandlingResult(events=[], handled=True)

    if runtime_input_mode == "messages_graph" and not event.get("parent_ids"):
        graph_output = event.get("data", {}).get("output", {})
        events: list[dict[str, Any]] = []
        final_output = ""
        if isinstance(graph_output, dict):
            result_card = graph_output.get("result_card")
            if isinstance(result_card, dict) and result_card.get("type") == "result_card":
                events.append(result_card)
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
