"""
流式文本解析器

从 AgentService.ainvoke() 中提取，将 LLM 流式输出的原始文本块
解析为结构化事件（thinking / final）。支持两种模型：
- OpenAI Tools 模式：处理 <think> 标签
- ReAct 模式：检测 Thought/Action 边界

纯函数，无外部依赖。
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass
class StreamState:
    """流式解析器的累积状态"""

    in_thinking_block: bool = False
    """OpenAI Tools 模式：是否在 <think> 标签内"""

    content_buffer: str = ""
    """累积的原始文本缓冲区"""

    in_react_action: bool = False
    """ReAct 模式：是否已进入 Action 块"""

    react_emitted_len: int = 0
    """ReAct 模式：已输出的思考文本长度（用于截断最新 chunk 的尾部）"""

    final_output: str = ""
    """累积的最终输出文本"""

    workflow_meta_started: bool = False
    """是否已为当前 turn 发出 workflow_start（去重）"""

    workflow_meta_ended: bool = False
    """是否已为当前 turn 发出 workflow_end（去重）"""


def process_text_chunk(
    text: str, state: StreamState, suppress_final: bool = False
) -> tuple[list[dict], StreamState]:
    """
    处理 OpenAI Tools 模式的流式文本块。

    识别 <think>...</think> 标签：
    - <think> 内的内容 → {"type": "thinking", "content": "..."}
    - <think> 外的内容 → {"type": "final", "content": "..."}

    Parameters
    ----------
    text : str
        新到达的文本块
    state : StreamState
        当前累积状态
    suppress_final : bool
        True 时不产出 final 事件（ReAct 模式用）

    Returns
    -------
    tuple[list[dict], StreamState]
        (事件列表, 新状态)
    """
    state.content_buffer += text
    events: list[dict] = []

    while True:
        buf = state.content_buffer
        if not state.in_thinking_block:
            # 寻找 <think> 开始标签
            idx = buf.find("<think>")
            if idx != -1:
                # <think> 之前的内容是 final 输出
                if idx > 0:
                    pre_text = buf[:idx]
                    if not suppress_final:
                        state.final_output += pre_text
                        events.append({"type": "final", "content": pre_text})
                state.in_thinking_block = True
                state.content_buffer = buf[idx + 7 :]  # 跳过 <think>
                continue

            # 检查是否有不完整的 <think> 标签（尾部 "<thi" 等）
            last_lt = buf.rfind("<")
            if last_lt != -1 and len(buf) - last_lt < 7:
                if last_lt > 0:
                    pre_text = buf[:last_lt]
                    if not suppress_final:
                        state.final_output += pre_text
                        events.append({"type": "final", "content": pre_text})
                    state.content_buffer = buf[last_lt:]
                break
            else:
                # 安全的，全部输出
                if buf:
                    if not suppress_final:
                        state.final_output += buf
                        events.append({"type": "final", "content": buf})
                    state.content_buffer = ""
                break
        else:
            # 在 <think> 块内，寻找 </think> 结束标签
            idx = buf.find("</think>")
            if idx != -1:
                if idx > 0:
                    events.append({"type": "thinking", "content": buf[:idx]})
                state.in_thinking_block = False
                state.content_buffer = buf[idx + 8 :]  # 跳过 </think>
                continue

            # 检查是否有不完整的 </think> 标签（尾部 "</thin" 等）
            last_lt = buf.rfind("<")
            if last_lt != -1 and len(buf) - last_lt < 8:
                if last_lt > 0:
                    events.append({"type": "thinking", "content": buf[:last_lt]})
                    state.content_buffer = buf[last_lt:]
                break
            else:
                if buf:
                    events.append({"type": "thinking", "content": buf})
                    state.content_buffer = ""
                break

    return events, state


def process_react_chunk(text: str, state: StreamState) -> tuple[list[dict], StreamState]:
    """
    处理 ReAct 模式的流式文本块。

    检测 Action: 或 JSON 开头标记，将之前的文本作为 thinking 输出。
    进入 Action 块后不再产出 thinking 事件。

    Parameters
    ----------
    text : str
        新到达的文本块
    state : StreamState
        当前累积状态

    Returns
    -------
    tuple[list[dict], StreamState]
        (事件列表, 新状态)
    """
    state.content_buffer += text
    events: list[dict] = []

    if state.in_react_action:
        return events, state

    # 检测 Action 块起始标记
    markers = ["Action:", "```json", '{"action"', '{ "action"', '{\n  "action"']
    idx = -1
    for m in markers:
        i = state.content_buffer.find(m)
        if i != -1:
            if idx == -1 or i < idx:
                idx = i

    if idx != -1:
        # 找到 Action 标记，之前的文本是 thinking
        state.in_react_action = True
        thought_text = state.content_buffer[state.react_emitted_len : idx]
        if state.react_emitted_len == 0 and thought_text.startswith("Thought:"):
            thought_text = thought_text[8:].lstrip()
        if thought_text:
            events.append({"type": "thinking", "content": thought_text})
        return events, state

    # 没有找到 Action，安全地输出思考文本（保留可能是不完整 marker 的尾部）
    tail_safe = max(len(m) for m in markers)  # 最长 marker 约 12 字符
    safe_len = max(0, len(state.content_buffer) - tail_safe)
    if safe_len > state.react_emitted_len:
        thought_text = state.content_buffer[state.react_emitted_len : safe_len]
        if state.react_emitted_len == 0 and state.content_buffer.startswith("Thought:"):
            if safe_len >= 8:
                thought_text = state.content_buffer[8:safe_len].lstrip()
                state.react_emitted_len = safe_len
                if thought_text:
                    events.append({"type": "thinking", "content": thought_text})
        else:
            state.react_emitted_len = safe_len
            if thought_text:
                events.append({"type": "thinking", "content": thought_text})

    return events, state


def flush_buffer(
    state: StreamState, suppress_final: bool = False
) -> tuple[list[dict], StreamState]:
    """
    流结束时刷新剩余缓冲区。

    将 content_buffer 中未处理完的内容作为 final 或 thinking 输出。
    """
    events: list[dict] = []
    buf = state.content_buffer

    if not buf:
        return events, state

    if suppress_final:
        # ReAct 模式：未进入 Action 的余量作为 thinking
        if not state.in_react_action and len(buf) > state.react_emitted_len:
            thought_text = buf[state.react_emitted_len :]
            if state.react_emitted_len == 0 and thought_text.startswith("Thought:"):
                if len(thought_text) >= 8:
                    thought_text = thought_text[8:].lstrip()
                else:
                    thought_text = ""
            if thought_text:
                events.append({"type": "thinking", "content": thought_text})
    else:
        # OpenAI Tools 模式
        if state.in_thinking_block:
            events.append({"type": "thinking", "content": buf})
        else:
            state.final_output += buf
            events.append({"type": "final", "content": buf})

    state.content_buffer = ""
    return events, state


def parse_react_final_answer(raw_output: str) -> str:
    """
    从 ReAct agent 的最终输出中提取 Final Answer 文本。

    处理格式: {"action": "Final Answer", "action_input": "..."}
    """
    if "Final Answer" not in raw_output:
        return raw_output

    try:
        import json

        start = raw_output.find("{")
        end = raw_output.rfind("}")
        if start != -1 and end != -1 and end > start:
            json_str = raw_output[start : end + 1]
            try:
                data = json.loads(json_str, strict=False)
                if data.get("action") == "Final Answer" and "action_input" in data:
                    return str(data["action_input"])
            except Exception:
                pass

            import re

            match = re.search(r'"action_input"\s*:\s*"(.*?)"\s*\}', json_str, re.DOTALL)
            if match:
                extracted = match.group(1)
                return extracted.replace("\\n", "\n").replace('\\"', '"')
            else:
                prefix = raw_output[:start].strip()
                if prefix and not prefix.startswith("Thought:"):
                    return prefix
    except Exception:
        pass

    # 回退清理
    result = raw_output.replace("Action:", "").replace("Thought:", "").strip()
    if result.startswith("```json"):
        result = result[7:]
    if result.endswith("```"):
        result = result[:-3]
    result = result.strip()
    return result or raw_output
