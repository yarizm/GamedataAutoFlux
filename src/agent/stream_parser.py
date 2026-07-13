"""
流式文本解析器

将 LLM 流式输出的原始文本块解析为结构化事件（thinking / final）。
识别 <think>...</think> 标签。

纯函数，无外部依赖。
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass
class StreamState:
    """流式解析器的累积状态"""

    in_thinking_block: bool = False
    """是否在 <think> 标签内"""

    content_buffer: str = ""
    """累积的原始文本缓冲区"""

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
    处理流式文本块，识别 <think>...</think> 标签：
    - <think> 内的内容 → {"type": "thinking", "content": "..."}
    - <think> 外的内容 → {"type": "final", "content": "..."}
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


def flush_buffer(
    state: StreamState, suppress_final: bool = False
) -> tuple[list[dict], StreamState]:
    """流结束时刷新剩余缓冲区。"""
    events: list[dict] = []
    buf = state.content_buffer

    if not buf:
        return events, state

    if suppress_final:
        pass
    else:
        if state.in_thinking_block:
            events.append({"type": "thinking", "content": buf})
        else:
            state.final_output += buf
            events.append({"type": "final", "content": buf})

    state.content_buffer = ""
    return events, state
