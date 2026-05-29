"""stream_parser 测试"""

from src.agent.stream_parser import (
    StreamState,
    process_text_chunk,
    process_react_chunk,
    flush_buffer,
    parse_react_final_answer,
)


# ==================== process_text_chunk ====================


class TestProcessTextChunk:
    def test_plain_text_becomes_final(self):
        """纯文本 → final 事件"""
        events, state = process_text_chunk("你好世界", StreamState())
        assert len(events) == 1
        assert events[0] == {"type": "final", "content": "你好世界"}
        assert state.final_output == "你好世界"

    def test_think_block_becomes_thinking(self):
        """<think> 包裹的内容 → thinking 事件"""
        events, state = process_text_chunk("<think>让我想想</think>答案是42", StreamState())
        # 先出 thinking，再出 final
        assert len(events) == 2
        assert events[0] == {"type": "thinking", "content": "让我想想"}
        assert events[1] == {"type": "final", "content": "答案是42"}

    def test_think_tag_across_chunks(self):
        """<think> 标签跨 chunk 仍能正确解析"""
        state = StreamState()
        events1, state = process_text_chunk("前面<thi", state)
        # "<thi" 可能是不完整的 <think>，前面安全文本输出，<thi 保留在 buffer
        assert len(events1) == 1
        assert events1[0] == {"type": "final", "content": "前面"}
        assert state.content_buffer == "<thi"

        events2, state = process_text_chunk("nk>思考中</think>结束", state)
        # content_buffer 现在是 "<think>思考中</think>结束"
        assert len(events2) >= 2
        event_types = [e["type"] for e in events2]
        assert "thinking" in event_types
        assert "final" in event_types

    def test_close_think_across_chunks(self):
        """</think> 跨 chunk"""
        state = StreamState(in_thinking_block=True, content_buffer="")
        events1, state = process_text_chunk("分析中</thi", state)
        # "</thi" 可能是不完整的 </think>，前面安全文本作为 thinking 输出
        assert len(events1) == 1
        assert events1[0] == {"type": "thinking", "content": "分析中"}
        assert state.content_buffer == "</thi"

        events2, state = process_text_chunk("nk>结果", state)
        # content_buffer 现在是 "</think>结果"
        assert len(events2) >= 1
        assert "final" in [e["type"] for e in events2]

    def test_suppress_final(self):
        """suppress_final=True 时不产出 final 事件"""
        events, state = process_text_chunk("文本", StreamState(), suppress_final=True)
        assert len(events) == 0
        # final_output 也不累积
        assert state.final_output == ""

    def test_no_think_tag_all_text(self):
        """没有 think 标签时全部作为 final"""
        events, state = process_text_chunk("根据我的分析，这个游戏的评分是4.5分。", StreamState())
        assert all(e["type"] == "final" for e in events)

    def test_multiple_think_blocks(self):
        """多个 think 块"""
        events, state = process_text_chunk("<think>1</think>A<think>2</think>B", StreamState())
        thinking = [e for e in events if e["type"] == "thinking"]
        final = [e for e in events if e["type"] == "final"]
        assert len(thinking) == 2
        assert "".join(f["content"] for f in final) == "AB"


# ==================== process_react_chunk ====================


class TestProcessReactChunk:
    def test_thought_becomes_thinking(self):
        """ReAct Thought: → thinking（流式场景：多 chunk + flush 出完整内容）"""
        state = StreamState()
        # 第一个 chunk：因 tail-safe 留白，只产出安全的头部
        events1, state = process_react_chunk("Thought: 我需要查询", state)
        # 第二个 chunk：继续产出剩余
        events2, state = process_react_chunk("数据库的最新状态", state)
        # flush 收尾
        events3, state = flush_buffer(state, suppress_final=True)

        all_events = events1 + events2 + events3
        thinking_contents = "".join(e["content"] for e in all_events if e["type"] == "thinking")
        assert "查询数据库" in thinking_contents

    def test_action_marker_stops_thinking(self):
        """检测到 Action: 后不再产出 thinking"""
        events, state = process_react_chunk("Thought: 分析完成\nAction:", StreamState())
        assert state.in_react_action

    def test_json_action_marker(self):
        """{"action" 格式的 Action 标记"""
        events, state = process_react_chunk(
            'Thought: 准备调用工具\n{"action": "tool_name"', StreamState()
        )
        assert state.in_react_action

    def test_action_across_chunks(self):
        """Action 标记跨 chunk"""
        state = StreamState()
        events1, state = process_react_chunk("Thought: 分析数据\nAct", state)
        events2, state = process_react_chunk("ion:\n```json\n...", state)
        assert state.in_react_action

    def test_after_action_no_events(self):
        """进入 Action 块后不再产出新事件"""
        state = StreamState(in_react_action=True, content_buffer="...")
        events, state = process_react_chunk("更多action内容", state)
        assert len(events) == 0


# ==================== flush_buffer ====================


class TestFlushBuffer:
    def test_flush_thinking_buffer(self):
        """刷新在 <think> 内的缓冲区"""
        state = StreamState(in_thinking_block=True, content_buffer="未完的思考")
        events, state = flush_buffer(state)
        assert len(events) == 1
        assert events[0] == {"type": "thinking", "content": "未完的思考"}
        assert state.content_buffer == ""

    def test_flush_final_buffer(self):
        """刷新普通文本缓冲区"""
        state = StreamState(content_buffer="最终文本")
        events, state = flush_buffer(state)
        assert len(events) == 1
        assert events[0] == {"type": "final", "content": "最终文本"}
        assert state.final_output == "最终文本"

    def test_flush_react_remaining_thought(self):
        """ReAct 模式刷新剩余 thought"""
        state = StreamState(content_buffer="余量思考")
        events, state = flush_buffer(state, suppress_final=True)
        assert len(events) == 1
        assert events[0]["type"] == "thinking"

    def test_flush_empty_buffer(self):
        """空缓冲区不产出事件"""
        events, state = flush_buffer(StreamState())
        assert len(events) == 0

    def test_flush_react_thought_prefix(self):
        """ReAct 模式以 Thought: 开头"""
        state = StreamState(content_buffer="Thought: 最后的分析")
        events, state = flush_buffer(state, suppress_final=True)
        assert len(events) == 1
        assert events[0]["type"] == "thinking"
        assert "最后的分析" in events[0]["content"]


# ==================== parse_react_final_answer ====================


class TestParseReactFinalAnswer:
    def test_extract_json_final_answer(self):
        """从 JSON 提取 Final Answer"""
        raw = 'Thought: 完成\nAction:\n```\n{"action": "Final Answer", "action_input": "答案是42"}\n```'
        result = parse_react_final_answer(raw)
        assert result == "答案是42"

    def test_no_final_answer_passthrough(self):
        """没有 Final Answer 时原样返回"""
        raw = "普通文本输出"
        result = parse_react_final_answer(raw)
        assert result == "普通文本输出"

    def test_strip_markdown_fences(self):
        """清理 ```json 围栏"""
        raw = '```json\n{"action": "Final Answer", "action_input": "结果"}\n```'
        result = parse_react_final_answer(raw)
        assert result == "结果"

    def test_fallback_cleanup(self):
        """回退清理 Action:/Thought: 前缀"""
        raw = "Action: 最终结果"
        result = parse_react_final_answer(raw)
        assert "最终结果" in result
