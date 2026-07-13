"""stream_parser 测试"""

from src.agent.stream_parser import (
    StreamState,
    process_text_chunk,
    flush_buffer,
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

    def test_flush_empty_buffer(self):
        """空缓冲区不产出事件"""
        events, state = flush_buffer(StreamState())
        assert len(events) == 0
