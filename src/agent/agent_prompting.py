"""Prompt-building helpers for the LangGraph Agent runtime."""

from __future__ import annotations

from collections.abc import Sequence

from langchain_core.tools import BaseTool


def build_openai_tools_system_prompt(
    system_prompt: str,
    tools: Sequence[BaseTool],
) -> str:
    tool_desc = "\n".join([f"- {tool.name}: {tool.description}" for tool in tools])
    return (
        f"{system_prompt}\n\n"
        f"====== 非常重要：工具使用规范 ======\n"
        f"你必须优先使用以下工具来获取事实信息或执行操作，而不能仅凭记忆臆断。\n"
        f"**绝对禁止伪造或虚构工具调用结果！绝对禁止在没有实际发起函数调用的情况下回复用户你已经完成了操作！**\n"
        f"所有的查询、页面导航、数据采集动作都必须通过实际的工具调用（Tool Call/Function Call）来执行。\n"
        f"切勿在正文中手写类似 `⚙ xxx` 或 `### Result` 的伪造日志！必须使用原生工具调用。\n"
        f"在你决定调用工具或输出最终结果之前，请务必先进行充分的逻辑推理和步骤规划，并将所有思考过程包裹在 `<think>` 和 `</think>` 标签内。例如：\n"
        f"<think>\n为了完成这个任务，我需要先搜索...然后再提取...\n</think>\n"
        f"当你需要查询数据、管理任务或进行网络请求时，必须主动且直接地调用对应的工具：\n"
        f"{tool_desc}\n"
        f"===================================\n"
    )


def default_system_prompt() -> str:
    """Minimal fallback prompt; detailed rules live in settings.yaml."""
    return (
        "你是一个游戏数据采集与分析系统的 AI 助手。"
        "你可以帮助用户查看和管理任务、配置 Pipeline、设置定时采集、"
        "浏览数据以及生成报告。"
        "请根据用户的自然语言指令，选择合适的工具完成任务。"
        "如果意图不明确，请主动询问细节。"
        "所有响应请使用中文。"
    )
