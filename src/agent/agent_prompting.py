"""Prompt-building and parsing-error helpers for Agent runtimes."""

from __future__ import annotations

from collections.abc import Callable, Sequence

from langchain_core.messages import SystemMessage
from langchain_core.prompts import ChatPromptTemplate, MessagesPlaceholder
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


def build_openai_tools_prompt(
    system_prompt: str,
    tools: Sequence[BaseTool],
) -> ChatPromptTemplate:
    system_content = build_openai_tools_system_prompt(system_prompt, tools)
    return ChatPromptTemplate.from_messages(
        [
            SystemMessage(content=system_content),
            MessagesPlaceholder("chat_history", optional=True),
            ("human", "{input}"),
            MessagesPlaceholder("agent_scratchpad"),
        ]
    )


def build_react_prompt(
    system_prompt: str,
    tools: Sequence[BaseTool],
) -> ChatPromptTemplate:
    del tools
    prefix = f"""Respond to the human as helpfully and accurately as possible. You have access to the following tools:

{{tools}}

Use a json blob to specify a tool by providing an action key (tool name) and an action_input key (tool input).

Valid "action" values: "Final Answer" or {{tool_names}}

Provide only ONE action per $JSON_BLOB, as shown:

```
{{{{
  "action": $TOOL_NAME,
  "action_input": $INPUT
}}}}
```

Follow this format:

Question: input question to answer
Thought: consider previous and subsequent steps
Action:
```
$JSON_BLOB
```
Observation: action result
... (repeat Thought/Action/Observation N times)
Thought: I know what to respond
Action:
```
{{{{
  "action": "Final Answer",
  "action_input": "Final response to human"
}}}}
```

Additional Rules:
{system_prompt}
"""
    suffix = (
        "Begin! Reminder to ALWAYS respond with a valid json blob of a single action. "
        "Use tools if necessary. Respond directly if appropriate. "
        "Format is Action:```$JSON_BLOB```then Observation"
    )

    return ChatPromptTemplate.from_messages(
        [
            ("system", prefix),
            MessagesPlaceholder("chat_history", optional=True),
            ("human", "{input}\n\n" + suffix + "\n\n{agent_scratchpad}"),
        ]
    )


def next_parsing_error_response(
    current_count: int,
    error: Exception,
    *,
    redact_stream_text: Callable[[str], str],
) -> tuple[int, str]:
    next_count = current_count + 1
    if next_count >= 3:
        return (
            0,
            "Action failed because the action format was incorrect 3 times in a row. Stop using tools and ask the user for clarification.",
        )

    safe_error = redact_stream_text(str(error))
    return (
        next_count,
        "Check your json formatting. It must be valid json with 'action' and "
        f"'action_input' keys. Error: {safe_error}",
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
