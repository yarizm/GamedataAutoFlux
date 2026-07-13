"""LangGraph-only Agent runtime.

Builds a root StateGraph (workflow routing + general_agent via create_agent).
Legacy AgentExecutor / langchain_classic backends have been removed.
"""

from __future__ import annotations

import inspect
from collections.abc import Callable
from typing import Any

from langchain_core.tools import BaseTool
from langchain_openai import ChatOpenAI
from loguru import logger

from src.agent.checkpointer import create_agent_checkpointer
from src.agent.workflows import build_langgraph_root_graph
from src.core.config import get as get_config


def recursion_limit_from_max_iterations(max_iterations: int) -> int:
    """Map tool-loop budget to LangGraph recursion_limit (tool steps cost 2+)."""
    value = int(max_iterations) if max_iterations is not None else 10
    return max(10, value * 2 + 5)


def create_runtime(
    *,
    build_openai_tools_system_prompt: Callable[[list[BaseTool]], str],
    create_mcp_manager: Callable[[], Any],
) -> "LangGraphAgentRuntime":
    """Build the LangGraph agent runtime."""
    return LangGraphAgentRuntime(
        build_openai_tools_system_prompt=build_openai_tools_system_prompt,
        create_mcp_manager=create_mcp_manager,
    )


class LangGraphAgentRuntime:
    """LangGraph compiled root graph runtime."""

    backend_name = "langgraph_agent"
    input_mode = "messages_graph"

    def __init__(
        self,
        *,
        build_openai_tools_system_prompt: Callable[[list[BaseTool]], str],
        create_mcp_manager: Callable[[], Any],
    ) -> None:
        self._build_openai_tools_system_prompt = build_openai_tools_system_prompt
        self._create_mcp_manager = create_mcp_manager

        self.llm: ChatOpenAI | None = None
        self.agent_executor: Any = None
        self.mcp_manager: Any | None = None
        self.initialized: bool = False
        self.active_tools: list[BaseTool] = []
        self._max_iterations: int = 10

        self._checkpointer_bundle = create_agent_checkpointer()
        self.thread_checkpoint_backend = self._checkpointer_bundle.backend_name
        self.thread_checkpoint_storage_path = self._checkpointer_bundle.storage_path

    def configured_agent_type(self) -> str:
        """Tool-calling semantics only (openai_tools)."""
        return "openai_tools"

    def effective_agent_type(self) -> str:
        return "openai_tools"

    def uses_legacy_react_parser(self) -> bool:
        """Always False; ReAct text protocol removed."""
        return False

    def compatibility_warnings(self) -> list[str]:
        return []

    def recursion_limit(self) -> int:
        return recursion_limit_from_max_iterations(self._max_iterations)

    def ensure_initialized(
        self,
        *,
        provider_override: str | None,
        base_tools: list[BaseTool],
        max_iterations: int,
    ) -> None:
        """Initialize the underlying LLM and graph if needed."""
        if self.initialized:
            return

        provider = provider_override or get_config("llm.provider", "qwen")
        api_key = get_config(f"llm.{provider}.api_key", "")
        base_url = get_config(f"llm.{provider}.base_url", "")
        model = get_config(f"llm.{provider}.model", "qwen-max")
        temperature = get_config(f"llm.{provider}.temperature", 0.3)
        max_tokens = get_config(f"llm.{provider}.max_tokens", 2000)

        if not api_key:
            if provider == "local":
                api_key = "local"
            else:
                raise ValueError(
                    f"LLM provider '{provider}' 的 api_key 为空，请检查系统环境变量或 .env 文件中是否设置了对应的 key"
                )

        self.llm = ChatOpenAI(
            model=model,
            temperature=temperature,
            max_tokens=max_tokens,
            api_key=api_key,
            base_url=base_url if base_url else None,
            streaming=True,
        )
        self._max_iterations = int(max_iterations)
        self.agent_executor = self._build_executor(
            llm=self.llm,
            tools=list(base_tools),
            max_iterations=max_iterations,
        )
        self.active_tools = list(base_tools)
        self.initialized = True
        logger.info(
            "Agent runtime 初始化完成 (backend={}, provider={}, model={})",
            self.backend_name,
            provider,
            model,
        )

    async def ensure_async(
        self,
        *,
        provider_override: str | None,
        base_tools: list[BaseTool],
        max_iterations: int,
    ) -> None:
        """Ensure runtime readiness, including optional MCP tool injection."""
        if not self.initialized:
            self.ensure_initialized(
                provider_override=provider_override,
                base_tools=base_tools,
                max_iterations=max_iterations,
            )
        else:
            self._max_iterations = int(max_iterations)

        if not get_config("agent.playwright_mcp.enabled", False):
            return

        if not self.mcp_manager:
            self.mcp_manager = self._create_mcp_manager()

        mcp_restarted = False
        if not self.mcp_manager._is_running:
            logger.info("MCP 未运行，尝试启动/重连 Playwright MCP Server...")
            await self.mcp_manager.start()
            mcp_restarted = True

        mcp_tools = self.mcp_manager.get_langchain_tools()
        current_tool_names = {tool.name for tool in self.active_tools}
        mcp_tool_names = {tool.name for tool in mcp_tools}

        if mcp_tools and (mcp_restarted or not mcp_tool_names.issubset(current_tool_names)):
            all_tools = list(base_tools) + list(mcp_tools)
            self.agent_executor = self._build_executor(
                llm=self.llm,
                tools=all_tools,
                max_iterations=max_iterations,
            )
            self.active_tools = list(all_tools)
            logger.info("成功将 {} 个 MCP 工具注入 Agent", len(mcp_tools))

    def build_invoke_payload(
        self,
        *,
        user_input: str,
        history: list[Any],
        thread_id: str,
    ) -> dict[str, Any]:
        messages = [] if self._should_resume_from_checkpointer(thread_id) else list(history)
        messages.append(("human", user_input))
        return {"messages": messages}

    def _should_resume_from_checkpointer(self, thread_id: str) -> bool:
        if not thread_id or not self._checkpointer_bundle.enabled:
            return False
        checkpointer = self._checkpointer_bundle.checkpointer
        get_tuple = getattr(checkpointer, "get_tuple", None)
        if get_tuple is None:
            return False
        return get_tuple({"configurable": {"thread_id": thread_id}}) is not None

    def mark_thread_checkpointed(self, thread_id: str) -> None:
        """Checkpoint presence is discovered lazily from the store."""

    async def forget_thread(self, thread_id: str) -> None:
        if not thread_id or not self._checkpointer_bundle.enabled:
            return

        checkpointer = self._checkpointer_bundle.checkpointer
        for attr_name in ("adelete_thread", "delete_thread"):
            delete_thread = getattr(checkpointer, attr_name, None)
            if delete_thread is None:
                continue
            result = delete_thread(thread_id)
            if inspect.isawaitable(result):
                await result
            return

    def reset(self) -> None:
        """Drop in-memory runtime state and force next call to reinitialize."""
        self.initialized = False
        self.llm = None
        self.agent_executor = None
        self.mcp_manager = None
        self.active_tools = []
        self._max_iterations = 10
        self._checkpointer_bundle = create_agent_checkpointer()
        self.thread_checkpoint_backend = self._checkpointer_bundle.backend_name
        self.thread_checkpoint_storage_path = self._checkpointer_bundle.storage_path

    def get_active_tools(self, base_tools: list[BaseTool]) -> list[BaseTool]:
        """Return the current tool inventory for diagnostics."""
        executor_tools = list(getattr(self.agent_executor, "tools", []) or [])
        if executor_tools:
            return executor_tools
        if self.active_tools:
            return list(self.active_tools)
        return list(base_tools)

    def _build_executor(
        self,
        *,
        llm: ChatOpenAI | None,
        tools: list[BaseTool],
        max_iterations: int,
    ) -> Any:
        if llm is None:
            raise RuntimeError("LLM 尚未初始化，无法构建 agent graph")

        self._max_iterations = int(max_iterations)
        system_prompt = self._build_openai_tools_system_prompt(tools)
        return build_langgraph_root_graph(
            model=llm,
            tools=tools,
            system_prompt=system_prompt,
            checkpointer=self._checkpointer_bundle.checkpointer,
            debug=False,
            name="GamedataAutoFluxAgent",
        )


# Alias kept for existing imports / type hints.
BaseAgentRuntime = LangGraphAgentRuntime
