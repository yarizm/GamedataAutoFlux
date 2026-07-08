"""Agent runtime backends.

Phase 1 keeps the existing LangChain-based execution model, while introducing a
thin backend abstraction so the project can migrate toward LangGraph-native
agents without rewriting AgentService, SSE routes, or tool implementations in
one pass.
"""

from __future__ import annotations

import inspect
from abc import ABC, abstractmethod
from collections.abc import Callable
from typing import Any

from langchain_core.tools import BaseTool
from langchain_openai import ChatOpenAI
from loguru import logger

from src.agent.checkpointer import create_agent_checkpointer
from src.agent.workflows import build_langgraph_root_graph
try:
    from langchain_classic.agents import (
        AgentExecutor,
        create_openai_tools_agent,
        create_structured_chat_agent,
    )
except ImportError:
    from langchain.agents import (
        AgentExecutor,
        create_openai_tools_agent,
        create_structured_chat_agent,
    )

from src.core.config import get as get_config


def create_runtime(
    *,
    build_openai_tools_system_prompt: Callable[[list[BaseTool]], str],
    build_openai_tools_prompt: Callable[[list[BaseTool]], Any],
    build_react_prompt: Callable[[list[BaseTool]], Any],
    create_mcp_manager: Callable[[], Any],
    handle_parsing_error: Callable[[Exception], str],
) -> "BaseAgentRuntime":
    """Build the configured runtime backend."""
    backend = str(get_config("agent.runtime_backend", "langgraph_agent") or "").strip()
    if backend == "langgraph_agent":
        return LangGraphAgentRuntime(
            build_openai_tools_system_prompt=build_openai_tools_system_prompt,
            build_openai_tools_prompt=build_openai_tools_prompt,
            build_react_prompt=build_react_prompt,
            create_mcp_manager=create_mcp_manager,
            handle_parsing_error=handle_parsing_error,
        )
    return LangChainAgentRuntime(
        build_openai_tools_system_prompt=build_openai_tools_system_prompt,
        build_openai_tools_prompt=build_openai_tools_prompt,
        build_react_prompt=build_react_prompt,
        create_mcp_manager=create_mcp_manager,
        handle_parsing_error=handle_parsing_error,
    )


class BaseAgentRuntime(ABC):
    """Shared runtime lifecycle interface."""

    backend_name = "base"
    input_mode = "legacy_executor"

    def __init__(
        self,
        *,
        build_openai_tools_system_prompt: Callable[[list[BaseTool]], str],
        build_openai_tools_prompt: Callable[[list[BaseTool]], Any],
        build_react_prompt: Callable[[list[BaseTool]], Any],
        create_mcp_manager: Callable[[], Any],
        handle_parsing_error: Callable[[Exception], str],
    ) -> None:
        self._build_openai_tools_system_prompt = build_openai_tools_system_prompt
        self._build_openai_tools_prompt = build_openai_tools_prompt
        self._build_react_prompt = build_react_prompt
        self._create_mcp_manager = create_mcp_manager
        self._handle_parsing_error = handle_parsing_error

        self.llm: ChatOpenAI | None = None
        self.agent_executor: Any = None
        self.mcp_manager: Any | None = None
        self.initialized: bool = False
        self.active_tools: list[BaseTool] = []
        self.thread_checkpoint_backend: str = "disabled"
        self.thread_checkpoint_storage_path: str | None = None

    def configured_agent_type(self) -> str:
        """Return the raw agent_type requested in config."""
        value = str(get_config("agent.agent_type", "openai_tools") or "").strip()
        return value or "openai_tools"

    def effective_agent_type(self) -> str:
        """Return the runtime's effective execution mode."""
        return self.configured_agent_type()

    def uses_legacy_react_parser(self) -> bool:
        """Whether AgentService should parse streaming output as legacy ReAct text."""
        return self.effective_agent_type() == "react"

    def compatibility_warnings(self) -> list[str]:
        """Return compatibility notes for the current runtime/config combination."""
        return []

    def ensure_initialized(
        self,
        *,
        provider_override: str | None,
        base_tools: list[BaseTool],
        max_iterations: int,
    ) -> None:
        """Initialize the underlying LLM and executor if needed."""
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
        current_tool_names = {
            tool.name for tool in (getattr(self.agent_executor, "tools", []) or [])
        }
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
        """Translate AgentService state into backend input payload."""
        return {
            "input": user_input,
            "chat_history": history,
        }

    def mark_thread_checkpointed(self, thread_id: str) -> None:
        """Mark one thread as checkpoint-backed after a successful run."""

    async def forget_thread(self, thread_id: str) -> None:
        """Drop backend-specific cached state for one thread."""

    def reset(self) -> None:
        """Drop in-memory runtime state and force next call to reinitialize."""
        self.initialized = False
        self.llm = None
        self.agent_executor = None
        self.mcp_manager = None
        self.active_tools = []
        self.thread_checkpoint_backend = "disabled"
        self.thread_checkpoint_storage_path = None

    def get_active_tools(self, base_tools: list[BaseTool]) -> list[BaseTool]:
        """Return the current tool inventory for diagnostics."""
        executor_tools = list(getattr(self.agent_executor, "tools", []) or [])
        if executor_tools:
            return executor_tools
        if self.active_tools:
            return list(self.active_tools)
        return list(base_tools)

    @abstractmethod
    def _build_executor(
        self,
        *,
        llm: ChatOpenAI | None,
        tools: list[BaseTool],
        max_iterations: int,
    ) -> Any:
        """Build backend-specific executor/graph object."""


class LangChainAgentRuntime(BaseAgentRuntime):
    """Current runtime backend backed by LangChain tool-calling agents."""

    backend_name = "langchain_classic"
    input_mode = "legacy_executor"

    def _build_executor(
        self,
        *,
        llm: ChatOpenAI | None,
        tools: list[BaseTool],
        max_iterations: int,
    ) -> AgentExecutor:
        if llm is None:
            raise RuntimeError("LLM 尚未初始化，无法构建 AgentExecutor")

        agent_type = get_config("agent.agent_type", "openai_tools")
        if agent_type == "react":
            prompt = self._build_react_prompt(tools)
            agent = create_structured_chat_agent(llm, tools, prompt)
        else:
            prompt = self._build_openai_tools_prompt(tools)
            agent = create_openai_tools_agent(llm, tools, prompt)

        return AgentExecutor(
            agent=agent,
            tools=tools,
            max_iterations=max_iterations,
            handle_parsing_errors=self._handle_parsing_error,
            verbose=False,
        )


class LangGraphAgentRuntime(BaseAgentRuntime):
    """LangChain v1 agent runtime backed by LangGraph compiled graphs."""

    backend_name = "langgraph_agent"
    input_mode = "messages_graph"

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self._checkpointer_bundle = create_agent_checkpointer()
        self.thread_checkpoint_backend = self._checkpointer_bundle.backend_name
        self.thread_checkpoint_storage_path = self._checkpointer_bundle.storage_path
        self._compatibility_warning_logged = False

    def effective_agent_type(self) -> str:
        """LangGraph runtime currently always uses tool-calling semantics."""
        return "openai_tools"

    def compatibility_warnings(self) -> list[str]:
        if self.configured_agent_type() != "react":
            return []
        return [
            "agent.agent_type=react is ignored when agent.runtime_backend=langgraph_agent; "
            "the runtime uses openai_tools-style tool calling and SSE parsing."
        ]

    def _log_compatibility_warning(self) -> None:
        warnings = self.compatibility_warnings()
        if not warnings or self._compatibility_warning_logged:
            return
        logger.warning(warnings[0])
        self._compatibility_warning_logged = True

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

    def _build_executor(
        self,
        *,
        llm: ChatOpenAI | None,
        tools: list[BaseTool],
        max_iterations: int,
    ) -> Any:
        if llm is None:
            raise RuntimeError("LLM 尚未初始化，无法构建 agent graph")

        self._log_compatibility_warning()
        system_prompt = self._build_openai_tools_system_prompt(tools)
        return build_langgraph_root_graph(
            model=llm,
            tools=tools,
            system_prompt=system_prompt,
            checkpointer=self._checkpointer_bundle.checkpointer,
            debug=False,
            name="GamedataAutoFluxAgent",
        )

    def reset(self) -> None:
        super().reset()
        self._checkpointer_bundle = create_agent_checkpointer()
        self.thread_checkpoint_backend = self._checkpointer_bundle.backend_name
        self.thread_checkpoint_storage_path = self._checkpointer_bundle.storage_path
        self._compatibility_warning_logged = False
