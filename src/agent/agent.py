"""LangChain Agent service."""

from __future__ import annotations

import asyncio
import time
from collections.abc import AsyncIterator
from typing import Any

from langchain_core.messages import BaseMessage, HumanMessage
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.tools import BaseTool

from loguru import logger

from src.agent.agent_invoke_lifecycle import (
    AgentInvokeState,
    append_turn_history,
    finalize_stream_tail,
    should_discard_partial_runtime_state,
)
from src.agent.agent_invoke_orchestration import (
    prepare_agent_invoke,
    recover_agent_invoke,
)
from src.agent.agent_invoke_stream import (
    AgentInvokeStreamContext,
    stream_agent_executor_events,
)
from src.agent.agent_history_state import (
    build_thread_snapshots,
    cap_thread_histories,
    collect_stale_thread_ids,
    merge_loaded_threads,
    summarize_session_metrics,
)
from src.agent.agent_prompting import (
    build_openai_tools_prompt,
    build_openai_tools_system_prompt,
    build_react_prompt,
    default_system_prompt,
    next_parsing_error_response,
)
from src.agent.agent_redaction import (
    redact_history_message,
    redact_message_content,
    redact_stream_event,
    redact_stream_text,
    redact_stream_value,
)
from src.agent.agent_status_summary import (
    build_agent_status_summary,
    build_history_recovery_warnings,
    describe_tool_action,
)
from src.agent.runtime import BaseAgentRuntime, create_runtime
from src.agent.thread_store import create_thread_store
from src.agent.tools import ALL_TOOLS
from src.agent.workflow_bridge_events import (
    build_workflow_chain_start_events,
)
from src.agent.workflows import workflow_node_bridge_map
from src.core.config import get as get_config
from src.core.sensitive import redact_sensitive_text


class AgentService:
    """Manage Agent conversations and streaming execution."""

    def __init__(self, session_service: Any) -> None:
        self._runtime = self._build_runtime()
        self._histories: dict[str, list[BaseMessage]] = {}
        self._sessions_timestamps: dict[str, float] = {}
        self._provider_override: str | None = None
        self._lock = asyncio.Lock()
        self._init_lock = asyncio.Lock()
        self._last_save_time: float = 0
        self._session_service = session_service
        self._thread_store = create_thread_store(session_service)
        self._threads_pending_history_recovery: set[str] = set()
        self._history_load_failed: bool = False

        self._max_iterations: int = get_config("agent.max_iterations", 10)
        self._session_timeout: int = get_config("agent.session_timeout_minutes", 60) * 60
        self._histories_loaded: bool = False

        self._system_prompt: str = get_config(
            "agent.system_prompt",
            default_system_prompt(),
        )

    def _build_openai_tools_system_prompt(self, tools: list[BaseTool]) -> str:
        """Build the shared tool-calling system prompt text."""
        return build_openai_tools_system_prompt(self._system_prompt, tools)
    def _build_prompt_with_tools(self, tools: list[BaseTool]) -> ChatPromptTemplate:
        """Build the tool-calling prompt with the active tool set."""
        return build_openai_tools_prompt(self._system_prompt, tools)
    def _build_react_prompt_with_tools(self, tools: list[BaseTool]) -> ChatPromptTemplate:
        """Build the ReAct prompt with the active tool set."""
        return build_react_prompt(self._system_prompt, tools)
    def _handle_parsing_error(self, error: Exception) -> str:
        """Track parser errors and stop repeated malformed tool output."""
        current_count = int(getattr(self, "_parsing_error_count", 0) or 0)
        next_count, response = next_parsing_error_response(
            current_count,
            error,
            redact_stream_text=redact_stream_text,
        )
        self._parsing_error_count = next_count
        logger.warning(
            "Agent JSON 瑙ｆ瀽閿欒 (娆℃暟: {}): {}",
            current_count + 1,
            redact_sensitive_text(str(error)),
        )
        return response
    def _build_runtime(self) -> BaseAgentRuntime:
        """Instantiate the currently configured runtime backend."""
        return create_runtime(
            build_openai_tools_system_prompt=self._build_openai_tools_system_prompt,
            build_openai_tools_prompt=self._build_prompt_with_tools,
            build_react_prompt=self._build_react_prompt_with_tools,
            create_mcp_manager=self._create_mcp_manager,
            handle_parsing_error=self._handle_parsing_error,
        )
    def _ensure_initialized(self) -> None:
        """Lazily initialize the runtime LLM and executor."""
        self._runtime.ensure_initialized(
            provider_override=self._provider_override,
            base_tools=list(ALL_TOOLS),
            max_iterations=self._max_iterations,
        )

    def _create_mcp_manager(self) -> Any:
        try:
            from src.agent.mcp_client import PlaywrightMcpManager
        except ImportError as exc:
            raise RuntimeError(
                "Playwright MCP is enabled, but the mcp dependency is missing. "
                "Install project dependencies or set agent.playwright_mcp.enabled to false."
            ) from exc
        return PlaywrightMcpManager()
    async def _async_ensure_initialized(self) -> None:
        """Ensure runtime readiness and optional MCP startup."""
        async with self._init_lock:
            await self._ensure_histories_loaded_locked()

            if not self._initialized:
                self._ensure_initialized()

            await self._runtime.ensure_async(
                provider_override=self._provider_override,
                base_tools=list(ALL_TOOLS),
                max_iterations=self._max_iterations,
            )

    async def ensure_histories_loaded(self) -> None:
        """Load persisted histories on demand for read-only Agent APIs."""
        async with self._init_lock:
            await self._ensure_histories_loaded_locked()

    async def _ensure_histories_loaded_locked(self) -> None:
        """Load persisted histories exactly once while holding _init_lock."""
        if self._histories_loaded:
            if self._threads_pending_history_recovery and not self._history_load_failed:
                await self._save_histories(force=True)
            return
        loaded, needs_resave = await self._load_histories()
        self._histories_loaded = loaded
        self._history_load_failed = not loaded
        if loaded and needs_resave:
            await self._save_histories(force=True)
    async def _get_history(self, session_id: str) -> list[BaseMessage]:
        """Return the current session history and refresh its activity timestamp."""
        async with self._lock:
            if session_id not in self._histories:
                self._histories[session_id] = []
            self._sessions_timestamps[session_id] = time.time()
            return list(self._histories[session_id])

    async def _mark_pending_history_recovery(self, session_id: str) -> None:
        async with self._lock:
            self._threads_pending_history_recovery.add(session_id)

    async def _prepare_invoke(self, session_id: str):
        return await prepare_agent_invoke(
            session_id=session_id,
            histories_loaded=self._histories_loaded,
            mark_pending_history_recovery=self._mark_pending_history_recovery,
            cleanup_stale_sessions=self._cleanup_stale_sessions,
            get_history=self._get_history,
            uses_legacy_react_parser=self._runtime.uses_legacy_react_parser,
            workflow_node_bridge_map=workflow_node_bridge_map,
        )

    async def ainvoke(self, user_input: str, session_id: str = "default") -> AsyncIterator[dict]:
        """ʽִ Agent yield ¼

        ¼:
          {"type": "thinking", "content": "..."}          ˼
          {"type": "tool_call", "name": "...", "args": {...}}  ߵÿʼ
          {"type": "tool_result", "name": "...", "content": "..."}  ִн
          {"type": "final", "content": "..."}             ıظ
          {"type": "error", "content": "..."}             Ϣ
        """
        invoke_state = AgentInvokeState()

        try:
            await self._async_ensure_initialized()
            prepared_invoke = await self._prepare_invoke(session_id)

            # ʷأٻʵ߳״̬ȡģ
            # ֶԻõ history
            self._parsing_error_count = 0

            stream_state = prepared_invoke.stream_state
            suppress_final_stream = prepared_invoke.suppress_final_stream
            workflow_bridges = prepared_invoke.workflow_bridges

            try:
                executor = self._agent_executor
                if not executor:
                    yield {"type": "error", "content": "Agent was re-initialized during request."}
                    return

                invoke_payload = self._runtime.build_invoke_payload(
                    user_input=user_input,
                    history=prepared_invoke.history,
                    thread_id=session_id,
                )

                stream_context = AgentInvokeStreamContext(
                    stream_state=stream_state,
                    invoke_state=invoke_state,
                    suppress_final_stream=suppress_final_stream,
                    workflow_bridges=workflow_bridges,
                    runtime_input_mode=self._runtime.input_mode,
                )
                async for rendered_event in stream_agent_executor_events(
                    executor,
                    invoke_payload=invoke_payload,
                    session_id=session_id,
                    context=stream_context,
                    build_workflow_chain_start_events=build_workflow_chain_start_events,
                    redact_value=redact_stream_value,
                    redact_stream_event=redact_stream_event,
                    redact_text=redact_stream_text,
                    describe_tool_action=describe_tool_action,
                ):
                    yield rendered_event
                stream_state = stream_context.stream_state

                # ˢʣ໺
                rendered_events, rendered_final_output, stream_state = finalize_stream_tail(
                    stream_state,
                    suppress_final_stream=suppress_final_stream,
                    redact_stream_event=redact_stream_event,
                    redact_stream_text=redact_stream_text,
                )
                for rendered_event in rendered_events:
                    yield rendered_event
                invoke_state.final_output += rendered_final_output

                # ʷ
                await self._save_invoke_history(
                    session_id=session_id,
                    user_input=user_input,
                    invoke_state=invoke_state,
                )

            except asyncio.CancelledError:
                logger.info(f"Agent stream cancelled for session {session_id}.")
                await recover_agent_invoke(
                    session_id=session_id,
                    user_input=user_input,
                    invoke_state=invoke_state,
                    discard_partial_runtime_state=self._discard_partial_invoke_runtime_state,
                    save_invoke_history=self._save_invoke_history,
                )
                raise
            except Exception:
                await recover_agent_invoke(
                    session_id=session_id,
                    user_input=user_input,
                    invoke_state=invoke_state,
                    discard_partial_runtime_state=self._discard_partial_invoke_runtime_state,
                    save_invoke_history=self._save_invoke_history,
                )
                raise

        except Exception as e:
            logger.opt(exception=True).error(f"Agent ִг: {redact_sensitive_text(str(e))}")
            content = redact_stream_text(f"ִг: {e}")
            if get_config("debug", False):
                content += f"\n\n(쳣: {type(e).__name__})"
            yield {"type": "error", "content": content}
    async def _save_invoke_history(
        self,
        *,
        session_id: str,
        user_input: str,
        invoke_state: AgentInvokeState,
    ) -> None:
        if invoke_state.saved:
            return
        invoke_state.saved = True
        async with self._lock:
            append_turn_history(
                self._histories,
                self._threads_pending_history_recovery,
                session_id=session_id,
                user_input=user_input,
                final_output=invoke_state.final_output,
                redact_message_content=redact_message_content,
            )
        await self._save_histories()

    async def _discard_partial_invoke_runtime_state(
        self,
        *,
        session_id: str,
        invoke_state: AgentInvokeState,
    ) -> None:
        if not should_discard_partial_runtime_state(invoke_state):
            return
        try:
            await self._runtime.forget_thread(session_id)
        except Exception as exc:
            logger.warning(
                "Failed to discard partial Agent runtime state for thread {}: {}",
                session_id,
                redact_sensitive_text(str(exc)),
            )

    async def clear_thread(self, thread_id: str = "default") -> None:
        """Clear one thread's in-memory and persisted conversation state."""
        async with self._lock:
            self._histories.pop(thread_id, None)
            self._sessions_timestamps.pop(thread_id, None)
        await self._save_histories(force=True)
        await self._delete_persisted_threads([thread_id])
    async def clear_history(self, session_id: str = "default") -> None:
        """Clear one session history."""
        await self.clear_thread(session_id)

    async def _save_histories(self, force: bool = False) -> bool:
        """Persist a snapshot of session state without holding the Agent lock."""
        if self._history_load_failed:
            return False

        async with self._lock:
            capped_sessions = self._cap_sessions_locked()
            histories = {sid: list(messages) for sid, messages in self._histories.items()}
            timestamps = dict(self._sessions_timestamps)
            last_save_time = self._last_save_time
            pending_recovery_threads = set(self._threads_pending_history_recovery)

        thread_snapshots = build_thread_snapshots(histories, timestamps)
        next_save_time = await self._thread_store.save_threads(
            thread_snapshots,
            last_save_time=last_save_time,
            force=force,
        )
        persisted_ok = next_save_time > last_save_time or not thread_snapshots
        async with self._lock:
            self._last_save_time = max(self._last_save_time, next_save_time)
            if persisted_ok:
                for thread_id in pending_recovery_threads:
                    self._threads_pending_history_recovery.discard(thread_id)
        await self._delete_persisted_threads(capped_sessions)
        return persisted_ok

    async def _load_histories(self) -> tuple[bool, bool]:
        """从数据库恢复 Agent 会话历史"""
        try:
            threads = await self._thread_store.load_threads()
            async with self._lock:
                merge_result = merge_loaded_threads(
                    threads,
                    current_histories=self._histories,
                    current_timestamps=self._sessions_timestamps,
                    pending_recovery_threads=set(self._threads_pending_history_recovery),
                    transform_message=redact_history_message,
                )
                for tid in merge_result.cleared_pending_recovery_threads:
                    self._threads_pending_history_recovery.discard(tid)
                self._histories = merge_result.histories
                self._sessions_timestamps = merge_result.timestamps
            restored_count = merge_result.restored_count
            if merge_result.restored_count:
                logger.info(f"已恢?{restored_count} ?Agent 会话历史")
            return True, merge_result.needs_resave
        except Exception as e:
            logger.warning(f"加载 Agent 会话历史失败: {redact_sensitive_text(str(e))}")
            return False, False
    async def _cleanup_stale_sessions(self) -> None:
        """Drop expired in-memory and persisted session state."""
        if not self._session_timeout:
            return

        now = time.time()
        async with self._lock:
            stale = collect_stale_thread_ids(
                self._sessions_timestamps,
                now=now,
                timeout_seconds=self._session_timeout,
            )
            for sid in stale:
                self._histories.pop(sid, None)
                self._sessions_timestamps.pop(sid, None)

        if stale:
            await self._delete_persisted_threads(stale)

    def _cap_sessions_locked(self) -> list[str]:
        max_sessions = int(getattr(self._thread_store, "max_threads", 0) or 0)
        return cap_thread_histories(
            self._histories,
            self._sessions_timestamps,
            max_threads=max_sessions,
        )

    async def _delete_persisted_threads(self, thread_ids: list[str]) -> None:
        thread_ids = [thread_id for thread_id in thread_ids if thread_id]
        if not thread_ids:
            return
        async with self._lock:
            for thread_id in thread_ids:
                self._threads_pending_history_recovery.discard(thread_id)
        await self._thread_store.delete_threads(thread_ids)
        for thread_id in thread_ids:
            await self._runtime.forget_thread(thread_id)

    async def _delete_persisted_sessions(self, session_ids: list[str]) -> None:
        await self._delete_persisted_threads(session_ids)

    def list_thread_ids(self) -> list[str]:
        """Return active thread ids using the future LangGraph naming."""
        return list(self._histories.keys())

    def list_sessions(self) -> list[str]:
        """返回当前有聊天历史的 session ID 列表"""
        return self.list_thread_ids()

    def get_thread_history(self, thread_id: str) -> list[dict[str, str]]:
        """Return one thread's redacted message history."""
        messages = self._histories.get(thread_id, [])
        result = []
        for msg in messages:
            role = "user" if isinstance(msg, HumanMessage) else "assistant"
            result.append({"role": role, "content": redact_message_content(msg.content)})
        return result
    def get_session_history(self, session_id: str) -> list[dict[str, str]]:
        """Return one session history as role/content pairs."""
        return self.get_thread_history(session_id)

    def get_status_summary(self) -> dict[str, Any]:
        """Return a lightweight runtime summary for diagnostics and Agent self-checks."""
        provider = self.get_active_provider()
        model = get_config(f"llm.{provider}.model", "")
        executor_tools = self._runtime.get_active_tools(list(ALL_TOOLS))
        base_tool_names = [tool.name for tool in ALL_TOOLS]
        active_tool_names = [tool.name for tool in executor_tools] or base_tool_names
        mcp_tools = (
            self._mcp_manager.get_langchain_tools()
            if self._mcp_manager and getattr(self._mcp_manager, "_is_running", False)
            else []
        )
        mcp_tool_names = [tool.name for tool in mcp_tools]
        available_providers = self.get_available_providers()
        session_metrics = summarize_session_metrics(
            self._histories,
            self._sessions_timestamps,
            timeout_seconds=self._session_timeout,
        )
        configured_agent_type = self._runtime.configured_agent_type()
        effective_agent_type = self._runtime.effective_agent_type()
        compatibility_warnings = self._runtime.compatibility_warnings()
        history_recovery_warnings = self._build_history_recovery_warnings()
        return build_agent_status_summary(
            provider=provider,
            model=model,
            available_providers=available_providers,
            effective_agent_type=effective_agent_type,
            configured_agent_type=configured_agent_type,
            legacy_react_parser_enabled=self._runtime.uses_legacy_react_parser(),
            compatibility_warnings=compatibility_warnings,
            history_recovery_warnings=history_recovery_warnings,
            initialized=self._initialized,
            runtime_backend=getattr(self._runtime, "backend_name", "unknown"),
            thread_checkpoint_backend=getattr(
                self._runtime, "thread_checkpoint_backend", "disabled"
            ),
            thread_checkpoint_storage_path=getattr(
                self._runtime, "thread_checkpoint_storage_path", None
            ),
            max_iterations=self._max_iterations,
            session_timeout_seconds=self._session_timeout,
            histories_loaded=self._histories_loaded,
            history_load_failed=self._history_load_failed,
            pending_history_recovery_thread_count=len(self._threads_pending_history_recovery),
            session_count=len(self._histories),
            thread_count=len(self._histories),
            session_metrics=session_metrics,
            base_tool_names=base_tool_names,
            active_tool_names=active_tool_names,
            mcp_enabled=bool(get_config("agent.playwright_mcp.enabled", False)),
            mcp_running=bool(
                self._mcp_manager and getattr(self._mcp_manager, "_is_running", False)
            ),
            mcp_tool_names=mcp_tool_names,
        )

    def _build_history_recovery_warnings(self) -> list[str]:
        return build_history_recovery_warnings(
            history_load_failed=self._history_load_failed,
            pending_recovery_thread_count=len(self._threads_pending_history_recovery),
        )

    def get_active_provider(self) -> str:
        """返回当前生效?LLM provider 名称"""
        return self._provider_override or get_config("llm.provider", "qwen")

    def set_provider(self, provider_name: str) -> None:
        """运行时切?LLM provider，下?ainvoke 生效"""
        available = self.get_available_providers()
        if provider_name not in {p["key"] for p in available}:
            raise ValueError(f"未知?provider: {provider_name}")
        self._provider_override = provider_name
        self.reset_runtime()
        logger.info(f"Agent LLM provider 已切? {provider_name}")

    def reload_config(self, provider_name: str | None = None) -> None:
        """重新读取配置并丢弃已初始化的 LLM/AgentExecutor?"""
        if provider_name:
            available = self.get_available_providers()
            if provider_name not in {p["key"] for p in available}:
                raise ValueError(f"未知?provider: {provider_name}")
            self._provider_override = provider_name
        else:
            self._provider_override = None

        self._max_iterations = get_config("agent.max_iterations", 10)
        self._session_timeout = get_config("agent.session_timeout_minutes", 60) * 60
        self._system_prompt = get_config("agent.system_prompt", default_system_prompt())
        self._runtime = self._build_runtime()
        logger.info(f"Agent 配置已重新加?(provider={self.get_active_provider()})")
    def reset_runtime(self) -> None:
        """Drop the current runtime so the next call reinitializes it."""
        self._runtime.reset()

    def _set_runtime_for_testing(self, runtime: BaseAgentRuntime) -> None:
        """Testing hook for swapping runtime backends without touching config."""
        self._runtime = runtime

    @property
    def _initialized(self) -> bool:
        return self._runtime.initialized

    @_initialized.setter
    def _initialized(self, value: bool) -> None:
        self._runtime.initialized = bool(value)

    @property
    def _llm(self) -> Any:
        return self._runtime.llm

    @_llm.setter
    def _llm(self, value: Any) -> None:
        self._runtime.llm = value

    @property
    def _agent_executor(self) -> Any:
        return self._runtime.agent_executor

    @_agent_executor.setter
    def _agent_executor(self, value: Any) -> None:
        self._runtime.agent_executor = value

    @property
    def _mcp_manager(self) -> Any:
        return self._runtime.mcp_manager

    @_mcp_manager.setter
    def _mcp_manager(self, value: Any) -> None:
        self._runtime.mcp_manager = value

    @staticmethod
    def get_available_providers() -> list[dict]:
        """Discover configured LLM providers from settings."""
        llm_config = get_config("llm", {})
        providers: list[dict] = []
        for key, cfg in llm_config.items():
            if key == "provider" or not isinstance(cfg, dict):
                continue
            model = cfg.get("model")
            if model:
                providers.append(
                    {
                        "key": key,
                        "label": key.capitalize(),
                        "model": str(model),
                    }
                )
        return providers


_redact_stream_value = redact_stream_value
_redact_stream_event = redact_stream_event
_redact_stream_text = redact_stream_text
_redact_message_content = redact_message_content
_redact_history_message = redact_history_message
