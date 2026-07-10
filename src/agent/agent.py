"""LangChain Agent service."""

from __future__ import annotations

import asyncio
import time
from collections.abc import AsyncIterator
from typing import Any

from langchain_core.messages import BaseMessage
from loguru import logger

from src.agent.agent_history_persistence import (
    cleanup_stale_agent_threads,
    delete_persisted_agent_threads,
    load_agent_histories,
    save_agent_histories,
)
from src.agent.agent_history_state import (
    clear_thread_history_state,
    get_or_create_thread_history,
    list_active_thread_ids,
    mark_pending_thread_history_recovery,
    render_thread_history,
)
from src.agent.agent_initialization import (
    ensure_agent_runtime_ready,
    ensure_histories_loaded_once,
)
from src.agent.agent_invoke_lifecycle import (
    AgentInvokeState,
    append_turn_history,
    should_discard_partial_runtime_state,
)
from src.agent.agent_invoke_orchestration import (
    prepare_agent_invoke,
    run_prepared_agent_invoke,
)
from src.agent.agent_prompting import (
    default_system_prompt,
)
from src.agent.agent_redaction import (
    redact_history_message,
    redact_message_content,
    redact_stream_event,
    redact_stream_text,
    redact_stream_value,
)
from src.agent.agent_runtime_facade import (
    build_agent_runtime,
    handle_agent_parsing_error,
)
from src.agent.agent_runtime_config import (
    build_runtime_config_snapshot,
    discover_available_providers,
    resolve_active_provider,
    validate_provider_selection,
)
from src.agent.agent_status_summary import (
    describe_tool_action,
    summarize_agent_runtime_status,
)
from src.agent.runtime import BaseAgentRuntime
from src.agent.thread_store import create_thread_store
from src.agent.tools import ALL_TOOLS
from src.agent.workflow_bridge_events import build_workflow_chain_start_events
from src.agent.workflows import workflow_node_bridge_map
from src.core.config import get as get_config
from src.core.sensitive import redact_sensitive_text


class AgentService:
    """Manage Agent conversations and streaming execution."""

    def __init__(self, session_service: Any) -> None:
        self._histories: dict[str, list[BaseMessage]] = {}
        self._sessions_timestamps: dict[str, float] = {}
        self._provider_override: str | None = None
        self._lock = asyncio.Lock()
        self._init_lock = asyncio.Lock()
        self._last_save_time: float = 0
        self._session_service = session_service
        self._thread_store = create_thread_store(session_service)
        self._threads_pending_history_recovery: set[str] = set()
        self._history_load_failed = False

        self._max_iterations: int = get_config("agent.max_iterations", 10)
        self._session_timeout: int = get_config("agent.session_timeout_minutes", 60) * 60
        self._histories_loaded = False
        self._system_prompt = get_config("agent.system_prompt", default_system_prompt())
        self._runtime = self._build_runtime()

    def _handle_parsing_error(self, error: Exception) -> str:
        """Track parser errors and stop repeated malformed tool output."""
        current_count = int(getattr(self, "_parsing_error_count", 0) or 0)
        next_count, response = handle_agent_parsing_error(
            current_count=current_count,
            error=error,
            redact_stream_text=redact_stream_text,
        )
        self._parsing_error_count = next_count
        return response

    def _build_runtime(self) -> BaseAgentRuntime:
        """Instantiate the currently configured runtime backend."""
        return build_agent_runtime(
            system_prompt=self._system_prompt,
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
            await ensure_agent_runtime_ready(
                ensure_histories_loaded_locked=self._ensure_histories_loaded_locked,
                initialized=self._initialized,
                ensure_initialized=self._ensure_initialized,
                ensure_runtime_async=self._runtime.ensure_async,
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
        result = await ensure_histories_loaded_once(
            histories_loaded=self._histories_loaded,
            history_load_failed=self._history_load_failed,
            pending_history_recovery_thread_count=len(self._threads_pending_history_recovery),
            load_histories=self._load_histories,
        )
        self._histories_loaded = result.histories_loaded
        self._history_load_failed = result.history_load_failed
        if result.should_resave_pending_recovery or result.needs_resave:
            await self._save_histories(force=True)

    async def _get_history(self, session_id: str) -> list[BaseMessage]:
        """Return the current session history and refresh its activity timestamp."""
        return await get_or_create_thread_history(
            lock=self._lock,
            histories=self._histories,
            timestamps=self._sessions_timestamps,
            thread_id=session_id,
            now=time.time(),
        )

    async def _mark_pending_history_recovery(self, session_id: str) -> None:
        await mark_pending_thread_history_recovery(
            lock=self._lock,
            pending_recovery_threads=self._threads_pending_history_recovery,
            thread_id=session_id,
        )

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
        """Stream Agent output events."""
        invoke_state = AgentInvokeState()

        try:
            await self._async_ensure_initialized()
            prepared_invoke = await self._prepare_invoke(session_id)
            self._parsing_error_count = 0

            async for rendered_event in run_prepared_agent_invoke(
                executor=self._agent_executor,
                session_id=session_id,
                user_input=user_input,
                invoke_state=invoke_state,
                prepared_invoke=prepared_invoke,
                runtime_input_mode=self._runtime.input_mode,
                build_invoke_payload=self._runtime.build_invoke_payload,
                build_workflow_chain_start_events=build_workflow_chain_start_events,
                redact_value=redact_stream_value,
                redact_stream_event=redact_stream_event,
                redact_stream_text=redact_stream_text,
                describe_tool_action=describe_tool_action,
                save_invoke_history=self._save_invoke_history,
                discard_partial_runtime_state=self._discard_partial_invoke_runtime_state,
            ):
                yield rendered_event

        except Exception as exc:
            logger.opt(exception=True).error(
                "Agent invoke failed: {}", redact_sensitive_text(str(exc))
            )
            content = redact_stream_text(f"执行失败: {exc}")
            if get_config("debug", False):
                content += f"\n\n(类型: {type(exc).__name__})"
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
        await self._clear_thread_state(thread_id)
        await self._save_histories(force=True)
        await self._delete_persisted_threads([thread_id])

    async def clear_history(self, session_id: str = "default") -> None:
        """Clear one session history."""
        await self.clear_thread(session_id)

    async def _save_histories(self, force: bool = False) -> bool:
        """Persist a snapshot of session state without holding the Agent lock."""
        save_result = await save_agent_histories(
            history_load_failed=self._history_load_failed,
            lock=self._lock,
            histories=self._histories,
            timestamps=self._sessions_timestamps,
            pending_recovery_threads=self._threads_pending_history_recovery,
            thread_store=self._thread_store,
            read_last_save_time=lambda: self._last_save_time,
            force=force,
        )
        async with self._lock:
            self._last_save_time = max(self._last_save_time, save_result.next_save_time)
            if save_result.persisted_ok:
                for thread_id in save_result.pending_recovery_threads:
                    self._threads_pending_history_recovery.discard(thread_id)

        await self._delete_persisted_threads(save_result.removed_thread_ids)
        return save_result.persisted_ok

    async def _load_histories(self) -> tuple[bool, bool]:
        """Restore persisted Agent conversation history."""
        load_result = await load_agent_histories(
            lock=self._lock,
            histories=self._histories,
            timestamps=self._sessions_timestamps,
            pending_recovery_threads=self._threads_pending_history_recovery,
            thread_store=self._thread_store,
            transform_message=redact_history_message,
        )
        if not load_result.loaded:
            logger.warning(
                "Failed to load Agent conversation history: {}",
                redact_sensitive_text(str(load_result.error)),
            )
            return False, False

        if load_result.restored_count:
            logger.info(
                "Restored {} persisted Agent conversation thread(s).",
                load_result.restored_count,
            )
        return True, load_result.needs_resave

    async def _cleanup_stale_sessions(self) -> None:
        """Drop expired in-memory and persisted session state."""
        if not self._session_timeout:
            return

        stale_thread_ids = await cleanup_stale_agent_threads(
            lock=self._lock,
            histories=self._histories,
            timestamps=self._sessions_timestamps,
            timeout_seconds=self._session_timeout,
            now=time.time(),
        )
        if stale_thread_ids:
            await self._delete_persisted_threads(stale_thread_ids)

    async def _delete_persisted_threads(self, thread_ids: list[str]) -> None:
        await delete_persisted_agent_threads(
            thread_ids=thread_ids,
            lock=self._lock,
            pending_recovery_threads=self._threads_pending_history_recovery,
            thread_store=self._thread_store,
            forget_thread=self._runtime.forget_thread,
        )

    async def _clear_thread_state(self, thread_id: str) -> None:
        await clear_thread_history_state(
            lock=self._lock,
            histories=self._histories,
            timestamps=self._sessions_timestamps,
            thread_id=thread_id,
        )

    def list_thread_ids(self) -> list[str]:
        """Return active thread ids using the future LangGraph naming."""
        return list_active_thread_ids(self._histories)

    def list_sessions(self) -> list[str]:
        """Return active session ids."""
        return self.list_thread_ids()

    def get_thread_history(self, thread_id: str) -> list[dict[str, str]]:
        """Return one thread's redacted message history."""
        return render_thread_history(
            self._histories,
            thread_id=thread_id,
            redact_message_content=redact_message_content,
        )

    def get_session_history(self, session_id: str) -> list[dict[str, str]]:
        """Return one session history as role/content pairs."""
        return self.get_thread_history(session_id)

    def get_status_summary(self) -> dict[str, Any]:
        """Return a lightweight runtime summary for diagnostics and Agent self-checks."""
        provider = self.get_active_provider()
        return summarize_agent_runtime_status(
            provider=provider,
            model=get_config(f"llm.{provider}.model", ""),
            available_providers=self.get_available_providers(),
            runtime=self._runtime,
            base_tools=list(ALL_TOOLS),
            mcp_manager=self._mcp_manager,
            histories=self._histories,
            timestamps=self._sessions_timestamps,
            session_timeout_seconds=self._session_timeout,
            max_iterations=self._max_iterations,
            histories_loaded=self._histories_loaded,
            history_load_failed=self._history_load_failed,
            pending_history_recovery_thread_count=len(self._threads_pending_history_recovery),
            mcp_enabled=bool(get_config("agent.playwright_mcp.enabled", False)),
            initialized=self._initialized,
        )

    def get_active_provider(self) -> str:
        """Return the active LLM provider name."""
        return resolve_active_provider(
            self._provider_override,
            default_provider=str(get_config("llm.provider", "qwen")),
        )

    def set_provider(self, provider_name: str) -> None:
        """Switch the runtime LLM provider for the next invoke."""
        validate_provider_selection(provider_name, self.get_available_providers())
        self._provider_override = provider_name
        self.reset_runtime()
        logger.info("Agent LLM provider switched to {}", provider_name)

    def reload_config(self, provider_name: str | None = None) -> None:
        """Reload config and discard the current initialized runtime."""
        snapshot = build_runtime_config_snapshot(
            provider_name=provider_name,
            available_providers=self.get_available_providers(),
            config_get=get_config,
            default_system_prompt_text=default_system_prompt(),
        )
        self._provider_override = snapshot.provider_override
        self._max_iterations = snapshot.max_iterations
        self._session_timeout = snapshot.session_timeout
        self._system_prompt = snapshot.system_prompt
        self._runtime = self._build_runtime()
        logger.info("Agent config reloaded (provider={})", self.get_active_provider())

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
        return discover_available_providers(get_config("llm", {}))


_redact_stream_value = redact_stream_value
_redact_stream_event = redact_stream_event
_redact_stream_text = redact_stream_text
_redact_message_content = redact_message_content
_redact_history_message = redact_history_message
