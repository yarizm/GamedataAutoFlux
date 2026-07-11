import asyncio
import json
import time
from typing import Any, Sequence

import pytest
from langchain.agents import create_agent
from langchain_core.language_models.fake_chat_models import FakeMessagesListChatModel
from langchain_core.messages import AIMessage, AIMessageChunk, HumanMessage
from langchain_core.outputs import ChatGenerationChunk
from langchain_core.tools import BaseTool, tool

from src.agent.agent import AgentService
from src.agent.runtime import LangGraphAgentRuntime
from src.core.config import get as base_get_config


def force_runtime_backend(monkeypatch, backend: str) -> None:
    def fake_runtime_get_config(key, default=None):
        if key == "agent.runtime_backend":
            return backend
        return default

    monkeypatch.setattr("src.agent.runtime.get_config", fake_runtime_get_config)


def disable_runtime_mcp(monkeypatch) -> None:
    """Disable Playwright MCP and allow LLM client init without real API keys.

    CI has no secrets; unit tests only need a non-empty key so ChatOpenAI
    construction succeeds. Workflow tests never call the live LLM.
    """

    def fake_runtime_get_config(key, default=None):
        if key == "agent.playwright_mcp.enabled":
            return False
        value = base_get_config(key, default)
        if (
            isinstance(key, str)
            and key.startswith("llm.")
            and key.endswith(".api_key")
            and not str(value or "").strip()
        ):
            return "ci-test-dummy-key"
        return value

    monkeypatch.setattr("src.agent.runtime.get_config", fake_runtime_get_config)


class SlowSessionService:
    def __init__(self) -> None:
        self._max_sessions = 50
        self.save_started = asyncio.Event()
        self.release_save = asyncio.Event()
        self.saved_histories = None
        self.saved_timestamps = None
        self.deleted_sessions = []

    async def save_histories(self, histories, timestamps, last_save_time, force=False):
        self.saved_histories = histories
        self.saved_timestamps = timestamps
        self.save_started.set()
        await self.release_save.wait()
        return time.time()

    async def delete_sessions(self, session_ids):
        self.deleted_sessions.extend(session_ids)


class ImmediateSessionService:
    def __init__(self) -> None:
        self._max_sessions = 50
        self.saved_histories = None
        self.deleted_sessions = []

    async def save_histories(self, histories, timestamps, last_save_time, force=False):
        self.saved_histories = histories
        return time.time()

    async def delete_sessions(self, session_ids):
        self.deleted_sessions.extend(session_ids)


class LoadedHistorySessionService:
    def __init__(self) -> None:
        self._max_sessions = 50

    async def load_histories(self):
        return (
            {
                "loaded": [
                    HumanMessage(content="api_key=loaded-secret"),
                    AIMessage(content='{"token": "assistant-secret", "value": "ok"}'),
                ]
            },
            {"loaded": time.time()},
        )


class ExistingHistorySessionService:
    def __init__(self) -> None:
        self._max_sessions = 50
        self.saved_histories = None

    async def load_histories(self):
        return (
            {
                "loaded-session": [
                    HumanMessage(content="persisted question"),
                    AIMessage(content="persisted answer"),
                ]
            },
            {"loaded-session": time.time() - 60},
        )

    async def save_histories(self, histories, timestamps, last_save_time, force=False):
        self.saved_histories = histories
        return time.time()


class FlakyExistingHistorySessionService:
    def __init__(self) -> None:
        self._max_sessions = 50
        self.load_calls = 0
        self.saved_histories = None

    async def load_histories(self):
        self.load_calls += 1
        if self.load_calls == 1:
            raise RuntimeError("temporary storage failure")
        return (
            {
                "loaded-session": [
                    HumanMessage(content="persisted question"),
                    AIMessage(content="persisted answer"),
                ]
            },
            {"loaded-session": time.time() - 60},
        )

    async def save_histories(self, histories, timestamps, last_save_time, force=False):
        self.saved_histories = histories
        return time.time()


class FlakyExistingHistoryAndResaveSessionService:
    def __init__(self) -> None:
        self._max_sessions = 50
        self.load_calls = 0
        self.save_calls = 0
        self.saved_histories = None

    async def load_histories(self):
        self.load_calls += 1
        if self.load_calls == 1:
            raise RuntimeError("temporary storage failure")
        return (
            {
                "loaded-session": [
                    HumanMessage(content="persisted question"),
                    AIMessage(content="persisted answer"),
                ]
            },
            {"loaded-session": time.time() - 60},
        )

    async def save_histories(self, histories, timestamps, last_save_time, force=False):
        self.save_calls += 1
        if self.save_calls == 1:
            return last_save_time
        self.saved_histories = histories
        return time.time()


class DeferredExistingHistorySessionService:
    def __init__(self) -> None:
        self._max_sessions = 50
        self.allow_load = False
        self.load_calls = 0
        self.saved_histories = None

    async def load_histories(self):
        self.load_calls += 1
        if not self.allow_load:
            raise RuntimeError("temporary storage failure")
        return (
            {
                "loaded-session": [
                    HumanMessage(content="persisted question"),
                    AIMessage(content="persisted answer"),
                ]
            },
            {"loaded-session": time.time() - 60},
        )

    async def save_histories(self, histories, timestamps, last_save_time, force=False):
        self.saved_histories = histories
        return time.time()


def attach_forget_recorder(service: AgentService) -> list[str]:
    forgotten_thread_ids: list[str] = []

    async def record_forget(thread_id: str) -> None:
        forgotten_thread_ids.append(thread_id)

    setattr(service._runtime, "forget_thread", record_forget)
    return forgotten_thread_ids


@pytest.mark.asyncio
async def test_agent_save_histories_does_not_hold_lock_during_persistence() -> None:
    session_service = SlowSessionService()
    service = AgentService(session_service=session_service)
    service._histories["existing"] = [HumanMessage(content="hello")]
    service._sessions_timestamps["existing"] = time.time()

    save_task = asyncio.create_task(service._save_histories(force=True))
    await asyncio.wait_for(session_service.save_started.wait(), timeout=1)

    history = await asyncio.wait_for(service._get_history("new-session"), timeout=0.2)

    session_service.release_save.set()
    await save_task

    assert history == []
    assert "existing" in session_service.saved_histories
    assert "new-session" not in session_service.saved_histories


@pytest.mark.asyncio
async def test_agent_cleanup_stale_sessions_deletes_persisted_rows_outside_lock() -> None:
    session_service = SlowSessionService()
    session_service.release_save.set()
    service = AgentService(session_service=session_service)
    forgotten_thread_ids = attach_forget_recorder(service)
    service._session_timeout = 10
    service._histories["active"] = [HumanMessage(content="active")]
    service._histories["stale"] = [HumanMessage(content="stale")]
    service._sessions_timestamps["active"] = time.time()
    service._sessions_timestamps["stale"] = time.time() - 100
    service._threads_pending_history_recovery.add("stale")

    await service._cleanup_stale_sessions()

    assert "active" in service._histories
    assert "stale" not in service._histories
    assert session_service.deleted_sessions == ["stale"]
    assert forgotten_thread_ids == ["stale"]
    assert "stale" not in service._threads_pending_history_recovery


@pytest.mark.asyncio
async def test_agent_clear_history_deletes_persisted_session() -> None:
    session_service = SlowSessionService()
    session_service.release_save.set()
    service = AgentService(session_service=session_service)
    forgotten_thread_ids = attach_forget_recorder(service)
    service._histories["target"] = [HumanMessage(content="old")]
    service._sessions_timestamps["target"] = time.time()
    service._threads_pending_history_recovery.add("target")

    await service.clear_history("target")

    assert "target" not in service._histories
    assert session_service.deleted_sessions == ["target"]
    assert forgotten_thread_ids == ["target"]
    assert "target" not in service._threads_pending_history_recovery


@pytest.mark.asyncio
async def test_agent_save_history_deletes_sessions_removed_by_cap() -> None:
    session_service = SlowSessionService()
    session_service._max_sessions = 1
    session_service.release_save.set()
    service = AgentService(session_service=session_service)
    forgotten_thread_ids = attach_forget_recorder(service)
    service._histories["old"] = [HumanMessage(content="old")]
    service._histories["new"] = [HumanMessage(content="new")]
    service._sessions_timestamps["old"] = time.time() - 100
    service._sessions_timestamps["new"] = time.time()
    service._threads_pending_history_recovery.add("old")

    await service._save_histories(force=True)

    assert list(service._histories) == ["new"]
    assert list(session_service.saved_histories) == ["new"]
    assert session_service.deleted_sessions == ["old"]
    assert forgotten_thread_ids == ["old"]
    assert "old" not in service._threads_pending_history_recovery


@pytest.mark.asyncio
async def test_agent_load_histories_redacts_sensitive_content() -> None:
    service = AgentService(session_service=LoadedHistorySessionService())

    await service._load_histories()

    history_text = str([message.content for message in service._histories["loaded"]])
    assert "loaded-secret" not in history_text
    assert "assistant-secret" not in history_text
    assert "[REDACTED]" in history_text


def test_agent_get_session_history_redacts_sensitive_content() -> None:
    service = AgentService(session_service=object())
    service._histories["session"] = [
        HumanMessage(content="password=plain-secret"),
        AIMessage(content='{"token": "assistant-secret", "value": "ok"}'),
    ]

    payload = service.get_session_history("session")
    rendered = str(payload)

    assert "plain-secret" not in rendered
    assert "assistant-secret" not in rendered
    assert "ok" in rendered


@pytest.mark.asyncio
async def test_agent_ainvoke_does_not_revive_expired_session(monkeypatch) -> None:
    class CapturingExecutor:
        def __init__(self) -> None:
            self.received_history = None
            self.received_config = None

        async def astream_events(self, payload, config, version):
            self.received_history = payload["chat_history"]
            self.received_config = config
            yield {
                "event": "on_chain_end",
                "name": "AgentExecutor",
                "data": {"output": {"output": "done"}},
            }

    force_runtime_backend(monkeypatch, "langchain_classic")
    session_service = ImmediateSessionService()
    service = AgentService(session_service=session_service)
    service._session_timeout = 10
    service._histories["expired"] = [HumanMessage(content="old context")]
    service._sessions_timestamps["expired"] = time.time() - 100
    executor = CapturingExecutor()
    service._agent_executor = executor

    async def initialized_noop() -> None:
        return None

    monkeypatch.setattr(service, "_async_ensure_initialized", initialized_noop)

    events = [event async for event in service.ainvoke("new request", "expired")]

    assert executor.received_history == []
    assert executor.received_config["configurable"]["session_id"] == "expired"
    assert executor.received_config["configurable"]["thread_id"] == "expired"
    assert session_service.deleted_sessions == ["expired"]
    assert service._histories["expired"][0].content == "new request"
    assert not any(event["type"] == "error" for event in events)


@pytest.mark.asyncio
async def test_agent_ainvoke_loads_persisted_history_before_first_request(monkeypatch) -> None:
    class CapturingExecutor:
        def __init__(self) -> None:
            self.received_history = None

        async def astream_events(self, payload, config, version):
            self.received_history = payload["chat_history"]
            yield {
                "event": "on_chain_end",
                "name": "AgentExecutor",
                "data": {"output": {"output": "done"}},
            }

    force_runtime_backend(monkeypatch, "langchain_classic")
    session_service = ExistingHistorySessionService()
    service = AgentService(session_service=session_service)
    service._initialized = True
    executor = CapturingExecutor()
    service._agent_executor = executor

    async def ensure_async_noop(**kwargs) -> None:
        return None

    monkeypatch.setattr(service._runtime, "ensure_async", ensure_async_noop)

    events = [event async for event in service.ainvoke("new request", "loaded-session")]

    assert [message.content for message in executor.received_history] == [
        "persisted question",
        "persisted answer",
    ]
    saved_history = session_service.saved_histories["loaded-session"]
    assert [message.content for message in saved_history] == [
        "persisted question",
        "persisted answer",
        "new request",
        "done",
    ]
    assert not any(event["type"] == "error" for event in events)


@pytest.mark.asyncio
async def test_agent_ainvoke_preserves_existing_persisted_history_after_load_retry(
    monkeypatch,
) -> None:
    class CapturingExecutor:
        def __init__(self) -> None:
            self.received_history = None

        async def astream_events(self, payload, config, version):
            self.received_history = payload["chat_history"]
            yield {
                "event": "on_chain_end",
                "name": "AgentExecutor",
                "data": {"output": {"output": "done"}},
            }

    force_runtime_backend(monkeypatch, "langchain_classic")
    session_service = FlakyExistingHistorySessionService()
    service = AgentService(session_service=session_service)
    service._initialized = True
    executor = CapturingExecutor()
    service._agent_executor = executor

    async def ensure_async_noop(**kwargs) -> None:
        return None

    monkeypatch.setattr(service._runtime, "ensure_async", ensure_async_noop)

    _ = [event async for event in service.ainvoke("new request", "loaded-session")]

    assert executor.received_history == []
    assert service._histories_loaded is False
    degraded_status = service.get_status_summary()
    assert degraded_status["status_health"] == "warning"
    assert degraded_status["status_warnings"]
    assert degraded_status["history_load_failed"] is True
    assert degraded_status["pending_history_recovery_thread_count"] == 1
    assert degraded_status["history_recovery_warnings"]
    assert session_service.saved_histories is None

    await service.ensure_histories_loaded()

    assert service._histories_loaded is True
    recovered_status = service.get_status_summary()
    assert recovered_status["status_health"] == "ok"
    assert recovered_status["status_warnings"] == []
    assert recovered_status["history_load_failed"] is False
    assert recovered_status["pending_history_recovery_thread_count"] == 0
    saved_history = session_service.saved_histories["loaded-session"]
    assert [message.content for message in saved_history] == [
        "persisted question",
        "persisted answer",
        "new request",
        "done",
    ]


@pytest.mark.asyncio
async def test_agent_keeps_pending_recovery_until_resave_succeeds(monkeypatch) -> None:
    class CapturingExecutor:
        def __init__(self) -> None:
            self.received_history = None

        async def astream_events(self, payload, config, version):
            self.received_history = payload["chat_history"]
            yield {
                "event": "on_chain_end",
                "name": "AgentExecutor",
                "data": {"output": {"output": "done"}},
            }

    force_runtime_backend(monkeypatch, "langchain_classic")
    session_service = FlakyExistingHistoryAndResaveSessionService()
    service = AgentService(session_service=session_service)
    service._initialized = True
    executor = CapturingExecutor()
    service._agent_executor = executor

    async def ensure_async_noop(**kwargs) -> None:
        return None

    monkeypatch.setattr(service._runtime, "ensure_async", ensure_async_noop)

    _ = [event async for event in service.ainvoke("new request", "loaded-session")]

    assert executor.received_history == []
    await service.ensure_histories_loaded()

    degraded_status = service.get_status_summary()
    assert degraded_status["history_load_failed"] is False
    assert degraded_status["pending_history_recovery_thread_count"] == 1
    assert degraded_status["status_health"] == "warning"
    assert session_service.saved_histories is None

    await service.ensure_histories_loaded()

    recovered_status = service.get_status_summary()
    assert recovered_status["pending_history_recovery_thread_count"] == 0
    assert recovered_status["status_health"] == "ok"
    saved_history = session_service.saved_histories["loaded-session"]
    assert [message.content for message in saved_history] == [
        "persisted question",
        "persisted answer",
        "new request",
        "done",
    ]


@pytest.mark.asyncio
async def test_agent_pending_recovery_thread_keeps_all_buffered_turns_until_merge(
    monkeypatch,
) -> None:
    class CountingExecutor:
        def __init__(self) -> None:
            self.calls = 0

        async def astream_events(self, payload, config, version):
            self.calls += 1
            yield {
                "event": "on_chain_end",
                "name": "AgentExecutor",
                "data": {"output": {"output": f"done {self.calls}"}},
            }

    session_service = DeferredExistingHistorySessionService()
    service = AgentService(session_service=session_service)
    service._initialized = True
    service._agent_executor = CountingExecutor()

    async def ensure_async_noop(**kwargs) -> None:
        return None

    monkeypatch.setattr(service._runtime, "ensure_async", ensure_async_noop)

    for turn in range(25):
        events = [
            event
            async for event in service.ainvoke(
                f"new request {turn}",
                "loaded-session",
            )
        ]
        assert not any(event["type"] == "error" for event in events)

    buffered_history = service._histories["loaded-session"]
    assert len(buffered_history) == 50
    assert buffered_history[0].content == "new request 0"
    assert buffered_history[-1].content == "done 25"
    assert service.get_status_summary()["pending_history_recovery_thread_count"] == 1
    assert session_service.saved_histories is None

    session_service.allow_load = True
    await service.ensure_histories_loaded()

    saved_history = session_service.saved_histories["loaded-session"]
    assert [message.content for message in saved_history[:4]] == [
        "persisted question",
        "persisted answer",
        "new request 0",
        "done 1",
    ]
    assert len(saved_history) == 52
    assert saved_history[-2].content == "new request 24"
    assert saved_history[-1].content == "done 25"
    assert service.get_status_summary()["pending_history_recovery_thread_count"] == 0


@pytest.mark.asyncio
async def test_agent_ainvoke_resets_parsing_error_count_per_request(monkeypatch) -> None:
    class CapturingExecutor:
        def __init__(self, service: AgentService) -> None:
            self.service = service
            self.count_at_start = None

        async def astream_events(self, payload, config, version):
            self.count_at_start = self.service._parsing_error_count
            yield {
                "event": "on_chain_end",
                "name": "AgentExecutor",
                "data": {"output": {"output": "done"}},
            }

    session_service = ImmediateSessionService()
    service = AgentService(session_service=session_service)
    service._parsing_error_count = 2
    executor = CapturingExecutor(service)
    service._agent_executor = executor

    async def initialized_noop() -> None:
        return None

    monkeypatch.setattr(service, "_async_ensure_initialized", initialized_noop)

    _ = [event async for event in service.ainvoke("fresh request", "fresh-session")]

    assert executor.count_at_start == 0


@pytest.mark.asyncio
async def test_agent_ainvoke_cancellation_discards_partial_runtime_state(monkeypatch) -> None:
    class CancelledExecutor:
        async def astream_events(self, payload, config, version):
            yield {"event": "on_chat_model_start"}
            raise asyncio.CancelledError()

    session_service = ImmediateSessionService()
    service = AgentService(session_service=session_service)
    forgotten_thread_ids = attach_forget_recorder(service)
    service._agent_executor = CancelledExecutor()

    async def initialized_noop() -> None:
        return None

    monkeypatch.setattr(service, "_async_ensure_initialized", initialized_noop)

    with pytest.raises(asyncio.CancelledError):
        _ = [event async for event in service.ainvoke("cancel me", "cancelled-session")]

    assert forgotten_thread_ids == ["cancelled-session"]


@pytest.mark.asyncio
async def test_langgraph_runtime_ignores_legacy_react_stream_parser(monkeypatch) -> None:
    class FakeChunk:
        def __init__(self, content: str) -> None:
            self.content = content
            self.additional_kwargs = {}

    class StreamingGraphExecutor:
        async def astream_events(self, payload, config, version):
            yield {"event": "on_chat_model_start"}
            yield {"event": "on_chat_model_stream", "data": {"chunk": FakeChunk("hello")}}
            yield {
                "event": "on_chain_end",
                "name": "LangGraph",
                "data": {"output": {"messages": [AIMessage(content="hello")]}}
            }

    def fake_get_config(key, default=None):
        if key == "agent.agent_type":
            return "react"
        return default

    session_service = ImmediateSessionService()
    service = AgentService(session_service=session_service)
    graph_runtime = LangGraphAgentRuntime(
        build_openai_tools_system_prompt=lambda tools: "system",
        build_openai_tools_prompt=lambda tools: None,
        build_react_prompt=lambda tools: None,
        create_mcp_manager=lambda: None,
        handle_parsing_error=lambda exc: str(exc),
    )
    service._set_runtime_for_testing(graph_runtime)
    service._agent_executor = StreamingGraphExecutor()

    async def initialized_noop() -> None:
        return None

    monkeypatch.setattr(service, "_async_ensure_initialized", initialized_noop)
    monkeypatch.setattr("src.agent.agent.get_config", fake_get_config)
    monkeypatch.setattr("src.agent.runtime.get_config", fake_get_config)

    events = [event async for event in service.ainvoke("hello", "graph-react-config")]

    final_events = [event for event in events if event["type"] == "final"]
    thinking_events = [event for event in events if event["type"] == "thinking"]
    assert [event["content"] for event in final_events] == ["hello"]
    assert thinking_events == [{"type": "thinking", "content": "正在分析您的请求..."}]


@pytest.mark.asyncio
async def test_agent_ainvoke_redacts_llm_stream_events_and_saved_output(monkeypatch) -> None:
    class FakeChunk:
        def __init__(self, content, additional_kwargs=None) -> None:
            self.content = content
            self.additional_kwargs = additional_kwargs or {}

    class StreamingExecutor:
        async def astream_events(self, payload, config, version):
            yield {"event": "on_chat_model_start"}
            yield {
                "event": "on_chat_model_stream",
                "data": {
                    "chunk": FakeChunk(
                        "Final says token=final-secret",
                        {"reasoning_content": "Thinking api_key=reason-secret"},
                    )
                },
            }
            yield {
                "event": "on_chain_end",
                "name": "AgentExecutor",
                "data": {"output": {"output": "done"}},
            }

    session_service = ImmediateSessionService()
    service = AgentService(session_service=session_service)
    service._agent_executor = StreamingExecutor()

    async def initialized_noop() -> None:
        return None

    monkeypatch.setattr(service, "_async_ensure_initialized", initialized_noop)
    monkeypatch.setattr(
        "src.agent.agent.get_config",
        lambda key, default=None: "openai_tools" if key == "agent.agent_type" else default,
    )

    events = [
        event
        async for event in service.ainvoke(
            "User asks with password=user-secret",
            "stream-redact",
        )
    ]
    rendered_events = str(events)
    rendered_saved = str(session_service.saved_histories)

    assert "reason-secret" not in rendered_events
    assert "final-secret" not in rendered_events
    assert "user-secret" not in rendered_saved
    assert "final-secret" not in rendered_saved
    assert "api_key=[REDACTED]" in rendered_events
    assert "token=[REDACTED]" in rendered_events
    assert "password=[REDACTED]" in rendered_saved


@pytest.mark.asyncio
async def test_agent_ainvoke_uses_graph_runtime_payload_and_final_output(monkeypatch) -> None:
    class GraphExecutor:
        def __init__(self) -> None:
            self.received_payload = None

        async def astream_events(self, payload, config, version):
            self.received_payload = payload
            yield {
                "event": "on_chain_end",
                "name": "LangGraph",
                "data": {
                    "output": {
                        "messages": [
                            HumanMessage(content="old context"),
                            AIMessage(content="graph final answer"),
                        ]
                    }
                },
            }

    session_service = ImmediateSessionService()
    service = AgentService(session_service=session_service)
    service._histories["graph-session"] = [HumanMessage(content="old context")]
    service._sessions_timestamps["graph-session"] = time.time()
    graph_runtime = LangGraphAgentRuntime(
        build_openai_tools_system_prompt=lambda tools: "system",
        build_openai_tools_prompt=lambda tools: None,
        build_react_prompt=lambda tools: None,
        create_mcp_manager=lambda: None,
        handle_parsing_error=lambda exc: str(exc),
    )
    service._set_runtime_for_testing(graph_runtime)
    executor = GraphExecutor()
    service._agent_executor = executor

    async def initialized_noop() -> None:
        return None

    monkeypatch.setattr(service, "_async_ensure_initialized", initialized_noop)

    events = [event async for event in service.ainvoke("new request", "graph-session")]

    assert executor.received_payload == {
        "messages": [HumanMessage(content="old context"), ("human", "new request")]
    }
    assert any(
        event["type"] == "final" and event["content"] == "graph final answer" for event in events
    )


@pytest.mark.asyncio
async def test_agent_ainvoke_with_real_langgraph_agent_emits_tool_and_final_events(
    monkeypatch,
) -> None:
    class BindToolsFakeModel(FakeMessagesListChatModel):
        def bind_tools(
            self,
            tools: Sequence[BaseTool | dict | type | Any],
            *,
            tool_choice: Any = None,
            **kwargs: Any,
        ):
            return self

    @tool
    def echo(text: str) -> str:
        """Echo text."""
        return f"ECHO:{text}"

    model = BindToolsFakeModel(
        responses=[
            AIMessage(
                content="",
                tool_calls=[{"name": "echo", "args": {"text": "hi"}, "id": "call_1", "type": "tool_call"}],
            ),
            AIMessage(content="done final"),
        ]
    )
    agent_graph = create_agent(model=model, tools=[echo], system_prompt="You are helpful")

    session_service = ImmediateSessionService()
    service = AgentService(session_service=session_service)
    graph_runtime = LangGraphAgentRuntime(
        build_openai_tools_system_prompt=lambda tools: "You are helpful",
        build_openai_tools_prompt=lambda tools: None,
        build_react_prompt=lambda tools: None,
        create_mcp_manager=lambda: None,
        handle_parsing_error=lambda exc: str(exc),
    )
    service._set_runtime_for_testing(graph_runtime)
    service._agent_executor = agent_graph

    async def initialized_noop() -> None:
        return None

    monkeypatch.setattr(service, "_async_ensure_initialized", initialized_noop)

    events = [event async for event in service.ainvoke("hello", "graph-real")]

    assert any(event["type"] == "tool_call" and event["name"] == "echo" for event in events)
    assert any(
        event["type"] == "tool_result" and "ECHO:hi" in event["content"] for event in events
    )
    final_events = [event for event in events if event["type"] == "final"]
    assert len(final_events) == 1
    assert final_events[0]["content"] == "done final"


@pytest.mark.asyncio
async def test_agent_ainvoke_with_streaming_langgraph_agent_saves_final_once(
    monkeypatch,
) -> None:
    class StreamingBindToolsFakeModel(FakeMessagesListChatModel):
        def bind_tools(
            self,
            tools: Sequence[BaseTool | dict | type | Any],
            *,
            tool_choice: Any = None,
            **kwargs: Any,
        ):
            return self

        async def _astream(
            self,
            messages,
            stop=None,
            run_manager=None,
            **kwargs: Any,
        ):
            response = self.responses[self.i]
            if self.i < len(self.responses) - 1:
                self.i += 1
            else:
                self.i = 0
            content = getattr(response, "content", "")
            for index, char in enumerate(str(content)):
                chunk_position = "last" if index == len(str(content)) - 1 else None
                yield ChatGenerationChunk(
                    message=AIMessageChunk(content=char, chunk_position=chunk_position)
                )

    model = StreamingBindToolsFakeModel(responses=[AIMessage(content="stream final")])
    agent_graph = create_agent(model=model, tools=None, system_prompt="You are helpful")

    session_service = ImmediateSessionService()
    service = AgentService(session_service=session_service)
    graph_runtime = LangGraphAgentRuntime(
        build_openai_tools_system_prompt=lambda tools: "You are helpful",
        build_openai_tools_prompt=lambda tools: None,
        build_react_prompt=lambda tools: None,
        create_mcp_manager=lambda: None,
        handle_parsing_error=lambda exc: str(exc),
    )
    service._set_runtime_for_testing(graph_runtime)
    service._agent_executor = agent_graph

    async def initialized_noop() -> None:
        return None

    monkeypatch.setattr(service, "_async_ensure_initialized", initialized_noop)

    events = [event async for event in service.ainvoke("hello", "graph-stream")]

    final_events = [event for event in events if event["type"] == "final"]
    assert final_events
    assert "".join(event["content"] for event in final_events) == "stream final"

    saved_history = session_service.saved_histories["graph-stream"]
    assert saved_history[-1].content == "stream final"


@pytest.mark.asyncio
async def test_agent_ainvoke_langgraph_report_precheck_workflow_emits_tool_chain(
    monkeypatch,
) -> None:
    disable_runtime_mcp(monkeypatch)
    session_service = ImmediateSessionService()
    service = AgentService(session_service=session_service)
    graph_runtime = LangGraphAgentRuntime(
        build_openai_tools_system_prompt=lambda tools: "You are helpful",
        build_openai_tools_prompt=lambda tools: None,
        build_react_prompt=lambda tools: None,
        create_mcp_manager=lambda: None,
        handle_parsing_error=lambda exc: str(exc),
    )
    service._set_runtime_for_testing(graph_runtime)

    async def fake_task_detail_tool(payload):
        assert payload == {"task_id": "task-success"}
        return json.dumps(
            {
                "status": "ok",
                "summary": "task detail ok",
                "data": {
                    "id": "task-success",
                    "name": "Counter-Strike 2 Steam Task",
                    "collector_name": "steam",
                    "agent_guidance": "Run report precheck to verify coverage before generating.",
                    "recommended_actions": [
                        {"recommended_tool": "precheck_report"},
                        {"recommended_tool": "generate_report"},
                    ],
                },
            },
            ensure_ascii=False,
        )

    async def fake_review_tool(payload):
        assert payload == {"task_id": "task-success", "auto_retry": False}
        return json.dumps(
            {
                "task_id": "task-success",
                "task_name": "Counter-Strike 2 Steam Task",
                "completeness": "full",
                "record_count": 2,
                "record_summaries": [
                    {"key": "record:steam", "source": "steam"},
                    {"key": "record:gtrends", "source": "gtrends"},
                ],
                "suggestions": [],
            },
            ensure_ascii=False,
        )

    async def fake_precheck_tool(payload):
        assert payload == {
            "prompt": "请基于任务 Counter-Strike 2 Steam Task 的采集结果生成一份数据分析报告，总结核心指标、趋势变化、用户反馈和潜在风险。",
            "template": "steam_game",
            "record_keys": ["record:steam", "record:gtrends"],
        }
        return json.dumps(
            {
                "success": True,
                "status": "partial",
                "selected_records": 2,
                "usable_records": 2,
                "can_generate": True,
                "should_collect_more": True,
                "missing_collectors": ["monitor"],
                "next_best_action": {
                    "collector": "monitor",
                    "collector_label": "Monitor",
                    "recommended_sequence": ["create_task"],
                },
            },
            ensure_ascii=False,
        )

    monkeypatch.setattr(
        "src.agent.workflows._ainvoke_task_detail_tool",
        fake_task_detail_tool,
    )
    monkeypatch.setattr(
        "src.agent.workflows._ainvoke_review_collection_results_tool",
        fake_review_tool,
    )
    monkeypatch.setattr(
        "src.agent.workflows._ainvoke_precheck_report_tool",
        fake_precheck_tool,
    )

    await service._async_ensure_initialized()
    events = [
        event
        async for event in service.ainvoke(
            "请帮我预检 task-success 的报告是否可以开始生成",
            "graph-report-precheck",
        )
    ]

    tool_calls = [event["name"] for event in events if event["type"] == "tool_call"]
    assert tool_calls == [
        "get_task_detail",
        "review_collection_results",
        "precheck_report",
    ]
    final_events = [event for event in events if event["type"] == "final"]
    assert len(final_events) == 1
    assert "任务 `task-success` 的报告预检结果：部分可用。" in final_events[0]["content"]
    assert "缺失数据源：monitor。" in final_events[0]["content"]
    assert "下一步建议：优先处理 Monitor，推荐顺序：create_task" in final_events[0]["content"]


@pytest.mark.asyncio
async def test_agent_ainvoke_langgraph_report_generate_workflow_runs_generate_report(
    monkeypatch,
) -> None:
    disable_runtime_mcp(monkeypatch)
    session_service = ImmediateSessionService()
    service = AgentService(session_service=session_service)
    graph_runtime = LangGraphAgentRuntime(
        build_openai_tools_system_prompt=lambda tools: "You are helpful",
        build_openai_tools_prompt=lambda tools: None,
        build_react_prompt=lambda tools: None,
        create_mcp_manager=lambda: None,
        handle_parsing_error=lambda exc: str(exc),
    )
    service._set_runtime_for_testing(graph_runtime)

    async def fake_task_detail_tool(payload):
        return json.dumps(
            {
                "status": "ok",
                "summary": "task detail ok",
                "data": {
                    "id": "task-generate",
                    "name": "CS2 Full Task",
                    "collector_name": "steam",
                },
            },
            ensure_ascii=False,
        )

    async def fake_review_tool(payload):
        return json.dumps(
            {
                "task_id": "task-generate",
                "task_name": "CS2 Full Task",
                "completeness": "full",
                "record_count": 2,
                "record_summaries": [
                    {"key": "record:steam", "source": "steam"},
                    {"key": "record:gtrends", "source": "gtrends"},
                ],
            },
            ensure_ascii=False,
        )

    async def fake_precheck_tool(payload):
        return json.dumps(
            {
                "success": True,
                "status": "complete",
                "selected_records": 2,
                "usable_records": 2,
                "can_generate": True,
                "should_collect_more": False,
                "missing_collectors": [],
            },
            ensure_ascii=False,
        )

    async def fake_generate_tool(payload):
        assert payload == {
            "prompt": "请基于任务 CS2 Full Task 的采集结果生成一份数据分析报告，总结核心指标、趋势变化、用户反馈和潜在风险。",
            "template": "steam_game",
            "record_keys": ["record:steam", "record:gtrends"],
        }
        return json.dumps(
            {
                "success": True,
                "report_id": "report-001",
                "title": "CS2 Weekly Report",
                "download_url": "/api/reports/report-001/download",
                "quality_status": "complete",
                "missing_collectors": [],
            },
            ensure_ascii=False,
        )

    monkeypatch.setattr(
        "src.agent.workflows._ainvoke_task_detail_tool",
        fake_task_detail_tool,
    )
    monkeypatch.setattr(
        "src.agent.workflows._ainvoke_review_collection_results_tool",
        fake_review_tool,
    )
    monkeypatch.setattr(
        "src.agent.workflows._ainvoke_precheck_report_tool",
        fake_precheck_tool,
    )
    monkeypatch.setattr(
        "src.agent.workflows._ainvoke_generate_report_tool",
        fake_generate_tool,
    )

    await service._async_ensure_initialized()
    events = [
        event
        async for event in service.ainvoke(
            "请直接为 task-generate 生成报告",
            "graph-report-generate",
        )
    ]

    tool_calls = [event["name"] for event in events if event["type"] == "tool_call"]
    assert tool_calls == [
        "get_task_detail",
        "review_collection_results",
        "precheck_report",
        "generate_report",
    ]
    final_events = [event for event in events if event["type"] == "final"]
    assert len(final_events) == 1
    assert "已基于任务 `task-generate` 的采集结果生成报告《CS2 Weekly Report》" in final_events[0]["content"]
    assert "报告 ID：`report-001`" in final_events[0]["content"]
    assert "下载地址：`/api/reports/report-001/download`" in final_events[0]["content"]
    assert "质量状态：完整可用" in final_events[0]["content"]


@pytest.mark.asyncio
async def test_agent_ainvoke_langgraph_task_review_workflow_emits_review_chain(
    monkeypatch,
) -> None:
    disable_runtime_mcp(monkeypatch)
    session_service = ImmediateSessionService()
    service = AgentService(session_service=session_service)
    graph_runtime = LangGraphAgentRuntime(
        build_openai_tools_system_prompt=lambda tools: "You are helpful",
        build_openai_tools_prompt=lambda tools: None,
        build_react_prompt=lambda tools: None,
        create_mcp_manager=lambda: None,
        handle_parsing_error=lambda exc: str(exc),
    )
    service._set_runtime_for_testing(graph_runtime)

    async def fake_task_detail_tool(payload):
        assert payload == {"task_id": "task-review"}
        return json.dumps(
            {
                "status": "ok",
                "summary": "task detail ok",
                "data": {
                    "id": "task-review",
                    "name": "Review Task",
                    "collector_name": "steam",
                    "agent_guidance": (
                        "Task produced source data. Run report precheck to verify coverage before generating."
                    ),
                    "recommended_actions": [
                        {"recommended_tool": "precheck_report"},
                        {"recommended_tool": "generate_report"},
                    ],
                },
            },
            ensure_ascii=False,
        )

    async def fake_review_tool(payload):
        assert payload == {"task_id": "task-review", "auto_retry": False}
        return json.dumps(
            {
                "task_id": "task-review",
                "task_name": "Review Task",
                "completeness": "partial",
                "record_count": 1,
                "source_coverage": {"steam": 1},
                "issues": [
                    {
                        "level": "warning",
                        "category": "empty_data",
                        "message": "记录 review:empty 数据为空",
                    }
                ],
                "suggestions": ["部分数据不完整，可考虑调整采集参数重新采集"],
                "record_summaries": [{"key": "review:empty", "source": "steam"}],
            },
            ensure_ascii=False,
        )

    monkeypatch.setattr(
        "src.agent.workflows._ainvoke_task_detail_tool",
        fake_task_detail_tool,
    )
    monkeypatch.setattr(
        "src.agent.workflows._ainvoke_review_collection_results_tool",
        fake_review_tool,
    )

    await service._async_ensure_initialized()
    events = [
        event
        async for event in service.ainvoke(
            "请帮我复查一下 task-review 当前任务的问题",
            "graph-task-review",
        )
    ]

    tool_calls = [event["name"] for event in events if event["type"] == "tool_call"]
    assert tool_calls == ["get_task_detail", "review_collection_results"]
    final_events = [event for event in events if event["type"] == "final"]
    assert len(final_events) == 1
    assert "任务 `task-review` 的采集复查结果：部分可用。" in final_events[0]["content"]
    assert "主要发现：记录 review:empty 数据为空" in final_events[0]["content"]
    assert "建议：部分数据不完整，可考虑调整采集参数重新采集" in final_events[0]["content"]


@pytest.mark.asyncio
async def test_agent_ainvoke_langgraph_task_review_workflow_supports_auto_retry(
    monkeypatch,
) -> None:
    disable_runtime_mcp(monkeypatch)
    session_service = ImmediateSessionService()
    service = AgentService(session_service=session_service)
    graph_runtime = LangGraphAgentRuntime(
        build_openai_tools_system_prompt=lambda tools: "You are helpful",
        build_openai_tools_prompt=lambda tools: None,
        build_react_prompt=lambda tools: None,
        create_mcp_manager=lambda: None,
        handle_parsing_error=lambda exc: str(exc),
    )
    service._set_runtime_for_testing(graph_runtime)

    async def fake_task_detail_tool(payload):
        return json.dumps(
            {
                "status": "ok",
                "summary": "task detail ok",
                "data": {
                    "id": "task-retry",
                    "name": "Retry Task",
                    "collector_name": "steam",
                },
            },
            ensure_ascii=False,
        )

    async def fake_review_tool(payload):
        assert payload == {"task_id": "task-retry", "auto_retry": True}
        return json.dumps(
            {
                "task_id": "task-retry",
                "task_name": "Retry Task",
                "completeness": "empty",
                "record_count": 0,
                "issues": [
                    {
                        "level": "error",
                        "category": "task_failed",
                        "message": "任务状态为 failed，错误: timeout",
                    }
                ],
                "suggestions": ["已自动创建重试任务 retry-task"],
                "retry_created": True,
                "retry_task_id": "retry-task",
                "retry_task_name": "Retry Task (review retry)",
            },
            ensure_ascii=False,
        )

    monkeypatch.setattr(
        "src.agent.workflows._ainvoke_task_detail_tool",
        fake_task_detail_tool,
    )
    monkeypatch.setattr(
        "src.agent.workflows._ainvoke_review_collection_results_tool",
        fake_review_tool,
    )

    await service._async_ensure_initialized()
    events = [
        event
        async for event in service.ainvoke(
            "请直接帮我重试 task-retry",
            "graph-task-retry",
        )
    ]

    tool_calls = [event["name"] for event in events if event["type"] == "tool_call"]
    assert tool_calls == ["get_task_detail", "review_collection_results"]
    final_events = [event for event in events if event["type"] == "final"]
    assert len(final_events) == 1
    assert "任务 `task-retry` 的采集复查结果：缺少有效数据。" in final_events[0]["content"]
    assert "已自动创建重试任务 `retry-task`（Retry Task (review retry)）。" in final_events[0]["content"]


@pytest.mark.asyncio
async def test_agent_ainvoke_langgraph_pipeline_workflow_creates_dynamic_pipeline(
    monkeypatch,
) -> None:
    disable_runtime_mcp(monkeypatch)
    session_service = ImmediateSessionService()
    service = AgentService(session_service=session_service)
    graph_runtime = LangGraphAgentRuntime(
        build_openai_tools_system_prompt=lambda tools: "You are helpful",
        build_openai_tools_prompt=lambda tools: None,
        build_react_prompt=lambda tools: None,
        create_mcp_manager=lambda: None,
        handle_parsing_error=lambda exc: str(exc),
    )
    service._set_runtime_for_testing(graph_runtime)

    async def fake_create_dynamic_pipeline_tool(payload):
        assert payload["url"] == "https://example.com/game/cs2"
        assert payload["wait_strategy_type"] == "networkidle"
        assert payload["wait_strategy_selector"] is None
        assert payload["pipeline_name"].startswith("example_com_")
        assert "document.title" in payload["js_script"]
        return json.dumps(
            {
                "status": "ok",
                "summary": "动态 Pipeline 已创建并保存。",
                "data": {
                    "pipeline_name": payload["pipeline_name"],
                    "steps_count": 4,
                },
            },
            ensure_ascii=False,
        )

    monkeypatch.setattr(
        "src.agent.workflows._ainvoke_create_dynamic_pipeline_tool",
        fake_create_dynamic_pipeline_tool,
    )

    await service._async_ensure_initialized()
    events = [
        event
        async for event in service.ainvoke(
            "请为 https://example.com/game/cs2 创建动态 pipeline",
            "graph-pipeline-create",
        )
    ]

    tool_calls = [event["name"] for event in events if event["type"] == "tool_call"]
    assert tool_calls == ["create_dynamic_pipeline"]
    tool_results = [event for event in events if event["type"] == "tool_result"]
    assert len(tool_results) == 1
    assert tool_results[0]["name"] == "create_dynamic_pipeline"
    assert '"status": "ok"' in tool_results[0]["content"]
    final_events = [event for event in events if event["type"] == "final"]
    assert len(final_events) == 1
    assert "动态 Pipeline 已创建并保存。" in final_events[0]["content"]
    assert "Pipeline 名称：`example_com_" in final_events[0]["content"]
    assert "目标 URL：`https://example.com/game/cs2`" in final_events[0]["content"]


@pytest.mark.asyncio
async def test_agent_ainvoke_langgraph_pipeline_workflow_reports_dynamic_pipeline_errors(
    monkeypatch,
) -> None:
    disable_runtime_mcp(monkeypatch)
    session_service = ImmediateSessionService()
    service = AgentService(session_service=session_service)
    graph_runtime = LangGraphAgentRuntime(
        build_openai_tools_system_prompt=lambda tools: "You are helpful",
        build_openai_tools_prompt=lambda tools: None,
        build_react_prompt=lambda tools: None,
        create_mcp_manager=lambda: None,
        handle_parsing_error=lambda exc: str(exc),
    )
    service._set_runtime_for_testing(graph_runtime)

    async def fake_create_dynamic_pipeline_tool(payload):
        assert payload["url"] == "http://127.0.0.1/private"
        return json.dumps(
            {
                "status": "error",
                "summary": "动态 Pipeline 配置不安全: 不允许访问本地或私网地址",
            },
            ensure_ascii=False,
        )

    monkeypatch.setattr(
        "src.agent.workflows._ainvoke_create_dynamic_pipeline_tool",
        fake_create_dynamic_pipeline_tool,
    )

    await service._async_ensure_initialized()
    events = [
        event
        async for event in service.ainvoke(
            "请为 http://127.0.0.1/private 创建动态 pipeline",
            "graph-pipeline-error",
        )
    ]

    tool_calls = [event["name"] for event in events if event["type"] == "tool_call"]
    assert tool_calls == ["create_dynamic_pipeline"]
    final_events = [event for event in events if event["type"] == "final"]
    assert len(final_events) == 1
    assert (
        "为 `http://127.0.0.1/private` 创建动态采集 Pipeline 失败："
        "动态 Pipeline 配置不安全: 不允许访问本地或私网地址"
    ) in final_events[0]["content"]
