import asyncio
import time

import pytest
from langchain_core.messages import AIMessage, HumanMessage

from src.agent.agent import AgentService


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
    service._session_timeout = 10
    service._histories["active"] = [HumanMessage(content="active")]
    service._histories["stale"] = [HumanMessage(content="stale")]
    service._sessions_timestamps["active"] = time.time()
    service._sessions_timestamps["stale"] = time.time() - 100

    await service._cleanup_stale_sessions()

    assert "active" in service._histories
    assert "stale" not in service._histories
    assert session_service.deleted_sessions == ["stale"]


@pytest.mark.asyncio
async def test_agent_clear_history_deletes_persisted_session() -> None:
    session_service = SlowSessionService()
    session_service.release_save.set()
    service = AgentService(session_service=session_service)
    service._histories["target"] = [HumanMessage(content="old")]
    service._sessions_timestamps["target"] = time.time()

    await service.clear_history("target")

    assert "target" not in service._histories
    assert session_service.deleted_sessions == ["target"]


@pytest.mark.asyncio
async def test_agent_save_history_deletes_sessions_removed_by_cap() -> None:
    session_service = SlowSessionService()
    session_service._max_sessions = 1
    session_service.release_save.set()
    service = AgentService(session_service=session_service)
    service._histories["old"] = [HumanMessage(content="old")]
    service._histories["new"] = [HumanMessage(content="new")]
    service._sessions_timestamps["old"] = time.time() - 100
    service._sessions_timestamps["new"] = time.time()

    await service._save_histories(force=True)

    assert list(service._histories) == ["new"]
    assert list(session_service.saved_histories) == ["new"]
    assert session_service.deleted_sessions == ["old"]


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

        async def astream_events(self, payload, config, version):
            self.received_history = payload["chat_history"]
            yield {
                "event": "on_chain_end",
                "name": "AgentExecutor",
                "data": {"output": {"output": "done"}},
            }

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
    assert session_service.deleted_sessions == ["expired"]
    assert service._histories["expired"][0].content == "new request"
    assert not any(event["type"] == "error" for event in events)


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
