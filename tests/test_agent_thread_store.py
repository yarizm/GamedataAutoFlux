import time

import pytest
from langchain_core.messages import AIMessage, HumanMessage

from src.agent.thread_store import AgentThreadSnapshot, SessionBackedAgentThreadStore


class FakeSessionService:
    def __init__(self) -> None:
        self._max_sessions = 7
        self.loaded_histories = {
            "legacy-session": [HumanMessage(content="hello"), AIMessage(content="world")]
        }
        self.loaded_timestamps = {"legacy-session": 123.0}
        self.saved_histories = None
        self.saved_timestamps = None
        self.saved_last_save_time = None
        self.saved_force = None
        self.deleted_sessions = []

    async def load_histories(self):
        return self.loaded_histories, self.loaded_timestamps

    async def save_histories(self, histories, timestamps, last_save_time, force=False):
        self.saved_histories = histories
        self.saved_timestamps = timestamps
        self.saved_last_save_time = last_save_time
        self.saved_force = force
        return time.time()

    async def delete_sessions(self, session_ids):
        self.deleted_sessions.extend(session_ids)


@pytest.mark.asyncio
async def test_session_backed_thread_store_loads_legacy_sessions_as_threads() -> None:
    store = SessionBackedAgentThreadStore(FakeSessionService())

    threads = await store.load_threads()

    assert store.max_threads == 7
    assert list(threads) == ["legacy-session"]
    assert threads["legacy-session"].thread_id == "legacy-session"
    assert threads["legacy-session"].last_active_at == 123.0
    assert threads["legacy-session"].messages[0].content == "hello"


@pytest.mark.asyncio
async def test_session_backed_thread_store_saves_threads_through_legacy_service() -> None:
    session_service = FakeSessionService()
    store = SessionBackedAgentThreadStore(session_service)

    next_save = await store.save_threads(
        {
            "thread-a": AgentThreadSnapshot(
                thread_id="thread-a",
                messages=[HumanMessage(content="question"), AIMessage(content="answer")],
                last_active_at=456.0,
            )
        },
        last_save_time=111.0,
        force=True,
    )

    assert next_save >= 111.0
    assert list(session_service.saved_histories) == ["thread-a"]
    assert session_service.saved_histories["thread-a"][1].content == "answer"
    assert session_service.saved_timestamps == {"thread-a": 456.0}
    assert session_service.saved_last_save_time == 111.0
    assert session_service.saved_force is True


@pytest.mark.asyncio
async def test_session_backed_thread_store_deletes_threads_through_legacy_service() -> None:
    session_service = FakeSessionService()
    store = SessionBackedAgentThreadStore(session_service)

    await store.delete_threads(["thread-a", "", "thread-b"])

    assert session_service.deleted_sessions == ["thread-a", "thread-b"]
