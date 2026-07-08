"""Thread-oriented persistence adapter for Agent conversations.

Phase 2 of the LangGraph migration treats the legacy ``session_id`` as a
thread identifier while preserving the current database schema and API
compatibility.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Protocol, runtime_checkable

from langchain_core.messages import BaseMessage


@dataclass(slots=True)
class AgentThreadSnapshot:
    """Persisted thread state for one Agent conversation."""

    thread_id: str
    messages: list[BaseMessage]
    last_active_at: float


@runtime_checkable
class AgentThreadStore(Protocol):
    """Persistence interface used by AgentService for thread state."""

    @property
    def max_threads(self) -> int:
        """Maximum number of persisted threads to keep."""

    async def load_threads(self) -> dict[str, AgentThreadSnapshot]:
        """Load persisted thread snapshots."""

    async def save_threads(
        self,
        threads: Mapping[str, AgentThreadSnapshot],
        *,
        last_save_time: float,
        force: bool = False,
    ) -> float:
        """Persist thread snapshots and return the next save timestamp."""

    async def delete_threads(self, thread_ids: Sequence[str]) -> None:
        """Delete persisted threads by id."""


class SessionBackedAgentThreadStore:
    """Adapter that maps thread semantics onto the legacy session store."""

    def __init__(self, session_service: Any) -> None:
        self._session_service = session_service

    @property
    def max_threads(self) -> int:
        return int(getattr(self._session_service, "_max_sessions", 0) or 0)

    async def load_threads(self) -> dict[str, AgentThreadSnapshot]:
        load_histories = getattr(self._session_service, "load_histories", None)
        if load_histories is None:
            return {}

        histories, timestamps = await load_histories()
        snapshots: dict[str, AgentThreadSnapshot] = {}
        for thread_id, messages in histories.items():
            snapshots[thread_id] = AgentThreadSnapshot(
                thread_id=thread_id,
                messages=list(messages),
                last_active_at=float(timestamps.get(thread_id, 0) or 0),
            )
        return snapshots

    async def save_threads(
        self,
        threads: Mapping[str, AgentThreadSnapshot],
        *,
        last_save_time: float,
        force: bool = False,
    ) -> float:
        save_histories = getattr(self._session_service, "save_histories", None)
        if save_histories is None:
            return last_save_time

        histories = {
            snapshot.thread_id: list(snapshot.messages) for snapshot in threads.values()
        }
        timestamps = {
            snapshot.thread_id: float(snapshot.last_active_at) for snapshot in threads.values()
        }
        return await save_histories(
            histories,
            timestamps,
            last_save_time=last_save_time,
            force=force,
        )

    async def delete_threads(self, thread_ids: Sequence[str]) -> None:
        normalized = [thread_id for thread_id in thread_ids if thread_id]
        if not normalized:
            return

        delete_sessions = getattr(self._session_service, "delete_sessions", None)
        if delete_sessions is not None:
            await delete_sessions(normalized)
            return

        cleanup_stale = getattr(self._session_service, "cleanup_stale", None)
        if cleanup_stale is not None:
            await cleanup_stale(
                {thread_id: [] for thread_id in normalized},
                {thread_id: 0 for thread_id in normalized},
            )


def create_thread_store(session_service: Any) -> AgentThreadStore:
    """Wrap the current persistence service in a thread-oriented adapter."""
    if isinstance(session_service, AgentThreadStore):
        return session_service
    return SessionBackedAgentThreadStore(session_service)
