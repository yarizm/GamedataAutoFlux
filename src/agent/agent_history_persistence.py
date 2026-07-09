"""Async helpers for Agent thread-history persistence orchestration."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass

from langchain_core.messages import BaseMessage

from src.agent.agent_history_state import (
    build_thread_snapshots,
    cap_thread_histories,
    collect_stale_thread_ids,
    merge_loaded_threads,
)
from src.agent.thread_store import AgentThreadStore


@dataclass(frozen=True)
class SaveHistoriesResult:
    persisted_ok: bool
    next_save_time: float
    removed_thread_ids: list[str]
    pending_recovery_threads: set[str]


@dataclass(frozen=True)
class LoadHistoriesResult:
    loaded: bool
    needs_resave: bool
    restored_count: int
    error: Exception | None = None


async def save_agent_histories(
    *,
    history_load_failed: bool,
    lock: asyncio.Lock,
    histories: dict[str, list[BaseMessage]],
    timestamps: dict[str, float],
    pending_recovery_threads: set[str],
    thread_store: AgentThreadStore,
    read_last_save_time: Callable[[], float],
    force: bool = False,
) -> SaveHistoriesResult:
    if history_load_failed:
        return SaveHistoriesResult(
            persisted_ok=False,
            next_save_time=float(read_last_save_time() or 0),
            removed_thread_ids=[],
            pending_recovery_threads=set(),
        )

    async with lock:
        removed_thread_ids = cap_thread_histories(
            histories,
            timestamps,
            max_threads=int(getattr(thread_store, "max_threads", 0) or 0),
        )
        history_snapshot = {thread_id: list(messages) for thread_id, messages in histories.items()}
        timestamp_snapshot = dict(timestamps)
        last_save_time = float(read_last_save_time() or 0)
        pending_snapshot = set(pending_recovery_threads)

    thread_snapshots = build_thread_snapshots(history_snapshot, timestamp_snapshot)
    next_save_time = await thread_store.save_threads(
        thread_snapshots,
        last_save_time=last_save_time,
        force=force,
    )
    persisted_ok = next_save_time > last_save_time or not thread_snapshots
    return SaveHistoriesResult(
        persisted_ok=persisted_ok,
        next_save_time=next_save_time,
        removed_thread_ids=removed_thread_ids,
        pending_recovery_threads=pending_snapshot if persisted_ok else set(),
    )


async def load_agent_histories(
    *,
    lock: asyncio.Lock,
    histories: dict[str, list[BaseMessage]],
    timestamps: dict[str, float],
    pending_recovery_threads: set[str],
    thread_store: AgentThreadStore,
    transform_message: Callable[[BaseMessage], BaseMessage],
) -> LoadHistoriesResult:
    try:
        threads = await thread_store.load_threads()
    except Exception as exc:
        return LoadHistoriesResult(
            loaded=False,
            needs_resave=False,
            restored_count=0,
            error=exc,
        )

    async with lock:
        merge_result = merge_loaded_threads(
            threads,
            current_histories=histories,
            current_timestamps=timestamps,
            pending_recovery_threads=set(pending_recovery_threads),
            transform_message=transform_message,
        )
        for thread_id in merge_result.cleared_pending_recovery_threads:
            pending_recovery_threads.discard(thread_id)
        histories.clear()
        histories.update(merge_result.histories)
        timestamps.clear()
        timestamps.update(merge_result.timestamps)

    return LoadHistoriesResult(
        loaded=True,
        needs_resave=merge_result.needs_resave,
        restored_count=merge_result.restored_count,
    )


async def cleanup_stale_agent_threads(
    *,
    lock: asyncio.Lock,
    histories: dict[str, list[BaseMessage]],
    timestamps: dict[str, float],
    timeout_seconds: int,
    now: float,
) -> list[str]:
    async with lock:
        stale_thread_ids = collect_stale_thread_ids(
            timestamps,
            now=now,
            timeout_seconds=timeout_seconds,
        )
        for thread_id in stale_thread_ids:
            histories.pop(thread_id, None)
            timestamps.pop(thread_id, None)
    return stale_thread_ids


async def delete_persisted_agent_threads(
    *,
    thread_ids: list[str],
    lock: asyncio.Lock,
    pending_recovery_threads: set[str],
    thread_store: AgentThreadStore,
    forget_thread: Callable[[str], Awaitable[None]],
) -> list[str]:
    normalized_thread_ids = [thread_id for thread_id in thread_ids if thread_id]
    if not normalized_thread_ids:
        return []

    async with lock:
        for thread_id in normalized_thread_ids:
            pending_recovery_threads.discard(thread_id)

    await thread_store.delete_threads(normalized_thread_ids)
    for thread_id in normalized_thread_ids:
        await forget_thread(thread_id)
    return normalized_thread_ids
