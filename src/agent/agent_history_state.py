"""Pure helpers for Agent thread-history state management."""

from __future__ import annotations

from collections.abc import Callable, Mapping, MutableMapping
from dataclasses import dataclass
import time
from typing import Any

from langchain_core.messages import BaseMessage, HumanMessage

from src.agent.thread_store import AgentThreadSnapshot


@dataclass(frozen=True)
class LoadedThreadMergeResult:
    histories: dict[str, list[BaseMessage]]
    timestamps: dict[str, float]
    needs_resave: bool
    restored_count: int
    cleared_pending_recovery_threads: set[str]


def build_thread_snapshots(
    histories: Mapping[str, list[BaseMessage]],
    timestamps: Mapping[str, float],
) -> dict[str, AgentThreadSnapshot]:
    return {
        thread_id: AgentThreadSnapshot(
            thread_id=thread_id,
            messages=list(messages),
            last_active_at=float(timestamps.get(thread_id, 0) or 0),
        )
        for thread_id, messages in histories.items()
    }


def merge_loaded_threads(
    loaded_threads: Mapping[str, AgentThreadSnapshot],
    *,
    current_histories: Mapping[str, list[BaseMessage]],
    current_timestamps: Mapping[str, float],
    pending_recovery_threads: set[str],
    transform_message: Callable[[BaseMessage], BaseMessage],
) -> LoadedThreadMergeResult:
    loaded_histories = {
        thread_id: [transform_message(message) for message in snapshot.messages]
        for thread_id, snapshot in loaded_threads.items()
    }
    loaded_timestamps = {
        thread_id: float(snapshot.last_active_at) for thread_id, snapshot in loaded_threads.items()
    }

    merged_histories = dict(loaded_histories)
    merged_timestamps = dict(loaded_timestamps)
    needs_resave = False

    for thread_id, messages in current_histories.items():
        current_ts = float(current_timestamps.get(thread_id, 0) or 0)
        loaded_ts = float(merged_timestamps.get(thread_id, 0) or 0)
        if thread_id in pending_recovery_threads:
            if thread_id in merged_histories:
                merged_histories[thread_id] = list(merged_histories[thread_id]) + list(messages)
                merged_timestamps[thread_id] = max(current_ts, loaded_ts)
            else:
                merged_histories[thread_id] = list(messages)
                merged_timestamps[thread_id] = current_ts
            needs_resave = True
            continue

        if thread_id not in merged_histories or current_ts >= loaded_ts:
            merged_histories[thread_id] = list(messages)
            merged_timestamps[thread_id] = current_ts

    cleared_pending = pending_recovery_threads - set(current_histories.keys())
    return LoadedThreadMergeResult(
        histories=merged_histories,
        timestamps=merged_timestamps,
        needs_resave=needs_resave,
        restored_count=len(loaded_histories),
        cleared_pending_recovery_threads=cleared_pending,
    )


def collect_stale_thread_ids(
    timestamps: Mapping[str, float],
    *,
    now: float | None = None,
    timeout_seconds: int,
) -> list[str]:
    if timeout_seconds <= 0:
        return []
    current_time = time.time() if now is None else now
    return [
        thread_id
        for thread_id, last_active_at in list(timestamps.items())
        if current_time - last_active_at > timeout_seconds
    ]


def cap_thread_histories(
    histories: dict[str, list[BaseMessage]],
    timestamps: dict[str, float],
    *,
    max_threads: int,
) -> list[str]:
    if max_threads <= 0 or len(histories) <= max_threads:
        return []

    sorted_thread_ids = sorted(
        histories.keys(),
        key=lambda thread_id: timestamps.get(thread_id, 0),
        reverse=True,
    )
    keep_thread_ids = set(sorted_thread_ids[:max_threads])
    removed_thread_ids: list[str] = []
    for thread_id in list(histories.keys()):
        if thread_id not in keep_thread_ids:
            histories.pop(thread_id, None)
            timestamps.pop(thread_id, None)
            removed_thread_ids.append(thread_id)
    return removed_thread_ids


def drop_thread_state(
    histories: MutableMapping[str, list[BaseMessage]],
    timestamps: MutableMapping[str, float],
    thread_id: str,
) -> None:
    histories.pop(thread_id, None)
    timestamps.pop(thread_id, None)


def list_active_thread_ids(histories: Mapping[str, list[BaseMessage]]) -> list[str]:
    return list(histories.keys())


def render_thread_history(
    histories: Mapping[str, list[BaseMessage]],
    *,
    thread_id: str,
    redact_message_content: Callable[[Any], str],
) -> list[dict[str, str]]:
    rendered_history: list[dict[str, str]] = []
    for message in histories.get(thread_id, []):
        role = "user" if isinstance(message, HumanMessage) else "assistant"
        rendered_history.append(
            {
                "role": role,
                "content": redact_message_content(message.content),
            }
        )
    return rendered_history


def summarize_session_metrics(
    histories: Mapping[str, list[BaseMessage]],
    timestamps: Mapping[str, float],
    *,
    timeout_seconds: int,
) -> dict[str, Any]:
    message_count = sum(len(messages) for messages in histories.values())
    now = time.time()
    ages = [
        max(0, int(now - ts))
        for thread_id, ts in timestamps.items()
        if thread_id in histories and isinstance(ts, (int, float))
    ]
    stale_count = sum(1 for age in ages if age > timeout_seconds) if timeout_seconds > 0 else 0
    average_messages = round(message_count / len(histories), 2) if histories else 0
    return {
        "history_message_count": message_count,
        "average_messages_per_session": average_messages,
        "average_messages_per_thread": average_messages,
        "stale_session_count": stale_count,
        "stale_thread_count": stale_count,
        "newest_session_age_seconds": min(ages) if ages else None,
        "newest_thread_age_seconds": min(ages) if ages else None,
        "oldest_session_age_seconds": max(ages) if ages else None,
        "oldest_thread_age_seconds": max(ages) if ages else None,
    }
