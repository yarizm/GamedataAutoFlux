import time

from langchain_core.messages import AIMessage, HumanMessage

from src.agent.agent_history_state import (
    build_thread_snapshots,
    cap_thread_histories,
    collect_stale_thread_ids,
    merge_loaded_threads,
    summarize_session_metrics,
)
from src.agent.thread_store import AgentThreadSnapshot


def test_build_thread_snapshots_preserves_messages_and_timestamps() -> None:
    histories = {
        "thread-a": [HumanMessage(content="question"), AIMessage(content="answer")],
    }
    snapshots = build_thread_snapshots(histories, {"thread-a": 123.0})

    assert list(snapshots) == ["thread-a"]
    assert snapshots["thread-a"].thread_id == "thread-a"
    assert snapshots["thread-a"].last_active_at == 123.0
    assert snapshots["thread-a"].messages[1].content == "answer"
    assert snapshots["thread-a"].messages is not histories["thread-a"]


def test_merge_loaded_threads_merges_pending_and_prefers_newer_current_threads() -> None:
    loaded_threads = {
        "pending": AgentThreadSnapshot(
            thread_id="pending",
            messages=[HumanMessage(content="persisted-question")],
            last_active_at=100.0,
        ),
        "older": AgentThreadSnapshot(
            thread_id="older",
            messages=[AIMessage(content="persisted-answer")],
            last_active_at=100.0,
        ),
    }
    result = merge_loaded_threads(
        loaded_threads,
        current_histories={
            "pending": [AIMessage(content="buffered-answer")],
            "older": [HumanMessage(content="newer-question")],
        },
        current_timestamps={
            "pending": 200.0,
            "older": 150.0,
        },
        pending_recovery_threads={"pending", "stale-missing"},
        transform_message=lambda message: message,
    )

    assert [message.content for message in result.histories["pending"]] == [
        "persisted-question",
        "buffered-answer",
    ]
    assert [message.content for message in result.histories["older"]] == ["newer-question"]
    assert result.timestamps["pending"] == 200.0
    assert result.timestamps["older"] == 150.0
    assert result.needs_resave is True
    assert result.restored_count == 2
    assert result.cleared_pending_recovery_threads == {"stale-missing"}


def test_collect_stale_thread_ids_respects_timeout() -> None:
    now = time.time()
    stale = collect_stale_thread_ids(
        {
            "fresh": now - 1,
            "stale": now - 100,
        },
        now=now,
        timeout_seconds=10,
    )

    assert stale == ["stale"]


def test_cap_thread_histories_keeps_newest_threads_and_returns_removed_ids() -> None:
    histories = {
        "old": [HumanMessage(content="old")],
        "new": [HumanMessage(content="new")],
        "mid": [HumanMessage(content="mid")],
    }
    timestamps = {
        "old": 100.0,
        "new": 300.0,
        "mid": 200.0,
    }

    removed = cap_thread_histories(histories, timestamps, max_threads=2)

    assert removed == ["old"]
    assert list(histories) == ["new", "mid"] or list(histories) == ["mid", "new"]
    assert "old" not in timestamps


def test_summarize_session_metrics_reports_counts_and_ages() -> None:
    now = time.time()
    real_time = time.time
    try:
        time.time = lambda: now
        summary = summarize_session_metrics(
            {
                "a": [HumanMessage(content="1"), AIMessage(content="2")],
                "b": [HumanMessage(content="3")],
            },
            {
                "a": now - 5,
                "b": now - 20,
            },
            timeout_seconds=10,
        )
    finally:
        time.time = real_time

    assert summary["history_message_count"] == 3
    assert summary["average_messages_per_session"] == 1.5
    assert summary["average_messages_per_thread"] == 1.5
    assert summary["stale_session_count"] == 1
    assert summary["stale_thread_count"] == 1
    assert summary["newest_session_age_seconds"] == 5
    assert summary["oldest_session_age_seconds"] == 20
