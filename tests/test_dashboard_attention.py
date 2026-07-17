"""Tests for server-side dashboard attention aggregation (shipped builder path)."""

from __future__ import annotations

from src.core.task import Task, TaskStatus
from src.services.dashboard_attention import (
    build_dashboard_attention,
    build_failed_task_digests,
    build_health_attention_items,
)


def test_failed_task_digests_include_structured_error_code():
    ok = Task(name="ok-task")
    ok.start()
    ok.complete({"ok": True})

    failed = Task(name="steam-fail")
    failed.start()
    failed.fail("need steamdb login", error_code="login_required")

    digests = build_failed_task_digests([ok, failed], limit=5)
    assert len(digests) == 1
    item = digests[0]
    assert item["id"] == failed.id
    assert item["name"] == "steam-fail"
    assert item["status"] == TaskStatus.FAILED.value
    assert item["error_code"] == "login_required"
    assert item["error_title"]  # derived presentation
    assert item["error_suggestion"]
    assert item["phase"] == "failed"


def test_health_attention_includes_non_ok_checks_and_skips_ok():
    health = {
        "status": "error",
        "checks": [
            {"id": "db", "name": "Database", "status": "error", "message": "down"},
            {"id": "cache", "name": "Cache", "status": "ok", "message": "fine"},
            {
                "id": "steam",
                "name": "SteamDB",
                "status": "warning",
                "message": "stale session",
                "error_code": "login_required",
            },
        ],
    }
    items = build_health_attention_items(health, None, limit=10)
    ids = {i["id"] for i in items}
    assert "overall" in ids
    assert "db" in ids
    assert "steam" in ids
    assert "cache" not in ids
    steam = next(i for i in items if i["id"] == "steam")
    assert steam["severity"] == "warning"
    assert steam["code"] == "login_required"


def test_build_dashboard_attention_shape():
    failed = Task(name="x")
    failed.start()
    failed.fail("rate limited by upstream", error_code="rate_limited")

    attention = build_dashboard_attention(
        [failed],
        health={"status": "ok", "checks": []},
        diagnostics={"status": "ok", "checks": []},
    )
    assert "failed_tasks" in attention
    assert "health_issues" in attention
    assert len(attention["failed_tasks"]) == 1
    assert attention["failed_tasks"][0]["error_code"] == "rate_limited"
    assert attention["health_issues"] == []


def test_task_service_get_stats_includes_attention_with_failed_code():
    """Drive real TaskService.get_stats path with an in-memory scheduler stub."""

    class _StubScheduler:
        def __init__(self, tasks: list[Task]) -> None:
            self._tasks = {t.id: t for t in tasks}

        def get_stats(self) -> dict:
            return {
                "total_tasks": len(self._tasks),
                "running_tasks": 0,
                "max_concurrent": 5,
                "status_counts": {"failed": 1},
                "cron_jobs": 0,
                "started": True,
            }

        def get_all_tasks(self) -> list[Task]:
            return list(self._tasks.values())

    from src.services.task_service import TaskService

    failed = Task(name="svc-fail")
    failed.start()
    failed.fail("missing api key", error_code="missing_credentials")

    svc = TaskService(_StubScheduler([failed]))
    stats = svc.get_stats()
    assert "attention" in stats
    failed_tasks = stats["attention"]["failed_tasks"]
    assert len(failed_tasks) >= 1
    assert any(t.get("error_code") == "missing_credentials" for t in failed_tasks)
    assert "health_issues" in stats["attention"]


def test_failed_digests_sort_mixed_naive_and_aware_datetimes():
    """Restored tasks may have naive datetimes; live fail() uses aware — must not TypeError."""
    from datetime import datetime, timezone

    restored = Task(name="restored-fail")
    restored.status = TaskStatus.FAILED
    restored.error = "old failure"
    restored.error_code = "unknown"
    restored.phase = "failed"
    # Simulate storage restore without tzinfo (common from model_validate JSON)
    restored.created_at = datetime(2026, 1, 1, 12, 0, 0)  # naive
    restored.completed_at = datetime(2026, 1, 1, 12, 5, 0)  # naive

    live = Task(name="live-fail")
    live.start()
    live.fail("rate limited", error_code="rate_limited")
    # live.completed_at is timezone-aware via fail()

    digests = build_failed_task_digests([restored, live], limit=5)
    assert len(digests) == 2
    codes = {d["error_code"] for d in digests}
    assert "rate_limited" in codes
    assert "unknown" in codes


def test_get_stats_still_lists_failed_when_naive_and_aware_tasks_mixed():
    """Regression: stats attention must not become empty after mixed-tz sort."""
    from datetime import datetime

    class _StubScheduler:
        def __init__(self, tasks: list[Task]) -> None:
            self._tasks = {t.id: t for t in tasks}

        def get_stats(self) -> dict:
            return {
                "total_tasks": len(self._tasks),
                "running_tasks": 0,
                "max_concurrent": 5,
                "status_counts": {"failed": 2},
                "cron_jobs": 0,
                "started": True,
            }

        def get_all_tasks(self) -> list[Task]:
            return list(self._tasks.values())

    from src.services.task_service import TaskService

    restored = Task(name="restored")
    restored.status = TaskStatus.FAILED
    restored.error = "cookie gone"
    restored.error_code = "login_required"
    restored.phase = "failed"
    restored.created_at = datetime(2026, 6, 1, 8, 0, 0)
    restored.completed_at = datetime(2026, 6, 1, 8, 1, 0)

    live = Task(name="live")
    live.start()
    live.fail("missing key", error_code="missing_credentials")

    stats = TaskService(_StubScheduler([restored, live])).get_stats()
    failed_tasks = stats["attention"]["failed_tasks"]
    assert len(failed_tasks) == 2
    assert {t["error_code"] for t in failed_tasks} == {
        "login_required",
        "missing_credentials",
    }
