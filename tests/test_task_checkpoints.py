import asyncio

import pytest
from fastapi.testclient import TestClient

from src.core.collector_resume import build_collector_cursor, parse_recovery_cursor
from src.core.scheduler import Scheduler
from src.core.sensitive import redact_sensitive
from src.core.task import Task, TaskTarget
from src.services.task_checkpoint_service import InMemoryTaskCheckpointService
from src.services.task_event_service import InMemoryTaskEventService


def test_redact_sensitive_preserves_youtube_page_token() -> None:
    """Pagination page_token must survive redaction (substring 'token' is not a secret)."""
    cursor = build_collector_cursor(
        collector_id="youtube_comments",
        target_key="video:vid",
        stage="comments_scan",
        payload={
            "page_token": "CDIQAA_real_page",
            "scanned_count": 100,
            "api_key": "should-redact",
            "access_token": "should-redact-too",
        },
    )
    redacted = redact_sensitive(cursor)
    assert redacted["payload"]["page_token"] == "CDIQAA_real_page"
    assert redacted["payload"]["scanned_count"] == 100
    assert redacted["payload"]["api_key"] == "[REDACTED]"
    assert redacted["payload"]["access_token"] == "[REDACTED]"


@pytest.mark.asyncio
async def test_checkpoint_append_preserves_page_token_for_resume() -> None:
    """Real persist path must keep YouTube page_token so resume can continue pagination."""
    service = InMemoryTaskCheckpointService()
    cursor = build_collector_cursor(
        collector_id="youtube_comments",
        target_key="video:abc",
        stage="comments_scan",
        payload={"page_token": "NEXT_PAGE_XYZ", "scanned_count": 200},
    )
    stored = await service.append(
        "task-yt-resume",
        pipeline_name="p",
        collector_name="youtube_comments",
        recovery_level="L1",
        cursor=cursor,
        state={"target_order": ["v1"], "next_target_index": 0},
    )

    assert stored.cursor["payload"]["page_token"] == "NEXT_PAGE_XYZ"
    latest = await service.latest_checkpoint("task-yt-resume")
    assert latest is not None
    recovery = latest.model_dump(mode="json")
    parsed = parse_recovery_cursor(
        recovery, collector_id="youtube_comments", target_key="video:abc"
    )
    assert parsed is not None
    assert parsed["payload"]["page_token"] == "NEXT_PAGE_XYZ"


@pytest.mark.asyncio
async def test_in_memory_task_checkpoint_service_orders_latest_and_redacts() -> None:
    service = InMemoryTaskCheckpointService()

    first = await service.append(
        "task-1",
        pipeline_name="p",
        collector_name="steam",
        worker_id="worker-1",
        recovery_level="L1",
        cursor={"api_key": "cursor-secret"},
        stats={"processed": 10},
    )
    second = await service.append(
        "task-1",
        recovery_level="L2",
        cursor={"page": 2},
        state={"next_target_index": 2, "target_order": ["A", "B"]},
        artifacts=[
            {
                "name": "snapshot.html",
                "path": "C:/secret/snapshot.html",
                "local_path": "D:/secret/local.html",
                "download_url": "javascript:alert(1)",
                "token": "artifact-secret",
            },
            {
                "name": "report.xlsx",
                "file_path": "C:/secret/report.xlsx",
                "download_url": "/api/reports/report-1/download",
            },
        ],
        metadata={"token": "metadata-secret"},
    )
    await service.append("task-2", recovery_level="L1")

    checkpoints = await service.list_checkpoints("task-1")
    latest = await service.latest_checkpoint("task-1")
    public_second = second.to_public_payload()
    rendered = str([checkpoint.to_public_payload() for checkpoint in checkpoints])

    assert first.seq == 1
    assert second.seq == 2
    assert [checkpoint.seq for checkpoint in checkpoints] == [2, 1]
    assert latest is second
    assert checkpoints[1].cursor["api_key"] == "[REDACTED]"
    assert checkpoints[0].metadata["token"] == "[REDACTED]"
    assert checkpoints[0].state["next_target_index"] == 2
    assert "state" not in public_second
    assert second.artifacts[0]["path"] == "C:/secret/snapshot.html"
    assert public_second["artifacts"][0]["path"] == ""
    assert public_second["artifacts"][0]["local_path"] == ""
    assert public_second["artifacts"][0]["download_url"] == ""
    assert public_second["artifacts"][0]["token"] == "[REDACTED]"
    assert public_second["artifacts"][1]["file_path"] == ""
    assert public_second["artifacts"][1]["download_url"] == "/api/reports/report-1/download"
    assert "cursor-secret" not in rendered
    assert "metadata-secret" not in rendered
    assert "artifact-secret" not in rendered
    assert "C:/secret" not in rendered


@pytest.mark.asyncio
async def test_scheduler_registers_checkpoint_and_emits_event() -> None:
    checkpoint_service = InMemoryTaskCheckpointService()
    event_service = InMemoryTaskEventService()
    scheduler = Scheduler(
        task_event_service=event_service,
        task_checkpoint_service=checkpoint_service,
    )
    task = Task(
        id="checkpoint-task", name="Checkpoint Task", pipeline_name="p", collector_name="steam"
    )

    checkpoint = await scheduler.register_task_checkpoint(
        task,
        worker_id="worker-1",
        recovery_level="L1",
        cursor={"offset": 20},
        stats={"processed": 20},
    )
    checkpoints = await scheduler.get_task_checkpoints(task.id)
    latest = await scheduler.get_latest_task_checkpoint(task.id)
    events = await event_service.list_events(task.id)

    assert checkpoint is not None
    assert checkpoints == [checkpoint]
    assert latest is checkpoint
    assert checkpoint.pipeline_name == "p"
    assert checkpoint.collector_name == "steam"
    assert events[-1].type == "checkpoint"


@pytest.mark.asyncio
async def test_scheduler_records_l1_checkpoint_from_pipeline_collect_event() -> None:
    checkpoint_service = InMemoryTaskCheckpointService()
    event_service = InMemoryTaskEventService()
    scheduler = Scheduler(
        task_event_service=event_service,
        task_checkpoint_service=checkpoint_service,
    )
    task = Task(
        id="pipeline-checkpoint-task",
        name="Pipeline Checkpoint",
        pipeline_name="gtrends_basic",
        collector_name="gtrends",
    )
    scheduler._tasks[task.id] = task

    await scheduler._on_task_event(
        task.id,
        "collect",
        "info",
        "采集完成: 2/3 成功",
        {
            "status": "failed",
            "component": "gtrends",
            "targets_count": 3,
            "success_count": 2,
            "failed_count": 1,
        },
    )

    checkpoints = await checkpoint_service.list_checkpoints(task.id)
    events = await event_service.list_events(task.id)

    assert len(checkpoints) == 1
    assert checkpoints[0].recovery_level == "L1"
    assert checkpoints[0].cursor == {
        "stage": "collect",
        "component": "gtrends",
        "status": "failed",
    }
    assert checkpoints[0].state == {
        "target_order": [],
        "next_target_index": 0,
        "completed_targets": [],
        "successful_targets": [],
        "failed_targets": [],
    }
    assert checkpoints[0].stats == {
        "targets_count": 3,
        "success_count": 2,
        "failed_count": 1,
    }
    assert [event.type for event in events] == ["collect", "checkpoint"]


@pytest.mark.asyncio
async def test_scheduler_skips_pipeline_checkpoint_for_l0_collector() -> None:
    checkpoint_service = InMemoryTaskCheckpointService()
    event_service = InMemoryTaskEventService()
    scheduler = Scheduler(
        task_event_service=event_service,
        task_checkpoint_service=checkpoint_service,
    )
    task = Task(
        id="pipeline-no-checkpoint-task",
        name="Pipeline No Checkpoint",
        pipeline_name="qimai_basic",
        collector_name="qimai",
    )
    scheduler._tasks[task.id] = task

    await scheduler._on_task_event(
        task.id,
        "collect",
        "info",
        "采集完成: 1/1 成功",
        {
            "status": "succeeded",
            "component": "qimai",
            "targets_count": 1,
            "success_count": 1,
            "failed_count": 0,
        },
    )

    assert await checkpoint_service.list_checkpoints(task.id) == []


@pytest.mark.asyncio
async def test_preferred_checkpoint_skips_empty_shell() -> None:
    from src.core.collector_resume import build_collector_cursor, select_preferred_checkpoint

    service = InMemoryTaskCheckpointService()
    await service.append(
        "task-pref",
        recovery_level="L1",
        cursor=build_collector_cursor(
            collector_id="steam",
            target_key="app:1",
            stage="api_reviews",
            payload={"review_cursor": "KEEP"},
        ),
        state={"target_order": ["A"], "next_target_index": 0},
    )
    await service.append(
        "task-pref",
        recovery_level="L1",
        cursor={"stage": "collect", "status": "failed", "component": "steam"},
        state={"target_order": ["A"], "next_target_index": 1, "completed_targets": ["A"]},
    )
    listed = await service.list_checkpoints("task-pref")
    preferred = select_preferred_checkpoint(listed)
    assert preferred is not None
    assert preferred.cursor.get("payload", {}).get("review_cursor") == "KEEP"

    scheduler = Scheduler(task_checkpoint_service=service)
    scheduler_preferred = await scheduler.get_preferred_task_checkpoint("task-pref")
    assert scheduler_preferred is not None
    assert scheduler_preferred.cursor.get("payload", {}).get("review_cursor") == "KEEP"
    # raw latest remains the empty shell
    latest = await scheduler.get_latest_task_checkpoint("task-pref")
    assert latest is not None
    assert latest.cursor.get("stage") == "collect"


@pytest.mark.asyncio
async def test_preferred_checkpoint_composes_deep_cursor_and_target_order_state() -> None:
    """Scheduler preferred merges deep mid-progress cursor with honest collect-complete state."""
    from src.core.collector_resume import build_collector_cursor

    service = InMemoryTaskCheckpointService()
    await service.append(
        "task-compose",
        recovery_level="L1",
        cursor={"stage": "collect", "status": "failed", "component": "steam"},
        state={
            "target_order": ["A", "B"],
            "next_target_index": 1,
            "completed_targets": ["A"],
        },
    )
    await service.append(
        "task-compose",
        recovery_level="L1",
        cursor=build_collector_cursor(
            collector_id="steam",
            target_key="app:1",
            stage="api_reviews",
            payload={"review_cursor": "DEEP"},
        ),
        state={},  # mid-progress: deep cursor, empty state
    )

    scheduler = Scheduler(task_checkpoint_service=service)
    preferred = await scheduler.get_preferred_task_checkpoint("task-compose")
    assert preferred is not None
    assert preferred.cursor.get("payload", {}).get("review_cursor") == "DEEP"
    assert preferred.state.get("target_order") == ["A", "B"]
    assert preferred.state.get("next_target_index") == 1
    # raw deep checkpoint still has empty state (compose does not mutate store)
    listed = await service.list_checkpoints("task-compose")
    assert listed[0].state == {}


@pytest.mark.asyncio
async def test_get_checkpoint_by_id() -> None:
    service = InMemoryTaskCheckpointService()
    first = await service.append("task-get", recovery_level="L1", cursor={"page": 1})
    second = await service.append("task-get", recovery_level="L1", cursor={"page": 2})

    found = await service.get_checkpoint("task-get", first.checkpoint_id)
    missing = await service.get_checkpoint("task-get", "does-not-exist")
    assert found is not None
    assert found.checkpoint_id == first.checkpoint_id
    assert found.cursor["page"] == 1
    assert missing is None
    assert (await service.get_checkpoint("task-get", second.checkpoint_id)) is second


@pytest.mark.asyncio
async def test_pipeline_checkpoint_counts_only_does_not_invent_success_prefix() -> None:
    checkpoint_service = InMemoryTaskCheckpointService()
    event_service = InMemoryTaskEventService()
    scheduler = Scheduler(
        task_event_service=event_service,
        task_checkpoint_service=checkpoint_service,
    )
    task = Task(
        id="pipeline-counts-only",
        name="Counts Only",
        pipeline_name="gtrends_basic",
        collector_name="gtrends",
        targets=[
            TaskTarget(name="A"),
            TaskTarget(name="B"),
            TaskTarget(name="C"),
        ],
    )
    scheduler._tasks[task.id] = task

    await scheduler._on_task_event(
        task.id,
        "collect",
        "info",
        "采集完成: 2/3 成功",
        {
            "status": "failed",
            "component": "gtrends",
            "targets_count": 3,
            "success_count": 2,
            "failed_count": 1,
        },
    )

    checkpoints = await checkpoint_service.list_checkpoints(task.id)
    assert len(checkpoints) == 1
    assert checkpoints[0].state == {
        "target_order": ["A", "B", "C"],
        "next_target_index": 0,
        "completed_targets": [],
        "successful_targets": [],
        "failed_targets": [],
    }


@pytest.mark.asyncio
async def test_pipeline_checkpoint_resume_state_excludes_failed_target() -> None:
    """Collect complete with honest resume_state must not mark failed targets completed."""
    from src.collectors.base import CollectResult, CollectTarget
    from src.core.pipeline import _build_collect_complete_payload

    checkpoint_service = InMemoryTaskCheckpointService()
    event_service = InMemoryTaskEventService()
    scheduler = Scheduler(
        task_event_service=event_service,
        task_checkpoint_service=checkpoint_service,
    )
    task = Task(
        id="pipeline-resume-honest",
        name="Resume Honest",
        pipeline_name="gtrends_basic",
        collector_name="gtrends",
        targets=[
            TaskTarget(name="A"),
            TaskTarget(name="B"),
            TaskTarget(name="C"),
        ],
    )
    scheduler._tasks[task.id] = task

    collect_results = [
        CollectResult(target=CollectTarget(name="A"), success=True, data={"ok": 1}),
        CollectResult(target=CollectTarget(name="B"), success=False, error="boom"),
        CollectResult(target=CollectTarget(name="C"), success=True, data={"ok": 1}),
    ]
    payload = _build_collect_complete_payload(
        component="gtrends",
        collect_results=collect_results,
        task=task,
        recovery_context={},
        error="boom",
    )

    assert payload["status"] == "failed"
    assert "B" in payload["failed_targets"]
    assert "B" not in payload["resume_state"]["completed_targets"]
    assert payload["resume_state"]["next_target_index"] == 1
    assert payload["resume_state"]["successful_targets"] == ["A", "C"]

    await scheduler._on_task_event(
        task.id,
        "collect",
        "warning",
        "Collect complete: 2/3 succeeded",
        payload,
    )

    checkpoints = await checkpoint_service.list_checkpoints(task.id)
    assert len(checkpoints) == 1
    state = checkpoints[0].state
    assert "B" not in state["completed_targets"]
    assert state["completed_targets"] == ["A"]
    assert state["next_target_index"] == 1
    assert state["successful_targets"] == ["A", "C"]
    assert state["failed_targets"] == ["B"]


@pytest.mark.asyncio
async def test_pipeline_finalize_event_records_checkpoint_from_resume_state() -> None:
    checkpoint_service = InMemoryTaskCheckpointService()
    event_service = InMemoryTaskEventService()
    scheduler = Scheduler(
        task_event_service=event_service,
        task_checkpoint_service=checkpoint_service,
    )
    task = Task(
        id="pipeline-finalize-cp",
        name="Finalize CP",
        pipeline_name="gtrends_basic",
        collector_name="gtrends",
        targets=[
            TaskTarget(name="A"),
            TaskTarget(name="B"),
        ],
    )
    scheduler._tasks[task.id] = task

    resume_state = {
        "target_order": ["A", "B"],
        "next_target_index": 1,
        "completed_targets": ["A"],
        "successful_targets": ["A"],
        "failed_targets": ["B"],
        "output_record_keys": ["k1"],
    }
    await scheduler._on_task_event(
        task.id,
        "pipeline",
        "warning",
        "Pipeline partially failed",
        {
            "status": "failed",
            "collect_count": 2,
            "storage_count": 1,
            "resume_state": resume_state,
        },
    )

    checkpoints = await checkpoint_service.list_checkpoints(task.id)
    assert len(checkpoints) == 1
    assert checkpoints[0].cursor.get("stage") == "pipeline"
    assert checkpoints[0].state["completed_targets"] == ["A"]
    assert "B" not in checkpoints[0].state["completed_targets"]
    assert checkpoints[0].state["output_record_keys"] == ["k1"]


def test_task_checkpoints_api_returns_latest(monkeypatch) -> None:
    from src.web import app as app_module
    from src.web.app import create_app

    service = InMemoryTaskCheckpointService()
    first = asyncio.run(service.append("task-api-checkpoint", recovery_level="L1"))
    second = asyncio.run(
        service.append(
            "task-api-checkpoint",
            recovery_level="L2",
            cursor={"page": 2},
            artifacts=[
                {
                    "name": "report.xlsx",
                    "path": "C:/secret/report.xlsx",
                    "download_url": "file:///C:/secret/report.xlsx",
                }
            ],
        )
    )

    class FakeTaskService:
        async def get_task_checkpoints(self, task_id: str, *, limit: int = 200, offset: int = 0):
            if task_id != "task-api-checkpoint":
                return None
            return [second, first], second

    monkeypatch.setattr(app_module, "get_task_service", lambda: FakeTaskService())

    client = TestClient(create_app())
    response = client.get("/api/tasks/task-api-checkpoint/checkpoints")

    assert response.status_code == 200
    payload = response.json()
    assert payload["latest"]["recovery_level"] == "L2"
    assert payload["latest"]["artifacts"][0]["path"] == ""
    assert payload["latest"]["artifacts"][0]["download_url"] == ""
    assert [item["seq"] for item in payload["checkpoints"]] == [2, 1]


def test_task_detail_api_includes_recovery_info(monkeypatch) -> None:
    from src.web import app as app_module
    from src.web.app import create_app

    task = Task(
        id="task-detail-recovery",
        name="Recovery Detail",
        pipeline_name="gtrends_basic",
        collector_name="gtrends",
    )

    class FakeTaskService:
        def get_task(self, task_id: str):
            return task if task_id == task.id else None

        async def get_task_recovery_info(self, task_id: str):
            if task_id != task.id:
                return None
            return {
                "collector_id": "gtrends",
                "supports_checkpoint": True,
                "recovery_level": "L1",
                "latest_checkpoint": {"checkpoint_id": "checkpoint-1", "seq": 1},
            }

        def get_task_collector_metadata(self, task_id: str):
            if task_id != task.id:
                return None
            return {
                "collector_id": "gtrends",
                "session_mode": "api_only",
                "supports_checkpoint": True,
                "recovery_level": "L1",
            }

        def get_task_session_diagnostics(self, task_id: str):
            if task_id != task.id:
                return None
            return {
                "collector_id": "gtrends",
                "session_mode": "api_only",
                "status": "ok",
            }

        def get_task_session_readiness(self, task_id: str):
            if task_id != task.id:
                return None
            return {
                "status": "not_required",
                "precheck_status": "ok",
                "summary": "No local session required for task submission.",
            }

    monkeypatch.setattr(app_module, "get_task_service", lambda: FakeTaskService())

    client = TestClient(create_app())
    response = client.get(f"/api/tasks/{task.id}")

    assert response.status_code == 200
    payload = response.json()
    assert payload["recovery"]["recovery_level"] == "L1"
    assert payload["recovery"]["latest_checkpoint"]["checkpoint_id"] == "checkpoint-1"
    assert payload["collector_metadata"]["collector_id"] == "gtrends"
    assert payload["session_diagnostics"]["status"] == "ok"
    assert payload["session_readiness"]["status"] == "not_required"


def test_task_detail_api_succeeds_when_session_inventory_sync_fails(monkeypatch) -> None:
    from src.web import app as app_module
    from src.web.app import create_app

    task = Task(
        id="task-detail-sync-failure",
        name="Recovery Detail",
        pipeline_name="gtrends_basic",
        collector_name="gtrends",
    )

    class FakeTaskService:
        def get_task(self, task_id: str):
            return task if task_id == task.id else None

        async def get_task_recovery_info(self, task_id: str):
            return {"collector_id": "gtrends"} if task_id == task.id else None

        def get_task_collector_metadata(self, task_id: str):
            return {"collector_id": "gtrends"} if task_id == task.id else None

        def get_task_session_diagnostics(self, task_id: str):
            if task_id != task.id:
                return None
            return {
                "collector_id": "gtrends",
                "session_mode": "api_only",
                "status": "ok",
            }

        def get_task_session_readiness(self, task_id: str):
            return (
                {"status": "not_required", "precheck_status": "ok"} if task_id == task.id else None
            )

    class BrokenRegistry:
        async def sync_from_diagnostics(self, diagnostics):
            raise RuntimeError("detail sync failed token=broken-secret")

    monkeypatch.setattr(app_module, "get_task_service", lambda: FakeTaskService())
    monkeypatch.setattr(app_module, "get_session_registry", lambda: BrokenRegistry())

    client = TestClient(create_app())
    response = client.get(f"/api/tasks/{task.id}")

    assert response.status_code == 200
    payload = response.json()
    assert payload["id"] == task.id
    assert payload["session_diagnostics"]["status"] == "ok"


def test_task_detail_api_succeeds_when_session_registry_lookup_fails(monkeypatch) -> None:
    from src.web import app as app_module
    from src.web.app import create_app

    task = Task(
        id="task-detail-lookup-failure",
        name="Recovery Detail Lookup Failure",
        pipeline_name="gtrends_basic",
        collector_name="gtrends",
    )

    class FakeTaskService:
        def get_task(self, task_id: str):
            return task if task_id == task.id else None

        async def get_task_recovery_info(self, task_id: str):
            return {"collector_id": "gtrends"} if task_id == task.id else None

        def get_task_collector_metadata(self, task_id: str):
            return {"collector_id": "gtrends"} if task_id == task.id else None

        def get_task_session_diagnostics(self, task_id: str):
            if task_id != task.id:
                return None
            return {
                "collector_id": "gtrends",
                "session_mode": "api_only",
                "status": "ok",
            }

        def get_task_session_readiness(self, task_id: str):
            return (
                {"status": "not_required", "precheck_status": "ok"} if task_id == task.id else None
            )

    def broken_registry_provider():
        raise RuntimeError("registry lookup failed token=detail-lookup-secret")

    monkeypatch.setattr(app_module, "get_task_service", lambda: FakeTaskService())
    monkeypatch.setattr(app_module, "get_session_registry", broken_registry_provider)

    client = TestClient(create_app())
    response = client.get(f"/api/tasks/{task.id}")

    assert response.status_code == 200
    payload = response.json()
    assert payload["id"] == task.id
    assert payload["session_diagnostics"]["status"] == "ok"
