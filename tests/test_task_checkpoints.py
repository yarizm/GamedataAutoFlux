import asyncio

import pytest
from fastapi.testclient import TestClient

from src.core.scheduler import Scheduler
from src.core.task import Task
from src.services.task_checkpoint_service import InMemoryTaskCheckpointService
from src.services.task_event_service import InMemoryTaskEventService


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
    task = Task(id="checkpoint-task", name="Checkpoint Task", pipeline_name="p", collector_name="steam")

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

    monkeypatch.setattr(app_module, "get_task_service", lambda: FakeTaskService())

    client = TestClient(create_app())
    response = client.get(f"/api/tasks/{task.id}")

    assert response.status_code == 200
    payload = response.json()
    assert payload["recovery"]["recovery_level"] == "L1"
    assert payload["recovery"]["latest_checkpoint"]["checkpoint_id"] == "checkpoint-1"
    assert payload["collector_metadata"]["collector_id"] == "gtrends"
    assert payload["session_diagnostics"]["status"] == "ok"
