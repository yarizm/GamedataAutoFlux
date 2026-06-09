import json
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest
from fastapi.testclient import TestClient

from src.core.pipeline import Pipeline
from src.core.scheduler import Scheduler
from src.core.task import Task, TaskStatus, TaskTarget
from src.services.task_artifact_service import InMemoryTaskArtifactService
from src.services.task_checkpoint_service import InMemoryTaskCheckpointService
from src.services.task_event_service import InMemoryTaskEventService
from src.services.task_repository import InMemoryTaskRepository
from src.services.worker_registry import InMemoryWorkerRegistry
from src.web.app import create_app


@pytest.mark.asyncio
async def test_in_memory_worker_registry_registers_and_heartbeats_redacted() -> None:
    registry = InMemoryWorkerRegistry()

    worker = await registry.register(
        worker_id="worker-1",
        hostname="host api_key=host-secret",
        capabilities=["steam", "qimai"],
        metadata={"token": "metadata-secret"},
    )
    updated = await registry.heartbeat(
        "worker-1",
        status="busy",
        current_task_ids=["task-1"],
        metadata={"password": "heartbeat-secret"},
    )
    workers = await registry.list_workers()
    rendered = json.dumps([item.to_public_payload() for item in workers], ensure_ascii=False)

    assert worker.worker_id == "worker-1"
    assert worker.hostname == "host api_key=[REDACTED]"
    assert updated is not None
    assert updated.status == "busy"
    assert updated.capabilities == ["steam", "qimai"]
    assert updated.current_task_ids == ["task-1"]
    assert workers[0].metadata["password"] == "[REDACTED]"
    assert "host-secret" not in rendered
    assert "metadata-secret" not in rendered
    assert "heartbeat-secret" not in rendered


@pytest.mark.asyncio
async def test_worker_registry_marks_stale_busy_worker_offline() -> None:
    registry = InMemoryWorkerRegistry()
    worker = await registry.register(worker_id="stale-worker", capabilities=["steam"])
    await registry.heartbeat("stale-worker", status="busy", current_task_ids=["task-1"])
    registry._workers[worker.worker_id] = registry._workers[worker.worker_id].model_copy(
        update={"last_heartbeat_at": datetime.now(timezone.utc) - timedelta(seconds=30)}
    )

    workers = await registry.list_workers(stale_after_seconds=1)

    assert workers[0].status == "offline"
    assert workers[0].current_task_ids == ["task-1"]


@pytest.mark.asyncio
async def test_worker_registry_missing_heartbeat_returns_none() -> None:
    registry = InMemoryWorkerRegistry()

    assert await registry.heartbeat("missing") is None
    assert await registry.get_worker("missing") is None


@pytest.mark.asyncio
async def test_scheduler_worker_claim_backend_claims_and_completes_task() -> None:
    event_service = InMemoryTaskEventService()
    artifact_service = InMemoryTaskArtifactService()
    checkpoint_service = InMemoryTaskCheckpointService()
    scheduler = Scheduler(
        max_concurrent=1,
        task_repo=InMemoryTaskRepository(),
        task_event_service=event_service,
        task_artifact_service=artifact_service,
        task_checkpoint_service=checkpoint_service,
        execution_backend="worker_claim",
    )
    scheduler._started = True
    scheduler._pipelines["worker_pipeline"] = Pipeline("worker_pipeline").add_collector("steam")
    task = Task(
        id="claim-task",
        name="Claim Task",
        pipeline_name="worker_pipeline",
        collector_name="steam",
        targets=[TaskTarget(name="CS2", params={"app_id": "730"})],
    )

    task_id = await scheduler.submit(task, pipeline_name="worker_pipeline")
    claim = await scheduler.claim_task_for_worker("worker-1", capabilities=["steam"])
    event = await scheduler.append_worker_task_event(
        "worker-1",
        task_id,
        "collect",
        message="worker started",
    )
    artifact = await scheduler.register_worker_task_artifact(
        "worker-1",
        task_id,
        "json",
        name="output.json",
        metadata={"kind": "worker_output"},
    )
    checkpoint = await scheduler.register_worker_task_checkpoint(
        "worker-1",
        task_id,
        recovery_level="L1",
        cursor={"offset": 10},
        stats={"processed": 10},
    )
    completed = await scheduler.complete_worker_task(
        "worker-1",
        task_id,
        result={
            "success": True,
            "storage_count": 2,
            "api_key": "worker-result-secret",
            "nested": {"token": "nested-result-secret"},
        },
    )

    stored_events = await event_service.list_events(task_id)
    stored_artifacts = await artifact_service.list_artifacts(task_id)
    stored_checkpoints = await checkpoint_service.list_checkpoints(task_id)
    rendered = json.dumps(
        {
            "task_result": scheduler.get_task(task_id).result,
            "events": [event.to_public_payload() for event in stored_events],
        },
        ensure_ascii=False,
    )

    assert claim is not None
    assert claim["task_id"] == task_id
    assert claim["pipeline"]["name"] == "worker_pipeline"
    assert claim["task"]["config"]["worker_claim"]["worker_id"] == "worker-1"
    assert claim["latest_checkpoint"] is None
    assert claim["recovery"]["collector_id"] == "steam"
    assert claim["recovery"]["recovery_level"] == "L1"
    assert scheduler.get_task(task_id).status == TaskStatus.SUCCESS
    assert completed is scheduler.get_task(task_id)
    assert scheduler.get_task(task_id).result["api_key"] == "[REDACTED]"
    assert scheduler.get_task(task_id).result["nested"]["token"] == "[REDACTED]"
    assert event is not None
    assert artifact is not None
    assert checkpoint is not None
    assert stored_artifacts[0].metadata["kind"] == "worker_output"
    assert stored_checkpoints[0].recovery_level == "L1"
    assert [item.type for item in stored_events] == [
        "queued",
        "claimed",
        "collect",
        "artifact",
        "checkpoint",
        "complete",
    ]
    assert "worker-result-secret" not in rendered
    assert "nested-result-secret" not in rendered


@pytest.mark.asyncio
async def test_scheduler_worker_claim_respects_capabilities() -> None:
    scheduler = Scheduler(
        max_concurrent=1,
        task_repo=InMemoryTaskRepository(),
        task_event_service=InMemoryTaskEventService(),
        execution_backend="worker_claim",
    )
    scheduler._started = True
    scheduler._pipelines["steam_pipeline"] = Pipeline("steam_pipeline").add_collector("steam")
    await scheduler.submit(
        Task(id="steam-claim", name="Steam Claim", pipeline_name="steam_pipeline"),
        pipeline_name="steam_pipeline",
    )

    assert await scheduler.claim_task_for_worker("worker-1", capabilities=["qimai"]) is None
    assert await scheduler.claim_task_for_worker("worker-1", capabilities=["steam_api"]) is not None


@pytest.mark.asyncio
async def test_scheduler_worker_failure_retries_and_can_be_reclaimed() -> None:
    event_service = InMemoryTaskEventService()
    checkpoint_service = InMemoryTaskCheckpointService()
    scheduler = Scheduler(
        max_concurrent=1,
        task_repo=InMemoryTaskRepository(),
        task_event_service=event_service,
        task_checkpoint_service=checkpoint_service,
        execution_backend="worker_claim",
    )
    scheduler._started = True
    scheduler._pipelines["worker_pipeline"] = Pipeline("worker_pipeline").add_collector("steam")
    task_id = await scheduler.submit(
        Task(
            id="retry-worker-task",
            name="Retry Worker Task",
            pipeline_name="worker_pipeline",
            collector_name="steam",
            max_retries=1,
        ),
        pipeline_name="worker_pipeline",
    )
    assert await scheduler.claim_task_for_worker("worker-1", capabilities=["steam"]) is not None

    checkpoint = await scheduler.register_worker_task_checkpoint(
        "worker-1",
        task_id,
        recovery_level="L1",
        cursor={"offset": 10},
        stats={"processed": 10},
    )
    retrying = await scheduler.fail_worker_task("worker-1", task_id, error="temporary timeout")
    reclaimed = await scheduler.claim_task_for_worker("worker-2", capabilities=["steam"])
    completed = await scheduler.complete_worker_task(
        "worker-2",
        task_id,
        result={"success": True, "storage_count": 1},
    )
    events = await event_service.list_events(task_id)

    assert retrying is not None
    assert retrying.retry_count == 1
    assert checkpoint is not None
    assert reclaimed is not None
    assert reclaimed["task"]["config"]["worker_claim"]["worker_id"] == "worker-2"
    assert reclaimed["latest_checkpoint"]["checkpoint_id"] == checkpoint.checkpoint_id
    assert reclaimed["latest_checkpoint"]["cursor"] == {"offset": 10}
    assert reclaimed["recovery"]["recommended_action"] == "review_checkpoint"
    assert completed is not None
    assert scheduler.get_task(task_id).status == TaskStatus.SUCCESS
    assert [event.type for event in events] == [
        "queued",
        "claimed",
        "checkpoint",
        "retry",
        "claimed",
        "complete",
    ]


@pytest.mark.asyncio
async def test_scheduler_worker_failure_redacts_reported_result() -> None:
    event_service = InMemoryTaskEventService()
    scheduler = Scheduler(
        max_concurrent=1,
        task_repo=InMemoryTaskRepository(),
        task_event_service=event_service,
        execution_backend="worker_claim",
    )
    scheduler._started = True
    scheduler._pipelines["worker_pipeline"] = Pipeline("worker_pipeline").add_collector("steam")
    task_id = await scheduler.submit(
        Task(
            id="failed-worker-redact",
            name="Failed Worker Redact",
            pipeline_name="worker_pipeline",
            collector_name="steam",
            max_retries=0,
        ),
        pipeline_name="worker_pipeline",
    )
    assert await scheduler.claim_task_for_worker("worker-1", capabilities=["steam"]) is not None

    failed = await scheduler.fail_worker_task(
        "worker-1",
        task_id,
        error="temporary token=error-secret",
        result={
            "success": False,
            "api_key": "worker-fail-result-secret",
            "errors": ["failed token=result-error-secret"],
        },
    )
    events = await event_service.list_events(task_id)
    rendered = json.dumps(
        {
            "task_result": scheduler.get_task(task_id).result,
            "task_error": scheduler.get_task(task_id).error,
            "events": [event.to_public_payload() for event in events],
        },
        ensure_ascii=False,
    )

    assert failed is scheduler.get_task(task_id)
    assert failed.status == TaskStatus.FAILED
    assert failed.result["api_key"] == "[REDACTED]"
    assert failed.result["errors"] == ["failed token=[REDACTED]"]
    assert failed.error == "temporary token=[REDACTED]"
    assert events[-1].type == "error"
    assert events[-1].payload["result"]["api_key"] == "[REDACTED]"
    assert "worker-fail-result-secret" not in rendered
    assert "result-error-secret" not in rendered
    assert "error-secret" not in rendered


@pytest.mark.asyncio
async def test_scheduler_interrupts_tasks_for_stale_worker() -> None:
    event_service = InMemoryTaskEventService()
    scheduler = Scheduler(
        max_concurrent=1,
        task_repo=InMemoryTaskRepository(),
        task_event_service=event_service,
        execution_backend="worker_claim",
    )
    scheduler._started = True
    scheduler._pipelines["worker_pipeline"] = Pipeline("worker_pipeline").add_collector("steam")
    task_id = await scheduler.submit(
        Task(
            id="stale-worker-task",
            name="Stale Worker Task",
            pipeline_name="worker_pipeline",
            collector_name="steam",
        ),
        pipeline_name="worker_pipeline",
    )
    assert await scheduler.claim_task_for_worker("stale-worker", capabilities=["steam"]) is not None

    interrupted = await scheduler.interrupt_worker_tasks(
        "stale-worker",
        reason="worker heartbeat stale token=secret",
    )
    events = await event_service.list_events(task_id)

    assert [task.id for task in interrupted] == [task_id]
    assert scheduler.get_task(task_id).status == TaskStatus.CANCELLED
    assert scheduler.get_task(task_id).error == "worker heartbeat stale token=[REDACTED]"
    assert events[-1].type == "interrupted"
    assert events[-1].payload["worker_id"] == "stale-worker"


def test_worker_api_register_heartbeat_and_list(monkeypatch) -> None:
    import src.web.app as app_module

    registry = InMemoryWorkerRegistry()
    monkeypatch.setattr(app_module, "get_worker_registry", lambda: registry)

    with TestClient(create_app()) as client:
        registered = client.post(
            "/api/workers/register",
            json={
                "worker_id": "api-worker",
                "hostname": "worker-host",
                "capabilities": ["steam"],
            },
        )
        heartbeat = client.post(
            "/api/workers/api-worker/heartbeat",
            json={
                "status": "busy",
                "current_task_ids": ["task-1"],
            },
        )
        listed = client.get("/api/workers")
        fetched = client.get("/api/workers/api-worker")

    assert registered.status_code == 200
    assert registered.json()["worker_id"] == "api-worker"
    assert heartbeat.status_code == 200
    assert heartbeat.json()["status"] == "busy"
    assert heartbeat.json()["current_task_ids"] == ["task-1"]
    assert listed.status_code == 200
    assert listed.json()[0]["worker_id"] == "api-worker"
    assert fetched.status_code == 200
    assert fetched.json()["capabilities"] == ["steam"]


def test_worker_api_validates_control_fields(monkeypatch) -> None:
    import src.web.app as app_module

    registry = InMemoryWorkerRegistry()
    monkeypatch.setattr(app_module, "get_worker_registry", lambda: registry)

    with TestClient(create_app()) as client:
        registered = client.post(
            "/api/workers/register",
            json={"worker_id": "schema-worker", "capabilities": ["steam"]},
        )
        normalized = client.post(
            "/api/workers/schema-worker/heartbeat",
            json={"status": "BUSY"},
        )
        invalid_status = client.post(
            "/api/workers/schema-worker/heartbeat",
            json={"status": "sleeping"},
        )
        invalid_event_level = client.post(
            "/api/workers/schema-worker/tasks/task-1/events",
            json={"type": "collect", "level": "panic"},
        )
        invalid_artifact_size = client.post(
            "/api/workers/schema-worker/tasks/task-1/artifacts",
            json={"type": "json", "name": "output.json", "size": -1},
        )
        invalid_checkpoint_level = client.post(
            "/api/workers/schema-worker/tasks/task-1/checkpoints",
            json={"recovery_level": "L9"},
        )
        invalid_fail_error = client.post(
            "/api/workers/schema-worker/tasks/task-1/fail",
            json={"error": ""},
        )

    assert registered.status_code == 200
    assert normalized.status_code == 200
    assert normalized.json()["status"] == "busy"
    assert invalid_status.status_code == 422
    assert invalid_event_level.status_code == 422
    assert invalid_artifact_size.status_code == 422
    assert invalid_checkpoint_level.status_code == 422
    assert invalid_fail_error.status_code == 422


def test_worker_api_reconcile_stale_tasks(monkeypatch) -> None:
    import src.web.app as app_module

    task = Task(
        id="interrupted-task",
        name="Interrupted Task",
        pipeline_name="worker_pipeline",
        collector_name="steam",
    )
    task.start()
    task.cancel()

    class FakeRegistry:
        async def list_workers(self, *, stale_after_seconds: int = 120):
            return [
                SimpleNamespace(worker_id="offline-worker", status="offline"),
                SimpleNamespace(worker_id="online-worker", status="online"),
            ]

    class FakeScheduler:
        def __init__(self) -> None:
            self.calls = []

        async def interrupt_worker_tasks(self, worker_id: str, *, reason: str = ""):
            self.calls.append((worker_id, reason))
            return [task] if worker_id == "offline-worker" else []

    fake_scheduler = FakeScheduler()
    monkeypatch.setattr(app_module, "get_worker_registry", lambda: FakeRegistry())

    with TestClient(create_app()) as client:
        original_scheduler = app_module.scheduler
        app_module.scheduler = fake_scheduler
        try:
            response = client.post("/api/workers/reconcile-stale-tasks?stale_after_seconds=7")
        finally:
            app_module.scheduler = original_scheduler

    assert response.status_code == 200
    payload = response.json()
    assert payload["offline_worker_ids"] == ["offline-worker"]
    assert payload["interrupted_tasks"][0]["id"] == "interrupted-task"
    assert fake_scheduler.calls == [
        ("offline-worker", "Worker offline-worker heartbeat exceeded 7s.")
    ]


def test_worker_api_claim_event_artifact_and_complete_flow() -> None:
    import src.web.app as app_module

    with TestClient(create_app()) as client:
        original_backend = app_module.scheduler._execution_backend
        app_module.scheduler._execution_backend = "worker_claim"
        try:
            pipeline_created = client.post(
                "/api/pipelines",
                json={
                    "name": "api_worker_pipeline",
                    "steps": [{"type": "collector", "name": "steam", "config": {}}],
                },
            )

            registered = client.post(
                "/api/workers/register",
                json={"worker_id": "api-claim-worker", "capabilities": ["steam"]},
            )
            created = client.post(
                "/api/tasks",
                json={
                    "name": "API Worker Task",
                    "pipeline_name": "api_worker_pipeline",
                    "collector_name": "steam",
                    "targets": [
                        {"name": "CS2", "target_type": "game", "params": {"app_id": "730"}}
                    ],
                    "config": {},
                },
            )
            claimed = client.post("/api/workers/api-claim-worker/claim-task", json={})
            task_id = claimed.json()["task_id"]
            client.post(
                "/api/workers/api-claim-worker/heartbeat",
                json={"status": "offline", "current_task_ids": []},
            )
            event = client.post(
                f"/api/workers/api-claim-worker/tasks/{task_id}/events",
                json={"type": "collect", "message": "worker event"},
            )
            listed_after_event = client.get("/api/workers/api-claim-worker")
            artifact = client.post(
                f"/api/workers/api-claim-worker/tasks/{task_id}/artifacts",
                json={"type": "json", "name": "output.json"},
            )
            checkpoint = client.post(
                f"/api/workers/api-claim-worker/tasks/{task_id}/checkpoints",
                json={
                    "recovery_level": "L1",
                    "cursor": {"offset": 1},
                    "stats": {"processed": 1},
                },
            )
            completed = client.post(
                f"/api/workers/api-claim-worker/tasks/{task_id}/complete",
                json={"result": {"success": True, "storage_count": 1}},
            )
            listed = client.get("/api/workers/api-claim-worker")
        finally:
            app_module.scheduler._execution_backend = original_backend

    assert registered.status_code == 200
    assert pipeline_created.status_code == 200
    assert created.status_code == 200
    assert claimed.status_code == 200
    assert claimed.json()["pipeline"]["name"] == "api_worker_pipeline"
    assert event.status_code == 200
    assert event.json()["event"]["type"] == "collect"
    assert listed_after_event.status_code == 200
    assert listed_after_event.json()["status"] == "busy"
    assert listed_after_event.json()["current_task_ids"] == [task_id]
    assert artifact.status_code == 200
    assert artifact.json()["artifact"]["type"] == "json"
    assert checkpoint.status_code == 200
    assert checkpoint.json()["checkpoint"]["recovery_level"] == "L1"
    assert completed.status_code == 200
    assert completed.json()["task"]["status"] == "success"
    assert listed.status_code == 200
    assert listed.json()["current_task_ids"] == []


def test_worker_api_missing_worker_returns_404(monkeypatch) -> None:
    import src.web.app as app_module

    registry = InMemoryWorkerRegistry()
    monkeypatch.setattr(app_module, "get_worker_registry", lambda: registry)

    with TestClient(create_app()) as client:
        heartbeat = client.post("/api/workers/missing/heartbeat", json={})
        fetched = client.get("/api/workers/missing")

    assert heartbeat.status_code == 404
    assert fetched.status_code == 404


def test_worker_task_scoped_api_requires_registered_worker(monkeypatch) -> None:
    import src.web.app as app_module

    registry = InMemoryWorkerRegistry()

    class FakeScheduler:
        def __init__(self) -> None:
            self.called = False

        async def append_worker_task_event(self, *args, **kwargs):
            self.called = True
            return SimpleNamespace(to_public_payload=lambda: {"type": "collect"})

    fake_scheduler = FakeScheduler()
    monkeypatch.setattr(app_module, "get_worker_registry", lambda: registry)

    with TestClient(create_app()) as client:
        original_scheduler = app_module.scheduler
        app_module.scheduler = fake_scheduler
        try:
            response = client.post(
                "/api/workers/missing-worker/tasks/claimed-task/events",
                json={"type": "collect", "message": "worker event"},
            )
        finally:
            app_module.scheduler = original_scheduler

    assert response.status_code == 404
    assert response.json()["detail"] == "Worker not found: missing-worker"
    assert fake_scheduler.called is False


def test_worker_api_is_admin_protected_for_non_local_requests(monkeypatch) -> None:
    import src.core.config as config

    original_get = config.get

    def fake_get(key, default=None):
        if key == "server.api_key":
            return ""
        return original_get(key, default)

    monkeypatch.setattr(config, "get", fake_get)

    with TestClient(create_app(), client=("203.0.113.10", 50000)) as client:
        response = client.get("/api/workers")

    assert response.status_code == 401
