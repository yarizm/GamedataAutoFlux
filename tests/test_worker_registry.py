import asyncio
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
from src.services.session_registry import InMemorySessionRegistry
from src.services.worker_registry import InMemoryWorkerRegistry, StorageWorkerRegistry
from src.storage.base import BaseStorage, QueryResult, StorageRecord
from src.web.app import create_app


class _WorkerRegistryStorage(BaseStorage):
    def __init__(self) -> None:
        super().__init__()
        self.records: dict[str, StorageRecord] = {}

    async def save(self, record: StorageRecord) -> None:
        self.records[record.key] = record

    async def load(self, key: str) -> StorageRecord | None:
        return self.records.get(key)

    async def query(self, query: str, limit: int = 10, **kwargs) -> QueryResult:
        prefix = query.removeprefix("key:") if query.startswith("key:") else query
        records = [record for key, record in self.records.items() if key.startswith(prefix)]
        return QueryResult(records=records[:limit], total=len(records), query=query)

    async def delete(self, key: str) -> bool:
        return self.records.pop(key, None) is not None


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
async def test_worker_registry_update_state_without_touching_heartbeat() -> None:
    registry = InMemoryWorkerRegistry()
    worker = await registry.register(worker_id="state-worker", capabilities=["steam"])
    original_heartbeat = worker.last_heartbeat_at

    updated = await registry.update_worker_state(
        "state-worker",
        status="offline",
        current_task_ids=[],
    )

    assert updated is not None
    assert updated.status == "offline"
    assert updated.current_task_ids == []
    assert updated.last_heartbeat_at == original_heartbeat


@pytest.mark.asyncio
async def test_storage_worker_registry_update_state_without_touching_heartbeat() -> None:
    registry = StorageWorkerRegistry(_WorkerRegistryStorage())
    worker = await registry.register(worker_id="storage-state-worker", capabilities=["steam"])
    original_heartbeat = worker.last_heartbeat_at

    updated = await registry.update_worker_state(
        "storage-state-worker",
        status="offline",
        current_task_ids=[],
    )
    loaded = await registry.get_worker("storage-state-worker")

    assert updated is not None
    assert updated.status == "offline"
    assert updated.current_task_ids == []
    assert updated.last_heartbeat_at == original_heartbeat
    assert loaded is not None
    assert loaded.status == "offline"
    assert loaded.current_task_ids == []


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
        state={"next_target_index": 1, "target_order": ["Retry Worker Task"]},
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
    assert claim["collector_metadata"]["collector_id"] == "steam"
    assert claim["session_diagnostics"]["session_mode"] == "api_only"
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
async def test_scheduler_worker_claim_requires_local_profile_session_capability() -> None:
    scheduler = Scheduler(
        max_concurrent=1,
        task_repo=InMemoryTaskRepository(),
        task_event_service=InMemoryTaskEventService(),
        execution_backend="worker_claim",
    )
    scheduler._started = True
    scheduler._pipelines["qimai_pipeline"] = Pipeline("qimai_pipeline").add_collector("qimai")
    await scheduler.submit(
        Task(id="qimai-claim", name="Qimai Claim", pipeline_name="qimai_pipeline"),
        pipeline_name="qimai_pipeline",
    )

    assert await scheduler.claim_task_for_worker("worker-1", capabilities=["qimai"]) is None
    assert (
        await scheduler.claim_task_for_worker(
            "worker-2",
            capabilities=["qimai", "session_mode:local_profile", "session:qimai_profile"],
        )
        is not None
    )


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
        state={"next_target_index": 1, "target_order": ["Retry Worker Task"]},
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
    assert reclaimed["latest_checkpoint"]["state"]["next_target_index"] == 1
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
async def test_local_profile_retry_claim_stays_on_same_worker() -> None:
    scheduler = Scheduler(
        max_concurrent=1,
        task_repo=InMemoryTaskRepository(),
        task_event_service=InMemoryTaskEventService(),
        execution_backend="worker_claim",
    )
    scheduler._started = True
    scheduler._pipelines["qimai_pipeline"] = Pipeline("qimai_pipeline").add_collector("qimai")
    task_id = await scheduler.submit(
        Task(
            id="retry-qimai-task",
            name="Retry Qimai Task",
            pipeline_name="qimai_pipeline",
            collector_name="qimai",
            max_retries=1,
        ),
        pipeline_name="qimai_pipeline",
    )
    qimai_caps = ["qimai", "session_mode:local_profile", "session:qimai_profile"]

    assert await scheduler.claim_task_for_worker("worker-1", capabilities=qimai_caps) is not None
    retrying = await scheduler.fail_worker_task("worker-1", task_id, error="temporary timeout")

    assert retrying is not None
    assert retrying.status == TaskStatus.RETRYING
    assert await scheduler.claim_task_for_worker("worker-2", capabilities=qimai_caps) is None
    assert await scheduler.claim_task_for_worker("worker-1", capabilities=qimai_caps) is not None


@pytest.mark.asyncio
async def test_managed_state_retry_claim_is_not_sticky(monkeypatch) -> None:
    monkeypatch.setattr(
        "src.core.collector_metadata.get_config",
        lambda key, default=None: "managed_state" if key == "qimai.session_mode" else default,
    )

    scheduler = Scheduler(
        max_concurrent=1,
        task_repo=InMemoryTaskRepository(),
        task_event_service=InMemoryTaskEventService(),
        execution_backend="worker_claim",
    )
    scheduler._started = True
    scheduler._pipelines["qimai_pipeline"] = Pipeline("qimai_pipeline").add_collector("qimai")
    task_id = await scheduler.submit(
        Task(
            id="retry-qimai-managed-task",
            name="Retry Qimai Managed Task",
            pipeline_name="qimai_pipeline",
            collector_name="qimai",
            max_retries=1,
        ),
        pipeline_name="qimai_pipeline",
    )
    qimai_caps = ["qimai", "session_mode:managed_state"]

    assert await scheduler.claim_task_for_worker("worker-1", capabilities=qimai_caps) is not None
    retrying = await scheduler.fail_worker_task("worker-1", task_id, error="temporary timeout")

    assert retrying is not None
    assert retrying.status == TaskStatus.RETRYING
    assert await scheduler.claim_task_for_worker("worker-2", capabilities=qimai_caps) is not None


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


@pytest.mark.asyncio
async def test_scheduler_reconcile_stale_worker_recovers_sticky_retry_task() -> None:
    event_service = InMemoryTaskEventService()
    scheduler = Scheduler(
        max_concurrent=1,
        task_repo=InMemoryTaskRepository(),
        task_event_service=event_service,
        execution_backend="worker_claim",
    )
    scheduler._started = True
    scheduler._pipelines["qimai_pipeline"] = Pipeline("qimai_pipeline").add_collector("qimai")
    task_id = await scheduler.submit(
        Task(
            id="stale-retry-task",
            name="Stale Retry Task",
            pipeline_name="qimai_pipeline",
            collector_name="qimai",
            max_retries=1,
        ),
        pipeline_name="qimai_pipeline",
    )
    qimai_caps = ["qimai", "session_mode:local_profile", "session:qimai_profile"]

    assert (
        await scheduler.claim_task_for_worker("stale-retry-worker", capabilities=qimai_caps)
        is not None
    )
    retrying = await scheduler.fail_worker_task(
        "stale-retry-worker",
        task_id,
        error="temporary timeout",
    )

    assert retrying is not None
    assert retrying.status == TaskStatus.RETRYING

    reconciled = await scheduler.reconcile_stale_worker_tasks(
        "stale-retry-worker",
        reason="worker stale secret=token",
    )
    events = await event_service.list_events(task_id)
    recovered = scheduler.get_task(task_id)

    assert reconciled["interrupted_tasks"] == []
    assert [task.id for task in reconciled["recovered_retry_tasks"]] == [task_id]
    assert recovered.status == TaskStatus.CANCELLED
    assert recovered.error == "worker stale secret=[REDACTED]"
    assert recovered.result["recovered_retry"] is True
    assert events[-1].type == "interrupted"
    assert events[-1].payload["recovered_retry"] is True
    assert events[-1].payload["previous_status"] == TaskStatus.RETRYING.value


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

        async def update_worker_state(
            self,
            worker_id: str,
            *,
            status: str | None = None,
            current_task_ids: list[str] | None = None,
            metadata: dict | None = None,
            touch_heartbeat: bool = False,
        ):
            return SimpleNamespace(
                worker_id=worker_id,
                status=status or "offline",
                current_task_ids=current_task_ids or [],
                metadata=metadata or {},
            )

    class FakeScheduler:
        def __init__(self) -> None:
            self.calls = []

        async def reconcile_stale_worker_tasks(self, worker_id: str, *, reason: str = ""):
            self.calls.append((worker_id, reason))
            return {
                "interrupted_tasks": [task] if worker_id == "offline-worker" else [],
                "recovered_retry_tasks": [],
            }

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
    assert payload["updated_worker_ids"] == ["offline-worker"]
    assert payload["interrupted_tasks"][0]["id"] == "interrupted-task"
    assert payload["recovered_retry_tasks"] == []
    assert fake_scheduler.calls == [
        ("offline-worker", "Worker offline-worker heartbeat exceeded 7s.")
    ]


def test_worker_api_reconcile_clears_stale_worker_task_ids_without_scheduler_tasks(
    monkeypatch,
) -> None:
    import src.web.app as app_module

    registry = InMemoryWorkerRegistry()
    monkeypatch.setattr(app_module, "get_worker_registry", lambda: registry)

    with TestClient(create_app()) as client:
        original_scheduler = app_module.scheduler
        client.post(
            "/api/workers/register",
            json={
                "worker_id": "stale-empty-worker",
                "capabilities": ["steam"],
                "current_task_ids": ["ghost-task-1", "ghost-task-2"],
            },
        )
        client.post(
            "/api/workers/stale-empty-worker/heartbeat",
            json={"status": "busy", "current_task_ids": ["ghost-task-1", "ghost-task-2"]},
        )
        worker = asyncio.run(registry.get_worker("stale-empty-worker"))
        registry._workers["stale-empty-worker"] = worker.model_copy(
            update={"last_heartbeat_at": datetime.now(timezone.utc) - timedelta(seconds=30)}
        )

        class FakeScheduler:
            async def reconcile_stale_worker_tasks(self, worker_id: str, *, reason: str = ""):
                return {
                    "interrupted_tasks": [],
                    "recovered_retry_tasks": [],
                }

        app_module.scheduler = FakeScheduler()
        try:
            reconciled = client.post("/api/workers/reconcile-stale-tasks?stale_after_seconds=1")
            worker_after = client.get("/api/workers/stale-empty-worker")
        finally:
            app_module.scheduler = original_scheduler

    assert reconciled.status_code == 200
    payload = reconciled.json()
    assert payload["offline_worker_ids"] == ["stale-empty-worker"]
    assert payload["updated_worker_ids"] == ["stale-empty-worker"]
    assert payload["interrupted_tasks"] == []
    assert payload["recovered_retry_tasks"] == []
    assert worker_after.status_code == 200
    assert worker_after.json()["status"] == "offline"
    assert worker_after.json()["current_task_ids"] == []


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
    assert claimed.json()["collector_metadata"]["collector_id"] == "steam"
    assert claimed.json()["session_diagnostics"]["session_state"]["health"] == "ready"
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


def test_worker_api_claim_succeeds_for_non_session_task_when_session_registry_lookup_fails(
    monkeypatch,
) -> None:
    import src.web.app as app_module

    def broken_registry_provider():
        raise RuntimeError("registry lookup failed token=claim-non-session-lookup-secret")

    monkeypatch.setattr(app_module, "get_session_registry", broken_registry_provider)

    with TestClient(create_app()) as client:
        original_backend = app_module.scheduler._execution_backend
        app_module.scheduler._execution_backend = "worker_claim"
        try:
            pipeline_created = client.post(
                "/api/pipelines",
                json={
                    "name": "api_worker_lookup_fail_pipeline",
                    "steps": [{"type": "collector", "name": "steam", "config": {}}],
                },
            )
            registered = client.post(
                "/api/workers/register",
                json={"worker_id": "api-claim-lookup-fail-worker", "capabilities": ["steam"]},
            )
            created = client.post(
                "/api/tasks",
                json={
                    "name": "API Worker Lookup Fail Task",
                    "pipeline_name": "api_worker_lookup_fail_pipeline",
                    "collector_name": "steam",
                    "targets": [
                        {"name": "CS2", "target_type": "game", "params": {"app_id": "730"}}
                    ],
                    "config": {},
                },
            )
            claimed = client.post("/api/workers/api-claim-lookup-fail-worker/claim-task", json={})
            task_response = client.get(f"/api/tasks/{created.json()['id']}")
        finally:
            app_module.scheduler._execution_backend = original_backend

    assert pipeline_created.status_code == 200
    assert registered.status_code == 200
    assert created.status_code == 200
    assert claimed.status_code == 200
    assert claimed.json()["claim_status"] == "claimed"
    assert claimed.json()["task_id"] == created.json()["id"]
    assert task_response.status_code == 200
    assert task_response.json()["status"] == "running"


def test_worker_api_claim_blocks_session_task_when_session_registry_lookup_fails(
    monkeypatch, tmp_path
) -> None:
    import src.web.app as app_module

    profile_dir = tmp_path / "qimai_profile"
    profile_dir.mkdir()

    values = {
        "qimai.user_data_dir": str(profile_dir),
        "qimai.cdp_enabled": False,
    }

    def fake_get_config(key: str, default=None):
        return values.get(key, default)

    def broken_registry_provider():
        raise RuntimeError("registry lookup failed token=claim-session-lookup-secret")

    monkeypatch.setattr("src.core.diagnostics.get_config", fake_get_config)
    monkeypatch.setattr("src.core.collector_metadata.get_config", fake_get_config)
    monkeypatch.setattr("src.core.session_runtime.get_config", fake_get_config)
    monkeypatch.setattr(app_module, "get_session_registry", broken_registry_provider)

    with TestClient(create_app()) as client:
        original_backend = app_module.scheduler._execution_backend
        app_module.scheduler._execution_backend = "worker_claim"
        try:
            pipeline_created = client.post(
                "/api/pipelines",
                json={
                    "name": "api_worker_session_lookup_fail_pipeline",
                    "steps": [{"type": "collector", "name": "qimai", "config": {}}],
                },
            )
            registered = client.post(
                "/api/workers/register",
                json={
                    "worker_id": "api-claim-session-lookup-fail-worker",
                    "capabilities": [
                        "qimai",
                        "session_mode:local_profile",
                        "session:qimai_profile",
                    ],
                },
            )
            created = client.post(
                "/api/tasks",
                json={
                    "name": "API Worker Session Lookup Fail Task",
                    "pipeline_name": "api_worker_session_lookup_fail_pipeline",
                    "collector_name": "qimai",
                    "targets": [
                        {
                            "name": "Example App",
                            "target_type": "app",
                            "params": {"app_id": "123456"},
                        }
                    ],
                    "config": {},
                },
            )
            claimed = client.post(
                "/api/workers/api-claim-session-lookup-fail-worker/claim-task",
                json={},
            )
            task_response = client.get(f"/api/tasks/{created.json()['id']}")
        finally:
            app_module.scheduler._execution_backend = original_backend

    assert pipeline_created.status_code == 200
    assert registered.status_code == 200
    assert created.status_code == 200
    assert claimed.status_code == 200
    payload = claimed.json()
    assert payload["task_id"] is None
    assert payload["claim_status"] == "blocked"
    assert payload["claim_reason"] == "session_registry_unavailable"
    assert payload["blocked_sessions"][0]["collector_id"] == "qimai"
    assert payload["blocked_sessions"][0]["reason"] == "session_registry_unavailable"
    assert task_response.status_code == 200
    assert task_response.json()["status"] == "pending"


def test_worker_api_complete_preserves_draining_status(monkeypatch) -> None:
    import src.web.app as app_module

    with TestClient(create_app()) as client:
        original_backend = app_module.scheduler._execution_backend
        app_module.scheduler._execution_backend = "worker_claim"
        try:
            client.post(
                "/api/pipelines",
                json={
                    "name": "draining_complete_pipeline",
                    "steps": [{"type": "collector", "name": "steam", "config": {}}],
                },
            )
            client.post(
                "/api/workers/register",
                json={"worker_id": "draining-complete-worker", "capabilities": ["steam"]},
            )
            client.post(
                "/api/tasks",
                json={
                    "name": "Draining Complete Task",
                    "pipeline_name": "draining_complete_pipeline",
                    "collector_name": "steam",
                    "targets": [
                        {"name": "CS2", "target_type": "game", "params": {"app_id": "730"}}
                    ],
                    "config": {},
                },
            )
            claimed = client.post("/api/workers/draining-complete-worker/claim-task", json={})
            task_id = claimed.json()["task_id"]
            client.post(
                "/api/workers/draining-complete-worker/heartbeat",
                json={"status": "draining", "current_task_ids": [task_id]},
            )
            completed = client.post(
                f"/api/workers/draining-complete-worker/tasks/{task_id}/complete",
                json={"result": {"success": True, "storage_count": 1}},
            )
            listed = client.get("/api/workers/draining-complete-worker")
        finally:
            app_module.scheduler._execution_backend = original_backend

    assert claimed.status_code == 200
    assert completed.status_code == 200
    assert completed.json()["task"]["status"] == "success"
    assert listed.status_code == 200
    assert listed.json()["status"] == "draining"
    assert listed.json()["current_task_ids"] == []


def test_worker_task_activity_preserves_draining_status() -> None:
    import src.web.app as app_module

    with TestClient(create_app()) as client:
        original_backend = app_module.scheduler._execution_backend
        app_module.scheduler._execution_backend = "worker_claim"
        try:
            client.post(
                "/api/pipelines",
                json={
                    "name": "draining_activity_pipeline",
                    "steps": [{"type": "collector", "name": "steam", "config": {}}],
                },
            )
            client.post(
                "/api/workers/register",
                json={"worker_id": "draining-activity-worker", "capabilities": ["steam"]},
            )
            client.post(
                "/api/tasks",
                json={
                    "name": "Draining Activity Task",
                    "pipeline_name": "draining_activity_pipeline",
                    "collector_name": "steam",
                    "targets": [
                        {"name": "CS2", "target_type": "game", "params": {"app_id": "730"}}
                    ],
                    "config": {},
                },
            )
            claimed = client.post("/api/workers/draining-activity-worker/claim-task", json={})
            task_id = claimed.json()["task_id"]
            client.post(
                "/api/workers/draining-activity-worker/heartbeat",
                json={"status": "draining", "current_task_ids": [task_id]},
            )
            event = client.post(
                f"/api/workers/draining-activity-worker/tasks/{task_id}/events",
                json={"type": "collect", "message": "worker event"},
            )
            listed = client.get("/api/workers/draining-activity-worker")
        finally:
            app_module.scheduler._execution_backend = original_backend

    assert claimed.status_code == 200
    assert event.status_code == 200
    assert listed.status_code == 200
    assert listed.json()["status"] == "draining"
    assert listed.json()["current_task_ids"] == [task_id]


def test_worker_task_updates_succeed_when_activity_heartbeat_fails(monkeypatch) -> None:
    import src.web.app as app_module

    class FailingHeartbeatRegistry:
        def __init__(self) -> None:
            self._delegate = InMemoryWorkerRegistry()

        def __getattr__(self, name: str):
            return getattr(self._delegate, name)

        async def heartbeat(self, *args, **kwargs):
            raise RuntimeError("heartbeat failed token=task-activity-secret")

    registry = FailingHeartbeatRegistry()
    monkeypatch.setattr(app_module, "get_worker_registry", lambda: registry)

    with TestClient(create_app()) as client:
        original_backend = app_module.scheduler._execution_backend
        app_module.scheduler._execution_backend = "worker_claim"
        try:
            client.post(
                "/api/pipelines",
                json={
                    "name": "activity_heartbeat_fail_pipeline",
                    "steps": [{"type": "collector", "name": "steam", "config": {}}],
                },
            )
            client.post(
                "/api/workers/register",
                json={"worker_id": "activity-fail-worker", "capabilities": ["steam"]},
            )
            created = client.post(
                "/api/tasks",
                json={
                    "name": "Activity Failure Task",
                    "pipeline_name": "activity_heartbeat_fail_pipeline",
                    "collector_name": "steam",
                    "targets": [
                        {"name": "CS2", "target_type": "game", "params": {"app_id": "730"}}
                    ],
                    "config": {},
                },
            )
            claimed = client.post("/api/workers/activity-fail-worker/claim-task", json={})
            task_id = claimed.json()["task_id"]
            event = client.post(
                f"/api/workers/activity-fail-worker/tasks/{task_id}/events",
                json={"type": "collect", "message": "worker event"},
            )
            artifact = client.post(
                f"/api/workers/activity-fail-worker/tasks/{task_id}/artifacts",
                json={"type": "json", "name": "output.json"},
            )
            checkpoint = client.post(
                f"/api/workers/activity-fail-worker/tasks/{task_id}/checkpoints",
                json={
                    "recovery_level": "L1",
                    "cursor": {"offset": 1},
                    "stats": {"processed": 1},
                },
            )
            events_response = client.get(f"/api/tasks/{task_id}/events")
            artifacts_response = client.get(f"/api/tasks/{task_id}/artifacts")
            checkpoints_response = client.get(f"/api/tasks/{task_id}/checkpoints")
        finally:
            app_module.scheduler._execution_backend = original_backend

    assert created.status_code == 200
    assert claimed.status_code == 200
    assert event.status_code == 200
    assert artifact.status_code == 200
    assert checkpoint.status_code == 200
    assert events_response.status_code == 200
    assert artifacts_response.status_code == 200
    assert checkpoints_response.status_code == 200
    event_types = [item["type"] for item in events_response.json()["events"]]
    assert "collect" in event_types
    assert "artifact" in event_types
    assert "checkpoint" in event_types
    assert artifacts_response.json()["artifacts"][0]["name"] == "output.json"
    assert checkpoints_response.json()["latest"]["recovery_level"] == "L1"


def test_worker_api_claim_succeeds_when_session_bind_fails(monkeypatch) -> None:
    import src.web.app as app_module

    class BindFailsRegistry(InMemorySessionRegistry):
        async def bind_session(self, diagnostics: dict, **kwargs):
            raise RuntimeError("bind failed token=claim-bind-secret")

    registry = BindFailsRegistry()
    monkeypatch.setattr(app_module, "get_session_registry", lambda: registry)

    with TestClient(create_app()) as client:
        original_backend = app_module.scheduler._execution_backend
        app_module.scheduler._execution_backend = "worker_claim"
        try:
            client.post(
                "/api/pipelines",
                json={
                    "name": "bind_fail_pipeline",
                    "steps": [{"type": "collector", "name": "steam", "config": {}}],
                },
            )
            client.post(
                "/api/workers/register",
                json={"worker_id": "bind-fail-worker", "capabilities": ["steam"]},
            )
            created = client.post(
                "/api/tasks",
                json={
                    "name": "Bind Failure Task",
                    "pipeline_name": "bind_fail_pipeline",
                    "collector_name": "steam",
                    "targets": [
                        {"name": "CS2", "target_type": "game", "params": {"app_id": "730"}}
                    ],
                    "config": {},
                },
            )
            claimed = client.post("/api/workers/bind-fail-worker/claim-task", json={})
            task_response = client.get(f"/api/tasks/{created.json()['id']}")
        finally:
            app_module.scheduler._execution_backend = original_backend

    assert created.status_code == 200
    assert claimed.status_code == 200
    assert claimed.json()["claim_status"] == "claimed"
    assert claimed.json()["task_id"]
    assert task_response.status_code == 200
    assert task_response.json()["status"] == "running"


def test_worker_api_complete_succeeds_when_session_release_fails(monkeypatch) -> None:
    import src.web.app as app_module

    class ReleaseFailsRegistry(InMemorySessionRegistry):
        async def release_session_by_id(self, session_id: str, **kwargs):
            return None

        async def release_session(self, diagnostics: dict, **kwargs):
            raise RuntimeError("release failed token=complete-release-secret")

    registry = ReleaseFailsRegistry()
    monkeypatch.setattr(app_module, "get_session_registry", lambda: registry)

    with TestClient(create_app()) as client:
        original_backend = app_module.scheduler._execution_backend
        app_module.scheduler._execution_backend = "worker_claim"
        try:
            client.post(
                "/api/pipelines",
                json={
                    "name": "release_fail_complete_pipeline",
                    "steps": [{"type": "collector", "name": "steam", "config": {}}],
                },
            )
            client.post(
                "/api/workers/register",
                json={"worker_id": "release-fail-complete-worker", "capabilities": ["steam"]},
            )
            created = client.post(
                "/api/tasks",
                json={
                    "name": "Release Failure Complete Task",
                    "pipeline_name": "release_fail_complete_pipeline",
                    "collector_name": "steam",
                    "targets": [
                        {"name": "CS2", "target_type": "game", "params": {"app_id": "730"}}
                    ],
                    "config": {},
                },
            )
            claimed = client.post("/api/workers/release-fail-complete-worker/claim-task", json={})
            task_id = claimed.json()["task_id"]
            completed = client.post(
                f"/api/workers/release-fail-complete-worker/tasks/{task_id}/complete",
                json={"result": {"success": True, "storage_count": 1}},
            )
            task_response = client.get(f"/api/tasks/{created.json()['id']}")
        finally:
            app_module.scheduler._execution_backend = original_backend

    assert claimed.status_code == 200
    assert completed.status_code == 200
    assert completed.json()["task"]["status"] == "success"
    assert task_response.status_code == 200
    assert task_response.json()["status"] == "success"


def test_worker_api_fail_succeeds_when_session_release_fails(monkeypatch) -> None:
    import src.web.app as app_module

    class ReleaseFailsRegistry(InMemorySessionRegistry):
        async def release_session_by_id(self, session_id: str, **kwargs):
            return None

        async def release_session(self, diagnostics: dict, **kwargs):
            raise RuntimeError("release failed token=fail-release-secret")

    registry = ReleaseFailsRegistry()
    monkeypatch.setattr(app_module, "get_session_registry", lambda: registry)

    with TestClient(create_app()) as client:
        original_backend = app_module.scheduler._execution_backend
        app_module.scheduler._execution_backend = "worker_claim"
        try:
            client.post(
                "/api/pipelines",
                json={
                    "name": "release_fail_fail_pipeline",
                    "steps": [{"type": "collector", "name": "steam", "config": {}}],
                },
            )
            client.post(
                "/api/workers/register",
                json={"worker_id": "release-fail-fail-worker", "capabilities": ["steam"]},
            )
            created = client.post(
                "/api/tasks",
                json={
                    "name": "Release Failure Fail Task",
                    "pipeline_name": "release_fail_fail_pipeline",
                    "collector_name": "steam",
                    "targets": [
                        {"name": "CS2", "target_type": "game", "params": {"app_id": "730"}}
                    ],
                    "config": {},
                },
            )
            task = app_module.scheduler.get_task(created.json()["id"])
            task.max_retries = 0
            claimed = client.post("/api/workers/release-fail-fail-worker/claim-task", json={})
            task_id = claimed.json()["task_id"]
            failed = client.post(
                f"/api/workers/release-fail-fail-worker/tasks/{task_id}/fail",
                json={"error": "hard failure", "result": {"success": False}},
            )
            task_response = client.get(f"/api/tasks/{created.json()['id']}")
        finally:
            app_module.scheduler._execution_backend = original_backend

    assert claimed.status_code == 200
    assert failed.status_code == 200
    assert failed.json()["task"]["status"] == "failed"
    assert task_response.status_code == 200
    assert task_response.json()["status"] == "failed"


def test_worker_api_missing_worker_returns_404(monkeypatch) -> None:
    import src.web.app as app_module

    registry = InMemoryWorkerRegistry()
    monkeypatch.setattr(app_module, "get_worker_registry", lambda: registry)

    with TestClient(create_app()) as client:
        heartbeat = client.post("/api/workers/missing/heartbeat", json={})
        fetched = client.get("/api/workers/missing")

    assert heartbeat.status_code == 404
    assert fetched.status_code == 404


def test_worker_api_claim_exposes_session_runtime_for_local_profile_collector(
    monkeypatch, tmp_path
) -> None:
    import src.web.app as app_module

    registry = InMemorySessionRegistry()
    monkeypatch.setattr(app_module, "get_session_registry", lambda: registry)

    profile_dir = tmp_path / "qimai_profile"
    profile_dir.mkdir()
    values = {
        "qimai.user_data_dir": str(profile_dir),
        "qimai.cdp_enabled": False,
    }

    def fake_get_config(key: str, default=None):
        return values.get(key, default)

    monkeypatch.setattr("src.core.diagnostics.get_config", fake_get_config)
    monkeypatch.setattr("src.core.collector_metadata.get_config", fake_get_config)
    monkeypatch.setattr("src.core.session_runtime.get_config", fake_get_config)

    with TestClient(create_app()) as client:
        original_backend = app_module.scheduler._execution_backend
        app_module.scheduler._execution_backend = "worker_claim"
        try:
            pipeline_created = client.post(
                "/api/pipelines",
                json={
                    "name": "api_qimai_pipeline",
                    "steps": [{"type": "collector", "name": "qimai", "config": {}}],
                },
            )
            registered = client.post(
                "/api/workers/register",
                json={
                    "worker_id": "api-qimai-worker",
                    "capabilities": [
                        "qimai",
                        "session_mode:local_profile",
                        "session:qimai_profile",
                    ],
                },
            )
            created = client.post(
                "/api/tasks",
                json={
                    "name": "API Qimai Task",
                    "pipeline_name": "api_qimai_pipeline",
                    "collector_name": "qimai",
                    "targets": [
                        {
                            "name": "Example App",
                            "target_type": "app",
                            "params": {"app_id": "123456"},
                        }
                    ],
                    "config": {},
                },
            )
            claimed = client.post("/api/workers/api-qimai-worker/claim-task", json={})
            inventory_response = client.get("/api/diagnostics/sessions-inventory?collectors=qimai")
        finally:
            app_module.scheduler._execution_backend = original_backend

    assert pipeline_created.status_code == 200
    assert registered.status_code == 200
    assert created.status_code == 200
    assert claimed.status_code == 200
    assert inventory_response.status_code == 200
    payload = claimed.json()
    inventory = inventory_response.json()
    assert payload["collector_metadata"]["session_mode"] == "local_profile"
    assert payload["session_diagnostics"]["worker_binding"] == "sticky"
    assert payload["session_diagnostics"]["session_account"]["account_kind"] == "local_profile"
    assert payload["session_diagnostics"]["session_lease"]["strategy"] == "sticky_worker"
    assert inventory["count"] == 1
    assert inventory["items"][0]["collector_id"] == "qimai"
    assert inventory["items"][0]["worker_binding"] == "sticky"
    assert inventory["items"][0]["lease_status"] == "claimed"
    assert inventory["items"][0]["lease_worker_id"] == "api-qimai-worker"


def test_worker_api_claim_skips_session_claimed_by_other_worker(monkeypatch, tmp_path) -> None:
    import src.web.app as app_module

    registry = InMemorySessionRegistry()
    monkeypatch.setattr(app_module, "get_session_registry", lambda: registry)

    profile_dir = tmp_path / "qimai_profile"
    profile_dir.mkdir()
    values = {
        "qimai.user_data_dir": str(profile_dir),
        "qimai.cdp_enabled": False,
    }

    def fake_get_config(key: str, default=None):
        return values.get(key, default)

    monkeypatch.setattr("src.core.diagnostics.get_config", fake_get_config)
    monkeypatch.setattr("src.core.collector_metadata.get_config", fake_get_config)
    monkeypatch.setattr("src.core.session_runtime.get_config", fake_get_config)

    with TestClient(create_app()) as client:
        original_backend = app_module.scheduler._execution_backend
        app_module.scheduler._execution_backend = "worker_claim"
        try:
            client.post(
                "/api/pipelines",
                json={
                    "name": "lease_guard_pipeline",
                    "steps": [{"type": "collector", "name": "qimai", "config": {}}],
                },
            )
            client.post(
                "/api/workers/register",
                json={
                    "worker_id": "lease-owner-worker",
                    "capabilities": [
                        "qimai",
                        "session_mode:local_profile",
                        "session:qimai_profile",
                    ],
                },
            )
            client.post(
                "/api/workers/register",
                json={
                    "worker_id": "lease-other-worker",
                    "capabilities": [
                        "qimai",
                        "session_mode:local_profile",
                        "session:qimai_profile",
                    ],
                },
            )
            first_created = client.post(
                "/api/tasks",
                json={
                    "name": "Lease Guard Owner Task",
                    "pipeline_name": "lease_guard_pipeline",
                    "collector_name": "qimai",
                    "targets": [
                        {
                            "name": "Example App",
                            "target_type": "app",
                            "params": {"app_id": "123456"},
                        }
                    ],
                    "config": {},
                },
            )
            first_claimed = client.post("/api/workers/lease-owner-worker/claim-task", json={})
            second_created = client.post(
                "/api/tasks",
                json={
                    "name": "Lease Guard Other Task",
                    "pipeline_name": "lease_guard_pipeline",
                    "collector_name": "qimai",
                    "targets": [
                        {
                            "name": "Example App 2",
                            "target_type": "app",
                            "params": {"app_id": "654321"},
                        }
                    ],
                    "config": {},
                },
            )
            second_claimed = client.post("/api/workers/lease-other-worker/claim-task", json={})
            second_task = client.get(f"/api/tasks/{second_created.json()['id']}")
            inventory_response = client.get("/api/diagnostics/sessions-inventory?collectors=qimai")
        finally:
            app_module.scheduler._execution_backend = original_backend

    assert first_created.status_code == 200
    assert first_claimed.status_code == 200
    assert second_created.status_code == 200
    assert second_claimed.status_code == 200
    assert second_claimed.json()["task_id"] is None
    assert second_claimed.json()["claim_status"] == "blocked"
    assert second_claimed.json()["claim_reason"] == "session_claimed"
    assert second_claimed.json()["blocked_sessions"][0]["collector_id"] == "qimai"
    assert second_claimed.json()["blocked_sessions"][0]["lease_worker_id"] == "lease-owner-worker"
    assert (
        second_claimed.json()["blocked_sessions"][0]["lease_task_id"]
        == first_claimed.json()["task_id"]
    )
    assert second_task.status_code == 200
    assert second_task.json()["status"] == "pending"
    inventory = inventory_response.json()
    assert inventory["summary"]["claimed"] == 1
    assert inventory["items"][0]["lease_worker_id"] == "lease-owner-worker"
    assert inventory["items"][0]["lease_task_id"] == first_claimed.json()["task_id"]


def test_worker_api_claim_stays_blocked_when_session_inventory_refresh_fails(
    monkeypatch, tmp_path
) -> None:
    import src.web.app as app_module

    class SyncFailsRegistry(InMemorySessionRegistry):
        def __init__(self) -> None:
            super().__init__()
            self.fail_sync = False

        async def sync_from_diagnostics(self, diagnostics: dict):
            if self.fail_sync:
                raise RuntimeError("sync failed token=blocked-sync-secret")
            return await super().sync_from_diagnostics(diagnostics)

    registry = SyncFailsRegistry()
    monkeypatch.setattr(app_module, "get_session_registry", lambda: registry)

    profile_dir = tmp_path / "qimai_profile"
    profile_dir.mkdir()
    values = {
        "qimai.user_data_dir": str(profile_dir),
        "qimai.cdp_enabled": False,
    }

    def fake_get_config(key: str, default=None):
        return values.get(key, default)

    monkeypatch.setattr("src.core.diagnostics.get_config", fake_get_config)
    monkeypatch.setattr("src.core.collector_metadata.get_config", fake_get_config)
    monkeypatch.setattr("src.core.session_runtime.get_config", fake_get_config)

    with TestClient(create_app()) as client:
        original_backend = app_module.scheduler._execution_backend
        app_module.scheduler._execution_backend = "worker_claim"
        try:
            client.post(
                "/api/pipelines",
                json={
                    "name": "lease_guard_sync_fail_pipeline",
                    "steps": [{"type": "collector", "name": "qimai", "config": {}}],
                },
            )
            client.post(
                "/api/workers/register",
                json={
                    "worker_id": "lease-sync-owner-worker",
                    "capabilities": [
                        "qimai",
                        "session_mode:local_profile",
                        "session:qimai_profile",
                    ],
                },
            )
            client.post(
                "/api/workers/register",
                json={
                    "worker_id": "lease-sync-other-worker",
                    "capabilities": [
                        "qimai",
                        "session_mode:local_profile",
                        "session:qimai_profile",
                    ],
                },
            )
            client.post(
                "/api/tasks",
                json={
                    "name": "Lease Sync Owner Task",
                    "pipeline_name": "lease_guard_sync_fail_pipeline",
                    "collector_name": "qimai",
                    "targets": [
                        {
                            "name": "Example App",
                            "target_type": "app",
                            "params": {"app_id": "123456"},
                        }
                    ],
                    "config": {},
                },
            )
            first_claimed = client.post("/api/workers/lease-sync-owner-worker/claim-task", json={})
            second_created = client.post(
                "/api/tasks",
                json={
                    "name": "Lease Sync Other Task",
                    "pipeline_name": "lease_guard_sync_fail_pipeline",
                    "collector_name": "qimai",
                    "targets": [
                        {
                            "name": "Example App 2",
                            "target_type": "app",
                            "params": {"app_id": "654321"},
                        }
                    ],
                    "config": {},
                },
            )
            registry.fail_sync = True
            second_claimed = client.post("/api/workers/lease-sync-other-worker/claim-task", json={})
            second_task = client.get(f"/api/tasks/{second_created.json()['id']}")
        finally:
            app_module.scheduler._execution_backend = original_backend

    assert first_claimed.status_code == 200
    assert second_created.status_code == 200
    assert second_claimed.status_code == 200
    assert second_claimed.json()["task_id"] is None
    assert second_claimed.json()["claim_status"] == "blocked"
    assert second_claimed.json()["claim_reason"] == "session_claimed"
    assert (
        second_claimed.json()["blocked_sessions"][0]["lease_worker_id"] == "lease-sync-owner-worker"
    )
    assert (
        second_claimed.json()["blocked_sessions"][0]["lease_task_id"]
        == first_claimed.json()["task_id"]
    )
    assert second_task.status_code == 200
    assert second_task.json()["status"] == "pending"


def test_worker_api_claim_allows_same_worker_to_reuse_claimed_session(
    monkeypatch, tmp_path
) -> None:
    import src.web.app as app_module

    registry = InMemorySessionRegistry()
    monkeypatch.setattr(app_module, "get_session_registry", lambda: registry)

    profile_dir = tmp_path / "qimai_profile"
    profile_dir.mkdir()
    values = {
        "qimai.user_data_dir": str(profile_dir),
        "qimai.cdp_enabled": False,
    }

    def fake_get_config(key: str, default=None):
        return values.get(key, default)

    monkeypatch.setattr("src.core.diagnostics.get_config", fake_get_config)
    monkeypatch.setattr("src.core.collector_metadata.get_config", fake_get_config)
    monkeypatch.setattr("src.core.session_runtime.get_config", fake_get_config)

    with TestClient(create_app()) as client:
        original_backend = app_module.scheduler._execution_backend
        app_module.scheduler._execution_backend = "worker_claim"
        try:
            client.post(
                "/api/pipelines",
                json={
                    "name": "lease_guard_same_worker_pipeline",
                    "steps": [{"type": "collector", "name": "qimai", "config": {}}],
                },
            )
            client.post(
                "/api/workers/register",
                json={
                    "worker_id": "lease-reuse-worker",
                    "capabilities": [
                        "qimai",
                        "session_mode:local_profile",
                        "session:qimai_profile",
                    ],
                },
            )
            client.post(
                "/api/tasks",
                json={
                    "name": "Lease Reuse Task 1",
                    "pipeline_name": "lease_guard_same_worker_pipeline",
                    "collector_name": "qimai",
                    "targets": [
                        {
                            "name": "Example App",
                            "target_type": "app",
                            "params": {"app_id": "123456"},
                        }
                    ],
                    "config": {},
                },
            )
            client.post(
                "/api/tasks",
                json={
                    "name": "Lease Reuse Task 2",
                    "pipeline_name": "lease_guard_same_worker_pipeline",
                    "collector_name": "qimai",
                    "targets": [
                        {
                            "name": "Example App 2",
                            "target_type": "app",
                            "params": {"app_id": "654321"},
                        }
                    ],
                    "config": {},
                },
            )
            first_claimed = client.post("/api/workers/lease-reuse-worker/claim-task", json={})
            second_claimed = client.post("/api/workers/lease-reuse-worker/claim-task", json={})
            first_completed = client.post(
                f"/api/workers/lease-reuse-worker/tasks/{first_claimed.json()['task_id']}/complete",
                json={"result": {"success": True, "storage_count": 1}},
            )
            inventory_response = client.get("/api/diagnostics/sessions-inventory?collectors=qimai")
        finally:
            app_module.scheduler._execution_backend = original_backend

    assert first_claimed.status_code == 200
    assert second_claimed.status_code == 200
    assert first_completed.status_code == 200
    assert first_claimed.json()["task_id"]
    assert second_claimed.json()["task_id"]
    assert first_claimed.json()["claim_status"] == "claimed"
    assert second_claimed.json()["claim_status"] == "claimed"
    assert second_claimed.json()["task_id"] != first_claimed.json()["task_id"]
    inventory = inventory_response.json()
    assert inventory["summary"]["claimed"] == 1
    assert inventory["items"][0]["lease_status"] == "claimed"
    assert inventory["items"][0]["lease_worker_id"] == "lease-reuse-worker"
    assert inventory["items"][0]["lease_task_id"] == second_claimed.json()["task_id"]


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


def test_worker_api_complete_releases_session_lease(monkeypatch, tmp_path) -> None:
    import src.web.app as app_module

    registry = InMemorySessionRegistry()
    monkeypatch.setattr(app_module, "get_session_registry", lambda: registry)

    profile_dir = tmp_path / "qimai_profile"
    profile_dir.mkdir()
    values = {
        "qimai.user_data_dir": str(profile_dir),
        "qimai.cdp_enabled": False,
    }

    def fake_get_config(key: str, default=None):
        return values.get(key, default)

    monkeypatch.setattr("src.core.diagnostics.get_config", fake_get_config)
    monkeypatch.setattr("src.core.collector_metadata.get_config", fake_get_config)
    monkeypatch.setattr("src.core.session_runtime.get_config", fake_get_config)

    with TestClient(create_app()) as client:
        original_backend = app_module.scheduler._execution_backend
        app_module.scheduler._execution_backend = "worker_claim"
        try:
            client.post(
                "/api/pipelines",
                json={
                    "name": "lease_complete_pipeline",
                    "steps": [{"type": "collector", "name": "qimai", "config": {}}],
                },
            )
            client.post(
                "/api/workers/register",
                json={
                    "worker_id": "lease-complete-worker",
                    "capabilities": [
                        "qimai",
                        "session_mode:local_profile",
                        "session:qimai_profile",
                    ],
                },
            )
            client.post(
                "/api/tasks",
                json={
                    "name": "Lease Complete Task",
                    "pipeline_name": "lease_complete_pipeline",
                    "collector_name": "qimai",
                    "targets": [
                        {
                            "name": "Example App",
                            "target_type": "app",
                            "params": {"app_id": "123456"},
                        }
                    ],
                    "config": {},
                },
            )
            claimed = client.post("/api/workers/lease-complete-worker/claim-task", json={})
            task_id = claimed.json()["task_id"]
            completed = client.post(
                f"/api/workers/lease-complete-worker/tasks/{task_id}/complete",
                json={"result": {"success": True, "storage_count": 1}},
            )
            inventory_response = client.get("/api/diagnostics/sessions-inventory?collectors=qimai")
        finally:
            app_module.scheduler._execution_backend = original_backend

    assert claimed.status_code == 200
    assert completed.status_code == 200
    inventory = inventory_response.json()
    assert inventory["count"] == 1
    assert inventory["items"][0]["lease_status"] == "released"
    assert inventory["items"][0]["lease_worker_id"] == ""
    assert inventory["items"][0]["last_worker_id"] == "lease-complete-worker"
    assert inventory["items"][0]["last_task_id"] == task_id


def test_worker_api_complete_succeeds_when_session_registry_lookup_fails(
    monkeypatch, tmp_path
) -> None:
    import src.web.app as app_module

    registry = InMemorySessionRegistry()

    def registry_provider():
        if registry_provider.fail_lookup:
            raise RuntimeError("registry lookup failed token=worker-complete-lookup-secret")
        return registry

    registry_provider.fail_lookup = False
    monkeypatch.setattr(app_module, "get_session_registry", registry_provider)

    profile_dir = tmp_path / "qimai_profile"
    profile_dir.mkdir()
    values = {
        "qimai.user_data_dir": str(profile_dir),
        "qimai.cdp_enabled": False,
    }

    def fake_get_config(key: str, default=None):
        return values.get(key, default)

    monkeypatch.setattr("src.core.diagnostics.get_config", fake_get_config)
    monkeypatch.setattr("src.core.collector_metadata.get_config", fake_get_config)
    monkeypatch.setattr("src.core.session_runtime.get_config", fake_get_config)

    with TestClient(create_app()) as client:
        original_backend = app_module.scheduler._execution_backend
        app_module.scheduler._execution_backend = "worker_claim"
        try:
            client.post(
                "/api/pipelines",
                json={
                    "name": "lease_complete_lookup_fail_pipeline",
                    "steps": [{"type": "collector", "name": "qimai", "config": {}}],
                },
            )
            client.post(
                "/api/workers/register",
                json={
                    "worker_id": "lease-complete-lookup-fail-worker",
                    "capabilities": [
                        "qimai",
                        "session_mode:local_profile",
                        "session:qimai_profile",
                    ],
                },
            )
            client.post(
                "/api/tasks",
                json={
                    "name": "Lease Complete Lookup Fail Task",
                    "pipeline_name": "lease_complete_lookup_fail_pipeline",
                    "collector_name": "qimai",
                    "targets": [
                        {
                            "name": "Example App",
                            "target_type": "app",
                            "params": {"app_id": "123456"},
                        }
                    ],
                    "config": {},
                },
            )
            claimed = client.post(
                "/api/workers/lease-complete-lookup-fail-worker/claim-task",
                json={},
            )
            registry_provider.fail_lookup = True
            completed = client.post(
                f"/api/workers/lease-complete-lookup-fail-worker/tasks/{claimed.json()['task_id']}/complete",
                json={"result": {"success": True, "storage_count": 1}},
            )
            task_response = client.get(f"/api/tasks/{claimed.json()['task_id']}")
        finally:
            app_module.scheduler._execution_backend = original_backend

    assert claimed.status_code == 200
    assert completed.status_code == 200
    assert task_response.status_code == 200
    assert task_response.json()["status"] == "success"


def test_worker_api_retrying_local_profile_task_retains_session_lease(
    monkeypatch, tmp_path
) -> None:
    import src.web.app as app_module

    registry = InMemorySessionRegistry()
    monkeypatch.setattr(app_module, "get_session_registry", lambda: registry)

    profile_dir = tmp_path / "qimai_profile"
    profile_dir.mkdir()
    values = {
        "qimai.user_data_dir": str(profile_dir),
        "qimai.cdp_enabled": False,
    }

    def fake_get_config(key: str, default=None):
        return values.get(key, default)

    monkeypatch.setattr("src.core.diagnostics.get_config", fake_get_config)
    monkeypatch.setattr("src.core.collector_metadata.get_config", fake_get_config)
    monkeypatch.setattr("src.core.session_runtime.get_config", fake_get_config)

    with TestClient(create_app()) as client:
        original_backend = app_module.scheduler._execution_backend
        app_module.scheduler._execution_backend = "worker_claim"
        try:
            client.post(
                "/api/pipelines",
                json={
                    "name": "lease_retry_pipeline",
                    "steps": [{"type": "collector", "name": "qimai", "config": {}}],
                },
            )
            client.post(
                "/api/workers/register",
                json={
                    "worker_id": "lease-retry-owner",
                    "capabilities": [
                        "qimai",
                        "session_mode:local_profile",
                        "session:qimai_profile",
                    ],
                },
            )
            client.post(
                "/api/workers/register",
                json={
                    "worker_id": "lease-retry-other",
                    "capabilities": [
                        "qimai",
                        "session_mode:local_profile",
                        "session:qimai_profile",
                    ],
                },
            )
            first_created = client.post(
                "/api/tasks",
                json={
                    "name": "Lease Retry Owner Task",
                    "pipeline_name": "lease_retry_pipeline",
                    "collector_name": "qimai",
                    "targets": [
                        {
                            "name": "Example App",
                            "target_type": "app",
                            "params": {"app_id": "123456"},
                        }
                    ],
                    "config": {},
                },
            )
            first_claimed = client.post("/api/workers/lease-retry-owner/claim-task", json={})
            first_failed = client.post(
                f"/api/workers/lease-retry-owner/tasks/{first_claimed.json()['task_id']}/fail",
                json={"error": "temporary timeout", "result": {"success": False}},
            )
            second_created = client.post(
                "/api/tasks",
                json={
                    "name": "Lease Retry Other Task",
                    "pipeline_name": "lease_retry_pipeline",
                    "collector_name": "qimai",
                    "targets": [
                        {
                            "name": "Example App 2",
                            "target_type": "app",
                            "params": {"app_id": "654321"},
                        }
                    ],
                    "config": {},
                },
            )
            second_claimed = client.post("/api/workers/lease-retry-other/claim-task", json={})
            inventory_response = client.get("/api/diagnostics/sessions-inventory?collectors=qimai")
        finally:
            app_module.scheduler._execution_backend = original_backend

    assert first_created.status_code == 200
    assert first_claimed.status_code == 200
    assert first_failed.status_code == 200
    assert first_failed.json()["task"]["status"] == "retrying"
    assert second_created.status_code == 200
    assert second_claimed.status_code == 200
    assert second_claimed.json()["task_id"] is None
    assert second_claimed.json()["claim_status"] == "blocked"
    inventory = inventory_response.json()
    assert inventory["count"] == 1
    assert inventory["items"][0]["lease_status"] == "claimed"
    assert inventory["items"][0]["lease_worker_id"] == "lease-retry-owner"
    assert inventory["items"][0]["lease_task_id"] == first_claimed.json()["task_id"]


def test_worker_api_retrying_managed_state_task_releases_session_lease(
    monkeypatch, tmp_path
) -> None:
    import src.web.app as app_module

    storage_state = tmp_path / "qimai_storage_state.json"
    storage_state.write_text("{}", encoding="utf-8")

    values = {
        "qimai.session_mode": "managed_state",
        "qimai.storage_state_path": str(storage_state),
        "qimai.cdp_enabled": False,
    }

    def fake_get_config(key: str, default=None):
        return values.get(key, default)

    monkeypatch.setattr("src.core.diagnostics.get_config", fake_get_config)
    monkeypatch.setattr("src.core.collector_metadata.get_config", fake_get_config)
    monkeypatch.setattr("src.core.session_runtime.get_config", fake_get_config)

    registry = InMemorySessionRegistry()
    monkeypatch.setattr(app_module, "get_session_registry", lambda: registry)

    with TestClient(create_app()) as client:
        original_backend = app_module.scheduler._execution_backend
        app_module.scheduler._execution_backend = "worker_claim"
        try:
            client.post(
                "/api/pipelines",
                json={
                    "name": "lease_retry_managed_pipeline",
                    "steps": [{"type": "collector", "name": "qimai", "config": {}}],
                },
            )
            client.post(
                "/api/workers/register",
                json={
                    "worker_id": "lease-retry-managed",
                    "capabilities": ["qimai", "session_mode:managed_state"],
                },
            )
            client.post(
                "/api/tasks",
                json={
                    "name": "Lease Retry Managed Task",
                    "pipeline_name": "lease_retry_managed_pipeline",
                    "collector_name": "qimai",
                    "targets": [
                        {
                            "name": "Example App",
                            "target_type": "app",
                            "params": {"app_id": "123456"},
                        }
                    ],
                    "config": {},
                },
            )
            claimed = client.post("/api/workers/lease-retry-managed/claim-task", json={})
            failed = client.post(
                f"/api/workers/lease-retry-managed/tasks/{claimed.json()['task_id']}/fail",
                json={"error": "temporary timeout", "result": {"success": False}},
            )
            inventory_response = client.get("/api/diagnostics/sessions-inventory?collectors=qimai")
        finally:
            app_module.scheduler._execution_backend = original_backend

    assert claimed.status_code == 200
    assert failed.status_code == 200
    assert failed.json()["task"]["status"] == "retrying"
    inventory = inventory_response.json()
    assert inventory["count"] == 1
    assert inventory["items"][0]["lease_status"] == "released"
    assert inventory["items"][0]["lease_worker_id"] == ""
    assert inventory["items"][0]["last_worker_id"] == "lease-retry-managed"


def test_worker_api_fail_succeeds_when_session_registry_lookup_fails(monkeypatch, tmp_path) -> None:
    import src.web.app as app_module

    storage_state = tmp_path / "qimai_storage_state.json"
    storage_state.write_text("{}", encoding="utf-8")

    values = {
        "qimai.session_mode": "managed_state",
        "qimai.storage_state_path": str(storage_state),
        "qimai.cdp_enabled": False,
    }

    def fake_get_config(key: str, default=None):
        return values.get(key, default)

    registry = InMemorySessionRegistry()

    def registry_provider():
        if registry_provider.fail_lookup:
            raise RuntimeError("registry lookup failed token=worker-fail-lookup-secret")
        return registry

    registry_provider.fail_lookup = False

    monkeypatch.setattr("src.core.diagnostics.get_config", fake_get_config)
    monkeypatch.setattr("src.core.collector_metadata.get_config", fake_get_config)
    monkeypatch.setattr("src.core.session_runtime.get_config", fake_get_config)
    monkeypatch.setattr(app_module, "get_session_registry", registry_provider)

    with TestClient(create_app()) as client:
        original_backend = app_module.scheduler._execution_backend
        app_module.scheduler._execution_backend = "worker_claim"
        try:
            client.post(
                "/api/pipelines",
                json={
                    "name": "lease_retry_lookup_fail_pipeline",
                    "steps": [{"type": "collector", "name": "qimai", "config": {}}],
                },
            )
            client.post(
                "/api/workers/register",
                json={
                    "worker_id": "lease-retry-lookup-fail-worker",
                    "capabilities": ["qimai", "session_mode:managed_state"],
                },
            )
            client.post(
                "/api/tasks",
                json={
                    "name": "Lease Retry Lookup Fail Task",
                    "pipeline_name": "lease_retry_lookup_fail_pipeline",
                    "collector_name": "qimai",
                    "targets": [
                        {
                            "name": "Example App",
                            "target_type": "app",
                            "params": {"app_id": "123456"},
                        }
                    ],
                    "config": {},
                },
            )
            claimed = client.post(
                "/api/workers/lease-retry-lookup-fail-worker/claim-task",
                json={},
            )
            registry_provider.fail_lookup = True
            failed = client.post(
                f"/api/workers/lease-retry-lookup-fail-worker/tasks/{claimed.json()['task_id']}/fail",
                json={"error": "temporary timeout", "result": {"success": False}},
            )
            task_response = client.get(f"/api/tasks/{claimed.json()['task_id']}")
        finally:
            app_module.scheduler._execution_backend = original_backend

    assert claimed.status_code == 200
    assert failed.status_code == 200
    assert failed.json()["task"]["status"] == "retrying"
    assert task_response.status_code == 200
    assert task_response.json()["status"] == "retrying"


def test_task_cancel_rejects_running_worker_claim_task(monkeypatch) -> None:
    import src.web.app as app_module

    with TestClient(create_app()) as client:
        original_backend = app_module.scheduler._execution_backend
        app_module.scheduler._execution_backend = "worker_claim"
        try:
            client.post(
                "/api/pipelines",
                json={
                    "name": "cancel_running_worker_pipeline",
                    "steps": [{"type": "collector", "name": "steam", "config": {}}],
                },
            )
            created = client.post(
                "/api/tasks",
                json={
                    "name": "Cancel Running Worker Task",
                    "pipeline_name": "cancel_running_worker_pipeline",
                    "collector_name": "steam",
                    "targets": [
                        {"name": "CS2", "target_type": "game", "params": {"app_id": "730"}}
                    ],
                    "config": {},
                },
            )
            client.post(
                "/api/workers/register",
                json={
                    "worker_id": "cancel-running-worker",
                    "capabilities": ["steam"],
                },
            )
            claimed = client.post("/api/workers/cancel-running-worker/claim-task", json={})
            cancel_response = client.post(f"/api/tasks/{created.json()['id']}/cancel")
            task_response = client.get(f"/api/tasks/{created.json()['id']}")
        finally:
            app_module.scheduler._execution_backend = original_backend

    assert created.status_code == 200
    assert claimed.status_code == 200
    assert cancel_response.status_code == 400
    assert cancel_response.json()["detail"] == f"无法取消任务: {created.json()['id']}"
    assert task_response.status_code == 200
    assert task_response.json()["status"] == "running"


def test_task_cancel_releases_retrying_worker_claim_session_lease(
    monkeypatch, tmp_path
) -> None:
    import src.web.app as app_module

    registry = InMemorySessionRegistry()
    monkeypatch.setattr(app_module, "get_session_registry", lambda: registry)

    profile_dir = tmp_path / "qimai_profile"
    profile_dir.mkdir()
    values = {
        "qimai.user_data_dir": str(profile_dir),
        "qimai.cdp_enabled": False,
    }

    def fake_get_config(key: str, default=None):
        return values.get(key, default)

    monkeypatch.setattr("src.core.diagnostics.get_config", fake_get_config)
    monkeypatch.setattr("src.core.collector_metadata.get_config", fake_get_config)
    monkeypatch.setattr("src.core.session_runtime.get_config", fake_get_config)

    with TestClient(create_app()) as client:
        original_backend = app_module.scheduler._execution_backend
        app_module.scheduler._execution_backend = "worker_claim"
        try:
            client.post(
                "/api/pipelines",
                json={
                    "name": "cancel_retrying_worker_pipeline",
                    "steps": [{"type": "collector", "name": "qimai", "config": {}}],
                },
            )
            created = client.post(
                "/api/tasks",
                json={
                    "name": "Cancel Retrying Worker Task",
                    "pipeline_name": "cancel_retrying_worker_pipeline",
                    "collector_name": "qimai",
                    "targets": [
                        {
                            "name": "Example App",
                            "target_type": "app",
                            "params": {"app_id": "123456"},
                        }
                    ],
                    "config": {},
                },
            )
            client.post(
                "/api/workers/register",
                json={
                    "worker_id": "cancel-retrying-worker",
                    "capabilities": [
                        "qimai",
                        "session_mode:local_profile",
                        "session:qimai_profile",
                    ],
                },
            )
            claimed = client.post("/api/workers/cancel-retrying-worker/claim-task", json={})
            failed = client.post(
                f"/api/workers/cancel-retrying-worker/tasks/{claimed.json()['task_id']}/fail",
                json={"error": "temporary timeout", "result": {"success": False}},
            )
            cancel_response = client.post(f"/api/tasks/{created.json()['id']}/cancel")
            task_response = client.get(f"/api/tasks/{created.json()['id']}")
            inventory_response = client.get("/api/diagnostics/sessions-inventory?collectors=qimai")
        finally:
            app_module.scheduler._execution_backend = original_backend

    assert created.status_code == 200
    assert claimed.status_code == 200
    assert failed.status_code == 200
    assert failed.json()["task"]["status"] == "retrying"
    assert cancel_response.status_code == 200
    assert task_response.status_code == 200
    assert task_response.json()["status"] == "cancelled"
    inventory = inventory_response.json()
    assert inventory["count"] == 1
    assert inventory["items"][0]["lease_status"] == "released"
    assert inventory["items"][0]["lease_worker_id"] == ""
    assert inventory["items"][0]["last_worker_id"] == "cancel-retrying-worker"


def test_worker_api_complete_releases_claimed_session_after_runtime_mode_changes(
    monkeypatch, tmp_path
) -> None:
    import src.web.app as app_module

    profile_dir = tmp_path / "qimai_profile"
    profile_dir.mkdir()
    storage_state = tmp_path / "qimai_storage_state.json"
    storage_state.write_text("{}", encoding="utf-8")

    values = {
        "qimai.user_data_dir": str(profile_dir),
        "qimai.storage_state_path": str(storage_state),
        "qimai.cdp_enabled": False,
    }

    def fake_get_config(key: str, default=None):
        return values.get(key, default)

    monkeypatch.setattr("src.core.diagnostics.get_config", fake_get_config)
    monkeypatch.setattr("src.core.collector_metadata.get_config", fake_get_config)
    monkeypatch.setattr("src.core.session_runtime.get_config", fake_get_config)

    registry = InMemorySessionRegistry()
    monkeypatch.setattr(app_module, "get_session_registry", lambda: registry)

    with TestClient(create_app()) as client:
        original_backend = app_module.scheduler._execution_backend
        app_module.scheduler._execution_backend = "worker_claim"
        try:
            client.post(
                "/api/pipelines",
                json={
                    "name": "lease_mode_change_pipeline",
                    "steps": [{"type": "collector", "name": "qimai", "config": {}}],
                },
            )
            client.post(
                "/api/workers/register",
                json={
                    "worker_id": "lease-mode-change-worker",
                    "capabilities": [
                        "qimai",
                        "session_mode:local_profile",
                        "session:qimai_profile",
                    ],
                },
            )
            client.post(
                "/api/tasks",
                json={
                    "name": "Lease Mode Change Task",
                    "pipeline_name": "lease_mode_change_pipeline",
                    "collector_name": "qimai",
                    "targets": [
                        {
                            "name": "Example App",
                            "target_type": "app",
                            "params": {"app_id": "123456"},
                        }
                    ],
                    "config": {},
                },
            )
            claimed = client.post("/api/workers/lease-mode-change-worker/claim-task", json={})
            task_id = claimed.json()["task_id"]

            values["qimai.session_mode"] = "managed_state"
            completed = client.post(
                f"/api/workers/lease-mode-change-worker/tasks/{task_id}/complete",
                json={"result": {"success": True, "storage_count": 1}},
            )
            inventory_response = client.get("/api/diagnostics/sessions-inventory?collectors=qimai")
        finally:
            app_module.scheduler._execution_backend = original_backend

    assert claimed.status_code == 200
    assert claimed.json()["session_diagnostics"]["session_mode"] == "local_profile"
    assert completed.status_code == 200
    inventory = inventory_response.json()
    assert inventory["count"] == 1
    assert inventory["items"][0]["session_mode"] == "local_profile"
    assert inventory["items"][0]["lease_status"] == "released"
    assert inventory["items"][0]["lease_worker_id"] == ""
    assert inventory["items"][0]["last_task_id"] == task_id


def test_task_detail_keeps_claimed_session_snapshot_after_runtime_mode_changes(
    monkeypatch, tmp_path
) -> None:
    import src.web.app as app_module

    profile_dir = tmp_path / "qimai_profile"
    profile_dir.mkdir()
    storage_state = tmp_path / "qimai_storage_state.json"
    storage_state.write_text("{}", encoding="utf-8")

    values = {
        "qimai.user_data_dir": str(profile_dir),
        "qimai.storage_state_path": str(storage_state),
        "qimai.cdp_enabled": False,
    }

    def fake_get_config(key: str, default=None):
        return values.get(key, default)

    monkeypatch.setattr("src.core.diagnostics.get_config", fake_get_config)
    monkeypatch.setattr("src.core.collector_metadata.get_config", fake_get_config)
    monkeypatch.setattr("src.core.session_runtime.get_config", fake_get_config)

    registry = InMemorySessionRegistry()
    monkeypatch.setattr(app_module, "get_session_registry", lambda: registry)

    with TestClient(create_app()) as client:
        original_backend = app_module.scheduler._execution_backend
        app_module.scheduler._execution_backend = "worker_claim"
        try:
            client.post(
                "/api/pipelines",
                json={
                    "name": "task_detail_mode_snapshot_pipeline",
                    "steps": [{"type": "collector", "name": "qimai", "config": {}}],
                },
            )
            created = client.post(
                "/api/tasks",
                json={
                    "name": "Task Detail Mode Snapshot",
                    "pipeline_name": "task_detail_mode_snapshot_pipeline",
                    "collector_name": "qimai",
                    "targets": [
                        {
                            "name": "Example App",
                            "target_type": "app",
                            "params": {"app_id": "123456"},
                        }
                    ],
                    "config": {},
                },
            )
            client.post(
                "/api/workers/register",
                json={
                    "worker_id": "task-detail-mode-worker",
                    "capabilities": [
                        "qimai",
                        "session_mode:local_profile",
                        "session:qimai_profile",
                    ],
                },
            )
            claimed = client.post("/api/workers/task-detail-mode-worker/claim-task", json={})
            values["qimai.session_mode"] = "managed_state"
            task_response = client.get(f"/api/tasks/{created.json()['id']}")
        finally:
            app_module.scheduler._execution_backend = original_backend

    assert created.status_code == 200
    assert claimed.status_code == 200
    assert claimed.json()["session_diagnostics"]["session_mode"] == "local_profile"
    assert task_response.status_code == 200
    payload = task_response.json()
    assert payload["session_diagnostics"]["session_mode"] == "local_profile"
    assert payload["session_diagnostics"]["worker_binding"] == "sticky"
    assert payload["session_readiness"]["status"] == "ready"
    assert payload["collector_metadata"]["session_mode"] == "local_profile"
    assert payload["collector_metadata"]["session_mode_source"] in {"metadata", "config"}
    assert "managed_state" in payload["collector_metadata"]["supported_session_modes"]
    assert payload["recovery"]["session_mode"] == "local_profile"
    assert payload["recovery"]["session_mode_source"] in {"metadata", "config"}


def test_worker_api_complete_releases_snapshot_session_when_registry_entry_missing(
    monkeypatch, tmp_path
) -> None:
    import src.web.app as app_module

    profile_dir = tmp_path / "qimai_profile"
    profile_dir.mkdir()
    storage_state = tmp_path / "qimai_storage_state.json"
    storage_state.write_text("{}", encoding="utf-8")

    values = {
        "qimai.user_data_dir": str(profile_dir),
        "qimai.storage_state_path": str(storage_state),
        "qimai.cdp_enabled": False,
    }

    def fake_get_config(key: str, default=None):
        return values.get(key, default)

    monkeypatch.setattr("src.core.diagnostics.get_config", fake_get_config)
    monkeypatch.setattr("src.core.collector_metadata.get_config", fake_get_config)
    monkeypatch.setattr("src.core.session_runtime.get_config", fake_get_config)

    registry = InMemorySessionRegistry()
    monkeypatch.setattr(app_module, "get_session_registry", lambda: registry)

    with TestClient(create_app()) as client:
        original_backend = app_module.scheduler._execution_backend
        app_module.scheduler._execution_backend = "worker_claim"
        try:
            client.post(
                "/api/pipelines",
                json={
                    "name": "lease_missing_registry_pipeline",
                    "steps": [{"type": "collector", "name": "qimai", "config": {}}],
                },
            )
            client.post(
                "/api/workers/register",
                json={
                    "worker_id": "lease-missing-registry-worker",
                    "capabilities": [
                        "qimai",
                        "session_mode:local_profile",
                        "session:qimai_profile",
                    ],
                },
            )
            client.post(
                "/api/tasks",
                json={
                    "name": "Lease Missing Registry Task",
                    "pipeline_name": "lease_missing_registry_pipeline",
                    "collector_name": "qimai",
                    "targets": [
                        {
                            "name": "Example App",
                            "target_type": "app",
                            "params": {"app_id": "123456"},
                        }
                    ],
                    "config": {},
                },
            )
            claimed = client.post("/api/workers/lease-missing-registry-worker/claim-task", json={})
            task_id = claimed.json()["task_id"]
            asyncio.run(registry.delete_session("qimai:local_profile:local:qimai_profile"))

            values["qimai.session_mode"] = "managed_state"
            completed = client.post(
                f"/api/workers/lease-missing-registry-worker/tasks/{task_id}/complete",
                json={"result": {"success": True, "storage_count": 1}},
            )
            inventory_response = client.get("/api/diagnostics/sessions-inventory?collectors=qimai")
        finally:
            app_module.scheduler._execution_backend = original_backend

    assert claimed.status_code == 200
    assert completed.status_code == 200
    inventory = inventory_response.json()
    assert inventory["count"] == 1
    assert inventory["items"][0]["session_mode"] == "local_profile"
    assert inventory["items"][0]["lease_status"] == "released"
    assert inventory["items"][0]["lease_worker_id"] == ""
    assert inventory["items"][0]["last_task_id"] == task_id


def test_worker_api_reconcile_marks_session_interrupted(monkeypatch, tmp_path) -> None:
    import src.web.app as app_module

    registry = InMemorySessionRegistry()
    monkeypatch.setattr(app_module, "get_session_registry", lambda: registry)

    profile_dir = tmp_path / "qimai_profile"
    profile_dir.mkdir()
    values = {
        "qimai.user_data_dir": str(profile_dir),
        "qimai.cdp_enabled": False,
    }

    def fake_get_config(key: str, default=None):
        return values.get(key, default)

    monkeypatch.setattr("src.core.diagnostics.get_config", fake_get_config)
    monkeypatch.setattr("src.core.collector_metadata.get_config", fake_get_config)
    monkeypatch.setattr("src.core.session_runtime.get_config", fake_get_config)

    with TestClient(create_app()) as client:
        original_backend = app_module.scheduler._execution_backend
        app_module.scheduler._execution_backend = "worker_claim"
        try:
            client.post(
                "/api/pipelines",
                json={
                    "name": "lease_interrupt_pipeline",
                    "steps": [{"type": "collector", "name": "qimai", "config": {}}],
                },
            )
            client.post(
                "/api/workers/register",
                json={
                    "worker_id": "lease-interrupt-worker",
                    "capabilities": [
                        "qimai",
                        "session_mode:local_profile",
                        "session:qimai_profile",
                    ],
                },
            )
            client.post(
                "/api/tasks",
                json={
                    "name": "Lease Interrupt Task",
                    "pipeline_name": "lease_interrupt_pipeline",
                    "collector_name": "qimai",
                    "targets": [
                        {
                            "name": "Example App",
                            "target_type": "app",
                            "params": {"app_id": "123456"},
                        }
                    ],
                    "config": {},
                },
            )
            claimed = client.post("/api/workers/lease-interrupt-worker/claim-task", json={})
            client.post(
                "/api/workers/lease-interrupt-worker/heartbeat",
                json={"status": "offline", "current_task_ids": []},
            )
            reconciled = client.post("/api/workers/reconcile-stale-tasks?stale_after_seconds=1")
            inventory_response = client.get("/api/diagnostics/sessions-inventory?collectors=qimai")
            worker_after = client.get("/api/workers/lease-interrupt-worker")
        finally:
            app_module.scheduler._execution_backend = original_backend

    assert claimed.status_code == 200
    assert reconciled.status_code == 200
    inventory = inventory_response.json()
    assert inventory["count"] == 1
    assert inventory["items"][0]["lease_status"] == "interrupted"
    assert inventory["items"][0]["last_worker_id"] == "lease-interrupt-worker"
    assert worker_after.status_code == 200
    assert worker_after.json()["status"] == "offline"
    assert worker_after.json()["current_task_ids"] == []


def test_worker_api_reconcile_succeeds_when_session_release_fails(monkeypatch) -> None:
    import src.web.app as app_module

    class ReleaseFailsRegistry(InMemorySessionRegistry):
        async def release_session_by_id(self, session_id: str, **kwargs):
            return None

        async def release_session(self, diagnostics: dict, **kwargs):
            raise RuntimeError("release failed token=reconcile-release-secret")

    registry = ReleaseFailsRegistry()
    monkeypatch.setattr(app_module, "get_session_registry", lambda: registry)

    with TestClient(create_app()) as client:
        original_backend = app_module.scheduler._execution_backend
        app_module.scheduler._execution_backend = "worker_claim"
        try:
            client.post(
                "/api/pipelines",
                json={
                    "name": "lease_interrupt_release_fail_pipeline",
                    "steps": [{"type": "collector", "name": "steam", "config": {}}],
                },
            )
            client.post(
                "/api/workers/register",
                json={
                    "worker_id": "lease-interrupt-release-fail-worker",
                    "capabilities": ["steam"],
                },
            )
            created = client.post(
                "/api/tasks",
                json={
                    "name": "Lease Interrupt Release Fail Task",
                    "pipeline_name": "lease_interrupt_release_fail_pipeline",
                    "collector_name": "steam",
                    "targets": [
                        {"name": "CS2", "target_type": "game", "params": {"app_id": "730"}}
                    ],
                    "config": {},
                },
            )
            claimed = client.post(
                "/api/workers/lease-interrupt-release-fail-worker/claim-task",
                json={},
            )
            client.post(
                "/api/workers/lease-interrupt-release-fail-worker/heartbeat",
                json={"status": "offline", "current_task_ids": []},
            )
            reconciled = client.post("/api/workers/reconcile-stale-tasks?stale_after_seconds=1")
            task_response = client.get(f"/api/tasks/{created.json()['id']}")
            worker_after = client.get("/api/workers/lease-interrupt-release-fail-worker")
        finally:
            app_module.scheduler._execution_backend = original_backend

    assert claimed.status_code == 200
    assert reconciled.status_code == 200
    assert reconciled.json()["interrupted_tasks"][0]["id"] == created.json()["id"]
    assert task_response.status_code == 200
    assert task_response.json()["status"] == "cancelled"
    assert worker_after.status_code == 200
    assert worker_after.json()["status"] == "offline"
    assert worker_after.json()["current_task_ids"] == []


def test_worker_api_reconcile_succeeds_when_session_registry_lookup_fails(
    monkeypatch, tmp_path
) -> None:
    import src.web.app as app_module

    registry = InMemorySessionRegistry()

    def registry_provider():
        if registry_provider.fail_lookup:
            raise RuntimeError("registry lookup failed token=reconcile-lookup-secret")
        return registry

    registry_provider.fail_lookup = False
    monkeypatch.setattr(app_module, "get_session_registry", registry_provider)

    profile_dir = tmp_path / "qimai_profile"
    profile_dir.mkdir()
    values = {
        "qimai.user_data_dir": str(profile_dir),
        "qimai.cdp_enabled": False,
    }

    def fake_get_config(key: str, default=None):
        return values.get(key, default)

    monkeypatch.setattr("src.core.diagnostics.get_config", fake_get_config)
    monkeypatch.setattr("src.core.collector_metadata.get_config", fake_get_config)
    monkeypatch.setattr("src.core.session_runtime.get_config", fake_get_config)

    with TestClient(create_app()) as client:
        original_backend = app_module.scheduler._execution_backend
        app_module.scheduler._execution_backend = "worker_claim"
        try:
            client.post(
                "/api/pipelines",
                json={
                    "name": "lease_interrupt_lookup_fail_pipeline",
                    "steps": [{"type": "collector", "name": "qimai", "config": {}}],
                },
            )
            client.post(
                "/api/workers/register",
                json={
                    "worker_id": "lease-interrupt-lookup-fail-worker",
                    "capabilities": [
                        "qimai",
                        "session_mode:local_profile",
                        "session:qimai_profile",
                    ],
                },
            )
            created = client.post(
                "/api/tasks",
                json={
                    "name": "Lease Interrupt Lookup Fail Task",
                    "pipeline_name": "lease_interrupt_lookup_fail_pipeline",
                    "collector_name": "qimai",
                    "targets": [
                        {
                            "name": "Example App",
                            "target_type": "app",
                            "params": {"app_id": "123456"},
                        }
                    ],
                    "config": {},
                },
            )
            claimed = client.post(
                "/api/workers/lease-interrupt-lookup-fail-worker/claim-task",
                json={},
            )
            client.post(
                "/api/workers/lease-interrupt-lookup-fail-worker/heartbeat",
                json={"status": "offline", "current_task_ids": []},
            )
            registry_provider.fail_lookup = True
            reconciled = client.post("/api/workers/reconcile-stale-tasks?stale_after_seconds=1")
            task_response = client.get(f"/api/tasks/{created.json()['id']}")
            worker_after = client.get("/api/workers/lease-interrupt-lookup-fail-worker")
        finally:
            app_module.scheduler._execution_backend = original_backend

    assert claimed.status_code == 200
    assert reconciled.status_code == 200
    assert task_response.status_code == 200
    assert task_response.json()["status"] == "cancelled"
    assert worker_after.status_code == 200
    assert worker_after.json()["status"] == "offline"
    assert worker_after.json()["current_task_ids"] == []


def test_worker_api_reconcile_recovers_retrying_sticky_task_and_releases_lease(
    monkeypatch, tmp_path
) -> None:
    import src.web.app as app_module

    registry = InMemorySessionRegistry()
    monkeypatch.setattr(app_module, "get_session_registry", lambda: registry)

    profile_dir = tmp_path / "qimai_profile"
    profile_dir.mkdir()
    values = {
        "qimai.user_data_dir": str(profile_dir),
        "qimai.cdp_enabled": False,
    }

    def fake_get_config(key: str, default=None):
        return values.get(key, default)

    monkeypatch.setattr("src.core.diagnostics.get_config", fake_get_config)
    monkeypatch.setattr("src.core.collector_metadata.get_config", fake_get_config)
    monkeypatch.setattr("src.core.session_runtime.get_config", fake_get_config)

    with TestClient(create_app()) as client:
        original_backend = app_module.scheduler._execution_backend
        app_module.scheduler._execution_backend = "worker_claim"
        try:
            client.post(
                "/api/pipelines",
                json={
                    "name": "lease_retry_reconcile_pipeline",
                    "steps": [{"type": "collector", "name": "qimai", "config": {}}],
                },
            )
            client.post(
                "/api/workers/register",
                json={
                    "worker_id": "lease-retry-reconcile-worker",
                    "capabilities": [
                        "qimai",
                        "session_mode:local_profile",
                        "session:qimai_profile",
                    ],
                },
            )
            client.post(
                "/api/tasks",
                json={
                    "name": "Lease Retry Reconcile Task",
                    "pipeline_name": "lease_retry_reconcile_pipeline",
                    "collector_name": "qimai",
                    "targets": [
                        {
                            "name": "Example App",
                            "target_type": "app",
                            "params": {"app_id": "123456"},
                        }
                    ],
                    "config": {},
                },
            )
            claimed = client.post("/api/workers/lease-retry-reconcile-worker/claim-task", json={})
            failed = client.post(
                f"/api/workers/lease-retry-reconcile-worker/tasks/{claimed.json()['task_id']}/fail",
                json={"error": "temporary timeout", "result": {"success": False}},
            )
            client.post(
                "/api/workers/lease-retry-reconcile-worker/heartbeat",
                json={"status": "offline", "current_task_ids": []},
            )
            reconciled = client.post("/api/workers/reconcile-stale-tasks?stale_after_seconds=1")
            inventory_response = client.get("/api/diagnostics/sessions-inventory?collectors=qimai")
            task_response = client.get(f"/api/tasks/{claimed.json()['task_id']}")
            worker_response = client.get("/api/workers/lease-retry-reconcile-worker")
        finally:
            app_module.scheduler._execution_backend = original_backend

    assert claimed.status_code == 200
    assert failed.status_code == 200
    assert failed.json()["task"]["status"] == "retrying"
    assert reconciled.status_code == 200
    payload = reconciled.json()
    assert payload["updated_worker_ids"] == ["lease-retry-reconcile-worker"]
    assert payload["interrupted_tasks"] == []
    assert len(payload["recovered_retry_tasks"]) == 1
    assert payload["recovered_retry_tasks"][0]["id"] == claimed.json()["task_id"]
    assert payload["recovered_retry_tasks"][0]["status"] == "cancelled"
    assert task_response.status_code == 200
    assert task_response.json()["status"] == "cancelled"
    assert worker_response.status_code == 200
    assert worker_response.json()["status"] == "offline"
    assert worker_response.json()["current_task_ids"] == []
    inventory = inventory_response.json()
    assert inventory["count"] == 1
    assert inventory["items"][0]["lease_status"] == "interrupted"
    assert inventory["items"][0]["last_worker_id"] == "lease-retry-reconcile-worker"
    assert inventory["items"][0]["last_task_id"] == claimed.json()["task_id"]


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
