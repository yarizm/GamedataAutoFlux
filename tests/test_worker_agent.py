import asyncio

import pytest
import httpx

from src.collectors.base import BaseCollector, CollectResult
from src.core.events import EventBus, TaskCompletedEvent
from src.core.hooks import ReportGenerationHook
from src.core.pipeline import Pipeline
from src.core.registry import registry
from src.core.scheduler import Scheduler
from src.core.task import Task, TaskTarget, TaskStatus
from src.reporting.generator import GeneratedReport
from src.services.task_artifact_service import InMemoryTaskArtifactService
from src.services.task_checkpoint_service import InMemoryTaskCheckpointService
from src.services.task_event_service import InMemoryTaskEventService
from src.services.task_repository import InMemoryTaskRepository
from src.web.app import create_app
from src.worker.agent import WorkerAgent, WorkerAgentConfig


_slow_collector_started: asyncio.Event | None = None
_slow_collector_continue: asyncio.Event | None = None


@registry.register("collector", "worker_agent_test")
class _WorkerAgentTestCollector(BaseCollector):
    async def collect(self, target):
        return CollectResult(
            target=target,
            data={
                "game_name": target.name,
                "collector": "worker_agent_test",
                "sequence": 1,
            },
            metadata={"kind": "worker-agent-test"},
        )


@registry.register("collector", "worker_agent_slow_test")
class _WorkerAgentSlowTestCollector(BaseCollector):
    async def collect(self, target):
        if _slow_collector_started is not None:
            _slow_collector_started.set()
        if _slow_collector_continue is not None:
            await _slow_collector_continue.wait()
        return CollectResult(
            target=target,
            data={"game_name": target.name, "collector": "worker_agent_slow_test"},
            metadata={"kind": "worker-agent-slow-test"},
        )


@pytest.mark.asyncio
async def test_worker_agent_executes_claimed_task_via_api() -> None:
    import src.web.app as app_module

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
    scheduler._event_bus = EventBus()
    scheduler._pipelines["worker_agent_pipeline"] = (
        Pipeline("worker_agent_pipeline")
        .add_collector("worker_agent_test")
        .add_storage("sqlalchemy")
    )

    completed_events: list[TaskCompletedEvent] = []

    async def capture_completed(event: TaskCompletedEvent) -> None:
        completed_events.append(event)

    scheduler._event_bus.on("task_completed", capture_completed)

    task = Task(
        id="worker-agent-task",
        name="Worker Agent Task",
        pipeline_name="worker_agent_pipeline",
        collector_name="gtrends",
        targets=[TaskTarget(name="Helldivers 2", params={"keyword": "Helldivers 2"})],
        config={},
    )

    await scheduler.submit(task, pipeline_name="worker_agent_pipeline")

    original_scheduler = app_module.scheduler
    original_worker_registry = app_module._worker_registry
    original_task_service = app_module._task_service
    app_module.scheduler = scheduler
    app_module._worker_registry = None
    app_module._task_service = None

    agent = None
    try:
        app = create_app()
        transport = httpx.ASGITransport(app=app)
        agent = WorkerAgent(
            WorkerAgentConfig(
                base_url="http://testserver",
                worker_id="worker-agent-1",
                capabilities=["worker_agent_test", "gtrends"],
                heartbeat_interval_seconds=60,
                claim_poll_interval_seconds=0.1,
                transport=transport,
            )
        )
        await agent.start()
        assert await agent.run_once() is True
        await agent.stop()
    finally:
        app_module.scheduler = original_scheduler
        app_module._worker_registry = original_worker_registry
        app_module._task_service = original_task_service

    stored_task = scheduler.get_task("worker-agent-task")
    stored_events = await event_service.list_events("worker-agent-task")

    assert stored_task is not None
    assert stored_task.status == TaskStatus.SUCCESS
    assert stored_task.result["success"] is True
    assert stored_task.result["storage_count"] == 1
    assert stored_task.result["collection_summary"]["status"] == "success"
    assert any(event.type == "collect" for event in stored_events)
    assert any(event.type == "progress" for event in stored_events)
    assert any(event.type == "complete" for event in stored_events)
    assert completed_events
    assert completed_events[-1].task_id == "worker-agent-task"
    assert completed_events[-1].success is True


@pytest.mark.asyncio
async def test_worker_event_collect_records_checkpoint_from_worker_path() -> None:
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
    scheduler._pipelines["worker_pipeline"] = Pipeline("worker_pipeline").add_collector("gtrends")
    task_id = await scheduler.submit(
        Task(
            id="worker-checkpoint-task",
            name="Worker Checkpoint Task",
            pipeline_name="worker_pipeline",
            collector_name="gtrends",
        ),
        pipeline_name="worker_pipeline",
    )
    assert await scheduler.claim_task_for_worker("worker-1", capabilities=["gtrends"]) is not None

    await scheduler.append_worker_task_event(
        "worker-1",
        task_id,
        "collect",
        message="worker collect finished",
        payload={
            "status": "succeeded",
            "component": "gtrends",
            "targets_count": 1,
            "success_count": 1,
            "failed_count": 0,
        },
    )

    checkpoints = await checkpoint_service.list_checkpoints(task_id)
    assert checkpoints
    assert checkpoints[0].recovery_level == "L1"
    assert checkpoints[0].cursor["component"] == "gtrends"


@pytest.mark.asyncio
async def test_worker_complete_emits_task_completed_event() -> None:
    scheduler = Scheduler(
        max_concurrent=1,
        task_repo=InMemoryTaskRepository(),
        task_event_service=InMemoryTaskEventService(),
        execution_backend="worker_claim",
    )
    scheduler._started = True
    scheduler._event_bus = EventBus()
    scheduler._pipelines["worker_pipeline"] = Pipeline("worker_pipeline").add_collector("steam")
    task_id = await scheduler.submit(
        Task(
            id="worker-completed-event",
            name="Worker Completed Event",
            pipeline_name="worker_pipeline",
            collector_name="steam",
        ),
        pipeline_name="worker_pipeline",
    )
    assert await scheduler.claim_task_for_worker("worker-1", capabilities=["steam"]) is not None

    received: list[TaskCompletedEvent] = []

    async def capture(event: TaskCompletedEvent) -> None:
        received.append(event)

    scheduler._event_bus.on("task_completed", capture)

    await scheduler.complete_worker_task(
        "worker-1",
        task_id,
        result={"success": True, "storage_count": 3},
    )

    assert received
    assert received[-1].task_id == task_id
    assert received[-1].success is True


@pytest.mark.asyncio
async def test_worker_complete_pipeline_result_supports_report_hook(tmp_path) -> None:
    scheduler = Scheduler(
        max_concurrent=1,
        task_repo=InMemoryTaskRepository(),
        task_event_service=InMemoryTaskEventService(),
        task_artifact_service=InMemoryTaskArtifactService(),
        execution_backend="worker_claim",
    )
    scheduler._started = True
    scheduler._event_bus = EventBus()
    scheduler._pipelines["worker_pipeline"] = Pipeline("worker_pipeline").add_collector("steam")

    report = GeneratedReport(
        id="worker-report-1",
        title="Worker Auto Report",
        prompt="prompt",
        data_source="steam",
        template="default",
        generated_at=task_time(),
        matched_records=1,
        content="report",
        excel_path=str(tmp_path / "worker-report.xlsx"),
    )
    report_generator = _FakeReportGenerator(report)
    scheduler._event_bus.on(
        "task_completed",
        ReportGenerationHook(report_generator, scheduler=scheduler).handle,
    )

    task_id = await scheduler.submit(
        Task(
            id="worker-report-task",
            name="Worker Report Task",
            pipeline_name="worker_pipeline",
            collector_name="steam",
            config={"report": {"enabled": True}},
        ),
        pipeline_name="worker_pipeline",
    )
    assert await scheduler.claim_task_for_worker("worker-1", capabilities=["steam"]) is not None

    await scheduler.complete_worker_task(
        "worker-1",
        task_id,
        result={
            "success": True,
            "storage_count": 1,
            "output_records": [
                {
                    "key": "worker:record:1",
                    "data": {"game_name": "CS2"},
                    "metadata": {"collector": "steam"},
                    "source": "steam",
                    "tags": ["steam"],
                }
            ],
        },
    )
    if scheduler._background_tasks:
        await asyncio.gather(*scheduler._background_tasks)

    stored_task = scheduler.get_task(task_id)
    artifacts = await scheduler.get_task_artifacts(task_id)

    assert stored_task is not None
    assert stored_task.result["generated_report_id"] == "worker-report-1"
    assert artifacts is not None
    assert any(artifact.type == "report_excel" for artifact in artifacts)


def test_worker_agent_config_defaults_from_script(monkeypatch) -> None:
    import scripts.worker_agent as worker_script

    monkeypatch.setattr(worker_script, "get_config", lambda key, default=None: {
        "server.host": "127.0.0.1",
        "server.port": 8011,
        "worker.capabilities": ["steam", "gtrends"],
    }.get(key, default))

    assert worker_script._resolve_base_url("") == "http://127.0.0.1:8011"
    assert worker_script._resolve_capabilities(None) == ["steam", "gtrends"]


def test_worker_agent_normalizes_session_capabilities(monkeypatch, tmp_path) -> None:
    values = {
        "qimai.user_data_dir": str(tmp_path / "missing_qimai_profile"),
        "qimai.cdp_enabled": False,
    }

    def fake_get_config(key: str, default=None):
        return values.get(key, default)

    monkeypatch.setattr("src.core.diagnostics.get_config", fake_get_config)
    agent = WorkerAgent(
        WorkerAgentConfig(
            base_url="http://127.0.0.1:8000",
            worker_id="worker-agent-capabilities",
            capabilities=["QIMAI", "session:qimai_profile", "qimai"],
        )
    )

    assert agent.capabilities == ["qimai"]


def test_worker_agent_derives_session_capabilities_when_runtime_ready(monkeypatch, tmp_path) -> None:
    profile_dir = tmp_path / "qimai_profile"
    profile_dir.mkdir()

    values = {
        "qimai.user_data_dir": str(profile_dir),
        "qimai.cdp_enabled": False,
    }

    def fake_get_config(key: str, default=None):
        return values.get(key, default)

    monkeypatch.setattr("src.core.diagnostics.get_config", fake_get_config)
    agent = WorkerAgent(
        WorkerAgentConfig(
            base_url="http://127.0.0.1:8000",
            worker_id="worker-agent-capabilities-ready",
            capabilities=["qimai"],
        )
    )

    assert agent.capabilities == [
        "qimai",
        "session:qimai_profile",
        "session_mode:local_profile",
    ]


def test_worker_agent_derives_managed_state_capability_without_profile(monkeypatch, tmp_path) -> None:
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
    agent = WorkerAgent(
        WorkerAgentConfig(
            base_url="http://127.0.0.1:8000",
            worker_id="worker-agent-capabilities-managed",
            capabilities=["qimai"],
        )
    )

    assert agent.capabilities == [
        "qimai",
        "session_mode:managed_state",
    ]


@pytest.mark.asyncio
async def test_worker_agent_reports_blocked_claim_status_in_heartbeat() -> None:
    requests: list[dict] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        payload = {}
        if request.content:
            import json

            payload = json.loads(request.content.decode())
        requests.append({"method": request.method, "path": request.url.path, "payload": payload})

        if request.url.path.endswith("/claim-task"):
            return httpx.Response(
                200,
                json={
                    "worker_id": "worker-agent-blocked",
                    "claim_status": "blocked",
                    "claim_reason": "session_claimed",
                    "blocked_sessions": [
                        {
                            "collector_id": "qimai",
                            "lease_worker_id": "other-worker",
                            "lease_task_id": "other-task",
                        }
                    ],
                },
            )
        return httpx.Response(200, json={"ok": True})

    agent = WorkerAgent(
        WorkerAgentConfig(
            base_url="http://testserver",
            worker_id="worker-agent-blocked",
            capabilities=["qimai"],
            heartbeat_interval_seconds=60,
            transport=httpx.MockTransport(handler),
        )
    )

    try:
        await agent.start()
        assert await agent.run_once() is False
        await agent._heartbeat("online")
    finally:
        await agent.stop()

    heartbeats = [item for item in requests if item["path"].endswith("/heartbeat")]
    assert heartbeats
    metadata = heartbeats[-1]["payload"]["metadata"]
    assert metadata["worker_claim"]["status"] == "blocked"
    assert metadata["worker_claim"]["reason"] == "session_claimed"
    assert metadata["worker_claim"]["blocked_sessions"][0]["lease_worker_id"] == "other-worker"


@pytest.mark.asyncio
async def test_worker_agent_heartbeat_loop_recovers_after_transient_failure() -> None:
    heartbeat_attempts = 0
    recovered = asyncio.Event()

    async def handler(request: httpx.Request) -> httpx.Response:
        nonlocal heartbeat_attempts
        if request.url.path.endswith("/register"):
            return httpx.Response(200, json={"worker_id": "worker-agent-heartbeat"})
        if request.url.path.endswith("/heartbeat"):
            heartbeat_attempts += 1
            if heartbeat_attempts == 1:
                return httpx.Response(500, json={"detail": "transient_failure"})
            recovered.set()
            return httpx.Response(200, json={"ok": True})
        return httpx.Response(200, json={"ok": True})

    agent = WorkerAgent(
        WorkerAgentConfig(
            base_url="http://testserver",
            worker_id="worker-agent-heartbeat",
            capabilities=["steam"],
            heartbeat_interval_seconds=0.1,
            drain_on_shutdown=False,
            transport=httpx.MockTransport(handler),
        )
    )

    try:
        await agent.start()
        await asyncio.wait_for(recovered.wait(), timeout=2)
    finally:
        await agent.stop()

    assert heartbeat_attempts >= 2


@pytest.mark.asyncio
async def test_worker_agent_reports_invalid_claim_payload_as_failed_task() -> None:
    requests: list[dict[str, object]] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        payload = {}
        if request.content:
            import json

            payload = json.loads(request.content.decode())
        requests.append({"method": request.method, "path": request.url.path, "payload": payload})

        if request.url.path.endswith("/claim-task"):
            return httpx.Response(
                200,
                json={
                    "worker_id": "worker-agent-invalid-claim",
                    "task_id": "broken-claim-task",
                    "claim_status": "claimed",
                    "task": {"id": "broken-claim-task"},
                    "pipeline": {"name": "broken-pipeline"},
                },
            )
        return httpx.Response(200, json={"ok": True})

    agent = WorkerAgent(
        WorkerAgentConfig(
            base_url="http://testserver",
            worker_id="worker-agent-invalid-claim",
            capabilities=["steam"],
            heartbeat_interval_seconds=60,
            drain_on_shutdown=False,
            transport=httpx.MockTransport(handler),
        )
    )

    try:
        await agent.start()
        assert await agent.run_once() is True
    finally:
        await agent.stop()

    fail_requests = [item for item in requests if item["path"].endswith("/tasks/broken-claim-task/fail")]
    assert fail_requests
    fail_payload = fail_requests[-1]["payload"]
    assert fail_payload["result"]["invalid_claim"] is True
    assert "Invalid claim payload" in fail_payload["error"]


@pytest.mark.asyncio
async def test_worker_agent_request_stop_drains_current_task_before_exit() -> None:
    import src.web.app as app_module

    global _slow_collector_started, _slow_collector_continue

    _slow_collector_started = asyncio.Event()
    _slow_collector_continue = asyncio.Event()

    event_service = InMemoryTaskEventService()
    scheduler = Scheduler(
        max_concurrent=1,
        task_repo=InMemoryTaskRepository(),
        task_event_service=event_service,
        task_artifact_service=InMemoryTaskArtifactService(),
        task_checkpoint_service=InMemoryTaskCheckpointService(),
        execution_backend="worker_claim",
    )
    scheduler._started = True
    scheduler._pipelines["worker_agent_slow_pipeline"] = (
        Pipeline("worker_agent_slow_pipeline")
        .add_collector("worker_agent_slow_test")
        .add_storage("sqlalchemy")
    )
    task_id = await scheduler.submit(
        Task(
            id="worker-agent-drain-task",
            name="Worker Agent Drain Task",
            pipeline_name="worker_agent_slow_pipeline",
            collector_name="worker_agent_slow_test",
            targets=[TaskTarget(name="CS2")],
        ),
        pipeline_name="worker_agent_slow_pipeline",
    )

    original_scheduler = app_module.scheduler
    original_worker_registry = app_module._worker_registry
    original_task_service = app_module._task_service
    app_module.scheduler = scheduler
    app_module._worker_registry = None
    app_module._task_service = None

    agent = None
    try:
        app = create_app()
        transport = httpx.ASGITransport(app=app)
        agent = WorkerAgent(
            WorkerAgentConfig(
                base_url="http://testserver",
                worker_id="worker-agent-drain",
                capabilities=["worker_agent_slow_test"],
                heartbeat_interval_seconds=0.5,
                claim_poll_interval_seconds=0.1,
                transport=transport,
            )
        )
        await agent.start()
        runner = asyncio.create_task(agent.run_forever())
        await asyncio.wait_for(_slow_collector_started.wait(), timeout=2)

        agent.request_stop()
        worker_while_draining = await asyncio.wait_for(
            _wait_for_worker_status(app_module, "worker-agent-drain", "draining"),
            timeout=2,
        )

        _slow_collector_continue.set()
        await asyncio.wait_for(runner, timeout=2)
    finally:
        if agent is not None:
            await agent.stop()
        app_module.scheduler = original_scheduler
        app_module._worker_registry = original_worker_registry
        app_module._task_service = original_task_service
        _slow_collector_started = None
        _slow_collector_continue = None

    stored_task = scheduler.get_task(task_id)
    stored_events = await event_service.list_events(task_id)

    assert worker_while_draining is not None
    assert worker_while_draining.status == "draining"
    assert worker_while_draining.current_task_ids == [task_id]
    assert stored_task is not None
    assert stored_task.status == TaskStatus.SUCCESS
    assert any(event.type == "complete" for event in stored_events)


async def _wait_for_worker_status(app_module, worker_id: str, status: str):
    while True:
        worker = await app_module.get_worker_registry().get_worker(worker_id)
        if worker is not None and worker.status == status:
            return worker
        await asyncio.sleep(0.01)


def task_time():
    from datetime import datetime

    return datetime.now()


class _FakeReportGenerator:
    def __init__(self, report: GeneratedReport) -> None:
        self.report = report

    async def generate_excel(self, **kwargs):
        if self.report.excel_path:
            from pathlib import Path

            Path(self.report.excel_path).write_bytes(b"xlsx")
        return self.report
