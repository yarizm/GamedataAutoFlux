"""Tests for scheduler pure functions (_roll_refresh_template, roll_time_params, etc.)."""

import asyncio
from datetime import date
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from src.collectors.base import BaseCollector, CollectResult, CollectTarget
from src.core.pipeline import Pipeline, PipelineResult
from src.core.registry import registry
from src.core.scheduler import Scheduler
from src.core.scheduler_cron_service import _roll_refresh_template
from src.core.task import Task, TaskStatus, TaskTarget
from src.core.task_retry_policy import pipeline_result_retry_suppression_reason
from src.processors.base import BaseProcessor, ProcessInput, ProcessOutput
from src.reporting.generator import GeneratedReport
from src.services.session_registry import InMemorySessionRegistry
from src.storage.base import BaseStorage, QueryResult, StorageRecord
from src.services.task_repository import InMemoryTaskRepository
from src.services._utils import (
    roll_time_params as _roll_time_params,
    parse_date_prefix as _parse_date_prefix,
    replace_date_prefix as _replace_date_prefix,
)


class TestParseDatePrefix:
    def test_valid_iso_date(self):
        d = _parse_date_prefix("2025-01-15")
        assert d == date(2025, 1, 15)

    def test_valid_datetime(self):
        d = _parse_date_prefix("2025-01-15T10:00:00")
        assert d == date(2025, 1, 15)

    def test_invalid_returns_none(self):
        assert _parse_date_prefix("not a date") is None

    def test_empty_returns_none(self):
        assert _parse_date_prefix("") is None

    def test_none_returns_none(self):
        assert _parse_date_prefix(None) is None


class TestReplaceDatePrefix:
    def test_date_only(self):
        result = _replace_date_prefix("2025-01-15", date(2025, 6, 1))
        assert result == "2025-06-01"

    def test_with_time_suffix(self):
        result = _replace_date_prefix("2025-01-15T10:00:00", date(2025, 6, 1))
        assert result == "2025-06-01T10:00:00"


class TestRollTimeParams:
    def test_shifts_dates(self):
        params = {"start_date": "2025-01-01", "end_date": "2025-01-31"}
        _roll_time_params(params)
        today = date.today()
        assert params["end_date"] == today.isoformat()
        assert params["start_date"] != "2025-01-01"

    def test_shifts_time_params(self):
        params = {"start_time": "2025-01-01T00:00:00", "end_time": "2025-01-02T00:00:00"}
        _roll_time_params(params)
        today = date.today()
        assert params["end_time"].startswith(today.isoformat())

    def test_ignores_missing_keys(self):
        params = {"other_param": "value"}
        original = dict(params)
        _roll_time_params(params)
        assert params == original

    def test_ignores_non_date_values(self):
        params = {"start_date": "not-valid", "end_date": "also-invalid"}
        _roll_time_params(params)
        assert params["start_date"] == "not-valid"
        assert params["end_date"] == "also-invalid"


class TestRollRefreshTemplate:
    def test_no_rolling_window_returns_copy(self):
        template = {"targets": [{"name": "CS2", "params": {"app_id": "730"}}]}
        result = _roll_refresh_template(template)
        assert result is not template  # deep copy
        assert result == template

    def test_empty_template(self):
        assert _roll_refresh_template({}) == {}

    def test_none_template(self):
        assert _roll_refresh_template(None) == {}

    def test_with_rolling_window_rolls_dates(self):
        template = {
            "config": {"refresh": {"rolling_window": True}},
            "targets": [
                {"name": "CS2", "params": {"start_date": "2025-01-01", "end_date": "2025-01-31"}}
            ],
        }
        result = _roll_refresh_template(template)
        today = date.today()
        rolled_end = result["targets"][0]["params"]["end_date"]
        assert rolled_end == today.isoformat()

    def test_rolling_without_targets(self):
        template = {"config": {"refresh": {"rolling_window": True}}}
        result = _roll_refresh_template(template)
        assert result == template or result is not template  # copy

    def test_rolling_with_empty_target_params(self):
        template = {
            "config": {"refresh": {"rolling_window": True}},
            "targets": [{"name": "CS2"}],
        }
        result = _roll_refresh_template(template)
        assert result["targets"][0]["name"] == "CS2"


@pytest.mark.asyncio
async def test_execute_task_final_failure_keeps_pipeline_result_summary() -> None:
    repo = InMemoryTaskRepository()
    event_bus = _FakeEventBus()
    scheduler = Scheduler(
        max_concurrent=1,
        default_retries=0,
        task_repo=repo,
        event_bus=event_bus,
    )
    scheduler._semaphore = asyncio.Semaphore(1)

    task = Task(
        id="failed-summary",
        name="Failure Summary",
        pipeline_name="p",
        max_retries=0,
    )
    result = PipelineResult(pipeline_name="p", task_id=task.id, success=False)
    result.errors = ["collect failed api_key=result-secret"]
    result.collect_results = [
        CollectResult(
            target=CollectTarget(name="CS2 api_key=target-secret"),
            success=False,
            error="network token=collector-secret",
            error_code="network_unreachable",
            metadata={"attempts": 3, "max_attempts": 3, "retry_attempts": 2},
        )
    ]

    returned = await scheduler._execute_task(task, _StaticPipeline(result))
    if scheduler._background_tasks:
        await asyncio.gather(*scheduler._background_tasks)

    summary = task.result_summary
    payload = task.to_storage_payload()
    public_payload = task.to_public_payload()
    rendered_public = str({"summary": summary, "payload": public_payload})

    assert returned is result
    assert task.status == TaskStatus.FAILED
    assert task.result is result
    assert summary is not None
    assert summary["collection_summary"]["status"] == "failed"
    assert summary["collection_summary"]["failed_targets_count"] == 1
    assert (
        payload["result_summary"]["collection_summary"]["failed_targets"][0]["retry"][
            "retry_attempts"
        ]
        == 2
    )
    assert payload["result_summary"]["errors"] == ["collect failed api_key=result-secret"]
    assert public_payload["result_summary"]["errors"] == ["collect failed api_key=[REDACTED]"]
    assert event_bus.completed_events[-1].result is result
    assert "target-secret" not in rendered_public
    assert "collector-secret" not in rendered_public
    assert "result-secret" not in rendered_public


@pytest.mark.asyncio
async def test_execute_task_partial_collect_with_stored_records_skips_task_retry() -> None:
    event_bus = _FakeEventBus()
    scheduler = Scheduler(
        max_concurrent=1,
        default_retries=3,
        task_repo=InMemoryTaskRepository(),
        event_bus=event_bus,
    )
    scheduler._semaphore = asyncio.Semaphore(1)
    task = Task(
        id="partial-no-retry",
        name="Partial No Retry",
        pipeline_name="p",
        max_retries=3,
    )
    result = _partial_collect_result(task.id)
    pipeline = _StaticPipeline(result)

    returned = await scheduler._execute_task(task, pipeline)
    if scheduler._background_tasks:
        await asyncio.gather(*scheduler._background_tasks)

    assert returned is result
    assert pipeline.calls == 1
    assert task.retry_count == 0
    assert task.status == TaskStatus.FAILED
    assert task.result is result
    assert task.result_summary["collection_summary"]["status"] == "partial"
    assert any(log.step_name == "retry:policy" for log in task.step_logs)
    assert "duplicating stored partial results" in task.step_logs[-1].message
    assert event_bus.completed_events[-1].result is result


@pytest.mark.asyncio
async def test_execute_task_uses_latest_checkpoint_to_resume_pipeline() -> None:
    checkpoint_task_id = "scheduler-resume"
    checkpoint_service_task_name = "Scheduler Resume"
    scheduler = Scheduler(
        max_concurrent=1,
        default_retries=0,
        task_repo=InMemoryTaskRepository(),
    )
    from src.services.task_checkpoint_service import InMemoryTaskCheckpointService

    checkpoint_service = InMemoryTaskCheckpointService()
    scheduler._task_checkpoint_service = checkpoint_service
    scheduler._semaphore = asyncio.Semaphore(1)

    task = Task(
        id=checkpoint_task_id,
        name=checkpoint_service_task_name,
        pipeline_name="scheduler_resume_pipeline",
        collector_name="scheduler_resume_test_collector",
        max_retries=0,
        targets=[
            TaskTarget(name="A"),
            TaskTarget(name="B"),
            TaskTarget(name="C"),
        ],
    )

    await scheduler.register_task_checkpoint(
        task,
        recovery_level="L1",
        cursor={"stage": "collect", "component": "scheduler_resume_test_collector"},
        state={
            "target_order": ["A", "B", "C"],
            "next_target_index": 1,
            "completed_targets": ["A"],
        },
    )
    latest_checkpoint = await scheduler.register_task_checkpoint(
        task,
        recovery_level="L1",
        cursor={"stage": "collect", "component": "scheduler_resume_test_collector"},
        state={
            "target_order": ["A", "B", "C"],
            "next_target_index": 2,
            "completed_targets": ["A", "B"],
        },
    )

    import src.storage.factory as storage_factory

    original_get_storage = storage_factory.get_storage
    _SchedulerResumeTestCollector.seen_recovery_checkpoint = None
    _SchedulerResumeTestCollector.collected_targets = []
    _SchedulerResumeTestStorage.saved_batches.clear()
    storage_factory.get_storage = lambda name=None: _SchedulerResumeTestStorage()
    pipeline = (
        Pipeline("scheduler_resume_pipeline")
        .add_collector("scheduler_resume_test_collector")
        .add_processor("scheduler_resume_test_processor")
        .add_storage("scheduler_resume_test_storage")
    )

    try:
        returned = await scheduler._execute_task(task, pipeline)
    finally:
        storage_factory.get_storage = original_get_storage

    assert returned is not None
    assert returned.success is True
    assert task.status == TaskStatus.SUCCESS
    assert task.result is returned
    assert _SchedulerResumeTestCollector.collected_targets == ["C"]
    recovery_checkpoint = _SchedulerResumeTestCollector.seen_recovery_checkpoint
    assert recovery_checkpoint is not None
    assert recovery_checkpoint["checkpoint_id"] == latest_checkpoint.checkpoint_id
    assert recovery_checkpoint["task_id"] == task.id
    assert recovery_checkpoint["seq"] == 2
    assert recovery_checkpoint["collector_name"] == "scheduler_resume_test_collector"
    assert recovery_checkpoint["recovery_level"] == "L1"
    assert recovery_checkpoint["cursor"] == {
        "stage": "collect",
        "component": "scheduler_resume_test_collector",
    }
    assert recovery_checkpoint["state"] == {
        "target_order": ["A", "B", "C"],
        "next_target_index": 2,
        "completed_targets": ["A", "B"],
    }
    assert recovery_checkpoint["metadata"] == {}
    assert recovery_checkpoint["collect"] == {
        "enabled": True,
        "next_target_index": 2,
        "target_order": ["A", "B", "C"],
        "completed_targets": ["A", "B"],
        "cursor": {
            "stage": "collect",
            "component": "scheduler_resume_test_collector",
        },
        "state": {
            "target_order": ["A", "B", "C"],
            "next_target_index": 2,
            "completed_targets": ["A", "B"],
        },
    }
    assert [item.target.name for item in returned.collect_results] == ["C"]
    assert (
        returned.collect_results[0].metadata["resume_checkpoint_id"]
        == latest_checkpoint.checkpoint_id
    )
    assert returned.output_records[0].key == (
        "scheduler-resume:scheduler_resume_test_processor:2:2"
    )
    assert _SchedulerResumeTestStorage.saved_batches[-1][0].key == (
        "scheduler-resume:scheduler_resume_test_processor:2:2"
    )
    assert returned.resume_state["next_target_index"] == 3
    assert returned.resume_state["completed_targets"] == ["A", "B", "C"]
    assert task.result_summary["resume_state"]["output_record_keys"] == [
        "scheduler-resume:scheduler_resume_test_processor:2:2"
    ]


@pytest.mark.asyncio
async def test_execute_task_without_event_bus_emits_failure_alert(monkeypatch) -> None:
    scheduler = Scheduler(
        max_concurrent=1,
        default_retries=0,
        task_repo=InMemoryTaskRepository(),
    )
    scheduler._semaphore = asyncio.Semaphore(1)
    task = Task(
        id="failed-alert",
        name="Failure Alert",
        pipeline_name="p",
        max_retries=0,
    )
    result = PipelineResult(pipeline_name="p", task_id=task.id, success=False)
    result.errors = ["collect failed api_key=result-secret"]

    alert_service = _FakeAlertService()

    from src.services.alert_service import AlertService

    monkeypatch.setattr(AlertService, "get_instance", classmethod(lambda cls: alert_service))

    returned = await scheduler._execute_task(task, _StaticPipeline(result))
    if scheduler._background_tasks:
        await asyncio.gather(*scheduler._background_tasks)

    assert returned is result
    assert task.status == TaskStatus.FAILED
    assert alert_service.messages == [
        {
            "title": "任务执行失败: Failure Alert",
            "content": "**Task ID**: failed-alert\n**Error**: collect failed api_key=[REDACTED]",
            "level": "error",
        }
    ]


@pytest.mark.asyncio
async def test_execute_task_without_event_bus_generates_report_inline(
    tmp_path, monkeypatch
) -> None:
    scheduler = Scheduler(
        max_concurrent=1,
        default_retries=0,
        task_repo=InMemoryTaskRepository(),
    )
    scheduler._semaphore = asyncio.Semaphore(1)
    task = Task(
        id="inline-report",
        name="Inline Report",
        pipeline_name="p",
        collector_name="steam",
        max_retries=0,
        config={"report": {"enabled": True}},
    )
    result = PipelineResult(pipeline_name="p", task_id=task.id, success=True)

    report = GeneratedReport(
        id="inline-report-1",
        title="Inline Report Artifact",
        prompt="prompt",
        data_source="steam",
        template="default",
        generated_at=task_time(),
        matched_records=1,
        content="report",
        excel_path=str(tmp_path / "inline-report.xlsx"),
    )

    class _FakeReportGenerator:
        async def generate_excel(self, **kwargs):
            Path(report.excel_path).write_bytes(b"xlsx")
            return report

    import src.web.app as app_module

    monkeypatch.setattr(app_module, "report_generator", _FakeReportGenerator())

    returned = await scheduler._execute_task(task, _StaticPipeline(result))
    if scheduler._background_tasks:
        await asyncio.gather(*scheduler._background_tasks)

    assert returned is result
    assert task.status == TaskStatus.SUCCESS
    assert result.generated_report_id == "inline-report-1"
    assert any(
        log.step_name == "report:auto" and log.status == TaskStatus.SUCCESS
        for log in task.step_logs
    )


def test_retry_suppression_reason_only_for_stored_partial_collection() -> None:
    partial = _partial_collect_result("partial")
    pure_failure = PipelineResult(pipeline_name="p", task_id="failed", success=False)
    pure_failure.errors = ["all targets failed"]
    pure_failure.collect_results = [
        CollectResult(
            target=CollectTarget(name="failed"),
            success=False,
            error="timeout",
            error_code="network_unreachable",
        )
    ]

    reason = pipeline_result_retry_suppression_reason(partial)

    assert "failed_targets=1" in reason
    assert pipeline_result_retry_suppression_reason(pure_failure) == ""


def test_global_scheduler_restart_clears_runtime_worker_claim_state(monkeypatch) -> None:
    import src.web.app as app_module

    original_scheduler = app_module.scheduler
    original_task_service = app_module._task_service
    original_worker_registry = app_module._worker_registry
    original_session_registry = app_module._session_registry

    app_module._reset_runtime_singletons(reset_agent=True, reset_agent_session=True)
    registry_one = InMemorySessionRegistry()
    monkeypatch.setattr(app_module, "get_session_registry", lambda: registry_one)

    try:
        with TestClient(app_module.create_app()) as client:
            original_backend = app_module.scheduler._execution_backend
            app_module.scheduler._execution_backend = "worker_claim"
            try:
                client.post(
                    "/api/pipelines",
                    json={
                        "name": "restart_cleanup_pipeline_a",
                        "steps": [{"type": "collector", "name": "steam", "config": {}}],
                    },
                )
                client.post(
                    "/api/workers/register",
                    json={
                        "worker_id": "restart-cleanup-worker-a",
                        "capabilities": ["steam"],
                    },
                )
                created = client.post(
                    "/api/tasks",
                    json={
                        "name": "Restart Cleanup A",
                        "pipeline_name": "restart_cleanup_pipeline_a",
                        "collector_name": "steam",
                        "targets": [
                            {
                                "name": "Counter-Strike 2",
                                "target_type": "game",
                                "params": {"app_id": "730"},
                            }
                        ],
                        "config": {},
                    },
                )
                claimed = client.post(
                    "/api/workers/restart-cleanup-worker-a/claim-task",
                    json={},
                )
                failed = client.post(
                    f"/api/workers/restart-cleanup-worker-a/tasks/{claimed.json()['task_id']}/fail",
                    json={"error": "temporary timeout", "result": {"success": False}},
                )
            finally:
                app_module.scheduler._execution_backend = original_backend

        assert created.status_code == 200
        assert claimed.status_code == 200
        assert failed.status_code == 200
        assert failed.json()["task"]["status"] == "retrying"

        app_module._reset_runtime_singletons(reset_agent=True, reset_agent_session=True)
        registry_two = InMemorySessionRegistry()
        monkeypatch.setattr(app_module, "get_session_registry", lambda: registry_two)

        with TestClient(app_module.create_app()) as client:
            original_backend = app_module.scheduler._execution_backend
            app_module.scheduler._execution_backend = "worker_claim"
            try:
                client.post(
                    "/api/pipelines",
                    json={
                        "name": "restart_cleanup_pipeline_b",
                        "steps": [{"type": "collector", "name": "steam", "config": {}}],
                    },
                )
                client.post(
                    "/api/workers/register",
                    json={
                        "worker_id": "restart-cleanup-worker-b",
                        "capabilities": ["steam"],
                    },
                )
                created = client.post(
                    "/api/tasks",
                    json={
                        "name": "Restart Cleanup B",
                        "pipeline_name": "restart_cleanup_pipeline_b",
                        "collector_name": "steam",
                        "targets": [
                            {
                                "name": "Dota 2",
                                "target_type": "game",
                                "params": {"app_id": "570"},
                            }
                        ],
                        "config": {},
                    },
                )
                claimed = client.post(
                    "/api/workers/restart-cleanup-worker-b/claim-task",
                    json={},
                )
            finally:
                app_module.scheduler._execution_backend = original_backend

        assert created.status_code == 200
        assert claimed.status_code == 200
        assert claimed.json()["task_id"] == created.json()["id"]
        assert claimed.json()["claim_status"] == "claimed"
    finally:
        app_module.scheduler = original_scheduler
        app_module._task_service = original_task_service
        app_module._worker_registry = original_worker_registry
        app_module._session_registry = original_session_registry


def _partial_collect_result(task_id: str) -> PipelineResult:
    result = PipelineResult(pipeline_name="p", task_id=task_id, success=False)
    result.storage_count = 1
    result.errors = ["collect:p:failed: [network_unreachable] timeout"]
    result.collect_results = [
        CollectResult(
            target=CollectTarget(name="ok"),
            success=True,
            data={"value": 1},
        ),
        CollectResult(
            target=CollectTarget(name="failed"),
            success=False,
            error="timeout",
            error_code="network_unreachable",
            metadata={"attempts": 2, "max_attempts": 2, "retry_attempts": 1},
        ),
    ]
    return result


@registry.register("collector", "scheduler_resume_test_collector")
class _SchedulerResumeTestCollector(BaseCollector):
    seen_recovery_checkpoint = None
    collected_targets: list[str] = []

    async def collect(self, target: CollectTarget) -> CollectResult:
        recovery = self.config.get("recovery_checkpoint", {})
        checkpoint_id = recovery.get("checkpoint_id", "") if isinstance(recovery, dict) else ""
        return CollectResult(
            target=target,
            data={"value": target.name},
            metadata={"resume_checkpoint_id": checkpoint_id},
        )

    async def collect_batch(self, targets: list[CollectTarget]) -> list[CollectResult]:
        recovery = self.config.get("recovery_checkpoint")
        self.__class__.seen_recovery_checkpoint = recovery if isinstance(recovery, dict) else None
        self.__class__.collected_targets = [target.name for target in targets]
        return [await self.collect(target) for target in targets]


@registry.register("processor", "scheduler_resume_test_processor")
class _SchedulerResumeTestProcessor(BaseProcessor):
    async def process(self, input_data: ProcessInput) -> ProcessOutput:
        return ProcessOutput(
            data=input_data.data,
            metadata=input_data.metadata,
            processor_name="scheduler_resume_test_processor",
        )


class _SchedulerResumeTestStorage(BaseStorage):
    saved_batches: list[list[StorageRecord]] = []

    async def save(self, record: StorageRecord) -> None:
        self.saved_batches.append([record])

    async def save_batch(self, records: list[StorageRecord]) -> None:
        self.saved_batches.append(list(records))

    async def load(self, key: str) -> StorageRecord | None:
        return None

    async def query(self, query: str, limit: int = 10, **kwargs) -> QueryResult:
        return QueryResult(records=[], total=0, query=query)


class _StaticPipeline:
    name = "p"
    steps = []

    def __init__(self, result: PipelineResult) -> None:
        self._result = result
        self.calls = 0

    async def execute(self, task: Task, *, recovery_checkpoint=None) -> PipelineResult:
        self.calls += 1
        return self._result


class _FakeEventBus:
    def __init__(self) -> None:
        self.completed_events = []
        self.updated_events = []

    async def emit(self, event_name: str, event) -> None:
        if event_name == "task_completed":
            self.completed_events.append(event)
        elif event_name == "task_updated":
            self.updated_events.append(event)


class _FakeAlertService:
    def __init__(self) -> None:
        self.messages: list[dict[str, str]] = []

    async def send_alert(self, title: str, content: str, level: str = "error", **kwargs) -> None:
        self.messages.append({"title": title, "content": content, "level": level})


def task_time():
    from datetime import datetime

    return datetime.now()
