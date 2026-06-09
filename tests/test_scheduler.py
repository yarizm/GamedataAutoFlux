"""Tests for scheduler pure functions (_roll_refresh_template, roll_time_params, etc.)."""

import asyncio
from datetime import date

import pytest

from src.collectors.base import CollectResult, CollectTarget
from src.core.pipeline import PipelineResult
from src.core.scheduler import (
    Scheduler,
    _pipeline_result_retry_suppression_reason,
    _roll_refresh_template,
)
from src.core.task import Task, TaskStatus
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
    assert payload["result_summary"]["collection_summary"]["failed_targets"][0]["retry"][
        "retry_attempts"
    ] == 2
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

    reason = _pipeline_result_retry_suppression_reason(partial)

    assert "failed_targets=1" in reason
    assert _pipeline_result_retry_suppression_reason(pure_failure) == ""


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


class _StaticPipeline:
    name = "p"

    def __init__(self, result: PipelineResult) -> None:
        self._result = result
        self.calls = 0

    async def execute(self, task: Task) -> PipelineResult:
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
