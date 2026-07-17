"""Tests for task resume / rerun service and API."""

from __future__ import annotations

import asyncio
from typing import Any

import pytest
from fastapi.testclient import TestClient

from src.core.pipeline import PipelineResult
from src.core.scheduler import Scheduler
from src.core.task import Task, TaskStatus
from src.services.task_checkpoint_service import InMemoryTaskCheckpointService
from src.services.task_event_service import InMemoryTaskEventService
from src.services.task_repository import InMemoryTaskRepository
from src.services.task_service import TaskService


class _StaticCapturePipeline:
    """Pipeline-like object that records recovery_checkpoint."""

    def __init__(self, name: str = "resume_test_pipeline") -> None:
        self.name = name
        self.steps: list = []
        self.calls: list[dict[str, Any] | None] = []

    async def execute(self, task: Task, *, recovery_checkpoint=None) -> PipelineResult:
        self.calls.append(recovery_checkpoint if isinstance(recovery_checkpoint, dict) else None)
        return PipelineResult(pipeline_name=self.name, task_id=task.id, success=True)


def test_requeue_from_terminal_does_not_burn_retry_count() -> None:
    task = Task(name="T", max_retries=3)
    task.start()
    task.fail("boom")
    task.retry_count = 2

    assert task.requeue_from_terminal() is True
    assert task.status == TaskStatus.PENDING
    assert task.retry_count == 2
    assert task.error is None
    assert task.error_code is None
    assert task.completed_at is None
    assert task.phase == "pending"
    assert task.progress == 0.0


def test_requeue_from_terminal_rejects_non_failed() -> None:
    task = Task(name="T")
    assert task.requeue_from_terminal() is False
    task.start()
    assert task.requeue_from_terminal() is False
    task.complete()
    assert task.requeue_from_terminal() is False


@pytest.mark.asyncio
async def test_task_service_resume_tags_config_and_requeues() -> None:
    event_service = InMemoryTaskEventService()
    checkpoint_service = InMemoryTaskCheckpointService()
    scheduler = Scheduler(
        max_concurrent=1,
        default_retries=0,
        task_repo=InMemoryTaskRepository(),
        task_event_service=event_service,
        task_checkpoint_service=checkpoint_service,
        execution_backend="worker_claim",
    )
    await scheduler.start()
    try:
        pipeline = _StaticCapturePipeline()
        scheduler.register_pipeline(pipeline)

        task = Task(
            id="resume-svc-1",
            name="Resume Me",
            pipeline_name=pipeline.name,
            max_retries=3,
            retry_count=3,
        )
        task.fail("collect failed")
        with scheduler._lock:
            scheduler._tasks[task.id] = task

        cp = await scheduler.register_task_checkpoint(
            task,
            recovery_level="L1",
            cursor={"stage": "api_reviews", "payload": {"review_cursor": "ABC"}},
            state={"target_order": ["A"], "next_target_index": 0},
        )
        assert cp is not None

        service = TaskService(scheduler)
        resumed = await service.resume(task.id, checkpoint_id=cp.checkpoint_id)

        assert resumed.id == task.id
        assert resumed.status == TaskStatus.PENDING
        assert resumed.retry_count == 3
        assert resumed.config.get("resume_mode") == "resume"
        assert resumed.config.get("resume_checkpoint_id") == cp.checkpoint_id
        assert "force_full_rerun" not in (resumed.config or {})

        events = await event_service.list_events(task.id)
        assert any(e.type == "resume_requested" for e in events)
    finally:
        await scheduler.stop()


@pytest.mark.asyncio
async def test_task_service_rerun_sets_force_full_rerun() -> None:
    event_service = InMemoryTaskEventService()
    scheduler = Scheduler(
        max_concurrent=1,
        default_retries=0,
        task_repo=InMemoryTaskRepository(),
        task_event_service=event_service,
        task_checkpoint_service=InMemoryTaskCheckpointService(),
        execution_backend="worker_claim",
    )
    await scheduler.start()
    try:
        pipeline = _StaticCapturePipeline()
        scheduler.register_pipeline(pipeline)
        task = Task(
            id="rerun-svc-1",
            name="Rerun Me",
            pipeline_name=pipeline.name,
            max_retries=2,
            retry_count=2,
            config={"resume_checkpoint_id": "stale"},
        )
        task.fail("timeout")
        with scheduler._lock:
            scheduler._tasks[task.id] = task

        service = TaskService(scheduler)
        reran = await service.rerun(task.id, reset_retry_count=True)

        assert reran.status == TaskStatus.PENDING
        assert reran.retry_count == 0
        assert reran.config.get("force_full_rerun") is True
        assert reran.config.get("resume_mode") == "rerun"
        assert "resume_checkpoint_id" not in (reran.config or {})

        events = await event_service.list_events(task.id)
        assert any(e.type == "rerun_requested" for e in events)
    finally:
        await scheduler.stop()


@pytest.mark.asyncio
async def test_task_service_resume_rejects_non_failed_and_missing() -> None:
    scheduler = Scheduler(
        max_concurrent=1,
        default_retries=0,
        task_repo=InMemoryTaskRepository(),
        execution_backend="worker_claim",
    )
    await scheduler.start()
    try:
        pipeline = _StaticCapturePipeline()
        scheduler.register_pipeline(pipeline)
        task = Task(id="ok-1", name="OK", pipeline_name=pipeline.name)
        with scheduler._lock:
            scheduler._tasks[task.id] = task

        service = TaskService(scheduler)
        with pytest.raises(KeyError):
            await service.resume("missing")
        with pytest.raises(ValueError, match="status"):
            await service.resume(task.id)
        with pytest.raises(ValueError, match="status"):
            await service.rerun(task.id)

        task.fail("x")
        with pytest.raises(ValueError, match="checkpoint not found"):
            await service.resume(task.id, checkpoint_id="no-such-checkpoint")
    finally:
        await scheduler.stop()


@pytest.mark.asyncio
async def test_execute_honors_force_full_rerun_and_resume_checkpoint_id() -> None:
    checkpoint_service = InMemoryTaskCheckpointService()
    scheduler = Scheduler(
        max_concurrent=1,
        default_retries=0,
        task_repo=InMemoryTaskRepository(),
        task_checkpoint_service=checkpoint_service,
    )
    scheduler._semaphore = asyncio.Semaphore(1)

    task = Task(
        id="exec-resume-1",
        name="Exec Resume",
        pipeline_name="p",
        max_retries=0,
    )
    older = await scheduler.register_task_checkpoint(
        task,
        recovery_level="L1",
        cursor={"stage": "api_reviews", "payload": {"review_cursor": "OLD"}},
    )
    preferred = await scheduler.register_task_checkpoint(
        task,
        recovery_level="L1",
        cursor={"stage": "api_reviews", "payload": {"review_cursor": "NEW"}},
    )
    assert older is not None and preferred is not None

    # 1) default preferred
    pipeline = _StaticCapturePipeline(name="p")
    await scheduler._execute_task(task, pipeline)
    assert pipeline.calls[-1] is not None
    assert pipeline.calls[-1]["checkpoint_id"] == preferred.checkpoint_id

    # 2) explicit older checkpoint
    task.status = TaskStatus.FAILED
    task.error = "again"
    task.phase = "failed"
    assert task.requeue_from_terminal() is True
    task.config = {
        **(task.config or {}),
        "resume_mode": "resume",
        "resume_checkpoint_id": older.checkpoint_id,
    }
    await scheduler._execute_task(task, pipeline)
    assert pipeline.calls[-1] is not None
    assert pipeline.calls[-1]["checkpoint_id"] == older.checkpoint_id
    assert "resume_checkpoint_id" not in (task.config or {})

    # 3) force full rerun
    task.status = TaskStatus.FAILED
    task.error = "again"
    task.phase = "failed"
    assert task.requeue_from_terminal() is True
    task.config = {**(task.config or {}), "force_full_rerun": True, "resume_mode": "rerun"}
    await scheduler._execute_task(task, pipeline)
    assert pipeline.calls[-1] is None
    assert "force_full_rerun" not in (task.config or {})


def test_resume_and_rerun_api_status_codes(monkeypatch) -> None:
    from src.web import app as app_module
    from src.web.app import app

    class _FakeService:
        def __init__(self) -> None:
            self.tasks: dict[str, Task] = {}
            failed = Task(id="api-resume-1", name="API Resume", pipeline_name="p")
            failed.fail("boom")
            self.tasks[failed.id] = failed

        async def resume(self, task_id: str, *, checkpoint_id=None, reset_retry_count=False):
            task = self.tasks.get(task_id)
            if task is None:
                raise KeyError(task_id)
            if task.status != TaskStatus.FAILED:
                raise ValueError(f"cannot resume task in status {task.status.value}")
            task.requeue_from_terminal()
            if checkpoint_id:
                task.config = {**(task.config or {}), "resume_checkpoint_id": checkpoint_id}
            return task

        async def rerun(self, task_id: str, *, reset_retry_count=False):
            task = self.tasks.get(task_id)
            if task is None:
                raise KeyError(task_id)
            if task.status != TaskStatus.FAILED:
                raise ValueError(f"cannot rerun task in status {task.status.value}")
            task.requeue_from_terminal()
            task.config = {**(task.config or {}), "force_full_rerun": True}
            return task

    fake = _FakeService()
    monkeypatch.setattr(app_module, "get_task_service", lambda: fake)

    with TestClient(app) as client:
        missing = client.post("/api/tasks/no-such/resume", json={})
        assert missing.status_code == 404

        ok = client.post("/api/tasks/api-resume-1/resume", json={})
        assert ok.status_code == 200, ok.text
        assert ok.json()["id"] == "api-resume-1"
        assert ok.json()["status"] == "pending"

        conflict = client.post("/api/tasks/api-resume-1/resume", json={})
        assert conflict.status_code == 409

        live = fake.tasks["api-resume-1"]
        live.fail("again")
        rerun = client.post("/api/tasks/api-resume-1/rerun", json={})
        assert rerun.status_code == 200, rerun.text
        assert rerun.json()["status"] == "pending"
