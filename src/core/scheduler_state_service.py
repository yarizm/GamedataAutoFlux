"""Persistence and restore coordination for scheduler runtime state."""

from __future__ import annotations

from typing import Any, Awaitable, Callable

from loguru import logger

from src.core.events import TaskUpdatedEvent
from src.core.pipeline import Pipeline
from src.core.sensitive import redact_sensitive_text
from src.core.task import Task, TaskStatus
from src.services.cron_repository import CronJobConfig, CronRepository
from src.services.pipeline_repository import PipelineRepository
from src.services.task_repository import TaskRepository
from src.storage.base import BaseStorage, StorageRecord

CreateBackgroundTaskFn = Callable[[Awaitable[Any]], Any]


class SchedulerStateService:
    """Coordinates scheduler snapshot persistence and runtime restore."""

    def __init__(
        self,
        *,
        get_task_repo: Callable[[], TaskRepository | None],
        get_pipeline_repo: Callable[[], PipelineRepository | None],
        get_cron_repo: Callable[[], CronRepository | None],
        get_task_store: Callable[[], BaseStorage | None],
        get_event_bus: Callable[[], Any],
        create_background_task: CreateBackgroundTaskFn,
    ) -> None:
        self._get_task_repo = get_task_repo
        self._get_pipeline_repo = get_pipeline_repo
        self._get_cron_repo = get_cron_repo
        self._get_task_store = get_task_store
        self._get_event_bus = get_event_bus
        self._create_background_task = create_background_task

    async def persist_task(self, task: Task) -> None:
        """Persist a task snapshot and publish the public task update."""
        task_repo = self._get_task_repo()
        task_store = self._get_task_store()

        storage_payload = task.to_storage_payload()
        public_payload = task.to_public_payload()

        if task_repo is not None:
            await task_repo.save(task)
        elif task_store is not None:
            await task_store.save(
                StorageRecord(
                    key=f"task:{task.id}",
                    data=storage_payload,
                    metadata={
                        "kind": "task",
                        "status": task.status.value,
                        "pipeline_name": task.pipeline_name,
                    },
                    source="scheduler",
                    tags=["task", task.status.value],
                )
            )

        event_bus = self._get_event_bus()
        if event_bus is not None:
            self._create_background_task(
                event_bus.emit(
                    "task_updated",
                    TaskUpdatedEvent(
                        task_id=task.id,
                        payload=public_payload,
                        status=task.status.value,
                        pipeline_name=task.pipeline_name,
                    ),
                )
            )
        else:
            try:
                from src.web.routes.ws import manager

                self._create_background_task(
                    manager.broadcast({"type": "task_update", "task": public_payload})
                )
            except Exception as exc:
                logger.debug(f"Failed to broadcast task update: {exc}")

    async def persist_pipeline(self, pipeline: Pipeline) -> None:
        """Persist a pipeline snapshot."""
        pipeline_repo = self._get_pipeline_repo()
        task_store = self._get_task_store()
        if pipeline_repo is not None:
            await pipeline_repo.save(pipeline)
            return
        if task_store is None:
            return
        await task_store.save(
            StorageRecord(
                key=f"pipeline:{pipeline.name}",
                data=pipeline.to_config(),
                metadata={
                    "kind": "pipeline",
                    "pipeline_name": pipeline.name,
                },
                source="scheduler",
                tags=["pipeline"],
            )
        )

    async def persist_cron_job(
        self,
        *,
        name: str,
        pipeline_name: str,
        cron_expr: str,
        task_template: dict[str, Any],
    ) -> None:
        """Persist a cron job snapshot."""
        cron_repo = self._get_cron_repo()
        task_store = self._get_task_store()
        if cron_repo is not None:
            await cron_repo.save(
                CronJobConfig(
                    name=name,
                    pipeline_name=pipeline_name,
                    cron_expr=cron_expr,
                    task_template=task_template,
                )
            )
            return
        if task_store is None:
            return

        refresh = {}
        if isinstance(task_template, dict):
            config = task_template.get("config", {})
            if isinstance(config, dict) and isinstance(config.get("refresh"), dict):
                refresh = config["refresh"]
        await task_store.save(
            StorageRecord(
                key=f"cron:{name}",
                data={
                    "name": name,
                    "pipeline_name": pipeline_name,
                    "cron_expr": cron_expr,
                    "task_template": task_template,
                },
                metadata={
                    "kind": "cron",
                    "pipeline_name": pipeline_name,
                    "refresh_kind": refresh.get("refresh_kind", ""),
                },
                source="scheduler",
                tags=["cron"],
            )
        )

    async def restore_pipelines(self) -> dict[str, Pipeline]:
        """Restore persisted pipelines from repository or task store."""
        pipeline_repo = self._get_pipeline_repo()
        task_store = self._get_task_store()
        restored: dict[str, Pipeline] = {}
        if pipeline_repo is not None:
            pipelines = await pipeline_repo.list_all()
            for pipeline in pipelines:
                restored[pipeline.name] = pipeline
            return restored
        if task_store is None:
            return restored

        result = await task_store.query("key:pipeline:", limit=1000)
        for record in result.records:
            if not isinstance(record.data, dict):
                continue
            try:
                pipeline = Pipeline.from_config(record.data)
            except Exception as exc:
                logger.warning(f"Failed to restore pipeline {record.key}: {exc}")
                continue
            restored[pipeline.name] = pipeline
        return restored

    async def restore_tasks(self) -> list[Task]:
        """Restore persisted tasks and normalize non-terminal tasks to cancelled."""
        task_repo = self._get_task_repo()
        task_store = self._get_task_store()
        tasks: list[Task] = []
        if task_repo is not None:
            tasks = await task_repo.query(limit=1000)
        elif task_store is not None:
            result = await task_store.query("key:task:", limit=1000)
            for record in result.records:
                if not isinstance(record.data, dict):
                    continue
                tasks.append(Task.from_storage_payload(record.data))

        for task in tasks:
            self._normalize_restored_task(task)
        return tasks

    async def restore_cron_jobs(self) -> list[CronJobConfig]:
        """Restore persisted cron job snapshots."""
        cron_repo = self._get_cron_repo()
        task_store = self._get_task_store()
        if cron_repo is not None:
            return await cron_repo.list_all()
        if task_store is None:
            return []

        result = await task_store.query("key:cron:", limit=1000)
        jobs: list[CronJobConfig] = []
        for record in result.records:
            job = _cron_job_from_record(record)
            if job is not None:
                jobs.append(job)
        return jobs

    async def delete_cron_job(self, name: str) -> None:
        """Delete a persisted cron job snapshot when available."""
        cron_repo = self._get_cron_repo()
        task_store = self._get_task_store()
        if cron_repo is not None:
            await cron_repo.delete(name)
            return
        if task_store is not None:
            await task_store.delete(f"cron:{name}")

    @staticmethod
    def _normalize_restored_task(task: Task) -> None:
        if task.status in (TaskStatus.PENDING, TaskStatus.RUNNING, TaskStatus.RETRYING):
            task.cancel()
            task.error = "Recovered from a previous session without a live worker; marked cancelled."


def _cron_job_from_record(record: Any) -> CronJobConfig | None:
    if not isinstance(getattr(record, "data", None), dict):
        return None
    data = record.data
    name = str(data.get("name") or "").strip()
    pipeline_name = str(data.get("pipeline_name") or "").strip()
    cron_expr = str(data.get("cron_expr") or "").strip()
    task_template = data.get("task_template", {})
    if not name or not pipeline_name or not cron_expr:
        return None
    if not isinstance(task_template, dict):
        task_template = {}
    return CronJobConfig(
        name=name,
        pipeline_name=pipeline_name,
        cron_expr=cron_expr,
        task_template=task_template,
    )


def on_background_task_done(task: Any, tasks_set: set[Any]) -> None:
    """Callback for fire-and-forget tasks to log exceptions."""
    tasks_set.discard(task)
    if task.cancelled():
        return
    exc = task.exception()
    if exc is not None:
        logger.error(f"Background task failed: {redact_sensitive_text(str(exc))}")
