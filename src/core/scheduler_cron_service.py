"""Cron job scheduling extracted from Scheduler.

Manages cron job lifecycle (add/remove/list/execute) for the scheduler.
"""

from __future__ import annotations

import copy
from datetime import datetime
from typing import Any, Awaitable, Callable

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from loguru import logger

from src.core.config import get as get_config
from src.core.task import Task


SubmitTaskFn = Callable[..., Awaitable[str]]


class SchedulerCronService:
    """Cron job lifecycle manager.

    Extracted from Scheduler to separate cron scheduling from task orchestration.
    Owns the APScheduler instance and its lifecycle.
    """

    def __init__(
        self,
        *,
        submit_task: SubmitTaskFn,
        persist_cron_job: Callable[..., Awaitable[None]],
        delete_cron_job: Callable[[str], Awaitable[None]],
        restore_cron_jobs: Callable[[], Awaitable[list[Any]]],
        create_background_task: Callable[[Any], Any] | None = None,
    ) -> None:
        self._cron_scheduler: AsyncIOScheduler | None = None
        self._submit_task = submit_task
        self._persist_cron_job = persist_cron_job
        self._delete_cron_job = delete_cron_job
        self._restore_cron_jobs = restore_cron_jobs
        self._create_background_task = create_background_task or (lambda coro: None)
        self._job_count = 0

    def start(self) -> None:
        """Create and start the APScheduler backend."""
        self._cron_scheduler = AsyncIOScheduler()
        self._cron_scheduler.start()

    def shutdown(self) -> None:
        """Shutdown the APScheduler backend."""
        if self._cron_scheduler is not None:
            self._cron_scheduler.shutdown(wait=False)
            self._cron_scheduler = None
        self._job_count = 0

    @property
    def job_count(self) -> int:
        """Return the number of active cron jobs (after dedup)."""
        if self._cron_scheduler is None:
            return 0
        return len(self._cron_scheduler.get_jobs())

    def add_cron_job(
        self,
        name: str,
        pipeline_name: str,
        cron_expr: str,
        task_template: dict[str, Any] | None = None,
        persist: bool = True,
    ) -> str:
        """Add a cron-driven scheduled task.

        Args:
            name: Cron job name
            pipeline_name: Pipeline name to execute
            cron_expr: Cron expression (e.g. "0 8 * * *")
            task_template: Task template parameters
            persist: Whether to persist the job for recovery

        Returns:
            APScheduler job ID
        """
        if self._cron_scheduler is None:
            raise RuntimeError("SchedulerCronService not started")

        parts = cron_expr.strip().split()
        if len(parts) != 5:
            raise ValueError(f"Invalid cron expression: {cron_expr}")

        trigger = CronTrigger(
            minute=parts[0],
            hour=parts[1],
            day=parts[2],
            month=parts[3],
            day_of_week=parts[4],
        )

        job = self._cron_scheduler.add_job(
            self._cron_execute,
            trigger=trigger,
            id=name,
            name=name,
            kwargs={
                "pipeline_name": pipeline_name,
                "task_template": task_template or {},
                "job_name": name,
            },
            replace_existing=True,
        )

        logger.info(f"Cron job added: {name} → [{cron_expr}] → Pipeline [{pipeline_name}]")
        if persist:
            self._create_background_task(
                self._persist_cron_job(
                    name=name,
                    pipeline_name=pipeline_name,
                    cron_expr=cron_expr,
                    task_template=task_template or {},
                )
            )
        return job.id

    def remove_cron_job(self, name: str) -> bool:
        """Remove a cron job."""
        if self._cron_scheduler is None:
            return False
        try:
            self._cron_scheduler.remove_job(name)
            self._create_background_task(self._delete_cron_job(name))
            logger.info(f"Cron job removed: {name}")
            return True
        except Exception:
            return False

    def list_cron_jobs(self) -> list[dict[str, Any]]:
        """List all cron jobs with their metadata."""
        if self._cron_scheduler is None:
            return []
        jobs = self._cron_scheduler.get_jobs()
        return [
            {
                "id": job.id,
                "name": job.name,
                "next_run": job.next_run_time.isoformat() if job.next_run_time else None,
                "trigger": str(job.trigger),
                "pipeline_name": (job.kwargs or {}).get("pipeline_name", ""),
                "task_template": (job.kwargs or {}).get("task_template", {}),
                "kind": (
                    (job.kwargs or {}).get("task_template", {}).get("config", {}).get("refresh", {})
                    or {}
                ).get("refresh_kind", "cron"),
            }
            for job in jobs
        ]

    def load_cron_jobs_from_config(self) -> None:
        """Load enabled cron jobs from settings.yaml config."""
        cron_jobs = get_config("scheduler.cron_jobs", [])
        if not isinstance(cron_jobs, list):
            logger.warning("scheduler.cron_jobs config format invalid, expected list")
            return

        for job in cron_jobs:
            if not isinstance(job, dict):
                continue
            if job.get("enabled", True) is False:
                continue

            name = job.get("name")
            pipeline_name = job.get("pipeline")
            cron_expr = job.get("cron")
            task_template = job.get("task_template", {})
            if not name or not pipeline_name or not cron_expr:
                logger.warning(f"Skipping invalid cron config: {job}")
                continue

            try:
                self.add_cron_job(
                    name=name,
                    pipeline_name=pipeline_name,
                    cron_expr=cron_expr,
                    task_template=task_template,
                    persist=False,
                )
            except Exception as exc:
                logger.warning(f"Failed to load cron job: {name} - {exc}")

    async def restore_cron_jobs_from_store(self) -> None:
        """Restore cron jobs from persistent storage."""
        jobs = await self._restore_cron_jobs()
        for job in jobs:
            self._restore_cron_job(
                name=job.name,
                pipeline_name=job.pipeline_name,
                cron_expr=job.cron_expr,
                task_template=job.task_template,
            )

    def _restore_cron_job(
        self,
        *,
        name: str,
        pipeline_name: str,
        cron_expr: str,
        task_template: Any,
    ) -> None:
        try:
            self.add_cron_job(
                name=name,
                pipeline_name=pipeline_name,
                cron_expr=cron_expr,
                task_template=task_template if isinstance(task_template, dict) else {},
                persist=False,
            )
        except Exception as exc:
            logger.warning(f"Failed to restore cron job {name}: {exc}")

    async def _cron_execute(
        self,
        pipeline_name: str,
        task_template: dict[str, Any],
        job_name: str,
    ) -> None:
        """Cron trigger entry point — creates and submits a task."""
        logger.info(f"Cron job triggered: {job_name}")
        task = Task(
            name=f"[Cron] {job_name} - {datetime.now().strftime('%Y%m%d_%H%M')}",
            pipeline_name=pipeline_name,
            **_roll_refresh_template(task_template),
        )
        try:
            await self._submit_task(task, pipeline_name=pipeline_name)
        except Exception as e:
            logger.error(f"Cron job submit failed: {job_name} - {e}")


def _roll_refresh_template(task_template: dict[str, Any]) -> dict[str, Any]:
    """Apply rolling window time parameter refresh to task template."""
    template = copy.deepcopy(task_template or {})
    config = template.get("config", {})
    refresh = config.get("refresh", {}) if isinstance(config, dict) else {}
    if not isinstance(refresh, dict) or not refresh.get("rolling_window"):
        return template

    from src.services._utils import roll_time_params

    for target in template.get("targets", []) or []:
        if not isinstance(target, dict):
            continue
        params = target.get("params")
        if isinstance(params, dict):
            roll_time_params(params)
    return template
