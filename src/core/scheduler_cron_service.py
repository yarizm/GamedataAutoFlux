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
from src.core.cron_schedule import (
    build_cron_public_view,
    default_timezone,
    resolve_timezone,
    validate_cron_expr,
)
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
        *,
        enabled: bool = True,
        timezone: str | None = None,
        schedule_meta: dict[str, Any] | None = None,
        description: str = "",
    ) -> str:
        """Add a cron-driven scheduled task.

        Returns:
            APScheduler job ID
        """
        if self._cron_scheduler is None:
            raise RuntimeError("SchedulerCronService not started")

        expr = validate_cron_expr(cron_expr)
        tz_name = (timezone or default_timezone()).strip() or default_timezone()
        tz = resolve_timezone(tz_name)
        template = task_template if isinstance(task_template, dict) else {}
        meta = schedule_meta if isinstance(schedule_meta, dict) else {}
        if not meta.get("human_label"):
            from src.core.cron_schedule import describe_cron

            meta = {
                **meta,
                "human_label": describe_cron(expr, timezone=tz_name),
                "timezone": tz_name,
                "mode": meta.get("mode") or "cron",
            }

        trigger = CronTrigger.from_crontab(expr, timezone=tz)
        job = self._cron_scheduler.add_job(
            self._cron_execute,
            trigger=trigger,
            id=name,
            name=name,
            kwargs={
                "pipeline_name": pipeline_name,
                "task_template": template,
                "job_name": name,
                "cron_expr": expr,
                "timezone": tz_name,
                "enabled": enabled,
                "schedule_meta": meta,
                "description": description or "",
            },
            replace_existing=True,
            misfire_grace_time=int(get_config("scheduler.cron_misfire_grace_seconds", 300) or 300),
        )
        if not enabled:
            try:
                job.pause()
            except Exception:
                pass

        logger.info(
            f"Cron job added: {name} → [{expr}] tz={tz_name} enabled={enabled} "
            f"→ Pipeline [{pipeline_name}]"
        )
        if persist:
            self._create_background_task(
                self._persist_cron_job(
                    name=name,
                    pipeline_name=pipeline_name,
                    cron_expr=expr,
                    task_template=template,
                    enabled=enabled,
                    timezone=tz_name,
                    schedule_meta=meta,
                    description=description or "",
                )
            )
        return job.id

    def update_cron_job(
        self,
        name: str,
        *,
        pipeline_name: str | None = None,
        cron_expr: str | None = None,
        task_template: dict[str, Any] | None = None,
        enabled: bool | None = None,
        timezone: str | None = None,
        schedule_meta: dict[str, Any] | None = None,
        description: str | None = None,
        persist: bool = True,
    ) -> str:
        """Update an existing cron job (replace in place)."""
        existing = self.get_cron_job(name)
        if existing is None:
            raise ValueError(f"Cron job not found: {name}")
        return self.add_cron_job(
            name=name,
            pipeline_name=pipeline_name if pipeline_name is not None else existing["pipeline_name"],
            cron_expr=cron_expr if cron_expr is not None else existing["cron_expr"],
            task_template=(
                task_template if task_template is not None else existing.get("task_template") or {}
            ),
            enabled=enabled if enabled is not None else bool(existing.get("enabled", True)),
            timezone=timezone if timezone is not None else existing.get("timezone"),
            schedule_meta=(
                schedule_meta if schedule_meta is not None else existing.get("schedule_meta")
            ),
            description=(
                description if description is not None else str(existing.get("description") or "")
            ),
            persist=persist,
        )

    def set_cron_job_enabled(self, name: str, enabled: bool) -> bool:
        """Pause/resume a cron job and persist enabled flag."""
        if self._cron_scheduler is None:
            return False
        try:
            job = self._cron_scheduler.get_job(name)
            if job is None:
                return False
            kwargs = dict(job.kwargs or {})
            kwargs["enabled"] = enabled
            job.modify(kwargs=kwargs)
            if enabled:
                job.resume()
            else:
                job.pause()
            self._create_background_task(
                self._persist_cron_job(
                    name=name,
                    pipeline_name=str(kwargs.get("pipeline_name") or ""),
                    cron_expr=str(kwargs.get("cron_expr") or ""),
                    task_template=kwargs.get("task_template") or {},
                    enabled=enabled,
                    timezone=str(kwargs.get("timezone") or default_timezone()),
                    schedule_meta=kwargs.get("schedule_meta") or {},
                    description=str(kwargs.get("description") or ""),
                )
            )
            logger.info(f"Cron job {'enabled' if enabled else 'disabled'}: {name}")
            return True
        except Exception as exc:
            logger.warning(f"Failed to set enabled={enabled} for cron {name}: {exc}")
            return False

    async def run_cron_job_now(self, name: str) -> str:
        """Submit one task immediately from the job template (does not alter schedule)."""
        existing = self.get_cron_job(name)
        if existing is None:
            raise LookupError(f"Cron job not found: {name}")
        pipeline_name = str(existing.get("pipeline_name") or "")
        template = existing.get("task_template") if isinstance(existing.get("task_template"), dict) else {}
        rolled = _roll_refresh_template(template)
        task_kwargs = {
            k: v
            for k, v in rolled.items()
            if k in {"collector_name", "targets", "config", "description"}
        }
        task = Task(
            name=f"[Cron] {name} - manual - {datetime.now().strftime('%Y%m%d_%H%M%S')}",
            pipeline_name=pipeline_name,
            **task_kwargs,
        )
        return await self._submit_task(task, pipeline_name=pipeline_name)

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

    def get_cron_job(self, name: str) -> dict[str, Any] | None:
        """Return one enriched cron job payload or None."""
        if self._cron_scheduler is None:
            return None
        job = self._cron_scheduler.get_job(name)
        if job is None:
            return None
        return self._job_to_public(job)

    def list_cron_jobs(self) -> list[dict[str, Any]]:
        """List all cron jobs with enriched metadata."""
        if self._cron_scheduler is None:
            return []
        return [self._job_to_public(job) for job in self._cron_scheduler.get_jobs()]

    def _job_to_public(self, job: Any) -> dict[str, Any]:
        kwargs = job.kwargs or {}
        template = kwargs.get("task_template", {})
        if not isinstance(template, dict):
            template = {}
        cron_expr = str(kwargs.get("cron_expr") or "").strip()
        if not cron_expr:
            # Legacy jobs without stored expr — best effort from trigger
            cron_expr = _cron_expr_from_trigger(job.trigger)
        return build_cron_public_view(
            name=str(job.name or job.id),
            pipeline_name=str(kwargs.get("pipeline_name") or ""),
            cron_expr=cron_expr,
            task_template=template,
            enabled=bool(kwargs.get("enabled", True)),
            timezone=str(kwargs.get("timezone") or default_timezone()),
            schedule_meta=(
                kwargs.get("schedule_meta")
                if isinstance(kwargs.get("schedule_meta"), dict)
                else {}
            ),
            description=str(kwargs.get("description") or ""),
            next_run=job.next_run_time.isoformat() if job.next_run_time else None,
            job_id=str(job.id),
            trigger=str(job.trigger),
        )

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
                    timezone=str(job.get("timezone") or default_timezone()),
                    description=str(job.get("description") or ""),
                )
            except Exception as exc:
                logger.warning(f"Failed to load cron job: {name} - {exc}")

    async def restore_cron_jobs_from_store(self) -> None:
        """Restore cron jobs from persistent storage."""
        jobs = await self._restore_cron_jobs()
        for job in jobs:
            self._restore_cron_job(job)

    def _restore_cron_job(self, job: Any) -> None:
        try:
            name = getattr(job, "name", None) or (job.get("name") if isinstance(job, dict) else None)
            pipeline_name = getattr(job, "pipeline_name", None) or (
                job.get("pipeline_name") if isinstance(job, dict) else None
            )
            cron_expr = getattr(job, "cron_expr", None) or (
                job.get("cron_expr") if isinstance(job, dict) else None
            )
            task_template = getattr(job, "task_template", None)
            if task_template is None and isinstance(job, dict):
                task_template = job.get("task_template", {})
            enabled = getattr(job, "enabled", True)
            if isinstance(job, dict) and "enabled" in job:
                enabled = job.get("enabled", True)
            timezone = getattr(job, "timezone", None) or (
                job.get("timezone") if isinstance(job, dict) else None
            )
            schedule_meta = getattr(job, "schedule_meta", None)
            if schedule_meta is None and isinstance(job, dict):
                schedule_meta = job.get("schedule_meta", {})
            description = getattr(job, "description", "") or (
                job.get("description") if isinstance(job, dict) else ""
            )
            self.add_cron_job(
                name=str(name),
                pipeline_name=str(pipeline_name),
                cron_expr=str(cron_expr),
                task_template=task_template if isinstance(task_template, dict) else {},
                persist=False,
                enabled=bool(enabled),
                timezone=str(timezone or default_timezone()),
                schedule_meta=schedule_meta if isinstance(schedule_meta, dict) else {},
                description=str(description or ""),
            )
        except Exception as exc:
            logger.warning(f"Failed to restore cron job {getattr(job, 'name', job)}: {exc}")

    async def _cron_execute(
        self,
        pipeline_name: str,
        task_template: dict[str, Any],
        job_name: str,
        **_extra: Any,
    ) -> None:
        """Cron trigger entry point — creates and submits a task."""
        if _extra.get("enabled") is False:
            logger.info(f"Cron job skipped (disabled): {job_name}")
            return
        logger.info(f"Cron job triggered: {job_name}")
        rolled = _roll_refresh_template(task_template if isinstance(task_template, dict) else {})
        # Task() accepts name, pipeline_name, collector_name, targets, config, description
        task_kwargs = {
            k: v
            for k, v in rolled.items()
            if k in {"collector_name", "targets", "config", "description"}
        }
        # targets may be list of dicts — Task model will coerce
        task = Task(
            name=f"[Cron] {job_name} - {datetime.now().strftime('%Y%m%d_%H%M')}",
            pipeline_name=pipeline_name,
            **task_kwargs,
        )
        try:
            await self._submit_task(task, pipeline_name=pipeline_name)
        except Exception as e:
            logger.error(f"Cron job submit failed: {job_name} - {e}")


def _cron_expr_from_trigger(trigger: Any) -> str:
    """Best-effort rebuild of 5-field cron from CronTrigger fields."""
    try:
        fields = getattr(trigger, "fields", None)
        if not fields:
            return str(trigger)
        # APScheduler field order: year month day week day_of_week hour minute second
        by_name = {f.name: str(f) for f in fields}
        minute = by_name.get("minute", "*")
        hour = by_name.get("hour", "*")
        day = by_name.get("day", "*")
        month = by_name.get("month", "*")
        dow = by_name.get("day_of_week", "*")
        return f"{minute} {hour} {day} {month} {dow}"
    except Exception:
        return str(trigger)


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
