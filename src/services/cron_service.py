"""Cron 业务门面（修复清单第三批：拆薄 Scheduler）。

定时任务的增删改查/启停/立即执行收敛到本服务；Scheduler 只保留
执行语义。Phase 1 委托 Scheduler 内部的 SchedulerCronService，
调用方面向本服务编程。
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Callable

if TYPE_CHECKING:
    from src.core.scheduler import Scheduler


class CronService:
    def __init__(self, get_scheduler: "Callable[[], Scheduler]") -> None:
        # 动态解析：容器/测试可随时替换 scheduler 实例（引擎细节）
        self._get_scheduler = get_scheduler

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
        return self._get_scheduler().cron_service.add_cron_job(
            name=name,
            pipeline_name=pipeline_name,
            cron_expr=cron_expr,
            task_template=task_template,
            persist=persist,
            enabled=enabled,
            timezone=timezone,
            schedule_meta=schedule_meta,
            description=description,
        )

    def update_cron_job(self, name: str, **kwargs: Any) -> str:
        return self._get_scheduler().cron_service.update_cron_job(name, **kwargs)

    def set_cron_job_enabled(self, name: str, enabled: bool) -> bool:
        return self._get_scheduler().cron_service.set_cron_job_enabled(name, enabled)

    async def run_cron_job_now(self, name: str) -> str:
        """立即按模板提交一次任务。"""
        return await self._get_scheduler().cron_service.run_cron_job_now(name)

    def get_cron_job(self, name: str) -> dict[str, Any] | None:
        return self._get_scheduler().cron_service.get_cron_job(name)

    def remove_cron_job(self, name: str) -> bool:
        return self._get_scheduler().cron_service.remove_cron_job(name)

    def list_cron_jobs(self) -> "list[dict[str, Any]]":
        return self._get_scheduler().cron_service.list_cron_jobs()
