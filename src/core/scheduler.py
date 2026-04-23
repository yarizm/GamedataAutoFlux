"""
任务调度器

管理任务的并发执行、队列调度和定时触发。
支持:
  - 手动提交任务
  - 并发控制（Semaphore）
  - 定时调度（APScheduler cron）
  - 任务取消
  - 自动重试
"""

from __future__ import annotations

import asyncio
from datetime import datetime
from typing import Any

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from loguru import logger

from src.core.config import get as get_config
from src.core.pipeline import Pipeline, PipelineResult
from src.core.task import Task, TaskStatus
from src.storage.base import StorageRecord
from src.storage.local_store import LocalStorage


class Scheduler:
    """
    任务调度器。

    管理任务队列、并发执行和定时调度。
    """

    def __init__(
        self,
        max_concurrent: int | None = None,
        default_retries: int | None = None,
        task_store_config: dict[str, Any] | None = None,
    ):
        self._max_concurrent = max_concurrent or get_config("scheduler.max_concurrent_tasks", 5)
        self._default_retries = default_retries or get_config("scheduler.default_retry_count", 3)
        self._task_store_config = task_store_config or {
            "db_name": get_config("scheduler.persistence.db_name", "scheduler.db"),
            "json_dir": get_config("scheduler.persistence.json_dir", "scheduler_tasks"),
        }

        self._semaphore: asyncio.Semaphore | None = None
        self._tasks: dict[str, Task] = {}
        self._pipelines: dict[str, Pipeline] = {}
        self._running_futures: dict[str, asyncio.Task] = {}
        self._task_store: LocalStorage | None = None

        # APScheduler 用于 cron 定时
        self._cron_scheduler = AsyncIOScheduler()
        self._started = False

    async def start(self) -> None:
        """启动调度器"""
        self._semaphore = asyncio.Semaphore(self._max_concurrent)
        self._task_store = LocalStorage(self._task_store_config)
        await self._task_store.initialize()
        await self._restore_pipelines()
        await self._restore_tasks()
        self._cron_scheduler.start()
        self._load_cron_jobs_from_config()
        self._started = True
        logger.info(
            f"调度器已启动: 最大并发={self._max_concurrent}, "
            f"默认重试={self._default_retries}"
        )

    async def stop(self) -> None:
        """停止调度器，取消所有运行中的任务"""
        self._started = False
        # 取消运行中的异步任务
        for task_id, future in list(self._running_futures.items()):
            if not future.done():
                future.cancel()
                logger.info(f"取消运行中的任务: {task_id}")
        self._running_futures.clear()
        self._cron_scheduler.shutdown(wait=False)
        if self._task_store is not None:
            await self._task_store.close()
            self._task_store = None
        logger.info("调度器已停止")

    def register_pipeline(self, pipeline: Pipeline) -> None:
        """注册 Pipeline 配置"""
        self._pipelines[pipeline.name] = pipeline
        if self._started:
            asyncio.create_task(self._persist_pipeline(pipeline))
        logger.info(f"Pipeline 已注册: {pipeline.name}")

    def get_pipeline(self, name: str) -> Pipeline | None:
        """获取已注册的 Pipeline"""
        return self._pipelines.get(name)

    async def save_pipeline(self, pipeline: Pipeline) -> None:
        """Save a pipeline and persist it."""
        self._pipelines[pipeline.name] = pipeline
        logger.info(f"Pipeline saved: {pipeline.name}")
        await self._persist_pipeline(pipeline)

    def get_all_pipelines(self) -> list[Pipeline]:
        """Return all registered pipelines."""
        return list(self._pipelines.values())

    async def delete_pipeline(self, name: str) -> bool:
        """Delete a pipeline and its persisted snapshot."""
        if name not in self._pipelines:
            return False

        del self._pipelines[name]
        if self._task_store is not None:
            await self._task_store.delete(f"pipeline:{name}")
        logger.info(f"Pipeline deleted: {name}")
        return True

    async def submit(
        self,
        task: Task,
        pipeline: Pipeline | None = None,
        pipeline_name: str | None = None,
    ) -> str:
        """
        提交任务到调度器。

        Args:
            task: 要执行的任务
            pipeline: Pipeline 实例（优先使用）
            pipeline_name: 已注册的 Pipeline 名称

        Returns:
            任务 ID

        Raises:
            ValueError: Pipeline 未指定或不存在
        """
        if not self._started:
            raise RuntimeError("调度器未启动，请先调用 start()")

        # 确定要使用的 Pipeline
        if pipeline is None:
            name = pipeline_name or task.pipeline_name
            pipeline = self._pipelines.get(name)
            if pipeline is None:
                raise ValueError(
                    f"Pipeline '{name}' 不存在。"
                    f"可用: {list(self._pipelines.keys())}"
                )

        task.pipeline_name = pipeline.name
        if task.max_retries == 3:  # 使用默认值
            task.max_retries = self._default_retries

        self._tasks[task.id] = task
        await self._persist_task(task)

        # 设置进度回调
        pipeline = Pipeline.from_config(pipeline.to_config())  # 克隆 pipeline
        pipeline.on_progress(self._on_task_progress)

        # 提交执行
        future = asyncio.create_task(self._execute_task(task, pipeline))
        self._running_futures[task.id] = future

        logger.info(f"任务已提交: [{task.id}] {task.name} → Pipeline [{pipeline.name}]")
        return task.id

    async def _execute_task(self, task: Task, pipeline: Pipeline) -> PipelineResult | None:
        """在信号量控制下执行任务"""
        async with self._semaphore:
            task.start()
            await self._persist_task(task)
            logger.info(f"任务开始执行: [{task.id}] {task.name}")

            try:
                result = await pipeline.execute(task)

                if result.success:
                    task.complete(result)
                    await self._persist_task(task)
                    logger.info(f"任务执行成功: [{task.id}] {task.name}")
                else:
                    error_msg = "; ".join(result.errors)
                    # 尝试重试
                    if task.retry():
                        await self._persist_task(task)
                        logger.warning(
                            f"任务失败，重试 ({task.retry_count}/{task.max_retries}): "
                            f"[{task.id}] {task.name} - {error_msg}"
                        )
                        return await self._execute_task(task, pipeline)
                    else:
                        task.fail(error_msg)
                        await self._persist_task(task)
                        logger.error(f"任务最终失败: [{task.id}] {task.name} - {error_msg}")

                return result

            except asyncio.CancelledError:
                task.cancel()
                await self._persist_task(task)
                logger.info(f"任务已取消: [{task.id}] {task.name}")
                return None

            except Exception as e:
                error_msg = str(e)
                if task.retry():
                    await self._persist_task(task)
                    logger.warning(
                        f"任务异常，重试 ({task.retry_count}/{task.max_retries}): "
                        f"[{task.id}] {task.name} - {error_msg}"
                    )
                    return await self._execute_task(task, pipeline)
                else:
                    task.fail(error_msg)
                    await self._persist_task(task)
                    logger.error(f"任务最终异常: [{task.id}] {task.name} - {error_msg}")
                    return None

            finally:
                self._running_futures.pop(task.id, None)

    async def cancel(self, task_id: str) -> bool:
        """
        取消任务。

        Args:
            task_id: 任务 ID

        Returns:
            是否成功取消
        """
        future = self._running_futures.get(task_id)
        if future and not future.done():
            future.cancel()
            return True

        task = self._tasks.get(task_id)
        if task and not task.is_terminal:
            task.cancel()
            await self._persist_task(task)
            return True

        return False

    async def delete_task(self, task_id: str) -> bool:
        """Delete a non-running task and its persisted snapshot."""
        future = self._running_futures.get(task_id)
        if future and not future.done():
            return False

        task = self._tasks.get(task_id)
        if task is None:
            return False

        del self._tasks[task_id]
        if self._task_store is not None:
            await self._task_store.delete(f"task:{task_id}")
        logger.info(f"Task deleted: [{task_id}] {task.name}")
        return True

    def get_task(self, task_id: str) -> Task | None:
        """获取任务"""
        return self._tasks.get(task_id)

    def get_all_tasks(self) -> list[Task]:
        """获取所有任务"""
        return list(self._tasks.values())

    def get_tasks_by_status(self, status: TaskStatus) -> list[Task]:
        """按状态筛选任务"""
        return [t for t in self._tasks.values() if t.status == status]

    async def _on_task_progress(self, task_id: str, progress: float, message: str) -> None:
        """内部进度回调"""
        task = self._tasks.get(task_id)
        if task:
            task.update_progress(progress, message)
            await self._persist_task(task)

    # ==================== 定时调度 ====================

    def add_cron_job(
        self,
        name: str,
        pipeline_name: str,
        cron_expr: str,
        task_template: dict[str, Any] | None = None,
    ) -> str:
        """
        添加定时任务。

        Args:
            name: 定时任务名称
            pipeline_name: Pipeline 名称
            cron_expr: Cron 表达式 (如 "0 8 * * *")
            task_template: 任务模板参数

        Returns:
            APScheduler job ID
        """
        parts = cron_expr.strip().split()
        if len(parts) != 5:
            raise ValueError(f"无效的 cron 表达式: {cron_expr}")

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

        logger.info(f"定时任务已添加: {name} → [{cron_expr}] → Pipeline [{pipeline_name}]")
        return job.id

    def remove_cron_job(self, name: str) -> bool:
        """移除定时任务"""
        try:
            self._cron_scheduler.remove_job(name)
            logger.info(f"定时任务已移除: {name}")
            return True
        except Exception:
            return False

    def list_cron_jobs(self) -> list[dict[str, Any]]:
        """列出所有定时任务"""
        jobs = self._cron_scheduler.get_jobs()
        return [
            {
                "id": job.id,
                "name": job.name,
                "next_run": job.next_run_time.isoformat() if job.next_run_time else None,
                "trigger": str(job.trigger),
            }
            for job in jobs
        ]

    async def _cron_execute(
        self,
        pipeline_name: str,
        task_template: dict[str, Any],
        job_name: str,
    ) -> None:
        """定时任务执行入口"""
        logger.info(f"定时任务触发: {job_name}")
        task = Task(
            name=f"[定时] {job_name} - {datetime.now().strftime('%Y%m%d_%H%M')}",
            pipeline_name=pipeline_name,
            **task_template,
        )
        try:
            await self.submit(task, pipeline_name=pipeline_name)
        except Exception as e:
            logger.error(f"定时任务提交失败: {job_name} - {e}")

    def get_stats(self) -> dict[str, Any]:
        """获取调度器统计信息"""
        status_counts = {}
        for task in self._tasks.values():
            status = task.status.value
            status_counts[status] = status_counts.get(status, 0) + 1

        return {
            "total_tasks": len(self._tasks),
            "running_tasks": len(self._running_futures),
            "max_concurrent": self._max_concurrent,
            "status_counts": status_counts,
            "cron_jobs": len(self._cron_scheduler.get_jobs()),
            "started": self._started,
        }

    def _load_cron_jobs_from_config(self) -> None:
        """从配置文件加载启用的定时任务。"""
        cron_jobs = get_config("scheduler.cron_jobs", [])
        if not isinstance(cron_jobs, list):
            logger.warning("scheduler.cron_jobs 配置格式无效，期望为列表")
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
                logger.warning(f"跳过无效 cron 配置: {job}")
                continue

            try:
                self.add_cron_job(
                    name=name,
                    pipeline_name=pipeline_name,
                    cron_expr=cron_expr,
                    task_template=task_template,
                )
            except Exception as exc:
                logger.warning(f"加载 cron 任务失败: {name} - {exc}")

    async def _persist_task(self, task: Task) -> None:
        """持久化任务快照。"""
        if self._task_store is None:
            return
        await self._task_store.save(
            StorageRecord(
                key=f"task:{task.id}",
                data=task.to_storage_payload(),
                metadata={
                    "kind": "task",
                    "status": task.status.value,
                    "pipeline_name": task.pipeline_name,
                },
                source="scheduler",
                tags=["task", task.status.value],
            )
        )

    async def _persist_pipeline(self, pipeline: Pipeline) -> None:
        """Persist a pipeline snapshot."""
        if self._task_store is None:
            return
        await self._task_store.save(
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

    async def _restore_pipelines(self) -> None:
        """Restore persisted pipelines from local storage."""
        if self._task_store is None:
            return
        result = await self._task_store.query("key:pipeline:", limit=1000)
        for record in result.records:
            if not isinstance(record.data, dict):
                continue
            try:
                pipeline = Pipeline.from_config(record.data)
            except Exception as exc:
                logger.warning(f"Failed to restore pipeline {record.key}: {exc}")
                continue
            self._pipelines[pipeline.name] = pipeline

    async def _restore_tasks(self) -> None:
        """从本地存储恢复任务快照。"""
        if self._task_store is None:
            return
        result = await self._task_store.query("key:task:", limit=1000)
        for record in result.records:
            if not isinstance(record.data, dict):
                continue
            task = Task.from_storage_payload(record.data)
            if task.status in (TaskStatus.PENDING, TaskStatus.RUNNING, TaskStatus.RETRYING):
                task.cancel()
                task.error = "Recovered from a previous session without a live worker; marked cancelled."
            self._tasks[task.id] = task
