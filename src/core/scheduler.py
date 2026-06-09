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
import copy
from datetime import datetime
from typing import Any

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from loguru import logger

from src.core.collector_metadata import build_collector_recovery_info, get_collector_metadata
from src.core.config import get as get_config
from src.core.events import EventBus
from src.core.pipeline import Pipeline, PipelineResult
from src.core.registry import registry
from src.core.sensitive import redact_sensitive, redact_sensitive_text
from src.core.task import Task, TaskStatus
from src.services.task_artifact_service import TaskArtifactService, StorageTaskArtifactService
from src.services.task_checkpoint_service import (
    StorageTaskCheckpointService,
    TaskCheckpointService,
)
from src.services.task_repository import TaskRepository
from src.services.cron_repository import CronRepository, CronJobConfig
from src.services.pipeline_repository import PipelineRepository
from src.services.task_event_service import TaskEventService, StorageTaskEventService
from src.storage.base import BaseStorage, StorageRecord


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
        task_repo: TaskRepository | None = None,
        cron_repo: CronRepository | None = None,
        pipeline_repo: PipelineRepository | None = None,
        event_bus: EventBus | None = None,
        task_event_service: TaskEventService | None = None,
        task_artifact_service: TaskArtifactService | None = None,
        task_checkpoint_service: TaskCheckpointService | None = None,
        execution_backend: str | None = None,
    ):
        self._max_concurrent = (
            max_concurrent
            if max_concurrent is not None
            else get_config("scheduler.max_concurrent_tasks", 5)
        )
        self._default_retries = (
            default_retries
            if default_retries is not None
            else get_config("scheduler.default_retry_count", 3)
        )
        self._task_store_config = task_store_config or {
            "provider": get_config("database.provider", "local"),
            "sqlalchemy_url": get_config("database.sqlalchemy_url")
            or "postgresql+asyncpg://postgres:postgres@localhost:5432/autoflux",
            "db_name": get_config("scheduler.persistence.db_name", "scheduler.db"),
            "json_dir": get_config("scheduler.persistence.json_dir", "scheduler_tasks"),
        }

        # 注入的仓储层（向后兼容：None 时走旧路径）
        self._task_repo = task_repo
        self._cron_repo = cron_repo
        self._pipeline_repo = pipeline_repo
        self._event_bus = event_bus
        self._task_event_service = task_event_service
        self._task_artifact_service = task_artifact_service
        self._task_checkpoint_service = task_checkpoint_service
        self._execution_backend = _normalize_execution_backend(
            execution_backend if execution_backend is not None else get_config(
                "scheduler.execution_backend",
                "in_process",
            )
        )

        self._semaphore: asyncio.Semaphore | None = None
        self._tasks: dict[str, Task] = {}
        self._pipelines: dict[str, Pipeline] = {}
        self._running_futures: dict[str, asyncio.Task] = {}
        self._task_store: BaseStorage | None = None
        self._background_tasks: set[asyncio.Task] = set()

        import threading
        # IMPORTANT: threading.Lock 仅用于保护同步 dict 操作（self._tasks,
        # self._running_futures）。锁保护区域内**绝对不能包含 await**，
        # 否则在多线程或 future asyncio.Lock 迁移时会导致死锁。
        self._lock = threading.Lock()

        self._cron_scheduler = AsyncIOScheduler()
        self._started = False

    def _create_background_task(self, coro) -> asyncio.Task:
        bg_task = asyncio.create_task(coro)
        bg_task.add_done_callback(lambda t: _on_background_task_done(t, self._background_tasks))
        self._background_tasks.add(bg_task)
        return bg_task

    async def start(self) -> None:
        """启动调度器"""
        self._semaphore = asyncio.Semaphore(self._max_concurrent)
        # Refresh from config to support test overrides
        self._task_store_config["provider"] = get_config("database.provider", "local")
        self._task_store_config["sqlalchemy_url"] = (
            get_config("database.sqlalchemy_url")
            or "postgresql+asyncpg://postgres:postgres@localhost:5432/autoflux"
        )

        provider = self._task_store_config.get("provider", "sqlalchemy")
        if provider == "sqlalchemy":
            provider = "sqlalchemy_scheduler"

        store_cls = registry.get("storage", provider)
        self._task_store = store_cls(self._task_store_config)

        await self._task_store.initialize()
        if self._task_event_service is None:
            self._task_event_service = StorageTaskEventService(self._task_store)
        if self._task_artifact_service is None:
            self._task_artifact_service = StorageTaskArtifactService(self._task_store)
        if self._task_checkpoint_service is None:
            self._task_checkpoint_service = StorageTaskCheckpointService(self._task_store)
        await self._restore_pipelines()
        await self._restore_tasks()
        self._cron_scheduler.start()
        self._load_cron_jobs_from_config()
        await self._restore_cron_jobs()
        self._started = True
        logger.info(
            f"调度器已启动: 最大并发={self._max_concurrent}, 默认重试={self._default_retries}"
        )

    async def stop(self) -> None:
        """停止调度器，取消所有运行中的任务"""
        self._started = False
        # 取消运行中的异步任务
        with self._lock:
            futures = list(self._running_futures.items())
        for task_id, future in futures:
            if not future.done():
                future.cancel()
                logger.info(f"取消运行中的任务: {task_id}")

        if futures:
            _, pending = await asyncio.wait(
                [f for _, f in futures],
                timeout=10.0,
            )
            if pending:
                logger.warning(f"停止超时，{len(pending)} 个任务仍在运行")

        with self._lock:
            self._running_futures.clear()
        
        if self._background_tasks:
            # Cancel running background tasks and wait for them to finish
            for bg_task in self._background_tasks:
                if not bg_task.done():
                    bg_task.cancel()
            await asyncio.gather(*self._background_tasks, return_exceptions=True)
            self._background_tasks.clear()

        self._cron_scheduler.shutdown(wait=False)
        if self._task_store is not None:
            await self._task_store.close()
            self._task_store = None
        logger.info("调度器已停止")

    def register_pipeline(self, pipeline: Pipeline) -> None:
        """注册 Pipeline 配置"""
        self._pipelines[pipeline.name] = pipeline
        if self._started:
            self._create_background_task(self._persist_pipeline(pipeline))
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
        if self._pipeline_repo is not None:
            await self._pipeline_repo.delete(name)
        elif self._task_store is not None:
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
                raise ValueError(f"Pipeline '{name}' 不存在。可用: {list(self._pipelines.keys())}")

        task.pipeline_name = pipeline.name
        if task.max_retries is None:  # 未显式设置，使用调度器默认值
            task.max_retries = self._default_retries

        with self._lock:
            self._tasks[task.id] = task
        await self._persist_task(task)

        if self._execution_backend == "worker_claim":
            await self._emit_task_event(
                task,
                "queued",
                "任务已进入 Worker 领取队列",
                payload={
                    "status": task.status.value,
                    "execution_backend": self._execution_backend,
                    "pipeline_name": task.pipeline_name,
                },
            )
            logger.info(
                f"任务已提交到 Worker 队列: [{task.id}] {task.name} → Pipeline [{pipeline.name}]"
            )
            return task.id

        # 设置进度回调
        pipeline = Pipeline.from_config(pipeline.to_config())  # 克隆 pipeline
        pipeline.on_progress(self._on_task_progress)
        pipeline.on_event(self._on_task_event)

        # 提交执行
        future = asyncio.create_task(self._execute_task(task, pipeline))
        with self._lock:
            self._running_futures[task.id] = future

        logger.info(f"任务已提交: [{task.id}] {task.name} → Pipeline [{pipeline.name}]")
        return task.id

    async def _execute_task(self, task: Task, pipeline: Pipeline) -> PipelineResult | None:
        """在信号量控制下执行任务，使用循环实现重试"""
        while True:
            should_retry = False
            backoff = 0

            async with self._semaphore:
                task.start()
                await self._persist_task(task)
                await self._emit_task_event(
                    task,
                    "status",
                    "任务开始执行",
                    payload={
                        "status": task.status.value,
                        "retry_count": task.retry_count,
                        "pipeline_name": task.pipeline_name,
                    },
                )
                logger.info(f"任务开始执行: [{task.id}] {task.name}")

                try:
                    result = await pipeline.execute(task)

                    if result.success:
                        if self._event_bus is not None:
                            # 通过 EventBus 触发报告生成和告警
                            from src.core.events import TaskCompletedEvent

                            await self._event_bus.emit(
                                "task_completed",
                                TaskCompletedEvent(
                                    task_id=task.id,
                                    success=True,
                                    result=result,
                                    task=task,
                                    pipeline=pipeline,
                                    errors=[],
                                ),
                            )
                        else:
                            # 旧路径：内联报告生成
                            if self._should_generate_report(task):
                                try:
                                    await self._generate_report_for_task(task, pipeline, result)
                                except Exception as e:
                                    safe_error = redact_sensitive_text(str(e))
                                    logger.error(
                                        f"任务报告生成失败 (不影响任务成功状态): [{task.id}] {safe_error}"
                                    )

                        # 重新检查：hook 可能已将 result.success 设为 False
                        if not result.success:
                            error_msg = _join_safe_error_messages(result.errors)
                            task.result = result
                            task.fail(error_msg)
                            await self._persist_task(task)
                            await self._emit_task_event(
                                task,
                                "error",
                                "任务执行失败",
                                level="error",
                                payload={
                                    "status": task.status.value,
                                    "error": error_msg,
                                    "errors": _safe_error_messages(result.errors),
                                },
                            )
                            logger.error(
                                f"任务执行失败（报告生成）: [{task.id}] {task.name} - {error_msg}"
                            )
                            return result

                        task.complete(result)
                        await self._persist_task(task)
                        await self._emit_task_event(
                            task,
                            "complete",
                            "任务执行成功",
                            payload={
                                "status": task.status.value,
                                "storage_count": result.storage_count,
                                "generated_report_id": result.generated_report_id,
                            },
                        )
                        logger.info(f"任务执行成功: [{task.id}] {task.name}")
                    else:
                        error_msg = _join_safe_error_messages(result.errors)
                        retry_suppression_reason = _pipeline_result_retry_suppression_reason(result)
                        if not retry_suppression_reason and task.retry():
                            await self._persist_task(task)
                            await self._emit_task_event(
                                task,
                                "retry",
                                f"任务失败，准备重试 ({task.retry_count}/{task.max_retries})",
                                level="warning",
                                payload={
                                    "status": task.status.value,
                                    "retry_count": task.retry_count,
                                    "max_retries": task.max_retries,
                                    "error": error_msg,
                                },
                            )
                            logger.warning(
                                f"任务失败，重试 ({task.retry_count}/{task.max_retries}): "
                                f"[{task.id}] {task.name} - {error_msg}"
                            )
                            should_retry = True
                            backoff = min(60, 2 ** task.retry_count)
                        else:
                            if retry_suppression_reason:
                                task.add_step_log(
                                    "retry:policy",
                                    TaskStatus.FAILED,
                                    "Auto retry skipped to avoid duplicating stored partial results.",
                                    error=retry_suppression_reason,
                                )
                            task.result = result
                            task.fail(error_msg)
                            await self._persist_task(task)
                            await self._emit_task_event(
                                task,
                                "error",
                                "任务执行失败",
                                level="error",
                                payload={
                                    "status": task.status.value,
                                    "error": error_msg,
                                    "errors": _safe_error_messages(result.errors),
                                },
                            )
                            logger.error(f"任务最终失败: [{task.id}] {task.name} - {error_msg}")
                            if self._event_bus is not None:
                                from src.core.events import TaskCompletedEvent

                                await self._event_bus.emit(
                                    "task_completed",
                                    TaskCompletedEvent(
                                        task_id=task.id,
                                        success=False,
                                        result=result,
                                        task=task,
                                        pipeline=pipeline,
                                        errors=_safe_error_messages(result.errors),
                                    ),
                                )
                            else:
                                from src.services.alert_service import AlertService

                                self._create_background_task(
                                    AlertService.get_instance().send_alert(
                                        f"任务执行失败: {redact_sensitive_text(task.name)}",
                                        f"**Task ID**: {task.id}\n**Error**: {error_msg}",
                                        level="error",
                                    )
                                )

                    if not should_retry:
                        return result

                except asyncio.CancelledError:
                    task.cancel()
                    await self._persist_task(task)
                    await self._emit_task_event(
                        task,
                        "cancelled",
                        "任务已取消",
                        level="warning",
                        payload={"status": task.status.value},
                    )
                    logger.info(f"任务已取消: [{task.id}] {task.name}")
                    return None

                except Exception as e:
                    error_msg = redact_sensitive_text(str(e))
                    task.fail(error_msg)  # 先标记为 FAILED，再判断是否可重试
                    if task.retry():
                        await self._persist_task(task)
                        await self._emit_task_event(
                            task,
                            "retry",
                            f"任务异常，准备重试 ({task.retry_count}/{task.max_retries})",
                            level="warning",
                            payload={
                                "status": task.status.value,
                                "retry_count": task.retry_count,
                                "max_retries": task.max_retries,
                                "error": error_msg,
                            },
                        )
                        logger.warning(
                            f"任务异常，重试 ({task.retry_count}/{task.max_retries}): "
                            f"[{task.id}] {task.name} - {error_msg}"
                        )
                        should_retry = True
                        backoff = min(60, 2 ** task.retry_count)
                    else:
                        await self._persist_task(task)
                        await self._emit_task_event(
                            task,
                            "error",
                            "任务执行异常",
                            level="error",
                            payload={
                                "status": task.status.value,
                                "error": error_msg,
                            },
                        )
                        logger.error(f"任务最终异常: [{task.id}] {task.name} - {error_msg}")
                        if self._event_bus is not None:
                            from src.core.events import TaskCompletedEvent

                            await self._event_bus.emit(
                                "task_completed",
                                TaskCompletedEvent(
                                    task_id=task.id,
                                    success=False,
                                    result=None,
                                    task=task,
                                    pipeline=pipeline,
                                    errors=[error_msg],
                                ),
                            )
                        else:
                            from src.services.alert_service import AlertService

                            self._create_background_task(
                                AlertService.get_instance().send_alert(
                                    f"任务执行异常: {redact_sensitive_text(task.name)}",
                                    f"**Task ID**: {task.id}\n**Exception**: {error_msg}",
                                    level="error",
                                )
                            )
                        return None

                finally:
                    if not should_retry:
                        with self._lock:
                            self._running_futures.pop(task.id, None)

            if should_retry:
                await asyncio.sleep(backoff)

    async def cancel(self, task_id: str) -> bool:
        """
        取消任务。

        Args:
            task_id: 任务 ID

        Returns:
            是否成功取消
        """
        with self._lock:
            future = self._running_futures.get(task_id)
            task = self._tasks.get(task_id)

        if future and not future.done():
            future.cancel()
            return True


        if task and not task.is_terminal:
            task.cancel()
            await self._persist_task(task)
            await self._emit_task_event(
                task,
                "cancelled",
                "任务已取消",
                level="warning",
                payload={"status": task.status.value},
            )
            return True

        return False

    async def delete_task(self, task_id: str) -> bool:
        """Delete a non-running task and its persisted snapshot."""
        with self._lock:
            future = self._running_futures.get(task_id)
            if future and not future.done():
                return False

            task = self._tasks.get(task_id)
            if task is None:
                return False

            del self._tasks[task_id]
        
        if self._task_repo is not None:
            await self._task_repo.delete(task_id)
        elif self._task_store is not None:
            await self._task_store.delete(f"task:{task_id}")
        if self._task_event_service is not None:
            await self._task_event_service.delete_events(task_id)
        if self._task_artifact_service is not None:
            await self._task_artifact_service.delete_artifacts(task_id)
        if self._task_checkpoint_service is not None:
            await self._task_checkpoint_service.delete_checkpoints(task_id)
        logger.info(f"Task deleted: [{task_id}] {task.name}")
        return True

    def get_task(self, task_id: str) -> Task | None:
        """获取任务"""
        with self._lock:
            return self._tasks.get(task_id)

    def get_all_tasks(self) -> list[Task]:
        """获取所有任务"""
        with self._lock:
            return list(self._tasks.values())

    def get_tasks_by_status(self, status: TaskStatus) -> list[Task]:
        """按状态筛选任务"""
        with self._lock:
            return [t for t in self._tasks.values() if t.status == status]

    async def claim_task_for_worker(
        self,
        worker_id: str,
        *,
        capabilities: list[str] | None = None,
    ) -> dict[str, Any] | None:
        """Claim the next pending task for a worker."""
        safe_worker_id = redact_sensitive_text(str(worker_id or "")).strip()
        if not safe_worker_id:
            raise ValueError("worker_id is required")

        with self._lock:
            pending = [
                task
                for task in self._tasks.values()
                if task.status in (TaskStatus.PENDING, TaskStatus.RETRYING)
                and _task_matches_worker_capabilities(task, self._pipelines, capabilities)
            ]
            pending.sort(key=lambda item: (-int(item.priority), item.created_at))
            task = pending[0] if pending else None
            if task is None:
                return None
            pipeline = self._pipelines.get(task.pipeline_name)
            if pipeline is None:
                return None

            task.start()
            worker_claim = {
                "worker_id": safe_worker_id,
                "claimed_at": datetime.now().isoformat(),
                "execution_backend": "worker_claim",
            }
            task.config = {**task.config, "worker_claim": worker_claim}

        await self._persist_task(task)
        await self._emit_task_event(
            task,
            "claimed",
            f"任务已被 Worker 领取: {safe_worker_id}",
            payload={
                "status": task.status.value,
                "worker_id": safe_worker_id,
                "execution_backend": "worker_claim",
            },
        )
        latest_checkpoint = await self.get_latest_task_checkpoint(task.id)
        latest_checkpoint_payload = (
            latest_checkpoint.to_public_payload() if latest_checkpoint is not None else None
        )
        collector_name = _task_collector_name(task, self._pipelines)
        return {
            "task_id": task.id,
            "task": task.to_storage_payload(),
            "pipeline": pipeline.to_config(),
            "latest_checkpoint": latest_checkpoint_payload,
            "recovery": build_collector_recovery_info(
                collector_name,
                latest_checkpoint=latest_checkpoint_payload,
            )
            if collector_name
            else {},
        }

    async def complete_worker_task(
        self,
        worker_id: str,
        task_id: str,
        *,
        result: dict[str, Any] | None = None,
    ) -> Task | None:
        """Mark a worker-claimed task as successful."""
        task = self._get_claimed_task_for_worker(worker_id, task_id)
        if task is None:
            return None
        safe_result = redact_sensitive(result or {})

        task.complete(
            safe_result
            or {
                "success": True,
                "task_id": task.id,
                "worker_id": redact_sensitive_text(str(worker_id)),
            }
        )
        await self._persist_task(task)
        await self._emit_task_event(
            task,
            "complete",
            "Worker 上报任务执行成功",
            payload={
                "status": task.status.value,
                "worker_id": redact_sensitive_text(str(worker_id)),
                "result": safe_result,
            },
        )
        return task

    async def fail_worker_task(
        self,
        worker_id: str,
        task_id: str,
        *,
        error: str,
        result: dict[str, Any] | None = None,
    ) -> Task | None:
        """Mark a worker-claimed task as failed."""
        task = self._get_claimed_task_for_worker(worker_id, task_id)
        if task is None:
            return None

        safe_error = redact_sensitive_text(str(error or "Worker task failed"))
        safe_result = redact_sensitive(result or {})
        failure_result = safe_result or {
            "success": False,
            "task_id": task.id,
            "worker_id": redact_sensitive_text(str(worker_id)),
            "errors": [safe_error],
        }
        task.result = failure_result
        task.fail(safe_error)
        if task.retry():
            task.result = None
            with self._lock:
                claim = task.config.get("worker_claim") if isinstance(task.config, dict) else {}
                if not isinstance(claim, dict):
                    claim = {}
                task.config = {
                    **task.config,
                    "worker_claim": {
                        **claim,
                        "worker_id": redact_sensitive_text(str(worker_id)),
                        "released_at": datetime.now().isoformat(),
                        "last_error": safe_error,
                    },
                }
            await self._persist_task(task)
            await self._emit_task_event(
                task,
                "retry",
                f"Worker task failed; queued for retry ({task.retry_count}/{task.max_retries})",
                level="warning",
                payload={
                    "status": task.status.value,
                    "worker_id": redact_sensitive_text(str(worker_id)),
                    "retry_count": task.retry_count,
                    "max_retries": task.max_retries,
                    "error": safe_error,
                    "result": safe_result,
                },
            )
            return task

        await self._persist_task(task)
        await self._emit_task_event(
            task,
            "error",
            "Worker 上报任务执行失败",
            level="error",
            payload={
                "status": task.status.value,
                "worker_id": redact_sensitive_text(str(worker_id)),
                "error": safe_error,
                "result": safe_result,
            },
        )
        return task

    async def interrupt_worker_tasks(
        self,
        worker_id: str,
        *,
        reason: str = "",
    ) -> list[Task]:
        """Cancel running tasks claimed by a worker that is no longer healthy."""
        safe_worker_id = redact_sensitive_text(str(worker_id or "")).strip()
        if not safe_worker_id:
            return []
        safe_reason = redact_sensitive_text(
            str(reason or "Worker heartbeat is stale; task was interrupted.")
        )

        with self._lock:
            claimed_tasks = [
                task
                for task in self._tasks.values()
                if task.status == TaskStatus.RUNNING
                and _claimed_worker_id(task) == safe_worker_id
            ]
            for task in claimed_tasks:
                claim = task.config.get("worker_claim") if isinstance(task.config, dict) else {}
                if not isinstance(claim, dict):
                    claim = {}
                task.config = {
                    **task.config,
                    "worker_claim": {
                        **claim,
                        "worker_id": safe_worker_id,
                        "interrupted_at": datetime.now().isoformat(),
                        "interruption_reason": safe_reason,
                    },
                }
                task.result = {
                    "success": False,
                    "task_id": task.id,
                    "worker_id": safe_worker_id,
                    "interrupted": True,
                    "error": safe_reason,
                }
                task.add_step_log(
                    "worker:interrupted",
                    TaskStatus.CANCELLED,
                    "Worker task interrupted",
                    error=safe_reason,
                )
                task.cancel()
                task.error = safe_reason

        for task in claimed_tasks:
            await self._persist_task(task)
            await self._emit_task_event(
                task,
                "interrupted",
                "Worker 失联，任务已中断",
                level="warning",
                payload={
                    "status": task.status.value,
                    "worker_id": safe_worker_id,
                    "error": safe_reason,
                },
            )
        return claimed_tasks

    async def append_worker_task_event(
        self,
        worker_id: str,
        task_id: str,
        event_type: str,
        *,
        level: str = "info",
        message: str = "",
        payload: dict[str, Any] | None = None,
    ):
        """Append an event for a worker-claimed task."""
        task = self._get_claimed_task_for_worker(worker_id, task_id)
        if task is None:
            return None
        next_payload = {
            "worker_id": redact_sensitive_text(str(worker_id)),
            **(payload or {}),
        }
        return await self._emit_task_event(
            task,
            event_type,
            message,
            level=level,
            payload=next_payload,
        )

    async def register_worker_task_artifact(
        self,
        worker_id: str,
        task_id: str,
        artifact_type: str,
        *,
        name: str,
        path: str = "",
        mime_type: str = "",
        size: int | None = None,
        download_url: str = "",
        metadata: dict[str, Any] | None = None,
    ):
        """Register an artifact for a worker-claimed task."""
        task = self._get_claimed_task_for_worker(worker_id, task_id)
        if task is None:
            return None
        next_metadata = {
            "worker_id": redact_sensitive_text(str(worker_id)),
            **(metadata or {}),
        }
        return await self.register_task_artifact(
            task,
            artifact_type,
            name=name,
            path=path,
            mime_type=mime_type,
            size=size,
            download_url=download_url,
            metadata=next_metadata,
        )

    async def register_worker_task_checkpoint(
        self,
        worker_id: str,
        task_id: str,
        *,
        recovery_level: str = "L0",
        cursor: dict[str, Any] | None = None,
        stats: dict[str, Any] | None = None,
        artifacts: list[dict[str, Any]] | None = None,
        metadata: dict[str, Any] | None = None,
    ):
        """Register a checkpoint for a worker-claimed task."""
        task = self._get_claimed_task_for_worker(worker_id, task_id)
        if task is None:
            return None
        next_metadata = {
            "worker_id": redact_sensitive_text(str(worker_id)),
            **(metadata or {}),
        }
        return await self.register_task_checkpoint(
            task,
            worker_id=redact_sensitive_text(str(worker_id)),
            recovery_level=recovery_level,
            cursor=cursor,
            stats=stats,
            artifacts=artifacts,
            metadata=next_metadata,
        )

    def _get_claimed_task_for_worker(self, worker_id: str, task_id: str) -> Task | None:
        safe_worker_id = redact_sensitive_text(str(worker_id or "")).strip()
        with self._lock:
            task = self._tasks.get(task_id)
            if task is None or task.status != TaskStatus.RUNNING:
                return None
            if _claimed_worker_id(task) != safe_worker_id:
                return None
            return task

    async def get_task_events(
        self,
        task_id: str,
        *,
        limit: int = 200,
        offset: int = 0,
        order: str = "asc",
    ):
        """获取任务结构化事件。"""
        if self._task_event_service is None:
            return []
        return await self._task_event_service.list_events(
            task_id,
            limit=limit,
            offset=offset,
            order=order,
        )

    async def get_task_artifacts(
        self,
        task_id: str,
        *,
        limit: int = 200,
        offset: int = 0,
    ):
        """获取任务产物列表。"""
        if self._task_artifact_service is None:
            return []
        return await self._task_artifact_service.list_artifacts(
            task_id,
            limit=limit,
            offset=offset,
        )

    async def get_task_checkpoints(
        self,
        task_id: str,
        *,
        limit: int = 200,
        offset: int = 0,
    ):
        """获取任务 checkpoint 列表。"""
        if self._task_checkpoint_service is None:
            return []
        return await self._task_checkpoint_service.list_checkpoints(
            task_id,
            limit=limit,
            offset=offset,
        )

    async def get_latest_task_checkpoint(self, task_id: str):
        """获取任务最近一次 checkpoint。"""
        if self._task_checkpoint_service is None:
            return None
        return await self._task_checkpoint_service.latest_checkpoint(task_id)

    async def register_task_artifact(
        self,
        task: Task,
        artifact_type: str,
        *,
        name: str,
        path: str = "",
        mime_type: str = "",
        size: int | None = None,
        download_url: str = "",
        metadata: dict[str, Any] | None = None,
    ):
        """登记任务产物，并发出 artifact 事件。"""
        if self._task_artifact_service is None:
            return None

        artifact = await self._task_artifact_service.append(
            task.id,
            artifact_type,
            name=name,
            path=path,
            mime_type=mime_type,
            size=size,
            download_url=download_url,
            metadata=metadata,
        )
        await self._emit_task_event(
            task,
            "artifact",
            f"任务产物已生成: {artifact.name}",
            payload={
                "artifact": artifact.to_public_payload(),
            },
        )
        return artifact

    async def register_report_artifact(self, task: Task, report) -> None:
        """登记自动生成的 Excel 报告产物。"""
        excel_path = str(getattr(report, "excel_path", "") or "")
        size = None
        if excel_path:
            try:
                from pathlib import Path

                path = Path(excel_path)
                if path.exists() and path.is_file():
                    size = path.stat().st_size
            except Exception:
                size = None

        await self.register_task_artifact(
            task,
            "report_excel",
            name=str(getattr(report, "title", "") or getattr(report, "id", "") or "Excel 报告"),
            path=excel_path,
            mime_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            size=size,
            download_url=f"/api/reports/{getattr(report, 'id', '')}/download",
            metadata={
                "report_id": getattr(report, "id", ""),
                "report_title": getattr(report, "title", ""),
                "template": getattr(report, "template", ""),
                "matched_records": getattr(report, "matched_records", 0),
            },
        )

    async def register_task_checkpoint(
        self,
        task: Task,
        *,
        worker_id: str = "",
        recovery_level: str = "L0",
        cursor: dict[str, Any] | None = None,
        stats: dict[str, Any] | None = None,
        artifacts: list[dict[str, Any]] | None = None,
        metadata: dict[str, Any] | None = None,
    ):
        """登记任务 checkpoint，并发出 checkpoint 事件。"""
        if self._task_checkpoint_service is None:
            return None

        checkpoint = await self._task_checkpoint_service.append(
            task.id,
            pipeline_name=task.pipeline_name,
            collector_name=task.collector_name,
            worker_id=worker_id,
            recovery_level=recovery_level,
            cursor=cursor,
            stats=stats,
            artifacts=artifacts,
            metadata=metadata,
        )
        await self._emit_task_event(
            task,
            "checkpoint",
            f"任务 checkpoint 已记录: {checkpoint.recovery_level}",
            payload={"checkpoint": checkpoint.to_public_payload()},
        )
        return checkpoint

    async def _on_task_progress(self, task_id: str, progress: float, message: str) -> None:
        """内部进度回调"""
        with self._lock:
            task = self._tasks.get(task_id)
        if task:
            task.update_progress(progress, message)
            await self._persist_task(task)
            await self._emit_task_event(
                task,
                "progress",
                message or "任务进度更新",
                payload={
                    "status": task.status.value,
                    "progress": task.progress,
                },
            )

    async def _on_task_event(
        self,
        task_id: str,
        event_type: str,
        level: str,
        message: str,
        payload: dict[str, Any] | None = None,
    ) -> None:
        """Pipeline 结构化事件回调。"""
        with self._lock:
            task = self._tasks.get(task_id)
        await self._emit_task_event(
            task or task_id,
            event_type,
            message,
            level=level,
            payload=payload,
        )
        if task is not None:
            await self._maybe_record_pipeline_checkpoint(task, event_type, payload)

    async def _maybe_record_pipeline_checkpoint(
        self,
        task: Task,
        event_type: str,
        payload: dict[str, Any] | None,
    ) -> None:
        """Record lightweight checkpoints from in-process Pipeline collector events."""
        if self._task_checkpoint_service is None:
            return
        if event_type != "collect" or not isinstance(payload, dict):
            return

        status = str(payload.get("status") or "").strip().lower()
        if status not in {"succeeded", "failed"}:
            return

        collector_name = str(payload.get("component") or task.collector_name or "").strip()
        metadata = get_collector_metadata(collector_name or task.collector_name)
        if metadata is None or not metadata.supports_checkpoint:
            return

        recovery_level = str(metadata.recovery_level or "L0").upper()
        if recovery_level == "L0":
            return

        await self.register_task_checkpoint(
            task,
            recovery_level=recovery_level,
            cursor={
                "stage": "collect",
                "component": collector_name,
                "status": status,
            },
            stats=_pipeline_checkpoint_stats(payload),
            metadata={
                "source": "pipeline_event",
                "event_type": event_type,
                "session_mode": metadata.session_mode,
            },
        )

    async def _emit_task_event(
        self,
        task_or_id: Task | str,
        event_type: str,
        message: str,
        *,
        level: str = "info",
        payload: dict[str, Any] | None = None,
    ):
        """写入任务结构化事件，并发布给实时通道。"""
        if self._task_event_service is None:
            return None

        if isinstance(task_or_id, Task):
            task_id = task_or_id.id
            base_payload: dict[str, Any] = {
                "task_status": task_or_id.status.value,
                "pipeline_name": task_or_id.pipeline_name,
                "collector_name": task_or_id.collector_name,
            }
        else:
            task_id = task_or_id
            base_payload = {}
        if payload:
            base_payload.update(payload)

        event = await self._task_event_service.append(
            task_id,
            event_type,
            level=level,
            message=message,
            payload=base_payload,
        )
        public_payload = event.to_public_payload()

        if self._event_bus is not None:
            from src.core.events import TaskEventCreatedEvent

            self._create_background_task(
                self._event_bus.emit(
                    "task_event",
                    TaskEventCreatedEvent(task_id=task_id, event=public_payload),
                )
            )
        else:
            try:
                from src.web.routes.ws import manager

                self._create_background_task(
                    manager.broadcast({"type": "task_event", "event": public_payload})
                )
            except Exception as exc:
                logger.debug(f"Failed to broadcast task event: {exc}")
        return event

    # ==================== 定时调度 ====================

    def add_cron_job(
        self,
        name: str,
        pipeline_name: str,
        cron_expr: str,
        task_template: dict[str, Any] | None = None,
        persist: bool = True,
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
        if persist and (self._cron_repo is not None or self._task_store is not None):
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
        """移除定时任务"""
        try:
            self._cron_scheduler.remove_job(name)
            if self._cron_repo is not None:
                self._create_background_task(self._cron_repo.delete(name))
            elif self._task_store is not None:
                self._create_background_task(self._task_store.delete(f"cron:{name}"))
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
                "pipeline_name": (job.kwargs or {}).get("pipeline_name", ""),
                "task_template": (job.kwargs or {}).get("task_template", {}),
                "kind": (
                    (job.kwargs or {}).get("task_template", {}).get("config", {}).get("refresh", {})
                    or {}
                ).get("refresh_kind", "cron"),
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
            **_roll_refresh_template(task_template),
        )
        try:
            await self.submit(task, pipeline_name=pipeline_name)
        except Exception as e:
            logger.error(f"定时任务提交失败: {job_name} - {e}")

    def get_stats(self) -> dict[str, Any]:
        """获取调度器统计信息"""
        status_counts = {}
        with self._lock:
            tasks_values = list(self._tasks.values())
            running_futures_len = len(self._running_futures)
            running_status_count = sum(1 for task in tasks_values if task.status == TaskStatus.RUNNING)
            
        for task in tasks_values:
            status = task.status.value
            status_counts[status] = status_counts.get(status, 0) + 1

        return {
            "total_tasks": len(tasks_values),
            "running_tasks": max(running_futures_len, running_status_count),
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
                    persist=False,
                )
            except Exception as exc:
                logger.warning(f"加载 cron 任务失败: {name} - {exc}")

    def _should_generate_report(self, task: Task) -> bool:
        report_config = task.config.get("report", {})
        return bool(report_config.get("enabled"))

    async def _generate_report_for_task(
        self,
        task: Task,
        pipeline: Pipeline,
        result: PipelineResult,
    ) -> None:
        from src.web.app import report_generator

        report_config = task.config.get("report", {})
        prompt = str(report_config.get("prompt") or self._build_default_report_prompt(task))
        template = str(report_config.get("template", "default"))
        params = dict(report_config.get("params", {}))
        if "use_vector" not in params:
            params["use_vector"] = any(
                step.step_type.value == "storage" and step.component_name == "vector"
                for step in pipeline.steps
            )

        task.add_step_log("report:auto", TaskStatus.RUNNING, "开始生成报告")
        await self._persist_task(task)

        try:
            report = await report_generator.generate_excel(
                prompt=prompt,
                data_source=str(
                    report_config.get("data_source") or task.collector_name or task.pipeline_name
                ),
                template=template,
                params=params,
                records=list(result.output_records),
                metadata={
                    "task_id": task.id,
                    "pipeline_name": task.pipeline_name,
                    "auto_generated": True,
                },
            )
        except Exception as exc:
            error_msg = f"auto_report: {redact_sensitive_text(str(exc))}"
            result.success = False
            result.errors.append(error_msg)
            task.result = result
            task.add_step_log("report:auto", TaskStatus.FAILED, "报告生成失败", error=error_msg)
            await self._persist_task(task)
            raise RuntimeError(error_msg) from exc

        result.generated_report_id = report.id
        result.generated_report_title = report.title
        result.generated_report_matched_records = report.matched_records
        await self.register_report_artifact(task, report)
        task.add_step_log("report:auto", TaskStatus.SUCCESS, f"报告生成完成: {report.title}")
        await self._persist_task(task)

    def _build_default_report_prompt(self, task: Task) -> str:
        targets = [target.name for target in task.targets if target.name]
        subject = "、".join(targets[:3]) if targets else task.name
        return f"基于本次采集结果，总结{subject}的核心表现、版本更新、评论反馈和关键事件。"

    async def _persist_task(self, task: Task) -> None:
        """持久化任务快照，并向前端广播状态。"""
        storage_payload = task.to_storage_payload()
        public_payload = task.to_public_payload()

        # 优先使用注入的 TaskRepository
        if self._task_repo is not None:
            await self._task_repo.save(task)
        elif self._task_store is not None:
            await self._task_store.save(
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

        if self._event_bus is not None:
            from src.core.events import TaskUpdatedEvent

            self._create_background_task(
                self._event_bus.emit(
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
            # 旧路径：直接 WebSocket 广播
            try:
                from src.web.routes.ws import manager

                self._create_background_task(
                    manager.broadcast({"type": "task_update", "task": public_payload})
                )
            except Exception as exc:
                logger.debug(f"Failed to broadcast task update: {exc}")

    async def _persist_pipeline(self, pipeline: Pipeline) -> None:
        """Persist a pipeline snapshot."""
        if self._pipeline_repo is not None:
            await self._pipeline_repo.save(pipeline)
        elif self._task_store is not None:
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

    async def _persist_cron_job(
        self,
        *,
        name: str,
        pipeline_name: str,
        cron_expr: str,
        task_template: dict[str, Any],
    ) -> None:
        if self._cron_repo is not None:
            await self._cron_repo.save(
                CronJobConfig(
                    name=name,
                    pipeline_name=pipeline_name,
                    cron_expr=cron_expr,
                    task_template=task_template,
                )
            )
        elif self._task_store is not None:
            refresh = {}
            if isinstance(task_template, dict):
                config = task_template.get("config", {})
                if isinstance(config, dict) and isinstance(config.get("refresh"), dict):
                    refresh = config["refresh"]
            await self._task_store.save(
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

    async def _restore_pipelines(self) -> None:
        """Restore persisted pipelines from repository or local storage."""
        if self._pipeline_repo is not None:
            pipelines = await self._pipeline_repo.list_all()
            for pipeline in pipelines:
                self._pipelines[pipeline.name] = pipeline
        elif self._task_store is not None:
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
        if self._task_repo is not None:
            tasks = await self._task_repo.query(limit=1000)
            for task in tasks:
                if task.status in (TaskStatus.PENDING, TaskStatus.RUNNING, TaskStatus.RETRYING):
                    task.cancel()
                    task.error = (
                        "Recovered from a previous session without a live worker; marked cancelled."
                    )
                with self._lock:
                    self._tasks[task.id] = task
        elif self._task_store is not None:
            result = await self._task_store.query("key:task:", limit=1000)
            for record in result.records:
                if not isinstance(record.data, dict):
                    continue
                task = Task.from_storage_payload(record.data)
                if task.status in (TaskStatus.PENDING, TaskStatus.RUNNING, TaskStatus.RETRYING):
                    task.cancel()
                    task.error = (
                        "Recovered from a previous session without a live worker; marked cancelled."
                    )
                with self._lock:
                    self._tasks[task.id] = task

    async def _restore_cron_jobs(self) -> None:
        if self._cron_repo is not None:
            jobs = await self._cron_repo.list_all()
            for job in jobs:
                try:
                    self.add_cron_job(
                        name=job.name,
                        pipeline_name=job.pipeline_name,
                        cron_expr=job.cron_expr,
                        task_template=job.task_template
                        if isinstance(job.task_template, dict)
                        else {},
                        persist=False,
                    )
                except Exception as exc:
                    logger.warning(f"Failed to restore cron job {job.name}: {exc}")
        elif self._task_store is not None:
            result = await self._task_store.query("key:cron:", limit=1000)
            for record in result.records:
                if not isinstance(record.data, dict):
                    continue
                name = str(record.data.get("name") or "").strip()
                pipeline_name = str(record.data.get("pipeline_name") or "").strip()
                cron_expr = str(record.data.get("cron_expr") or "").strip()
                task_template = record.data.get("task_template", {})
                if not name or not pipeline_name or not cron_expr:
                    continue
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


def _on_background_task_done(task: asyncio.Task, tasks_set: set[asyncio.Task]) -> None:
    """Callback for fire-and-forget tasks to log exceptions."""
    tasks_set.discard(task)
    if task.cancelled():
        return
    exc = task.exception()
    if exc is not None:
        logger.error(f"Background task failed: {redact_sensitive_text(str(exc))}")


def _safe_error_messages(errors: list[str]) -> list[str]:
    return [redact_sensitive_text(str(error or "")) for error in errors if str(error or "")]


def _join_safe_error_messages(errors: list[str]) -> str:
    return "; ".join(_safe_error_messages(errors))


def _pipeline_result_retry_suppression_reason(result: PipelineResult) -> str:
    """Return a reason when task-level retry would likely duplicate stored partial data."""
    if not _has_stored_partial_collection_result(result):
        return ""
    summary = result.collection_summary
    failed_count = _safe_int(summary.get("failed_targets_count"))
    stored_count = int(getattr(result, "storage_count", 0) or 0)
    output_count = len(getattr(result, "output_records", []) or [])
    return (
        "Partial collection already produced stored records "
        f"(stored={stored_count}, output_records={output_count}, failed_targets={failed_count}). "
        "Review collection failures and create targeted follow-up tasks instead of retrying "
        "the whole pipeline."
    )


def _has_stored_partial_collection_result(result: PipelineResult) -> bool:
    stored_count = int(getattr(result, "storage_count", 0) or 0)
    output_records = getattr(result, "output_records", []) or []
    if stored_count <= 0 and not output_records:
        return False

    summary = getattr(result, "collection_summary", {})
    if not isinstance(summary, dict):
        return False
    return summary.get("status") == "partial" and _safe_int(summary.get("failed_targets_count")) > 0


def _pipeline_checkpoint_stats(payload: dict[str, Any]) -> dict[str, Any]:
    return {
        "targets_count": _safe_int(payload.get("targets_count")),
        "success_count": _safe_int(payload.get("success_count")),
        "failed_count": _safe_int(payload.get("failed_count")),
    }


def _safe_int(value: Any, *, default: int = 0) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return default
    return parsed if parsed >= 0 else default


def _normalize_execution_backend(value: str) -> str:
    backend = str(value or "in_process").strip().lower()
    return backend if backend in {"in_process", "worker_claim"} else "in_process"


def _task_matches_worker_capabilities(
    task: Task,
    pipelines: dict[str, Pipeline],
    capabilities: list[str] | None,
) -> bool:
    worker_capabilities = {str(capability).strip() for capability in capabilities or [] if capability}
    if not worker_capabilities:
        return True

    collector_name = _task_collector_name(task, pipelines)
    if not collector_name:
        return True

    accepted = {collector_name}
    metadata = get_collector_metadata(collector_name)
    if metadata is not None:
        accepted.update(metadata.capabilities)
    return bool(accepted & worker_capabilities)


def _task_collector_name(task: Task, pipelines: dict[str, Pipeline]) -> str:
    collector_name = str(task.collector_name or "").strip()
    if collector_name:
        return collector_name
    pipeline = pipelines.get(task.pipeline_name)
    if pipeline is None:
        return ""
    collector_step = next(
        (step for step in pipeline.steps if step.step_type.value == "collector"),
        None,
    )
    return collector_step.component_name if collector_step is not None else ""


def _claimed_worker_id(task: Task) -> str:
    claim = task.config.get("worker_claim") if isinstance(task.config, dict) else None
    if not isinstance(claim, dict):
        return ""
    return redact_sensitive_text(str(claim.get("worker_id") or "")).strip()


def _roll_refresh_template(task_template: dict[str, Any]) -> dict[str, Any]:
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
