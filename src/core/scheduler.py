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
from typing import Any

from loguru import logger

from src.core.config import get as get_config
from src.core.events import EventBus
from src.core.pipeline import Pipeline, PipelineResult
from src.core.registry import registry
from src.core.sensitive import redact_sensitive_text
from src.core.scheduler_cron_service import SchedulerCronService
from src.core.scheduler_state_service import SchedulerStateService, on_background_task_done
from src.core.task import Task, TaskStatus
from src.core.task_execution_coordinator import TaskExecutionCoordinator
from src.core.task_observability_service import TaskObservabilityService
from src.core.task_report_service import TaskReportService
from src.core.task_retry_policy import pipeline_result_retry_suppression_reason
from src.core.worker_claim_coordinator import (
    WorkerClaimCoordinator,
    get_claimed_task_for_worker,
)
from src.services.task_artifact_service import TaskArtifactService, StorageTaskArtifactService
from src.services.task_checkpoint_service import (
    StorageTaskCheckpointService,
    TaskCheckpointService,
)
from src.services.task_repository import TaskRepository
from src.services.cron_repository import CronRepository
from src.services.pipeline_repository import PipelineRepository
from src.services.task_event_service import TaskEventService, StorageTaskEventService
from src.storage.base import BaseStorage


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
        self._owns_task_event_service = task_event_service is None
        self._owns_task_artifact_service = task_artifact_service is None
        self._owns_task_checkpoint_service = task_checkpoint_service is None
        self._execution_backend = _normalize_execution_backend(
            execution_backend
            if execution_backend is not None
            else get_config(
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
        self._task_observability = TaskObservabilityService(
            get_task_event_service=lambda: self._task_event_service,
            get_task_artifact_service=lambda: self._task_artifact_service,
            get_task_checkpoint_service=lambda: self._task_checkpoint_service,
            get_event_bus=lambda: self._event_bus,
            create_background_task=self._create_background_task,
            safe_error_messages=_safe_error_messages,
        )
        self._state_service = SchedulerStateService(
            get_task_repo=lambda: self._task_repo,
            get_pipeline_repo=lambda: self._pipeline_repo,
            get_cron_repo=lambda: self._cron_repo,
            get_task_store=lambda: self._task_store,
            get_event_bus=lambda: self._event_bus,
            create_background_task=self._create_background_task,
        )
        self._task_report_service = TaskReportService(
            register_report_artifact=self.register_report_artifact,
            persist_task=self._persist_task,
        )
        self._task_execution_coordinator = TaskExecutionCoordinator(
            persist_task=self._persist_task,
            emit_task_event=self._emit_task_event,
            get_latest_task_checkpoint=self.get_latest_task_checkpoint,
            get_event_bus=lambda: self._event_bus,
            emit_task_completed_event=self._emit_task_completed_event,
            task_report_service=self._task_report_service,
            safe_error_messages=_safe_error_messages,
            join_safe_error_messages=_join_safe_error_messages,
            retry_suppression_reason=pipeline_result_retry_suppression_reason,
            create_background_task=self._create_background_task,
        )
        self._worker_claim_coordinator = WorkerClaimCoordinator(
            persist_task=self._persist_task,
            emit_task_event=self._emit_task_event,
            emit_task_completed_event=self._emit_task_completed_event,
            get_latest_task_checkpoint=self.get_latest_task_checkpoint,
            register_task_artifact=self.register_task_artifact,
            register_task_checkpoint=self.register_task_checkpoint,
            get_pipeline=self.get_pipeline,
            get_pipelines=lambda: self._pipelines,
        )

        import threading

        # IMPORTANT: threading.Lock 仅用于保护同步 dict 操作（self._tasks,
        # self._running_futures）。锁保护区域内**绝对不能包含 await**，
        # 否则在多线程或 future asyncio.Lock 迁移时会导致死锁。
        self._lock = threading.Lock()

        self._cron_service = SchedulerCronService(
            submit_task=self._cron_submit_task,
            persist_cron_job=self._state_service.persist_cron_job,
            delete_cron_job=self._state_service.delete_cron_job,
            restore_cron_jobs=self._state_service.restore_cron_jobs,
            create_background_task=self._create_background_task,
        )
        self._started = False
        self._has_started_once = False

    def _create_background_task(self, coro) -> asyncio.Task:
        bg_task = asyncio.create_task(coro)
        bg_task.add_done_callback(lambda t: on_background_task_done(t, self._background_tasks))
        self._background_tasks.add(bg_task)
        return bg_task

    async def start(self) -> None:
        """Start the scheduler."""
        if self._has_started_once:
            self._reset_runtime_state_for_restart()
        self._semaphore = asyncio.Semaphore(self._max_concurrent)
        self._refresh_task_store_config()
        self._task_store = self._build_task_store()

        await self._task_store.initialize()
        self._ensure_storage_backed_services()
        await self._restore_runtime_state()
        self._cron_service.start()
        self._cron_service.load_cron_jobs_from_config()
        await self._cron_service.restore_cron_jobs_from_store()
        self._started = True
        self._has_started_once = True
        logger.info(
            f"Scheduler started: max_concurrent={self._max_concurrent}, default_retries={self._default_retries}"
        )

    async def stop(self) -> None:
        """Stop the scheduler and cancel running work."""
        self._started = False
        await self._cancel_running_tasks()
        await self._shutdown_background_tasks()
        self._cron_service.shutdown()
        await self._close_task_store()
        logger.info("Scheduler stopped")

    def _refresh_task_store_config(self) -> None:
        self._task_store_config["provider"] = get_config("database.provider", "local")
        self._task_store_config["sqlalchemy_url"] = (
            get_config("database.sqlalchemy_url")
            or "postgresql+asyncpg://postgres:postgres@localhost:5432/autoflux"
        )

    def _build_task_store(self) -> BaseStorage:
        provider = self._task_store_config.get("provider", "sqlalchemy")
        if provider == "sqlalchemy":
            provider = "sqlalchemy_scheduler"
        store_cls = registry.get("storage", provider)
        return store_cls(self._task_store_config)

    def _ensure_storage_backed_services(self) -> None:
        if self._owns_task_event_service:
            self._task_event_service = StorageTaskEventService(self._task_store)
        if self._owns_task_artifact_service:
            self._task_artifact_service = StorageTaskArtifactService(self._task_store)
        if self._owns_task_checkpoint_service:
            self._task_checkpoint_service = StorageTaskCheckpointService(self._task_store)

    def _reset_runtime_state_for_restart(self) -> None:
        with self._lock:
            self._tasks.clear()
            self._running_futures.clear()
        self._pipelines.clear()
        self._background_tasks.clear()

    async def _restore_runtime_state(self) -> None:
        self._pipelines.update(await self._state_service.restore_pipelines())
        for task in await self._state_service.restore_tasks():
            with self._lock:
                self._tasks[task.id] = task

    async def _cancel_running_tasks(self) -> None:
        with self._lock:
            futures = list(self._running_futures.items())
        for task_id, future in futures:
            if not future.done():
                future.cancel()
                logger.info(f"Cancelled running task: {task_id}")

        if futures:
            _, pending = await asyncio.wait([future for _, future in futures], timeout=10.0)
            if pending:
                logger.warning(f"Stop timeout, {len(pending)} task(s) still running")

        with self._lock:
            self._running_futures.clear()

    async def _shutdown_background_tasks(self) -> None:
        if not self._background_tasks:
            return
        for bg_task in self._background_tasks:
            if not bg_task.done():
                bg_task.cancel()
        await asyncio.gather(*self._background_tasks, return_exceptions=True)
        self._background_tasks.clear()

    async def _close_task_store(self) -> None:
        if self._task_store is not None:
            await self._task_store.close()
            self._task_store = None

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
        """Run a task through the extracted in-process execution coordinator."""
        return await self._task_execution_coordinator.execute(
            task,
            pipeline,
            semaphore=self._semaphore,
            release_running_future=self._release_running_future,
        )

    def _release_running_future(self, task_id: str) -> None:
        with self._lock:
            self._running_futures.pop(task_id, None)

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
        reserve_session_claim=None,
    ) -> dict[str, Any] | None:
        """Claim the next pending task for a worker."""
        with self._lock:
            tasks = list(self._tasks.values())
        return await self._worker_claim_coordinator.claim_task_for_worker(
            worker_id,
            tasks=tasks,
            capabilities=capabilities,
            reserve_session_claim=reserve_session_claim,
        )

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
        return await self._worker_claim_coordinator.complete_worker_task(
            worker_id,
            task,
            result=result,
        )

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
        return await self._worker_claim_coordinator.fail_worker_task(
            worker_id,
            task,
            error=error,
            result=result,
        )

    async def interrupt_worker_tasks(
        self,
        worker_id: str,
        *,
        reason: str = "",
    ) -> list[Task]:
        """Cancel running tasks claimed by a worker that is no longer healthy."""
        with self._lock:
            tasks = list(self._tasks.values())
        return await self._worker_claim_coordinator.interrupt_worker_tasks(
            worker_id,
            tasks=tasks,
            reason=reason,
        )

    async def reconcile_stale_worker_tasks(
        self,
        worker_id: str,
        *,
        reason: str = "",
    ) -> dict[str, list[Task]]:
        """Recover stale worker tasks, including sticky retry claims."""
        with self._lock:
            tasks = list(self._tasks.values())
        return await self._worker_claim_coordinator.reconcile_stale_worker_tasks(
            worker_id,
            tasks=tasks,
            reason=reason,
        )

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
        return await self._worker_claim_coordinator.append_worker_task_event(
            worker_id,
            task,
            event_type,
            level=level,
            message=message,
            payload=payload,
            maybe_record_checkpoint=self._maybe_record_pipeline_checkpoint,
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
        return await self._worker_claim_coordinator.register_worker_task_artifact(
            worker_id,
            task,
            artifact_type,
            name=name,
            path=path,
            mime_type=mime_type,
            size=size,
            download_url=download_url,
            metadata=metadata,
        )

    async def register_worker_task_checkpoint(
        self,
        worker_id: str,
        task_id: str,
        *,
        recovery_level: str = "L0",
        cursor: dict[str, Any] | None = None,
        state: dict[str, Any] | None = None,
        stats: dict[str, Any] | None = None,
        artifacts: list[dict[str, Any]] | None = None,
        metadata: dict[str, Any] | None = None,
    ):
        """Register a checkpoint for a worker-claimed task."""
        task = self._get_claimed_task_for_worker(worker_id, task_id)
        if task is None:
            return None
        return await self._worker_claim_coordinator.register_worker_task_checkpoint(
            worker_id,
            task,
            recovery_level=recovery_level,
            cursor=cursor,
            state=state,
            stats=stats,
            artifacts=artifacts,
            metadata=metadata,
        )

    def _get_claimed_task_for_worker(self, worker_id: str, task_id: str) -> Task | None:
        with self._lock:
            tasks = dict(self._tasks)
        return get_claimed_task_for_worker(worker_id, task_id, tasks)

    async def get_task_events(
        self,
        task_id: str,
        *,
        limit: int = 200,
        offset: int = 0,
        order: str = "asc",
    ):
        """获取任务结构化事件。"""
        return await self._task_observability.list_task_events(
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
        return await self._task_observability.list_task_artifacts(
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
        return await self._task_observability.list_task_checkpoints(
            task_id,
            limit=limit,
            offset=offset,
        )

    async def get_latest_task_checkpoint(self, task_id: str):
        """获取任务最近一次 checkpoint。"""
        return await self._task_observability.get_latest_task_checkpoint(task_id)

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
        return await self._task_observability.register_task_artifact(
            task,
            artifact_type,
            name=name,
            path=path,
            mime_type=mime_type,
            size=size,
            download_url=download_url,
            metadata=metadata,
        )

    async def register_report_artifact(self, task: Task, report) -> None:
        """登记自动生成的 Excel 报告产物。"""
        await self._task_observability.register_report_artifact(task, report)

    async def register_task_checkpoint(
        self,
        task: Task,
        *,
        worker_id: str = "",
        recovery_level: str = "L0",
        cursor: dict[str, Any] | None = None,
        state: dict[str, Any] | None = None,
        stats: dict[str, Any] | None = None,
        artifacts: list[dict[str, Any]] | None = None,
        metadata: dict[str, Any] | None = None,
    ):
        """登记任务 checkpoint，并发出 checkpoint 事件。"""
        return await self._task_observability.register_task_checkpoint(
            task,
            worker_id=worker_id,
            recovery_level=recovery_level,
            cursor=cursor,
            state=state,
            stats=stats,
            artifacts=artifacts,
            metadata=metadata,
        )

    async def _on_task_progress(self, task_id: str, progress: float, message: str) -> None:
        """内部进度回调"""
        with self._lock:
            task = self._tasks.get(task_id)
        if task:
            await self._task_observability.record_task_progress(
                task,
                progress,
                message,
                persist_task=self._persist_task,
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
        await self._task_observability.handle_pipeline_event(
            task_id,
            event_type,
            level,
            message,
            payload,
            task=task,
        )

    async def _maybe_record_pipeline_checkpoint(
        self,
        task: Task,
        event_type: str,
        payload: dict[str, Any] | None,
    ) -> None:
        """Record lightweight checkpoints from in-process Pipeline collector events."""
        await self._task_observability.maybe_record_pipeline_checkpoint(task, event_type, payload)

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
        return await self._task_observability.emit_task_event(
            task_or_id,
            event_type,
            message=message,
            level=level,
            payload=payload,
        )

    async def _emit_task_completed_event(
        self,
        task: Task,
        success: bool,
        result: Any,
        pipeline: Pipeline | None,
        errors: list[str],
    ) -> None:
        """Emit task_completed for in-process and worker executions."""
        await self._task_observability.emit_task_completed_event(
            task,
            success,
            result,
            pipeline,
            errors,
        )

    # ==================== 定时调度 ====================

    def add_cron_job(
        self,
        name: str,
        pipeline_name: str,
        cron_expr: str,
        task_template: dict[str, Any] | None = None,
        persist: bool = True,
    ) -> str:
        """添加定时任务。"""
        return self._cron_service.add_cron_job(
            name=name,
            pipeline_name=pipeline_name,
            cron_expr=cron_expr,
            task_template=task_template,
            persist=persist,
        )

    def remove_cron_job(self, name: str) -> bool:
        """移除定时任务"""
        return self._cron_service.remove_cron_job(name)

    def list_cron_jobs(self) -> list[dict[str, Any]]:
        """列出所有定时任务"""
        return self._cron_service.list_cron_jobs()

    async def _cron_submit_task(self, task: Task, *, pipeline_name: str) -> str:
        """Callback for SchedulerCronService to submit cron-triggered tasks."""
        return await self.submit(task, pipeline_name=pipeline_name)

    def get_stats(self) -> dict[str, Any]:
        """获取调度器统计信息"""
        status_counts = {}
        with self._lock:
            tasks_values = list(self._tasks.values())
            running_futures_len = len(self._running_futures)
            running_status_count = sum(
                1 for task in tasks_values if task.status == TaskStatus.RUNNING
            )

        for task in tasks_values:
            status = task.status.value
            status_counts[status] = status_counts.get(status, 0) + 1

        return {
            "total_tasks": len(tasks_values),
            "running_tasks": max(running_futures_len, running_status_count),
            "max_concurrent": self._max_concurrent,
            "status_counts": status_counts,
            "cron_jobs": self._cron_service.job_count,
            "started": self._started,
        }

    async def _persist_task(self, task: Task) -> None:
        """持久化任务快照，并向前端广播状态。"""
        await self._state_service.persist_task(task)

    async def _persist_pipeline(self, pipeline: Pipeline) -> None:
        """Persist a pipeline snapshot."""
        await self._state_service.persist_pipeline(pipeline)


def _safe_error_messages(errors: list[str]) -> list[str]:
    return [redact_sensitive_text(str(error or "")) for error in errors if str(error or "")]


def _join_safe_error_messages(errors: list[str]) -> str:
    return "; ".join(_safe_error_messages(errors))


def _normalize_execution_backend(value: str) -> str:
    backend = str(value or "in_process").strip().lower()
    return backend if backend in {"in_process", "worker_claim"} else "in_process"
