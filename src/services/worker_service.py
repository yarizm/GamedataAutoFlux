"""Worker 业务门面（修复清单第三批：拆薄 Scheduler）。

Worker 领取/完成/失败/对账与事件、产物、checkpoint 上报收敛到本服务；
Scheduler 只保留执行与并发语义。Phase 1 委托 Scheduler 内部的
WorkerClaimCoordinator，调用方面向本服务编程。
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Callable

if TYPE_CHECKING:
    from src.core.scheduler import Scheduler
    from src.core.task import Task
    from src.core.worker_protocol import WorkerClaimPayload


class WorkerService:
    def __init__(self, get_scheduler: "Callable[[], Scheduler]") -> None:
        # 动态解析：容器/测试可随时替换 scheduler 实例（引擎细节）
        self._get_scheduler = get_scheduler

    async def claim_task_for_worker(
        self,
        worker_id: str,
        *,
        capabilities: "list[str] | None" = None,
        reserve_session_claim=None,
    ) -> "WorkerClaimPayload | None":
        return await self._get_scheduler().claim_task_for_worker(
            worker_id, capabilities=capabilities, reserve_session_claim=reserve_session_claim
        )

    async def complete_worker_task(
        self,
        worker_id: str,
        task_id: str,
        *,
        result: "dict[str, Any] | None" = None,
    ) -> "Task | None":
        return await self._get_scheduler().complete_worker_task(worker_id, task_id, result=result)

    async def fail_worker_task(
        self,
        worker_id: str,
        task_id: str,
        *,
        error: str,
        result: "dict[str, Any] | None" = None,
    ) -> "Task | None":
        return await self._get_scheduler().fail_worker_task(
            worker_id, task_id, error=error, result=result
        )

    async def reconcile_stale_worker_tasks(
        self,
        worker_id: str,
        *,
        reason: str = "",
    ) -> "dict[str, list[Task]]":
        return await self._get_scheduler().reconcile_stale_worker_tasks(worker_id, reason=reason)

    async def append_worker_task_event(
        self,
        worker_id: str,
        task_id: str,
        event_type: str,
        *,
        level: str = "info",
        message: str = "",
        payload: "dict[str, Any] | None" = None,
    ) -> None:
        return await self._get_scheduler().append_worker_task_event(
            worker_id,
            task_id,
            event_type,
            level=level,
            message=message,
            payload=payload,
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
        size: "int | None" = None,
        download_url: str = "",
        metadata: "dict[str, Any] | None" = None,
    ) -> None:
        return await self._get_scheduler().register_worker_task_artifact(
            worker_id,
            task_id,
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
        cursor: "dict[str, Any] | None" = None,
        state: "dict[str, Any] | None" = None,
        stats: "dict[str, Any] | None" = None,
        artifacts: "list[dict[str, Any]] | None" = None,
        metadata: "dict[str, Any] | None" = None,
    ) -> None:
        return await self._get_scheduler().register_worker_task_checkpoint(
            worker_id,
            task_id,
            recovery_level=recovery_level,
            cursor=cursor,
            state=state,
            stats=stats,
            artifacts=artifacts,
            metadata=metadata,
        )
