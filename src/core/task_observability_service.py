"""Task observability coordination extracted from Scheduler."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Awaitable, Callable

from loguru import logger

from src.core.collector_metadata import get_collector_metadata, resolve_session_mode
from src.core.events import TaskCompletedEvent, TaskEventCreatedEvent
from src.core.task import Task

GetTaskEventServiceFn = Callable[[], Any]
GetTaskArtifactServiceFn = Callable[[], Any]
GetTaskCheckpointServiceFn = Callable[[], Any]
GetEventBusFn = Callable[[], Any]
CreateBackgroundTaskFn = Callable[[Awaitable[Any]], Any]
PersistTaskFn = Callable[[Task], Awaitable[None]]
SafeErrorMessagesFn = Callable[[list[str]], list[str]]


class TaskObservabilityService:
    """Coordinates task events, artifacts, checkpoints, and progress callbacks."""

    def __init__(
        self,
        *,
        get_task_event_service: GetTaskEventServiceFn,
        get_task_artifact_service: GetTaskArtifactServiceFn,
        get_task_checkpoint_service: GetTaskCheckpointServiceFn,
        get_event_bus: GetEventBusFn,
        create_background_task: CreateBackgroundTaskFn,
        safe_error_messages: SafeErrorMessagesFn,
    ) -> None:
        self._get_task_event_service = get_task_event_service
        self._get_task_artifact_service = get_task_artifact_service
        self._get_task_checkpoint_service = get_task_checkpoint_service
        self._get_event_bus = get_event_bus
        self._create_background_task = create_background_task
        self._safe_error_messages = safe_error_messages

    async def list_task_events(
        self,
        task_id: str,
        *,
        limit: int = 200,
        offset: int = 0,
        order: str = "asc",
    ):
        service = self._get_task_event_service()
        if service is None:
            return []
        return await service.list_events(
            task_id,
            limit=limit,
            offset=offset,
            order=order,
        )

    async def list_task_artifacts(
        self,
        task_id: str,
        *,
        limit: int = 200,
        offset: int = 0,
    ):
        service = self._get_task_artifact_service()
        if service is None:
            return []
        return await service.list_artifacts(
            task_id,
            limit=limit,
            offset=offset,
        )

    async def list_task_checkpoints(
        self,
        task_id: str,
        *,
        limit: int = 200,
        offset: int = 0,
    ):
        service = self._get_task_checkpoint_service()
        if service is None:
            return []
        return await service.list_checkpoints(
            task_id,
            limit=limit,
            offset=offset,
        )

    async def get_latest_task_checkpoint(self, task_id: str):
        service = self._get_task_checkpoint_service()
        if service is None:
            return None
        return await service.latest_checkpoint(task_id)

    async def emit_task_event(
        self,
        task_or_id: Task | str,
        event_type: str,
        message: str,
        *,
        level: str = "info",
        payload: dict[str, Any] | None = None,
    ):
        """Write a structured task event and publish it to real-time listeners."""
        service = self._get_task_event_service()
        if service is None:
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

        event = await service.append(
            task_id,
            event_type,
            level=level,
            message=message,
            payload=base_payload,
        )
        public_payload = event.to_public_payload()

        event_bus = self._get_event_bus()
        if event_bus is not None:
            self._create_background_task(
                event_bus.emit(
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

    async def emit_task_completed_event(
        self,
        task: Task,
        success: bool,
        result: Any,
        pipeline: Any,
        errors: list[str],
    ) -> None:
        """Emit task_completed for in-process and worker executions."""
        event_bus = self._get_event_bus()
        if event_bus is None:
            return
        await event_bus.emit(
            "task_completed",
            TaskCompletedEvent(
                task_id=task.id,
                success=success,
                result=result,
                task=task,
                pipeline=pipeline,
                errors=self._safe_error_messages(errors),
            ),
        )

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
        """Register a task artifact and emit an artifact event."""
        service = self._get_task_artifact_service()
        if service is None:
            return None

        artifact = await service.append(
            task.id,
            artifact_type,
            name=name,
            path=path,
            mime_type=mime_type,
            size=size,
            download_url=download_url,
            metadata=metadata,
        )
        await self.emit_task_event(
            task,
            "artifact",
            f"任务产物已生成: {artifact.name}",
            payload={"artifact": artifact.to_public_payload()},
        )
        return artifact

    async def register_report_artifact(self, task: Task, report) -> None:
        """Register an auto-generated Excel report artifact."""
        excel_path = str(getattr(report, "excel_path", "") or "")
        size = None
        if excel_path:
            try:
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
        state: dict[str, Any] | None = None,
        stats: dict[str, Any] | None = None,
        artifacts: list[dict[str, Any]] | None = None,
        metadata: dict[str, Any] | None = None,
    ):
        """Register a task checkpoint and emit a checkpoint event."""
        service = self._get_task_checkpoint_service()
        if service is None:
            return None

        checkpoint = await service.append(
            task.id,
            pipeline_name=task.pipeline_name,
            collector_name=task.collector_name,
            worker_id=worker_id,
            recovery_level=recovery_level,
            cursor=cursor,
            state=state,
            stats=stats,
            artifacts=artifacts,
            metadata=metadata,
        )
        await self.emit_task_event(
            task,
            "checkpoint",
            f"任务 checkpoint 已记录: {checkpoint.recovery_level}",
            payload={"checkpoint": checkpoint.to_public_payload()},
        )
        return checkpoint

    async def record_task_progress(
        self,
        task: Task,
        progress: float,
        message: str,
        *,
        persist_task: PersistTaskFn,
    ) -> None:
        """Persist task progress updates and emit structured progress events."""
        task.update_progress(progress, message)
        await persist_task(task)
        await self.emit_task_event(
            task,
            "progress",
            message or "任务进度更新",
            payload={
                "status": task.status.value,
                "progress": task.progress,
            },
        )

    async def handle_pipeline_event(
        self,
        task_id: str,
        event_type: str,
        level: str,
        message: str,
        payload: dict[str, Any] | None = None,
        *,
        task: Task | None = None,
    ) -> None:
        """Handle a pipeline event and record checkpoints when appropriate."""
        await self.emit_task_event(
            task or task_id,
            event_type,
            message,
            level=level,
            payload=payload,
        )
        if task is not None:
            await self.maybe_record_pipeline_checkpoint(task, event_type, payload)

    async def maybe_record_pipeline_checkpoint(
        self,
        task: Task,
        event_type: str,
        payload: dict[str, Any] | None,
    ) -> None:
        """Record lightweight checkpoints from collector events."""
        if self._get_task_checkpoint_service() is None:
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
            state=_build_pipeline_checkpoint_state(task, payload),
            stats=_pipeline_checkpoint_stats(payload),
            metadata={
                "source": "pipeline_event",
                "event_type": event_type,
                "session_mode": resolve_session_mode(metadata.collector_id),
            },
        )


def _pipeline_checkpoint_stats(payload: dict[str, Any]) -> dict[str, Any]:
    return {
        "targets_count": _safe_int(payload.get("targets_count")),
        "success_count": _safe_int(payload.get("success_count")),
        "failed_count": _safe_int(payload.get("failed_count")),
    }


def _build_pipeline_checkpoint_state(task: Task, payload: dict[str, Any]) -> dict[str, Any]:
    targets = [
        str(target.name or "").strip() for target in task.targets if str(target.name or "").strip()
    ]
    success_count = _safe_int(payload.get("success_count"))
    failed_count = _safe_int(payload.get("failed_count"))
    next_target_index = min(len(targets), success_count + failed_count)
    return {
        "target_order": targets,
        "next_target_index": next_target_index,
        "completed_targets": targets[:next_target_index],
        "successful_targets": targets[: min(len(targets), success_count)],
        "failed_targets": targets[success_count:next_target_index],
    }


def _safe_int(value: Any, *, default: int = 0) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return default
    return parsed if parsed >= 0 else default
