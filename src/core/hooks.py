"""
任务生命周期钩子

将报告生成、告警推送、WebSocket 广播从 Scheduler 中解耦，
通过 EventBus 事件驱动。

注册方式（在 web/app.py lifespan 中）:
    event_bus.on("task_completed", report_hook.handle)
    event_bus.on("task_completed", alert_hook.handle)
    event_bus.on("task_updated", ws_hook.handle)
"""

from __future__ import annotations

from typing import Any

from loguru import logger

from src.core.events import TaskCompletedEvent, TaskEventCreatedEvent, TaskUpdatedEvent
from src.core.task_report_service import TaskReportService
from src.core.sensitive import redact_sensitive_text


class ReportGenerationHook:
    """订阅 task_completed → 生成报告"""

    def __init__(self, report_generator: Any, scheduler: Any | None = None) -> None:
        self._task_report_service = TaskReportService(
            register_report_artifact=(
                scheduler.register_report_artifact if scheduler is not None else None
            ),
            report_generator=report_generator,
        )

    async def handle(self, event: TaskCompletedEvent) -> None:
        if not event.success:
            return

        task = event.task
        if not self._task_report_service.should_generate_report(task):
            return

        pipeline_result = event.result
        if pipeline_result is None:
            return

        try:
            await self._task_report_service.generate_report_for_task(
                task,
                event.pipeline,
                pipeline_result,
                fail_task_on_error=False,
            )
        except Exception as exc:
            safe_error = redact_sensitive_text(str(exc))
            logger.error(f"自动报告生成失败: {safe_error}")


class AlertHook:
    """订阅 task_completed → 发送告警"""

    def __init__(self, alert_service: Any) -> None:
        self._alert_service = alert_service

    async def handle(self, event: TaskCompletedEvent) -> None:
        if event.success:
            return

        task = event.task
        if event.errors:
            error_msg = "; ".join(redact_sensitive_text(str(error)) for error in event.errors)
        else:
            error_msg = redact_sensitive_text(task.error or "未知错误")
        try:
            await self._alert_service.send_alert(
                f"任务执行失败: {redact_sensitive_text(task.name)}",
                f"**Task ID**: {task.id}\n**Error**: {error_msg}",
                level="error",
            )
        except Exception as exc:
            logger.error(f"告警发送失败: {redact_sensitive_text(str(exc))}")


class WebSocketBroadcastHook:
    """订阅 task_updated → WebSocket 广播"""

    def __init__(self, manager: Any) -> None:
        self._manager = manager

    async def handle(self, event: TaskUpdatedEvent) -> None:
        try:
            await self._manager.broadcast({"type": "task_update", "task": event.payload})
        except Exception as exc:
            logger.debug(f"WebSocket broadcast failed: {redact_sensitive_text(str(exc))}")


class WebSocketTaskEventHook:
    """订阅 task_event → WebSocket 广播"""

    def __init__(self, manager: Any) -> None:
        self._manager = manager

    async def handle(self, event: TaskEventCreatedEvent) -> None:
        try:
            await self._manager.broadcast({"type": "task_event", "event": event.event})
        except Exception as exc:
            logger.debug(f"WebSocket task event broadcast failed: {redact_sensitive_text(str(exc))}")
