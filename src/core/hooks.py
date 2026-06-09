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
from src.core.sensitive import redact_sensitive_text


class ReportGenerationHook:
    """订阅 task_completed → 生成报告"""

    def __init__(self, report_generator: Any, scheduler: Any | None = None) -> None:
        self._report_generator = report_generator
        self._scheduler = scheduler

    async def handle(self, event: TaskCompletedEvent) -> None:
        if not event.success:
            return

        task = event.task
        report_config = task.config.get("report", {})
        if not report_config.get("enabled"):
            return

        pipeline_result = event.result
        if pipeline_result is None:
            return

        prompt = str(report_config.get("prompt") or self._build_default_prompt(task))
        template = str(report_config.get("template", "default"))
        params = dict(report_config.get("params", {}))
        if "use_vector" not in params and event.pipeline is not None:
            params["use_vector"] = any(
                step.step_type.value == "storage" and step.component_name == "vector"
                for step in event.pipeline.steps
            )

        from src.core.task import TaskStatus

        task.add_step_log("report:auto", TaskStatus.RUNNING, "开始生成报告")

        try:
            report = await self._report_generator.generate_excel(
                prompt=prompt,
                data_source=str(
                    report_config.get("data_source") or task.collector_name or task.pipeline_name
                ),
                template=template,
                params=params,
                records=list(pipeline_result.output_records)
                if hasattr(pipeline_result, "output_records")
                else [],
                metadata={
                    "task_id": task.id,
                    "pipeline_name": task.pipeline_name,
                    "auto_generated": True,
                },
            )
            task.add_step_log("report:auto", TaskStatus.SUCCESS, f"报告生成完成: {report.title}")
            if self._scheduler is not None:
                await self._scheduler.register_report_artifact(task, report)
            if pipeline_result is not None:
                pipeline_result.generated_report_id = report.id
                pipeline_result.generated_report_title = report.title
                pipeline_result.generated_report_matched_records = report.matched_records
            logger.info(f"报告自动生成完成: {report.title}")
        except Exception as exc:
            safe_error = redact_sensitive_text(str(exc))
            task.add_step_log("report:auto", TaskStatus.FAILED, "报告生成失败", error=safe_error)
            logger.error(f"自动报告生成失败: {safe_error}")

    @staticmethod
    def _build_default_prompt(task: Any) -> str:
        targets = [target.name for target in task.targets if target.name]
        subject = "、".join(targets[:3]) if targets else task.name
        return f"基于本次采集结果，总结{subject}的核心表现、版本更新、评论反馈和关键事件。"


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
