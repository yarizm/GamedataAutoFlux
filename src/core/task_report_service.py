"""Task report generation coordination shared by scheduler and hooks."""

from __future__ import annotations

from typing import Any, Awaitable, Callable

from loguru import logger

from src.core.pipeline import Pipeline, PipelineResult
from src.core.sensitive import redact_sensitive_text
from src.core.task import Task, TaskStatus

PersistTaskFn = Callable[[Task], Awaitable[None]]
RegisterReportArtifactFn = Callable[[Task, Any], Awaitable[None]]


class TaskReportService:
    """Coordinates auto-report generation for completed tasks."""

    def __init__(
        self,
        *,
        register_report_artifact: RegisterReportArtifactFn | None = None,
        persist_task: PersistTaskFn | None = None,
        report_generator: Any | None = None,
    ) -> None:
        self._register_report_artifact = register_report_artifact
        self._persist_task = persist_task
        self._report_generator = report_generator

    def should_generate_report(self, task: Task) -> bool:
        report_config = task.config.get("report", {})
        return bool(report_config.get("enabled"))

    async def generate_report_for_task(
        self,
        task: Task,
        pipeline: Pipeline | None,
        result: PipelineResult,
        *,
        fail_task_on_error: bool = True,
    ) -> None:
        report_generator = self._report_generator
        if report_generator is None:
            from src.web.app import report_generator as app_report_generator

            report_generator = app_report_generator

        report_config = task.config.get("report", {})
        prompt = str(report_config.get("prompt") or build_default_report_prompt(task))
        template = str(report_config.get("template", "default"))
        params = dict(report_config.get("params", {}))
        if "use_vector" not in params:
            params["use_vector"] = any(
                step.step_type.value == "storage" and step.component_name == "vector"
                for step in (pipeline.steps if pipeline is not None else [])
            )

        task.add_step_log("report:auto", TaskStatus.RUNNING, "开始生成报告")
        await self._persist_task_if_needed(task)

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
            task.add_step_log("report:auto", TaskStatus.FAILED, "报告生成失败", error=error_msg)
            if fail_task_on_error:
                result.success = False
                result.errors.append(error_msg)
                task.result = result
            await self._persist_task_if_needed(task)
            if fail_task_on_error:
                raise RuntimeError(error_msg) from exc
            logger.error(f"自动报告生成失败: {error_msg}")
            return

        result.generated_report_id = report.id
        result.generated_report_title = report.title
        result.generated_report_matched_records = report.matched_records
        if self._register_report_artifact is not None:
            await self._register_report_artifact(task, report)
        task.add_step_log("report:auto", TaskStatus.SUCCESS, f"报告生成完成: {report.title}")
        await self._persist_task_if_needed(task)

    async def _persist_task_if_needed(self, task: Task) -> None:
        if self._persist_task is not None:
            await self._persist_task(task)


def build_default_report_prompt(task: Task) -> str:
    targets = [target.name for target in task.targets if target.name]
    subject = "、".join(targets[:3]) if targets else task.name
    return f"基于本次采集结果，总结{subject}的核心表现、版本更新、评论反馈和关键事件。"
