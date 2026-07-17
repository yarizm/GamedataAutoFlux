"""Task execution orchestration extracted from Scheduler."""

from __future__ import annotations

import asyncio
from typing import Any, Awaitable, Callable

from loguru import logger

from src.core.errors import coerce_error_code, resolve_error_code
from src.core.pipeline import Pipeline, PipelineResult
from src.core.sensitive import redact_sensitive_text
from src.core.task import Task, TaskStatus
from src.core.task_report_service import TaskReportService


def _error_code_from_pipeline_result(result: PipelineResult | None) -> str | None:
    """Best-effort ErrorCode from pipeline errors / collection results."""
    if result is None:
        return None
    collect_results = getattr(result, "collect_results", None) or []
    for item in collect_results:
        code = getattr(item, "error_code", None)
        if code:
            coerced = coerce_error_code(code)
            if coerced is not None:
                return coerced.value
        if isinstance(item, dict) and item.get("error_code"):
            coerced = coerce_error_code(item.get("error_code"))
            if coerced is not None:
                return coerced.value
    for err in getattr(result, "errors", None) or []:
        text = str(err)
        # Prefer explicit "error_code=foo" patterns via resolve on message
        code = resolve_error_code(error_message=text)
        if code.value != "unknown" or "unknown" in text.lower():
            # still return even unknown if that's all we have — caller may re-resolve
            if code.value != "unknown":
                return code.value
    return None

PersistTaskFn = Callable[[Task], Awaitable[None]]
GetLatestCheckpointFn = Callable[[str], Awaitable[Any]]
GetEventBusFn = Callable[[], Any]
EmitCompletedEventFn = Callable[[Task, bool, Any, Pipeline, list[str]], Awaitable[None]]
GenerateReportFn = Callable[[Task, Pipeline, PipelineResult], Awaitable[None]]


class TaskExecutionCoordinator:
    """Coordinates in-process task execution, retries, and result handling."""

    def __init__(
        self,
        *,
        persist_task: PersistTaskFn,
        emit_task_event: Callable[..., Awaitable[Any]],
        get_latest_task_checkpoint: GetLatestCheckpointFn,
        get_event_bus: GetEventBusFn,
        emit_task_completed_event: EmitCompletedEventFn,
        task_report_service: TaskReportService,
        safe_error_messages: Callable[[list[str]], list[str]],
        join_safe_error_messages: Callable[[list[str]], str],
        retry_suppression_reason: Callable[[PipelineResult], str],
        create_background_task: Callable[[Awaitable[Any]], asyncio.Task],
    ) -> None:
        self._persist_task = persist_task
        self._emit_task_event = emit_task_event
        self._get_latest_task_checkpoint = get_latest_task_checkpoint
        self._get_event_bus = get_event_bus
        self._emit_task_completed_event = emit_task_completed_event
        self._task_report_service = task_report_service
        self._safe_error_messages = safe_error_messages
        self._join_safe_error_messages = join_safe_error_messages
        self._retry_suppression_reason = retry_suppression_reason
        self._create_background_task = create_background_task

    async def execute(
        self,
        task: Task,
        pipeline: Pipeline,
        *,
        semaphore: asyncio.Semaphore,
        release_running_future: Callable[[str], None],
    ) -> PipelineResult | None:
        """Execute a task with retry semantics under a scheduler-owned semaphore."""
        while True:
            should_retry = False
            backoff = 0

            async with semaphore:
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
                logger.info("任务开始执行: [{}] {}", task.id, task.name)

                try:
                    latest_checkpoint = await self._get_latest_task_checkpoint(task.id)
                    result = await pipeline.execute(
                        task,
                        recovery_checkpoint=(
                            latest_checkpoint.model_dump(mode="json")
                            if latest_checkpoint is not None
                            else None
                        ),
                    )

                    if result.success:
                        await self._handle_success(task, pipeline, result)
                    else:
                        should_retry, backoff = await self._handle_failure(task, pipeline, result)

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
                    logger.info("任务已取消: [{}] {}", task.id, task.name)
                    return None

                except Exception as exc:
                    should_retry, backoff, final_result = await self._handle_exception(
                        task,
                        pipeline,
                        exc,
                    )
                    if not should_retry:
                        return final_result

                finally:
                    if not should_retry:
                        release_running_future(task.id)

            if should_retry:
                await asyncio.sleep(backoff)

    async def _handle_success(
        self,
        task: Task,
        pipeline: Pipeline,
        result: PipelineResult,
    ) -> None:
        if self._get_event_bus() is not None:
            await self._emit_task_completed_event(task, True, result, pipeline, [])
        elif self._task_report_service.should_generate_report(task):
            try:
                await self._task_report_service.generate_report_for_task(
                    task,
                    pipeline,
                    result,
                    fail_task_on_error=True,
                )
            except Exception as exc:
                safe_error = redact_sensitive_text(str(exc))
                logger.error(
                    "任务报告生成失败 (不影响任务成功状态): [{}] {}",
                    task.id,
                    safe_error,
                )

        # 重新检查：hook 可能已将 result.success 设为 False
        if not result.success:
            error_msg = self._join_safe_error_messages(result.errors)
            task.result = result
            task.fail(error_msg, error_code=_error_code_from_pipeline_result(result))
            await self._persist_task(task)
            await self._emit_task_event(
                task,
                "error",
                "任务执行失败",
                level="error",
                payload={
                    "status": task.status.value,
                    "error": error_msg,
                    "error_code": task.error_code,
                    "errors": self._safe_error_messages(result.errors),
                    "resume_state": result.resume_state,
                },
            )
            logger.error("任务执行失败（报告生成）: [{}] {} - {}", task.id, task.name, error_msg)
            return

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
                "resume_state": result.resume_state,
            },
        )
        logger.info("任务执行成功: [{}] {}", task.id, task.name)

    async def _handle_failure(
        self,
        task: Task,
        pipeline: Pipeline,
        result: PipelineResult,
    ) -> tuple[bool, int]:
        error_msg = self._join_safe_error_messages(result.errors)
        retry_suppression_reason = self._retry_suppression_reason(result)
        error_code = _error_code_from_pipeline_result(result)

        # Align with exception/worker paths: fail first so retry() sees FAILED.
        task.result = result
        task.fail(error_msg, error_code=error_code)
        captured_code = task.error_code

        if not retry_suppression_reason and task.retry():
            task.result = None
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
                    "error_code": captured_code,
                },
            )
            logger.warning(
                "任务失败，重试 ({}/{}): [{}] {} - {}",
                task.retry_count,
                task.max_retries,
                task.id,
                task.name,
                error_msg,
            )
            return True, min(60, 2**task.retry_count)

        if retry_suppression_reason:
            task.add_step_log(
                "retry:policy",
                TaskStatus.FAILED,
                "Auto retry skipped to avoid duplicating stored partial results.",
                error=retry_suppression_reason,
            )

        await self._persist_task(task)
        await self._emit_task_event(
            task,
            "error",
            "任务执行失败",
            level="error",
            payload={
                "status": task.status.value,
                "error": error_msg,
                "error_code": task.error_code,
                "phase": task.phase,
                "errors": self._safe_error_messages(result.errors),
                "resume_state": result.resume_state,
            },
        )
        logger.error("任务最终失败: [{}] {} - {}", task.id, task.name, error_msg)
        if self._get_event_bus() is not None:
            await self._emit_task_completed_event(
                task,
                False,
                result,
                pipeline,
                self._safe_error_messages(result.errors),
            )
        else:
            self._schedule_failure_alert(task, error_msg, is_exception=False)
        return False, 0

    async def _handle_exception(
        self,
        task: Task,
        pipeline: Pipeline,
        exc: Exception,
    ) -> tuple[bool, int, PipelineResult | None]:
        error_msg = redact_sensitive_text(str(exc))
        task.fail(error_msg, exc=exc)
        captured_code = task.error_code
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
                    "error_code": captured_code,
                },
            )
            logger.warning(
                "任务异常，重试 ({}/{}): [{}] {} - {}",
                task.retry_count,
                task.max_retries,
                task.id,
                task.name,
                error_msg,
            )
            return True, min(60, 2**task.retry_count), None

        await self._persist_task(task)
        await self._emit_task_event(
            task,
            "error",
            "任务执行异常",
            level="error",
            payload={
                "status": task.status.value,
                "error": error_msg,
                "error_code": task.error_code,
                "phase": task.phase,
            },
        )
        logger.error("任务最终异常: [{}] {} - {}", task.id, task.name, error_msg)
        if self._get_event_bus() is not None:
            await self._emit_task_completed_event(task, False, None, pipeline, [error_msg])
        else:
            self._schedule_failure_alert(task, error_msg, is_exception=True)
        return False, 0, None

    def _schedule_failure_alert(self, task: Task, error_msg: str, *, is_exception: bool) -> None:
        from src.services.alert_service import AlertService

        title_prefix = "任务执行异常" if is_exception else "任务执行失败"
        detail_label = "Exception" if is_exception else "Error"
        self._create_background_task(
            AlertService.get_instance().send_alert(
                f"{title_prefix}: {redact_sensitive_text(task.name)}",
                f"**Task ID**: {task.id}\n**{detail_label}**: {error_msg}",
                level="error",
            )
        )
