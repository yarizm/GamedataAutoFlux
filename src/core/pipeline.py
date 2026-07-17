"""
Pipeline 编排引擎

将采集器 → 处理器 → 存储 串联为可配置的流水线。
支持 Builder 模式构建、进度回调和错误处理。

使用示例:
    pipeline = (
        Pipeline("steam_monitor")
        .add_collector("steam", config={...})
        .add_processor("cleaner")
        .add_processor("embedding")
        .add_storage("sqlalchemy")
        .add_storage("vector")
    )

    result = await pipeline.execute(task)
"""

from __future__ import annotations

import asyncio
import copy
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Awaitable

from loguru import logger

from src.collectors.base import BaseCollector, CollectTarget, CollectResult
from src.core.registry import registry
from src.core.pipeline_recovery import (
    apply_collect_resume_context,
    build_pipeline_recovery_context,
    build_pipeline_resume_state,
    build_storage_record_key,
    resolve_storage_resume_context,
)
from src.core.sensitive import redact_sensitive, redact_sensitive_text
from src.core.task import Task, TaskStatus
from src.services._utils import normalize_key
from src.processors.base import BaseProcessor, ProcessInput, ProcessOutput
from src.storage.base import BaseStorage, StorageRecord


def _build_collect_complete_payload(
    *,
    component: str,
    collect_results: list[CollectResult],
    task: Task,
    recovery_context: dict[str, Any] | None = None,
    error: str | None = None,
) -> dict[str, Any]:
    """Build collect-complete event payload with honest resume_state for checkpoints."""
    success_count = sum(1 for item in collect_results if getattr(item, "success", False))
    failed_count = len(collect_results) - success_count
    resume_state = build_pipeline_resume_state(
        task,
        recovery_context=recovery_context if isinstance(recovery_context, dict) else {},
        collect_results=collect_results,
        output_records=[],
    )
    return {
        "status": "failed" if failed_count else "succeeded",
        "component": component,
        "targets_count": len(collect_results),
        "success_count": success_count,
        "failed_count": failed_count,
        "error": error,
        "successful_targets": list(resume_state.get("successful_targets") or []),
        "failed_targets": list(resume_state.get("failed_targets") or []),
        "resume_state": resume_state,
    }


class StepType(str, Enum):
    """Pipeline 步骤类型"""

    COLLECTOR = "collector"
    PROCESSOR = "processor"
    STORAGE = "storage"


@dataclass
class PipelineStep:
    """Pipeline 步骤定义"""

    step_type: StepType
    component_name: str
    config: dict[str, Any] = field(default_factory=dict)
    instance: Any = None  # 运行时实例


@dataclass
class PipelineResult:
    """Pipeline 执行结果"""

    pipeline_name: str
    task_id: str
    success: bool = True
    collect_results: list[CollectResult] = field(default_factory=list)
    process_results: list[ProcessOutput] = field(default_factory=list)
    output_records: list[StorageRecord] = field(default_factory=list)
    storage_count: int = 0
    resume_state: dict[str, Any] = field(default_factory=dict)
    generated_report_id: str | None = None
    generated_report_title: str | None = None
    generated_report_matched_records: int = 0
    started_at: datetime = field(default_factory=datetime.now)
    completed_at: datetime | None = None
    errors: list[str] = field(default_factory=list)

    @property
    def duration_seconds(self) -> float | None:
        if self.completed_at is None:
            return None
        return (self.completed_at - self.started_at).total_seconds()

    @property
    def collection_summary(self) -> dict[str, Any]:
        """Lightweight, redacted summary of collector outcomes for task/API surfaces."""
        return _build_collection_summary(self.collect_results)


_COLLECTION_DETAIL_LIMIT = 50


def _build_collection_summary(
    collect_results: list[CollectResult],
    *,
    detail_limit: int = _COLLECTION_DETAIL_LIMIT,
) -> dict[str, Any]:
    total = len(collect_results)
    success_count = sum(1 for result in collect_results if result.success)
    failed_count = total - success_count
    status = (
        "empty"
        if total == 0
        else "success"
        if failed_count == 0
        else "failed"
        if success_count == 0
        else "partial"
    )

    failed_targets: list[dict[str, Any]] = []
    retried_targets: list[dict[str, Any]] = []
    error_codes: dict[str, int] = {}
    retry_attempts_total = 0
    retried_targets_count = 0

    for result in collect_results:
        result_summary = result.to_summary()
        retry = result_summary.get("retry") if isinstance(result_summary, dict) else None
        retry_attempts = 0
        if isinstance(retry, dict):
            retry_attempts = _summary_int(retry.get("retry_attempts"), default=0)
        if retry_attempts > 0:
            retry_attempts_total += retry_attempts
            retried_targets_count += 1
            if len(retried_targets) < detail_limit:
                retried_targets.append(result_summary)

        if result.success:
            continue

        error_code = str(result_summary.get("error_code") or result.error_code or "unknown")
        error_codes[error_code] = error_codes.get(error_code, 0) + 1
        if len(failed_targets) < detail_limit:
            failed_targets.append(result_summary)

    summary: dict[str, Any] = {
        "status": status,
        "total_targets": total,
        "successful_targets": success_count,
        "failed_targets_count": failed_count,
        "retried_targets_count": retried_targets_count,
        "retry_attempts_total": retry_attempts_total,
    }
    if error_codes:
        summary["error_codes"] = error_codes
    if failed_targets:
        summary["failed_targets"] = failed_targets
        if failed_count > len(failed_targets):
            summary["failed_targets_omitted"] = failed_count - len(failed_targets)
    if retried_targets:
        summary["retried_targets"] = retried_targets
        if retried_targets_count > len(retried_targets):
            summary["retried_targets_omitted"] = retried_targets_count - len(retried_targets)
    return redact_sensitive(summary)


def _summary_int(value: Any, *, default: int = 0) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return default
    return parsed if parsed >= 0 else default


def _collect_failure_message(result: CollectResult) -> str:
    summary = result.to_summary()
    target = str(summary.get("target") or result.target.name or "")
    error_code = str(summary.get("error_code") or result.error_code or "unknown")
    error = str(summary.get("error") or result.error or "")
    message = f"{target}: [{error_code}] {error or 'unknown error'}"

    retry = summary.get("retry")
    if isinstance(retry, dict):
        attempts = _summary_int(retry.get("attempts"), default=0)
        max_attempts = _summary_int(retry.get("max_attempts"), default=0)
        retry_attempts = _summary_int(retry.get("retry_attempts"), default=0)
        if retry_attempts > 0:
            message += (
                f" (attempts {attempts or 1}/{max_attempts or attempts or 1}, "
                f"retries {retry_attempts})"
            )
        last_retry_error = str(retry.get("last_retry_error") or "").strip()
        last_retry_error_code = str(retry.get("last_retry_error_code") or "").strip()
        if last_retry_error and last_retry_error_code:
            message += f" last retry [{last_retry_error_code}] {last_retry_error}"
        elif last_retry_error:
            message += f" last retry {last_retry_error}"
        elif last_retry_error_code:
            message += f" last retry code {last_retry_error_code}"
    return redact_sensitive_text(message)


# 进度回调类型：(task_id, progress, message)
ProgressCallback = Callable[[str, float, str], Awaitable[None] | None]
PipelineEventCallback = Callable[
    [str, str, str, str, dict[str, Any] | None],
    Awaitable[None] | None,
]


class Pipeline:
    """
    Pipeline 编排引擎。

    将 collector → processor(s) → storage(s) 串联为流水线。
    通过 Builder 模式构建，执行时自动管理组件生命周期。
    """

    def __init__(self, name: str):
        self.name = name
        self.steps: list[PipelineStep] = []
        self._progress_callback: ProgressCallback | None = None
        self._event_callback: PipelineEventCallback | None = None

    def add_collector(self, name: str, config: dict[str, Any] | None = None) -> Pipeline:
        """添加采集步骤"""
        self.steps.append(
            PipelineStep(
                step_type=StepType.COLLECTOR,
                component_name=name,
                config=config or {},
            )
        )
        return self

    def add_processor(self, name: str, config: dict[str, Any] | None = None) -> Pipeline:
        """添加处理步骤"""
        self.steps.append(
            PipelineStep(
                step_type=StepType.PROCESSOR,
                component_name=name,
                config=config or {},
            )
        )
        return self

    def add_storage(self, name: str, config: dict[str, Any] | None = None) -> Pipeline:
        """添加存储步骤（local 等历史名归一为 sqlalchemy）。"""
        from src.storage.factory import normalize_storage_name

        self.steps.append(
            PipelineStep(
                step_type=StepType.STORAGE,
                component_name=normalize_storage_name(name),
                config=config or {},
            )
        )
        return self

    def on_progress(self, callback: ProgressCallback) -> Pipeline:
        """设置进度回调"""
        self._progress_callback = callback
        return self

    def on_event(self, callback: PipelineEventCallback) -> Pipeline:
        """设置结构化事件回调"""
        self._event_callback = callback
        return self

    async def _report_progress(self, task_id: str, progress: float, message: str) -> None:
        """内部进度上报"""
        if self._progress_callback:
            result = self._progress_callback(task_id, progress, message)
            if asyncio.iscoroutine(result):
                await result

    async def _emit_event(
        self,
        task_id: str,
        event_type: str,
        message: str,
        *,
        level: str = "info",
        payload: dict[str, Any] | None = None,
    ) -> None:
        """内部结构化事件上报"""
        if self._event_callback:
            result = self._event_callback(task_id, event_type, level, message, payload)
            if asyncio.iscoroutine(result):
                await result

    def _get_collectors(self) -> list[PipelineStep]:
        return [s for s in self.steps if s.step_type == StepType.COLLECTOR]

    def _get_processors(self) -> list[PipelineStep]:
        return [s for s in self.steps if s.step_type == StepType.PROCESSOR]

    def _get_storages(self) -> list[PipelineStep]:
        return [s for s in self.steps if s.step_type == StepType.STORAGE]

    async def execute(
        self,
        task: Task,
        *,
        recovery_checkpoint: dict[str, Any] | None = None,
    ) -> PipelineResult:
        """Execute the configured pipeline for one task.

        默认委托 DAGExecutor 执行（pipeline_to_dag 转换后跑通用 DAG 引擎）。
        若 DAG 执行抛出异常，回退到原三段式逻辑以保证健壮性。
        """
        if self._should_use_dag_execution():
            try:
                return await self._execute_via_dag(task, recovery_checkpoint=recovery_checkpoint)
            except Exception as exc:
                logger.warning(
                    f"Pipeline [{self.name}] DAG execution failed, falling back to legacy path: {exc}"
                )
        return await self._execute_legacy(task, recovery_checkpoint=recovery_checkpoint)

    def _should_use_dag_execution(self) -> bool:
        """是否走 DAG 委托路径。受配置开关控制，默认开启。"""
        from src.core.config import get as get_config

        return bool(get_config("pipeline.use_dag_execution", True))

    async def _execute_via_dag(
        self,
        task: Task,
        *,
        recovery_checkpoint: dict[str, Any] | None = None,
    ) -> PipelineResult:
        """委托 DAGExecutor 执行，结果映射回 PipelineResult。"""
        from src.core.dag import pipeline_to_dag
        from src.core.dag_executor import DAGExecutor

        # 优先用持久化 graph（保留条件边/拓扑）；没有再从 steps 投影
        dag = await self._try_load_stored_dag()
        if dag is None:
            dag = pipeline_to_dag(self)
        executor = DAGExecutor()

        node_type_to_event = {"collector": "collect", "processor": "process", "storage": "storage"}
        recovery_context = build_pipeline_recovery_context(task, recovery_checkpoint)
        accumulated_collect_results: list[CollectResult] = []

        async def _on_event(task_id, node_spec, phase, *, out=None, error=None):
            event_type = node_type_to_event.get(node_spec.type, node_spec.type)
            if phase == "start":
                payload: dict[str, Any] = {
                    "status": "started",
                    "component": node_spec.component,
                }
                if node_spec.type == "collector":
                    payload["targets_count"] = len(task.targets)
                await self._emit_event(
                    task_id, event_type, f"{event_type} started", payload=payload
                )
            elif phase == "complete":
                records = (out or {}).get("records", []) if node_spec.type != "storage" else []
                if node_spec.type == "collector":
                    step_results = list(records) if isinstance(records, list) else []
                    accumulated_collect_results.extend(step_results)
                    success_count = sum(1 for r in step_results if getattr(r, "success", False))
                    failed_count = len(step_results) - success_count
                    payload = _build_collect_complete_payload(
                        component=node_spec.component,
                        collect_results=accumulated_collect_results,
                        task=task,
                        recovery_context=recovery_context,
                        error=None,
                    )
                    # Stats should describe this collector node's batch.
                    payload["targets_count"] = len(step_results)
                    payload["success_count"] = success_count
                    payload["failed_count"] = failed_count
                    payload["status"] = "succeeded" if not failed_count else "failed"
                    level = "warning" if failed_count else "info"
                    await self._emit_event(
                        task_id,
                        event_type,
                        f"Collect complete: {success_count}/{len(step_results)} succeeded",
                        level=level,
                        payload=payload,
                    )
                elif node_spec.type == "processor":
                    await self._emit_event(
                        task_id,
                        event_type,
                        "Process complete",
                        payload={"status": "succeeded", "component": node_spec.component},
                    )
                elif node_spec.type == "storage":
                    await self._emit_event(
                        task_id,
                        event_type,
                        "Storage complete",
                        payload={
                            "status": "succeeded",
                            "component": node_spec.component,
                            "stored_count": (out or {}).get("_stored", 0),
                        },
                    )
            elif phase == "error":
                await self._emit_event(
                    task_id,
                    event_type,
                    f"{event_type} failed: {error}",
                    level="error",
                    payload={"status": "failed", "component": node_spec.component, "error": error},
                )
            elif phase == "progress" and node_spec.type == "collector":
                out_payload = out if isinstance(out, dict) else {}
                await self._emit_event(
                    task_id,
                    event_type,
                    "collect progress checkpoint",
                    payload={
                        "status": "progress",
                        "component": node_spec.component,
                        "checkpoint_cursor": out_payload.get("checkpoint_cursor"),
                        "checkpoint_state": out_payload.get("checkpoint_state") or {},
                        "stats": out_payload.get("stats") or {},
                    },
                )

        dag_result = await executor.execute(
            task,
            dag,
            recovery_checkpoint=recovery_checkpoint,
            on_progress=self._report_progress,
            on_event=_on_event,
        )
        result = self._map_dag_result_to_pipeline_result(dag_result)
        # 发 pipeline 总结事件（复刻 _finalize_success 的契约）
        await self._emit_event(
            task.id,
            "pipeline",
            f"Pipeline {'succeeded' if result.success else 'partially failed'}",
            level="info" if result.success else "warning",
            payload={
                "status": "succeeded" if result.success else "failed",
                "collect_count": len(result.collect_results),
                "storage_count": result.storage_count,
                "duration_seconds": result.duration_seconds,
                "resume_state": result.resume_state,
            },
        )
        await self._report_progress(task.id, 1.0, "Pipeline completed")
        return result

    async def _try_load_stored_dag(self) -> Any | None:
        """尝试从持久化 graph 加载本 Pipeline 同名 DAG（失败返回 None）。"""
        try:
            from src.services.sqlalchemy_pipeline_repository import SQLAlchemyPipelineRepository
            from src.storage.session_factory import get_session_factory

            repo = SQLAlchemyPipelineRepository(get_session_factory())
            return await repo.load_as_dag(self.name)
        except Exception:
            return None

    def _map_dag_result_to_pipeline_result(self, dag_result: Any) -> PipelineResult:
        """逐字段把 DAGResult 映射回 PipelineResult。"""
        return PipelineResult(
            pipeline_name=dag_result.pipeline_name,
            task_id=dag_result.task_id,
            success=dag_result.success,
            collect_results=list(dag_result.collect_results),
            process_results=list(dag_result.process_results),
            output_records=list(dag_result.output_records),
            storage_count=dag_result.storage_count,
            resume_state=dag_result.resume_state,
            generated_report_id=dag_result.generated_report_id,
            generated_report_title=dag_result.generated_report_title,
            generated_report_matched_records=dag_result.generated_report_matched_records,
            started_at=dag_result.started_at,
            completed_at=dag_result.completed_at,
            errors=list(dag_result.errors),
        )

    async def _execute_legacy(
        self,
        task: Task,
        *,
        recovery_checkpoint: dict[str, Any] | None = None,
    ) -> PipelineResult:
        """原三段式执行逻辑（DAG 委托失败时的回退路径）。"""
        result = PipelineResult(pipeline_name=self.name, task_id=task.id)
        collector_steps = self._get_collectors()
        processor_steps = self._get_processors()
        storage_steps = self._get_storages()
        recovery_context = build_pipeline_recovery_context(task, recovery_checkpoint)

        total_phases = len(collector_steps) + len(processor_steps) + len(storage_steps)
        if total_phases == 0:
            logger.warning(f"Pipeline [{self.name}] has no configured steps")
            result.completed_at = datetime.now()
            return result

        current_phase = 0

        try:
            await self._instantiate_pipeline_steps(
                collector_steps,
                processor_steps,
                storage_steps,
            )
            current_phase = await self._run_collect_phase(
                task,
                collector_steps,
                result,
                recovery_context,
                current_phase=current_phase,
                total_phases=total_phases,
            )
            if collector_steps and not self._has_successful_collects(result.collect_results):
                await self._finalize_collect_failure(task, result)
                return result

            current_data, current_phase = await self._run_process_phase(
                task,
                processor_steps,
                self._build_process_inputs(result.collect_results),
                result,
                current_phase=current_phase,
                total_phases=total_phases,
            )
            records = self._build_output_records(task, current_data, recovery_context)
            result.output_records = list(records)
            current_phase = await self._run_storage_phase(
                task,
                storage_steps,
                records,
                result,
                current_phase=current_phase,
                total_phases=total_phases,
            )
            await self._finalize_success(task, result, recovery_context)
        except Exception as exc:
            await self._finalize_failure(task, result, recovery_context, exc)

        return result

    async def _instantiate_pipeline_steps(
        self,
        collector_steps: list[PipelineStep],
        processor_steps: list[PipelineStep],
        storage_steps: list[PipelineStep],
    ) -> None:
        logger.info(f"Pipeline [{self.name}] initializing components...")
        await self._instantiate_steps(collector_steps, "collector")
        await self._instantiate_steps(processor_steps, "processor")
        await self._instantiate_steps(storage_steps, "storage")

    async def _run_collect_phase(
        self,
        task: Task,
        collector_steps: list[PipelineStep],
        result: PipelineResult,
        recovery_context: dict[str, Any],
        *,
        current_phase: int,
        total_phases: int,
    ) -> int:
        targets = self._build_collect_targets(task, recovery_context)
        all_collect_results: list[CollectResult] = []

        for step in collector_steps:
            collector: BaseCollector = step.instance
            step_name = f"collect:{step.component_name}"
            if recovery_context:
                collector.config = {
                    **collector.config,
                    "recovery_checkpoint": copy.deepcopy(recovery_context),
                }
            await self._emit_event(
                task.id,
                "collect",
                f"Collecting ({len(targets)} targets)",
                payload={
                    "status": "started",
                    "component": step.component_name,
                    "targets_count": len(targets),
                },
            )
            task.add_step_log(
                step_name,
                TaskStatus.RUNNING,
                f"Collecting ({len(targets)} targets)",
            )
            logger.info(f"Pipeline [{self.name}] -> collect: {step.component_name}")

            await collector.setup(step.config)
            try:
                from src.core.dag_nodes import build_emit_checkpoint

                # Bridge PipelineEventCallback (level positional) to _emit_event kwargs form.
                def _pipeline_event(
                    _task_id: str,
                    event_type: str,
                    level: str,
                    message: str,
                    payload: dict[str, Any] | None = None,
                ):
                    return self._emit_event(
                        _task_id,
                        event_type,
                        message,
                        level=level or "info",
                        payload=payload,
                    )

                collector.config["_emit_checkpoint"] = build_emit_checkpoint(
                    task_id=task.id,
                    component=step.component_name,
                    emit_pipeline_event=_pipeline_event,
                )
                collect_results = await collector.collect_batch(targets)
                all_collect_results.extend(collect_results)

                success_count = sum(1 for item in collect_results if item.success)
                failed_results = [item for item in collect_results if not item.success]
                failed_error = "; ".join(_collect_failure_message(item) for item in failed_results)
                task.add_step_log(
                    step_name,
                    TaskStatus.SUCCESS if not failed_results else TaskStatus.FAILED,
                    f"Collect complete: {success_count}/{len(collect_results)} succeeded",
                    error=failed_error or None,
                )
                # Prefer cumulative results so multi-collector steps share one honest resume_state.
                payload = _build_collect_complete_payload(
                    component=step.component_name,
                    collect_results=all_collect_results,
                    task=task,
                    recovery_context=recovery_context,
                    error=failed_error or None,
                )
                # Stats should describe this step's batch, not the cumulative multi-step total.
                payload["targets_count"] = len(collect_results)
                payload["success_count"] = success_count
                payload["failed_count"] = len(failed_results)
                payload["status"] = "failed" if failed_results else "succeeded"
                await self._emit_event(
                    task.id,
                    "collect",
                    f"Collect complete: {success_count}/{len(collect_results)} succeeded",
                    level="warning" if failed_results else "info",
                    payload=payload,
                )
                result.errors.extend(
                    f"collect:{step.component_name}:{_collect_failure_message(item)}"
                    for item in failed_results
                )
            except Exception as exc:
                safe_error = redact_sensitive_text(str(exc))
                await self._emit_event(
                    task.id,
                    "collect",
                    f"Collect failed: {safe_error}",
                    level="error",
                    payload={
                        "status": "failed",
                        "component": step.component_name,
                        "error": safe_error,
                    },
                )
                raise
            finally:
                await collector.teardown()

            current_phase = await self._advance_phase(
                task,
                current_phase=current_phase,
                total_phases=total_phases,
                message=f"Collect complete: {step.component_name}",
            )

        result.collect_results = all_collect_results
        return current_phase

    async def _run_process_phase(
        self,
        task: Task,
        processor_steps: list[PipelineStep],
        current_data: list[ProcessInput],
        result: PipelineResult,
        *,
        current_phase: int,
        total_phases: int,
    ) -> tuple[list[ProcessInput], int]:
        for step in processor_steps:
            processor: BaseProcessor = step.instance
            step_name = f"process:{step.component_name}"
            await self._emit_event(
                task.id,
                "process",
                f"Processing {len(current_data)} records",
                payload={
                    "status": "started",
                    "component": step.component_name,
                    "input_count": len(current_data),
                },
            )
            task.add_step_log(
                step_name,
                TaskStatus.RUNNING,
                f"Processing {len(current_data)} records",
            )
            logger.info(f"Pipeline [{self.name}] -> process: {step.component_name}")

            await processor.setup()
            try:
                process_results = await processor.process_batch(current_data)
                result.process_results.extend(process_results)
                current_data = [
                    ProcessInput(
                        data=process_result.data,
                        metadata=process_result.metadata,
                        source=process_result.processor_name,
                    )
                    for process_result in process_results
                    if process_result.success and process_result.data is not None
                ]

                success_count = sum(1 for item in process_results if item.success)
                failed_count = len(process_results) - success_count
                task.add_step_log(
                    step_name,
                    TaskStatus.SUCCESS,
                    f"Process complete: {success_count}/{len(process_results)} succeeded",
                )
                await self._emit_event(
                    task.id,
                    "process",
                    f"Process complete: {success_count}/{len(process_results)} succeeded",
                    level="warning" if failed_count else "info",
                    payload={
                        "status": "failed" if failed_count else "succeeded",
                        "component": step.component_name,
                        "input_count": len(process_results),
                        "success_count": success_count,
                        "failed_count": failed_count,
                    },
                )
            except Exception as exc:
                safe_error = redact_sensitive_text(str(exc))
                await self._emit_event(
                    task.id,
                    "process",
                    f"Process failed: {safe_error}",
                    level="error",
                    payload={
                        "status": "failed",
                        "component": step.component_name,
                        "error": safe_error,
                    },
                )
                raise
            finally:
                await processor.teardown()

            current_phase = await self._advance_phase(
                task,
                current_phase=current_phase,
                total_phases=total_phases,
                message=f"Process complete: {step.component_name}",
            )

        return current_data, current_phase

    async def _run_storage_phase(
        self,
        task: Task,
        storage_steps: list[PipelineStep],
        records: list[StorageRecord],
        result: PipelineResult,
        *,
        current_phase: int,
        total_phases: int,
    ) -> int:
        for step in storage_steps:
            storage: BaseStorage = step.instance
            step_name = f"storage:{step.component_name}"
            await self._emit_event(
                task.id,
                "storage",
                f"Storing {len(records)} records",
                payload={
                    "status": "started",
                    "component": step.component_name,
                    "record_count": len(records),
                },
            )
            task.add_step_log(
                step_name,
                TaskStatus.RUNNING,
                f"Storing {len(records)} records",
            )
            logger.info(f"Pipeline [{self.name}] -> storage: {step.component_name}")

            try:
                await storage.initialize()
                await storage.save_batch(records)
                result.storage_count += len(records)

                task.add_step_log(
                    step_name,
                    TaskStatus.SUCCESS,
                    f"Storage complete: {len(records)} records",
                )
                await self._emit_event(
                    task.id,
                    "storage",
                    f"Storage complete: {len(records)} records",
                    payload={
                        "status": "succeeded",
                        "component": step.component_name,
                        "record_count": len(records),
                    },
                )
            except Exception as exc:
                safe_error = redact_sensitive_text(str(exc))
                logger.error(
                    f"Pipeline [{self.name}] storage {step.component_name} failed: {safe_error}"
                )
                task.add_step_log(step_name, TaskStatus.FAILED, f"Storage failed: {safe_error}")
                await self._emit_event(
                    task.id,
                    "storage",
                    f"Storage failed: {safe_error}",
                    level="error",
                    payload={
                        "status": "failed",
                        "component": step.component_name,
                        "record_count": len(records),
                        "error": safe_error,
                    },
                )
                result.success = False
                result.errors.append(f"storage:{step.component_name} failed: {safe_error}")
            finally:
                try:
                    await storage.close()
                except Exception as exc:
                    safe_error = redact_sensitive_text(str(exc))
                    logger.error(
                        f"Pipeline [{self.name}] storage {step.component_name} close failed: "
                        f"{safe_error}"
                    )

            current_phase = await self._advance_phase(
                task,
                current_phase=current_phase,
                total_phases=total_phases,
                message=f"Storage complete: {step.component_name}",
            )

        return current_phase

    async def _finalize_collect_failure(
        self,
        task: Task,
        result: PipelineResult,
    ) -> None:
        result.success = False
        if not result.errors:
            result.errors.append("all collect targets failed")
        result.completed_at = datetime.now()
        await self._emit_event(
            task.id,
            "error",
            "Pipeline failed: collect stage produced no usable results",
            level="error",
            payload={
                "stage": "collect",
                "errors": list(result.errors),
            },
        )
        await self._report_progress(task.id, 1.0, "Pipeline failed")
        task.update_progress(1.0, "Pipeline failed")
        logger.warning(
            f"Pipeline [{self.name}] collect stage produced no usable results: "
            f"{'; '.join(result.errors)}"
        )

    async def _finalize_success(
        self,
        task: Task,
        result: PipelineResult,
        recovery_context: dict[str, Any],
    ) -> None:
        result.success = not result.errors
        result.resume_state = build_pipeline_resume_state(
            task,
            recovery_context=recovery_context,
            collect_results=result.collect_results,
            output_records=result.output_records,
        )
        result.completed_at = datetime.now()
        await self._emit_event(
            task.id,
            "pipeline",
            f"Pipeline {'succeeded' if result.success else 'partially failed'}",
            level="info" if result.success else "warning",
            payload={
                "status": "succeeded" if result.success else "failed",
                "collect_count": len(result.collect_results),
                "storage_count": result.storage_count,
                "duration_seconds": result.duration_seconds,
                "resume_state": result.resume_state,
            },
        )
        await self._report_progress(task.id, 1.0, "Pipeline completed")
        task.update_progress(1.0, "Pipeline completed")

        status_label = "succeeded" if result.success else "partially failed"
        logger.info(
            f"Pipeline [{self.name}] {status_label}: "
            f"collect {len(result.collect_results)} items "
            f"stored {result.storage_count} records "
            f"duration {result.duration_seconds:.1f}s"
        )

    async def _finalize_failure(
        self,
        task: Task,
        result: PipelineResult,
        recovery_context: dict[str, Any],
        exc: Exception,
    ) -> None:
        safe_error = redact_sensitive_text(str(exc))
        logger.error(f"Pipeline [{self.name}] failed: {safe_error}")
        result.success = False
        result.errors.append(safe_error)
        result.resume_state = build_pipeline_resume_state(
            task,
            recovery_context=recovery_context,
            collect_results=result.collect_results,
            output_records=result.output_records,
        )
        result.completed_at = datetime.now()
        await self._emit_event(
            task.id,
            "error",
            f"Pipeline failed: {safe_error}",
            level="error",
            payload={
                "stage": "pipeline",
                "error": safe_error,
                "resume_state": result.resume_state,
            },
        )

    def _build_collect_targets(
        self,
        task: Task,
        recovery_context: dict[str, Any],
    ) -> list[CollectTarget]:
        targets = [
            CollectTarget(name=target.name, target_type=target.target_type, params=target.params)
            for target in task.targets
        ]
        return apply_collect_resume_context(
            targets,
            recovery_context.get("collect", {}),
        )

    def _build_process_inputs(
        self,
        collect_results: list[CollectResult],
    ) -> list[ProcessInput]:
        return [
            ProcessInput(
                data=collect_result.data,
                metadata={
                    **collect_result.metadata,
                    "target": collect_result.target.name,
                    "collected_at": collect_result.collected_at.isoformat(),
                },
                source=collect_result.target.name,
            )
            for collect_result in collect_results
            if collect_result.success and collect_result.data is not None
        ]

    def _build_output_records(
        self,
        task: Task,
        current_data: list[ProcessInput],
        recovery_context: dict[str, Any],
    ) -> list[StorageRecord]:
        storage_context = resolve_storage_resume_context(
            recovery_context,
            current_data=current_data,
        )
        return [
            StorageRecord(
                key=build_storage_record_key(
                    task,
                    process_input,
                    index=index,
                    storage_context=storage_context,
                ),
                data=process_input.data,
                metadata=_build_storage_metadata(task, process_input.metadata),
                source=process_input.source,
            )
            for index, process_input in enumerate(current_data)
        ]

    def _has_successful_collects(self, collect_results: list[CollectResult]) -> bool:
        return any(result.success and result.data is not None for result in collect_results)

    async def _advance_phase(
        self,
        task: Task,
        *,
        current_phase: int,
        total_phases: int,
        message: str,
    ) -> int:
        current_phase += 1
        progress = self._phase_progress(current_phase, total_phases)
        await self._report_progress(task.id, progress, message)
        task.update_progress(progress)
        return current_phase

    def _phase_progress(self, current_phase: int, total_phases: int) -> float:
        return current_phase / total_phases * 0.9

    async def _instantiate_steps(self, steps: list[PipelineStep], component_type: str) -> None:
        """实例化步骤中的组件"""
        for step in steps:
            if component_type == "storage":
                from src.storage.factory import get_storage, normalize_storage_name

                # 优先使用 step_name, fallback to default storage
                raw_name = step.component_name if step.component_name != "storage" else None
                storage_name = normalize_storage_name(raw_name) if raw_name else None
                step.component_name = storage_name or step.component_name
                step.instance = get_storage(storage_name)
                logger.debug(f"  实例化 [storage] via factory: {step.instance.__class__.__name__}")
            else:
                cls_ = registry.get(component_type, step.component_name)
                step.instance = cls_(config=step.config)
                logger.debug(f"  实例化 [{component_type}] {step.component_name}")

    def to_config(self) -> dict[str, Any]:
        """导出 Pipeline 配置（用于持久化和 API）"""
        return {
            "name": self.name,
            "steps": [
                {
                    "type": step.step_type.value,
                    "name": step.component_name,
                    "config": step.config,
                }
                for step in self.steps
            ],
        }

    @classmethod
    def from_config(cls, config: dict[str, Any]) -> Pipeline:
        """从配置字典恢复 Pipeline"""
        pipeline = cls(name=config["name"])
        for step_cfg in config.get("steps", []):
            step_type = step_cfg["type"]
            name = step_cfg["name"]
            step_config = step_cfg.get("config", {})
            if step_type == "collector":
                pipeline.add_collector(name, step_config)
            elif step_type == "processor":
                pipeline.add_processor(name, step_config)
            elif step_type == "storage":
                pipeline.add_storage(name, step_config)
        return pipeline


def _build_storage_metadata(task: Task, metadata: dict[str, Any]) -> dict[str, Any]:
    enriched = redact_sensitive(dict(metadata or {}))
    target_name = enriched.get("target")  # 提前初始化，避免后续作用域问题
    target_params = enriched.get("target_params")
    if not isinstance(target_params, dict):
        target_params = {}
        for target in task.targets:
            if not target_name or target.name == target_name:
                target_params = dict(target.params)
                enriched.setdefault("target_type", target.target_type)
                break

    data_group = task.config.get("data_group", {})
    if not isinstance(data_group, dict):
        data_group = {}
    group_name = str(data_group.get("name") or data_group.get("group_name") or "").strip()
    group_id = str(data_group.get("id") or data_group.get("group_id") or group_name).strip()

    # 自动派生 group：如果没有显式指定 data_group，从该记录实际 target 名称推导
    # 每条记录有自己的 target name（多目标任务中也能正确分组）
    if not group_name and not group_id:
        record_target = str(enriched.get("target", "") or target_name or "").strip()
        if not record_target and task.targets:
            record_target = str(task.targets[0].name or "").strip()
        if record_target:
            group_name = record_target
            group_id = normalize_key(record_target)

    enriched["source_task"] = {
        "task_id": task.id,
        "task_name": task.name,
        "pipeline_name": task.pipeline_name,
        "collector_name": task.collector_name,
        "target": enriched.get("target", ""),
        "target_type": enriched.get("target_type", ""),
        "target_params": redact_sensitive(target_params),
        "task_config": redact_sensitive(task.config),
        "created_at": task.created_at.isoformat(),
    }
    if group_name or group_id:
        enriched["group_id"] = group_id or group_name
        enriched["group_name"] = group_name or group_id

    # Promote key fields to top-level for storage column extraction
    enriched.setdefault("collector", task.collector_name)
    enriched.setdefault("task_id", task.id)
    enriched.setdefault("target", enriched.get("target") or "")
    enriched.setdefault(
        "game_name", str(enriched.get("target") or enriched.get("group_name") or task.name or "")
    )

    refresh = task.config.get("refresh", {})
    if isinstance(refresh, dict):
        for key in (
            "refresh_parent_key",
            "refresh_series_id",
            "refresh_run_id",
            "refresh_kind",
            "scheduled_job_id",
        ):
            if refresh.get(key) not in (None, ""):
                enriched[key] = refresh[key]

    return enriched
