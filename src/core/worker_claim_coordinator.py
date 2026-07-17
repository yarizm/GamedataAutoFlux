"""Worker-claim execution coordination extracted from Scheduler."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, Awaitable, Callable

from src.core.collector_metadata import (
    build_collector_recovery_info,
    collector_metadata_payload,
    get_collector_metadata,
    required_worker_capabilities,
    resolve_session_mode,
)
from src.core.diagnostics import build_collector_session_diagnostics
from src.core.pipeline import Pipeline, PipelineResult
from src.core.dag import pipeline_to_dag
from src.core.sensitive import redact_sensitive, redact_sensitive_text
from src.core.task import Task, TaskStatus
from src.storage.base import StorageRecord

PersistTaskFn = Callable[[Task], Awaitable[None]]
EmitTaskEventFn = Callable[..., Awaitable[Any]]
EmitTaskCompletedEventFn = Callable[[Task, bool, Any, Pipeline | None, list[str]], Awaitable[None]]
GetLatestCheckpointFn = Callable[[str], Awaitable[Any]]
GetTaskCheckpointFn = Callable[[str, str], Awaitable[Any]]
RegisterTaskArtifactFn = Callable[..., Awaitable[Any]]
RegisterTaskCheckpointFn = Callable[..., Awaitable[Any]]
GetPipelineFn = Callable[[str], Pipeline | None]
GetPipelinesFn = Callable[[], dict[str, Pipeline]]
ReserveSessionClaimFn = Callable[[Task, str, str, dict[str, Any]], Awaitable[Any]]


class WorkerClaimCoordinator:
    """Coordinates worker-claim task lifecycle and task-scoped worker updates."""

    def __init__(
        self,
        *,
        persist_task: PersistTaskFn,
        emit_task_event: EmitTaskEventFn,
        emit_task_completed_event: EmitTaskCompletedEventFn,
        get_latest_task_checkpoint: GetLatestCheckpointFn,
        register_task_artifact: RegisterTaskArtifactFn,
        register_task_checkpoint: RegisterTaskCheckpointFn,
        get_pipeline: GetPipelineFn,
        get_pipelines: GetPipelinesFn,
        get_task_checkpoint: GetTaskCheckpointFn | None = None,
    ) -> None:
        self._persist_task = persist_task
        self._emit_task_event = emit_task_event
        self._emit_task_completed_event = emit_task_completed_event
        self._get_latest_task_checkpoint = get_latest_task_checkpoint
        self._get_task_checkpoint = get_task_checkpoint
        self._register_task_artifact = register_task_artifact
        self._register_task_checkpoint = register_task_checkpoint
        self._get_pipeline = get_pipeline
        self._get_pipelines = get_pipelines

    async def claim_task_for_worker(
        self,
        worker_id: str,
        *,
        tasks: list[Task],
        capabilities: list[str] | None = None,
        reserve_session_claim: ReserveSessionClaimFn | None = None,
    ) -> dict[str, Any] | None:
        """Claim the next pending task for a worker."""
        safe_worker_id = redact_sensitive_text(str(worker_id or "")).strip()
        if not safe_worker_id:
            raise ValueError("worker_id is required")

        pipelines = self._get_pipelines()
        pending = [
            task
            for task in tasks
            if task.status in (TaskStatus.PENDING, TaskStatus.RETRYING)
            and _task_matches_worker_session_binding(task, safe_worker_id, pipelines)
            and _task_matches_worker_capabilities(task, pipelines, capabilities)
        ]
        pending.sort(key=lambda item: (-int(item.priority), item.created_at))
        selected_task: Task | None = None
        selected_pipeline: Pipeline | None = None
        selected_collector_name = ""
        selected_session_diagnostics: dict[str, Any] = {}
        session_reserved = False
        blocked_sessions: list[dict[str, Any]] = []

        for task in pending:
            pipeline = self._get_pipeline(task.pipeline_name)
            if pipeline is None:
                continue

            collector_name = _task_collector_name(task, pipelines)
            session_diagnostics = (
                build_collector_session_diagnostics(collector_name) if collector_name else {}
            )
            if reserve_session_claim is not None and session_diagnostics:
                reserve_result = await reserve_session_claim(
                    task,
                    safe_worker_id,
                    collector_name,
                    session_diagnostics,
                )
                session_reserved, blocked_session = _coerce_session_reservation_result(
                    reserve_result,
                    task=task,
                    collector_name=collector_name,
                    diagnostics=session_diagnostics,
                )
                if not session_reserved:
                    if blocked_session:
                        blocked_sessions.append(blocked_session)
                    continue

            selected_task = task
            selected_pipeline = pipeline
            selected_collector_name = collector_name
            selected_session_diagnostics = session_diagnostics
            break

        if selected_task is None or selected_pipeline is None:
            if blocked_sessions:
                claim_reason = str(blocked_sessions[0].get("reason") or "session_claimed")
                return {
                    "claim_status": "blocked",
                    "claim_reason": claim_reason,
                    "blocked_sessions": blocked_sessions,
                }
            return None

        task = selected_task
        pipeline = selected_pipeline

        task.start()
        task.config = {
            **task.config,
            "worker_claim": {
                "worker_id": safe_worker_id,
                "claimed_at": datetime.now().isoformat(),
                "execution_backend": "worker_claim",
                "collector_id": selected_collector_name,
                "session_diagnostics": redact_sensitive(selected_session_diagnostics),
            },
        }

        await self._persist_task(task)
        await self._emit_task_event(
            task,
            "claimed",
            f"Task claimed by worker: {safe_worker_id}",
            payload={
                "status": task.status.value,
                "worker_id": safe_worker_id,
                "execution_backend": "worker_claim",
            },
        )
        latest_checkpoint = await self._resolve_recovery_checkpoint_obj(task)
        latest_checkpoint_payload = (
            latest_checkpoint.to_worker_payload() if latest_checkpoint is not None else None
        )
        self._clear_one_shot_resume_flags(task)
        await self._persist_task(task)
        collector_name = selected_collector_name
        collector_metadata = get_collector_metadata(collector_name)
        graph_payload = None
        if pipeline is not None:
            # 优先持久化真图（条件边/拓扑），回退 pipeline_to_dag 投影
            try:
                from src.services.sqlalchemy_pipeline_repository import SQLAlchemyPipelineRepository
                from src.storage.session_factory import get_session_factory

                stored = await SQLAlchemyPipelineRepository(get_session_factory()).load_as_dag(
                    pipeline.name
                )
                if stored is not None:
                    graph_payload = stored.to_storage()
            except Exception:
                graph_payload = None
            if graph_payload is None:
                graph_payload = pipeline_to_dag(pipeline).to_storage()

        return {
            "task_id": task.id,
            "claim_status": "claimed",
            "claim_reason": "",
            "blocked_sessions": blocked_sessions,
            "task": task.to_storage_payload(),
            "pipeline": pipeline.to_config() if pipeline is not None else None,
            "graph": graph_payload,
            "payload_version": "2",
            "latest_checkpoint": latest_checkpoint_payload,
            "collector_metadata": (
                collector_metadata_payload(collector_name) if collector_metadata is not None else {}
            ),
            "session_diagnostics": (selected_session_diagnostics if collector_name else {}),
            "session_reserved": session_reserved,
            "recovery": build_collector_recovery_info(
                collector_name,
                latest_checkpoint=latest_checkpoint_payload,
            )
            if collector_name
            else {},
        }

    async def _resolve_recovery_checkpoint_obj(self, task: Task) -> Any | None:
        """Resolve recovery checkpoint object honoring resume/rerun one-shot flags."""
        cfg = task.config if isinstance(task.config, dict) else {}
        if cfg.get("force_full_rerun"):
            return None

        checkpoint_id = str(cfg.get("resume_checkpoint_id") or "").strip()
        if checkpoint_id and self._get_task_checkpoint is not None:
            return await self._get_task_checkpoint(task.id, checkpoint_id)

        return await self._get_latest_task_checkpoint(task.id)

    @staticmethod
    def _clear_one_shot_resume_flags(task: Task) -> None:
        if not isinstance(task.config, dict):
            return
        cfg = dict(task.config)
        changed = False
        for key in ("force_full_rerun", "resume_checkpoint_id", "resume_mode"):
            if key in cfg:
                cfg.pop(key, None)
                changed = True
        if changed:
            task.config = cfg

    async def complete_worker_task(
        self,
        worker_id: str,
        task: Task,
        *,
        result: dict[str, Any] | None = None,
    ) -> Task:
        """Mark a worker-claimed task as successful."""
        pipeline_result = _coerce_worker_pipeline_result(task, result)
        if pipeline_result is not None:
            await self._emit_task_completed_event(
                task,
                True,
                pipeline_result,
                self._get_pipeline(task.pipeline_name),
                [],
            )
        safe_result = _build_worker_result_summary(result, pipeline_result)

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
            "Worker reported task completion successfully.",
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
        task: Task,
        *,
        error: str,
        result: dict[str, Any] | None = None,
    ) -> Task:
        """Mark a worker-claimed task as failed."""
        safe_error = redact_sensitive_text(str(error or "Worker task failed"))
        pipeline_result = _coerce_worker_pipeline_result(task, result)
        safe_result = _build_worker_result_summary(result, pipeline_result)
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
            "Worker reported task failure.",
            level="error",
            payload={
                "status": task.status.value,
                "worker_id": redact_sensitive_text(str(worker_id)),
                "error": safe_error,
                "result": safe_result,
            },
        )
        await self._emit_task_completed_event(
            task,
            False,
            pipeline_result or task.result,
            self._get_pipeline(task.pipeline_name),
            [safe_error],
        )
        return task

    async def interrupt_worker_tasks(
        self,
        worker_id: str,
        *,
        tasks: list[Task],
        reason: str = "",
    ) -> list[Task]:
        """Cancel running tasks claimed by a worker that is no longer healthy."""
        reconciled = await self.reconcile_stale_worker_tasks(
            worker_id,
            tasks=tasks,
            reason=reason,
        )
        return list(reconciled["interrupted_tasks"])

    async def reconcile_stale_worker_tasks(
        self,
        worker_id: str,
        *,
        tasks: list[Task],
        reason: str = "",
    ) -> dict[str, list[Task]]:
        """Recover running tasks and sticky retry leases held by a stale worker."""
        safe_worker_id = redact_sensitive_text(str(worker_id or "")).strip()
        if not safe_worker_id:
            return {
                "interrupted_tasks": [],
                "recovered_retry_tasks": [],
            }
        safe_reason = redact_sensitive_text(
            str(reason or "Worker heartbeat is stale; task was interrupted.")
        )

        interrupted_tasks = [
            task
            for task in tasks
            if task.status == TaskStatus.RUNNING and _claimed_worker_id(task) == safe_worker_id
        ]
        recovered_retry_tasks = [
            task
            for task in tasks
            if task.status == TaskStatus.RETRYING
            and _claimed_worker_id(task) == safe_worker_id
            and should_retain_retry_session_claim(task)
        ]

        for task in interrupted_tasks:
            _mark_task_interrupted(
                task,
                worker_id=safe_worker_id,
                reason=safe_reason,
            )
        for task in recovered_retry_tasks:
            _mark_task_interrupted(
                task,
                worker_id=safe_worker_id,
                reason=safe_reason,
                recovered_retry=True,
            )

        for task in interrupted_tasks:
            await self._persist_task(task)
            await self._emit_task_event(
                task,
                "interrupted",
                "Worker heartbeat stale; running task interrupted.",
                level="warning",
                payload={
                    "status": task.status.value,
                    "worker_id": safe_worker_id,
                    "error": safe_reason,
                },
            )
        for task in recovered_retry_tasks:
            await self._persist_task(task)
            await self._emit_task_event(
                task,
                "interrupted",
                "Worker heartbeat stale; retry-held task recovered.",
                level="warning",
                payload={
                    "status": task.status.value,
                    "worker_id": safe_worker_id,
                    "error": safe_reason,
                    "recovered_retry": True,
                    "previous_status": TaskStatus.RETRYING.value,
                },
            )
        return {
            "interrupted_tasks": interrupted_tasks,
            "recovered_retry_tasks": recovered_retry_tasks,
        }

    async def append_worker_task_event(
        self,
        worker_id: str,
        task: Task,
        event_type: str,
        *,
        level: str = "info",
        message: str = "",
        payload: dict[str, Any] | None = None,
        maybe_record_checkpoint: Callable[[Task, str, dict[str, Any] | None], Awaitable[None]]
        | None = None,
    ):
        """Append an event for a worker-claimed task."""
        next_payload = {
            "worker_id": redact_sensitive_text(str(worker_id)),
            **(payload or {}),
        }
        event = await self._emit_task_event(
            task,
            event_type,
            message,
            level=level,
            payload=next_payload,
        )
        if maybe_record_checkpoint is not None:
            await maybe_record_checkpoint(task, event_type, next_payload)
        return event

    async def register_worker_task_artifact(
        self,
        worker_id: str,
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
        """Register an artifact for a worker-claimed task."""
        next_metadata = {
            "worker_id": redact_sensitive_text(str(worker_id)),
            **(metadata or {}),
        }
        return await self._register_task_artifact(
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
        task: Task,
        *,
        recovery_level: str = "L0",
        cursor: dict[str, Any] | None = None,
        state: dict[str, Any] | None = None,
        stats: dict[str, Any] | None = None,
        artifacts: list[dict[str, Any]] | None = None,
        metadata: dict[str, Any] | None = None,
    ):
        """Register a checkpoint for a worker-claimed task."""
        next_metadata = {
            "worker_id": redact_sensitive_text(str(worker_id)),
            **(metadata or {}),
        }
        return await self._register_task_checkpoint(
            task,
            worker_id=redact_sensitive_text(str(worker_id)),
            recovery_level=recovery_level,
            cursor=cursor,
            state=state,
            stats=stats,
            artifacts=artifacts,
            metadata=next_metadata,
        )


def get_claimed_task_for_worker(
    worker_id: str,
    task_id: str,
    tasks: dict[str, Task],
) -> Task | None:
    safe_worker_id = redact_sensitive_text(str(worker_id or "")).strip()
    task = tasks.get(task_id)
    if task is None or task.status != TaskStatus.RUNNING:
        return None
    if _claimed_worker_id(task) != safe_worker_id:
        return None
    return task


def should_retain_retry_session_claim(task: Task) -> bool:
    if task.status != TaskStatus.RETRYING:
        return False

    claim = task.config.get("worker_claim") if isinstance(task.config, dict) else None
    if isinstance(claim, dict):
        session_diagnostics = claim.get("session_diagnostics")
        if isinstance(session_diagnostics, dict):
            worker_binding = str(session_diagnostics.get("worker_binding") or "").strip().lower()
            session_mode = str(session_diagnostics.get("session_mode") or "").strip().lower()
            requires_session = bool(session_diagnostics.get("requires_session"))
            if requires_session and (worker_binding == "sticky" or session_mode == "local_profile"):
                return True

    collector_name = str(task.collector_name or "").strip()
    return bool(collector_name) and resolve_session_mode(collector_name) == "local_profile"


def _coerce_worker_pipeline_result(
    task: Task,
    payload: dict[str, Any] | None,
) -> PipelineResult | None:
    if not isinstance(payload, dict):
        return None

    output_records: list[StorageRecord] = []
    for item in payload.get("output_records", []):
        if isinstance(item, StorageRecord):
            output_records.append(item)
            continue
        if isinstance(item, dict):
            try:
                output_records.append(StorageRecord.model_validate(item))
            except Exception:
                continue

    started_at = task.started_at or task.created_at
    duration_seconds = payload.get("duration_seconds")
    completed_at = datetime.now(timezone.utc)
    if started_at is not None and duration_seconds is not None:
        try:
            completed_at = started_at + timedelta(seconds=float(duration_seconds))
        except (TypeError, ValueError, OverflowError):
            completed_at = datetime.now(timezone.utc)

    return PipelineResult(
        pipeline_name=str(payload.get("pipeline_name") or task.pipeline_name),
        task_id=str(payload.get("task_id") or task.id),
        success=bool(payload.get("success", True)),
        output_records=output_records,
        storage_count=_safe_int(payload.get("storage_count")),
        generated_report_id=_safe_optional_text(payload.get("generated_report_id")),
        generated_report_title=_safe_optional_text(payload.get("generated_report_title")),
        generated_report_matched_records=_safe_int(payload.get("generated_report_matched_records")),
        started_at=started_at,
        completed_at=completed_at,
        errors=_safe_error_messages(payload.get("errors") or []),
    )


def _build_worker_result_summary(
    raw_payload: dict[str, Any] | None,
    pipeline_result: PipelineResult | None,
) -> dict[str, Any]:
    summary = redact_sensitive(raw_payload or {})
    if pipeline_result is None:
        return summary

    output_records = summary.get("output_records")
    output_count = (
        len(output_records)
        if isinstance(output_records, list)
        else len(pipeline_result.output_records)
    )
    merged = {
        **summary,
        "success": pipeline_result.success,
        "pipeline_name": pipeline_result.pipeline_name,
        "task_id": pipeline_result.task_id,
        "storage_count": pipeline_result.storage_count,
        "errors": list(pipeline_result.errors),
        "duration_seconds": pipeline_result.duration_seconds,
        "collection_summary": summary.get("collection_summary", {}),
        "resume_state": summary.get("resume_state", pipeline_result.resume_state),
        "generated_report_id": pipeline_result.generated_report_id,
        "generated_report_title": pipeline_result.generated_report_title,
        "generated_report_matched_records": pipeline_result.generated_report_matched_records,
        "output_record_count": output_count,
    }
    merged.pop("output_records", None)
    return merged


def _safe_error_messages(errors: list[str]) -> list[str]:
    return [redact_sensitive_text(str(error or "")) for error in errors if str(error or "")]


def _safe_optional_text(value: Any) -> str | None:
    text = redact_sensitive_text(str(value or "")).strip()
    return text or None


def _safe_int(value: Any, *, default: int = 0) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return default
    return parsed if parsed >= 0 else default


def _task_matches_worker_capabilities(
    task: Task,
    pipelines: dict[str, Pipeline],
    capabilities: list[str] | None,
) -> bool:
    worker_capabilities = {
        str(capability).strip().lower() for capability in capabilities or [] if capability
    }

    collector_name = _task_collector_name(task, pipelines)
    if not collector_name:
        return True

    required = {
        capability.strip().lower()
        for capability in required_worker_capabilities(collector_name)
        if str(capability).strip()
    }
    if required and not required <= worker_capabilities:
        return False
    if not worker_capabilities:
        return True

    accepted = {collector_name.lower()}
    metadata = get_collector_metadata(collector_name)
    if metadata is not None:
        accepted.update(
            str(capability).strip().lower()
            for capability in metadata.capabilities
            if str(capability).strip()
        )
    return bool(accepted & worker_capabilities)


def _task_matches_worker_session_binding(
    task: Task,
    worker_id: str,
    pipelines: dict[str, Pipeline],
) -> bool:
    collector_name = _task_collector_name(task, pipelines)
    if not collector_name or task.status != TaskStatus.RETRYING:
        return True

    metadata = get_collector_metadata(collector_name)
    if metadata is None or resolve_session_mode(collector_name) != "local_profile":
        return True

    previous_worker_id = _claimed_worker_id(task)
    if not previous_worker_id:
        return True
    return previous_worker_id == worker_id


def _coerce_session_reservation_result(
    result: Any,
    *,
    task: Task,
    collector_name: str,
    diagnostics: dict[str, Any],
) -> tuple[bool, dict[str, Any]]:
    if isinstance(result, bool):
        if result:
            return True, {}
        return False, _blocked_session_payload(
            task=task,
            collector_name=collector_name,
            diagnostics=diagnostics,
            reason="session_claimed",
        )
    if not isinstance(result, dict):
        return bool(result), {}

    allowed = bool(result.get("allowed", result.get("reserved", False)))
    if allowed:
        return True, {}

    blocked = result.get("blocked_session")
    if isinstance(blocked, dict):
        return False, redact_sensitive(blocked)
    return False, _blocked_session_payload(
        task=task,
        collector_name=collector_name,
        diagnostics=diagnostics,
        reason=str(result.get("reason") or "session_claimed"),
    )


def _blocked_session_payload(
    *,
    task: Task,
    collector_name: str,
    diagnostics: dict[str, Any],
    reason: str,
) -> dict[str, Any]:
    account = diagnostics.get("session_account", {})
    if not isinstance(account, dict):
        account = {}
    return redact_sensitive(
        {
            "reason": reason,
            "task_id": task.id,
            "task_name": task.name,
            "collector_id": collector_name,
            "session_mode": diagnostics.get("session_mode", ""),
            "worker_binding": diagnostics.get("worker_binding", ""),
            "account_kind": account.get("account_kind", ""),
            "account_id": account.get("account_id", ""),
        }
    )


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


def _mark_task_interrupted(
    task: Task,
    *,
    worker_id: str,
    reason: str,
    recovered_retry: bool = False,
) -> None:
    claim = task.config.get("worker_claim") if isinstance(task.config, dict) else {}
    if not isinstance(claim, dict):
        claim = {}
    interrupted_at = datetime.now().isoformat()
    next_claim = {
        **claim,
        "worker_id": worker_id,
        "interrupted_at": interrupted_at,
        "interruption_reason": reason,
    }
    if recovered_retry:
        next_claim["retry_recovered_at"] = interrupted_at
    task.config = {
        **task.config,
        "worker_claim": next_claim,
    }
    task.result = {
        "success": False,
        "task_id": task.id,
        "worker_id": worker_id,
        "interrupted": True,
        "error": reason,
    }
    if recovered_retry:
        task.result["recovered_retry"] = True
    task.add_step_log(
        "worker:retry_recovered" if recovered_retry else "worker:interrupted",
        TaskStatus.CANCELLED,
        "Retrying worker task recovered after worker interruption"
        if recovered_retry
        else "Worker task interrupted",
        error=reason,
    )
    task.cancel()
    task.error = reason
