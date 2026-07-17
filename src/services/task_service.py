"""Task service — shared business logic for task CRUD, precheck, and pipeline operations."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from src.core.collector_metadata import (
    build_collector_recovery_info,
    collector_metadata_payload,
    get_collector_metadata,
    list_session_sensitive_collectors,
)
from src.core.diagnostics import build_collector_session_diagnostics
from src.core.diagnostics import build_session_readiness_summary
from src.core.sensitive import redact_sensitive_text
from src.core.task import Task, TaskTarget, TaskStatus
from src.services.session_inventory_sync import (
    release_task_session_claim_via_provider_best_effort,
    sync_session_inventory_via_provider_best_effort,
)
from src.services.task_precheck_service import TaskPrecheckService


class TaskService:
    def __init__(
        self,
        scheduler,
        *,
        get_session_registry: Callable[[], Any] | None = None,
    ):
        self._scheduler = scheduler
        self._precheck_service = TaskPrecheckService(scheduler)
        self._get_session_registry = get_session_registry

    # ------------------------------------------------------------------
    # Task CRUD
    # ------------------------------------------------------------------

    def list_tasks(self, status: str | None = None) -> list[Task]:
        if status:
            try:
                return self._scheduler.get_tasks_by_status(TaskStatus(status))
            except ValueError:
                raise ValueError(f"Invalid status: {status}")
        tasks = self._scheduler.get_all_tasks()
        return sorted(tasks, key=lambda t: t.created_at, reverse=True)

    def get_task(self, task_id: str) -> Task | None:
        return self._scheduler.get_task(task_id)

    def get_task_logs(self, task_id: str) -> list | None:
        task = self._scheduler.get_task(task_id)
        if task is None:
            return None
        return [
            {
                "step": redact_sensitive_text(log.step_name),
                "status": log.status.value,
                "message": redact_sensitive_text(log.message),
                "error": redact_sensitive_text(log.error) if log.error else None,
                "started_at": log.started_at.isoformat() if log.started_at else None,
                "completed_at": log.completed_at.isoformat() if log.completed_at else None,
            }
            for log in task.step_logs
        ]

    async def get_task_events(
        self,
        task_id: str,
        *,
        limit: int = 200,
        offset: int = 0,
        order: str = "asc",
    ):
        task = self._scheduler.get_task(task_id)
        if task is None:
            return None
        return await self._scheduler.get_task_events(
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
        task = self._scheduler.get_task(task_id)
        if task is None:
            return None
        return await self._scheduler.get_task_artifacts(task_id, limit=limit, offset=offset)

    async def get_task_checkpoints(
        self,
        task_id: str,
        *,
        limit: int = 200,
        offset: int = 0,
    ):
        task = self._scheduler.get_task(task_id)
        if task is None:
            return None
        checkpoints = await self._scheduler.get_task_checkpoints(
            task_id,
            limit=limit,
            offset=offset,
        )
        latest = await self._scheduler.get_latest_task_checkpoint(task_id)
        return checkpoints, latest

    async def cancel(self, task_id: str) -> bool:
        task = self._scheduler.get_task(task_id)
        if task is None:
            return False

        claim = task.config.get("worker_claim") if isinstance(task.config, dict) else None
        if isinstance(claim, dict) and task.status == TaskStatus.RUNNING:
            return False

        cancelled = await self._scheduler.cancel(task_id)
        if not cancelled:
            return False

        if (
            isinstance(claim, dict)
            and task.status == TaskStatus.CANCELLED
            and not self._should_retain_retry_session_claim(task)
            and self._get_session_registry is not None
        ):
            await release_task_session_claim_via_provider_best_effort(
                self._get_session_registry,
                task,
                context="task_cancel",
                disposition="released",
                worker_id=str(claim.get("worker_id") or ""),
                task_id=task.id,
            )
        return True

    async def delete(self, task_id: str) -> bool:
        return await self._scheduler.delete_task(task_id)

    def get_stats(self) -> dict[str, Any]:
        """Scheduler counters plus server-side Dashboard attention digests."""
        stats = dict(self._scheduler.get_stats())
        tasks = self._scheduler.get_all_tasks()
        health: dict[str, Any] | None = None
        diagnostics: dict[str, Any] | None = None

        try:
            from src.core.diagnostics import build_config_diagnostics, build_health_report

            health = build_health_report(stats)
            diagnostics = build_config_diagnostics()
        except Exception:
            # Diagnostics optional; still build failed-task digests below.
            health = None
            diagnostics = None

        try:
            from src.services.dashboard_attention import build_dashboard_attention

            stats["attention"] = build_dashboard_attention(
                tasks,
                health=health,
                diagnostics=diagnostics,
            )
        except Exception:
            # Never invent empty failed_tasks when digests fail mid-flight.
            # Prefer partial failed digests alone over wiping real failures.
            try:
                from src.services.dashboard_attention import (
                    build_failed_task_digests,
                    build_health_attention_items,
                )

                stats["attention"] = {
                    "failed_tasks": build_failed_task_digests(tasks),
                    "health_issues": build_health_attention_items(
                        health,
                        diagnostics,
                    ),
                }
            except Exception:
                stats.setdefault(
                    "attention",
                    {"failed_tasks": [], "health_issues": []},
                )
        return stats

    # ------------------------------------------------------------------
    # Task creation (shared between route and Agent)
    # ------------------------------------------------------------------

    async def create(
        self,
        name: str,
        pipeline_name: str,
        collector_name: str = "",
        targets: list[dict[str, Any]] | None = None,
        config: dict[str, Any] | None = None,
        description: str = "",
    ) -> Task:
        targets = targets or []
        config = config or {}

        # 允许仅存在 graph 的 DAG：先投影进 scheduler 内存，再 precheck/submit
        if hasattr(self._scheduler, "resolve_pipeline"):
            await self._scheduler.resolve_pipeline(pipeline_name)

        precheck = self.precheck(
            name=name,
            pipeline_name=pipeline_name,
            collector_name=collector_name,
            targets=targets,
            config=config,
        )
        if not precheck.can_submit:
            raise ValueError(f"Task precheck failed: {TaskPrecheckService.format_errors(precheck)}")

        resolved_collector = precheck.collector_name or collector_name

        task_targets = [
            TaskTarget(
                name=t.get("name", ""),
                target_type=t.get("target_type", "default"),
                params=t.get("params", {}),
            )
            for t in targets
        ]

        task = Task(
            name=name,
            description=description,
            pipeline_name=pipeline_name,
            collector_name=resolved_collector,
            targets=task_targets,
            config=config,
        )

        try:
            await self._scheduler.submit(task, pipeline_name=pipeline_name)
        except (ValueError, RuntimeError) as e:
            raise ValueError(str(e))

        if self._get_session_registry is not None and precheck.session_diagnostics:
            await sync_session_inventory_via_provider_best_effort(
                self._get_session_registry,
                diagnostics=precheck.session_diagnostics,
                context="task_create",
                collector_id=str(precheck.session_diagnostics.get("collector_id") or ""),
                task_id=task.id,
            )

        return task

    # ------------------------------------------------------------------
    # Precheck
    # ------------------------------------------------------------------

    def precheck(
        self,
        name: str,
        pipeline_name: str,
        collector_name: str = "",
        targets: list[dict[str, Any]] | None = None,
        config: dict[str, Any] | None = None,
        *,
        deep: bool = False,
    ):
        return self._precheck_service.precheck(
            name=name,
            pipeline_name=pipeline_name,
            collector_name=collector_name,
            targets=targets,
            config=config,
            deep=deep,
        )

    async def precheck_async(
        self,
        name: str,
        pipeline_name: str,
        collector_name: str = "",
        targets: list[dict[str, Any]] | None = None,
        config: dict[str, Any] | None = None,
        *,
        deep: bool | None = None,
    ):
        return await self._precheck_service.precheck_async(
            name=name,
            pipeline_name=pipeline_name,
            collector_name=collector_name,
            targets=targets,
            config=config,
            deep=deep,
        )

    # ------------------------------------------------------------------
    # Pipeline helpers
    # ------------------------------------------------------------------

    def get_pipeline(self, name: str):
        return self._scheduler.get_pipeline(name)

    def get_all_pipelines(self):
        return self._scheduler.get_all_pipelines()

    async def get_task_recovery_info(self, task_id: str) -> dict[str, Any] | None:
        task = self._scheduler.get_task(task_id)
        if task is None:
            return None
        latest = await self._scheduler.get_latest_task_checkpoint(task_id)
        latest_payload = latest.to_public_payload() if latest is not None else None
        payload = build_collector_recovery_info(
            task.collector_name,
            latest_checkpoint=latest_payload,
        )
        return self._apply_task_session_snapshot(payload, self._task_session_snapshot(task))

    def get_task_collector_metadata(self, task_id: str) -> dict[str, Any] | None:
        task = self._scheduler.get_task(task_id)
        if task is None:
            return None
        metadata = get_collector_metadata(task.collector_name)
        if metadata is None:
            return {}

        payload = collector_metadata_payload(task.collector_name)
        return self._apply_task_session_snapshot(payload, self._task_session_snapshot(task))

    def get_task_session_diagnostics(self, task_id: str) -> dict[str, Any] | None:
        task = self._scheduler.get_task(task_id)
        if task is None:
            return None
        snapshot = self._task_session_snapshot(task)
        if snapshot:
            return snapshot
        return build_collector_session_diagnostics(task.collector_name)

    def get_task_session_readiness(self, task_id: str) -> dict[str, Any] | None:
        diagnostics = self.get_task_session_diagnostics(task_id)
        if diagnostics is None:
            return None
        return build_session_readiness_summary(diagnostics)

    def list_session_diagnostics(
        self, collector_ids: list[str] | None = None
    ) -> list[dict[str, Any]]:
        ids = collector_ids or list_session_sensitive_collectors()
        return [build_collector_session_diagnostics(collector_id) for collector_id in ids]

    async def sync_from_diagnostics(self, diagnostics: dict[str, Any]):
        if self._get_session_registry is None:
            return None
        return await sync_session_inventory_via_provider_best_effort(
            self._get_session_registry,
            diagnostics,
            context="task_service_sync",
            collector_id=str(diagnostics.get("collector_id") or "")
            if isinstance(diagnostics, dict)
            else "",
        )

    def _task_session_snapshot(self, task: Task) -> dict[str, Any]:
        claim = task.config.get("worker_claim") if isinstance(task.config, dict) else None
        if not isinstance(claim, dict):
            return {}
        diagnostics = claim.get("session_diagnostics")
        return diagnostics if isinstance(diagnostics, dict) else {}

    def _apply_task_session_snapshot(
        self,
        payload: dict[str, Any],
        snapshot: dict[str, Any],
    ) -> dict[str, Any]:
        if not snapshot:
            return payload

        payload["requires_session"] = bool(
            snapshot.get("requires_session", payload.get("requires_session"))
        )

        for field in (
            "session_mode",
            "default_session_mode",
            "configured_session_mode",
            "session_mode_source",
            "session_mode_override_status",
            "worker_binding",
        ):
            if field in snapshot:
                payload[field] = str(snapshot.get(field) or "")

        if "supported_session_modes" in snapshot:
            payload["supported_session_modes"] = [
                str(item)
                for item in (snapshot.get("supported_session_modes") or [])
                if str(item or "").strip()
            ]
        return payload

    @staticmethod
    def _should_retain_retry_session_claim(task: Task) -> bool:
        if task.status != TaskStatus.RETRYING:
            return False

        claim = task.config.get("worker_claim") if isinstance(task.config, dict) else None
        if not isinstance(claim, dict):
            return False
        session_diagnostics = claim.get("session_diagnostics")
        if not isinstance(session_diagnostics, dict):
            return False

        worker_binding = str(session_diagnostics.get("worker_binding") or "").strip().lower()
        session_mode = str(session_diagnostics.get("session_mode") or "").strip().lower()
        requires_session = bool(session_diagnostics.get("requires_session"))
        return requires_session and (worker_binding == "sticky" or session_mode == "local_profile")
