"""Task service — shared business logic for task CRUD, precheck, and pipeline operations."""

from __future__ import annotations

import importlib.util
from typing import Any

from src.core.config import get as get_config
from src.core.task import Task, TaskTarget, TaskStatus


class TaskService:
    def __init__(self, scheduler):
        self._scheduler = scheduler

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
                "step": log.step_name,
                "status": log.status.value,
                "message": log.message,
                "error": log.error,
                "started_at": log.started_at.isoformat() if log.started_at else None,
                "completed_at": log.completed_at.isoformat() if log.completed_at else None,
            }
            for log in task.step_logs
        ]

    async def cancel(self, task_id: str) -> bool:
        return await self._scheduler.cancel(task_id)

    async def delete(self, task_id: str) -> bool:
        return await self._scheduler.delete_task(task_id)

    def get_stats(self) -> dict[str, Any]:
        return self._scheduler.get_stats()

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

        resolved_collector = collector_name
        pipeline = self._scheduler.get_pipeline(pipeline_name)
        if not resolved_collector and pipeline is not None:
            collector_step = next(
                (step for step in pipeline.steps if step.step_type.value == "collector"),
                None,
            )
            if collector_step is not None:
                resolved_collector = collector_step.component_name

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
            config=config or {},
        )

        try:
            await self._scheduler.submit(task, pipeline_name=pipeline_name)
        except (ValueError, RuntimeError) as e:
            raise ValueError(str(e))

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
    ):
        """Reusable precheck logic usable by both API route and Agent tools."""
        from src.schemas.tasks import TaskPrecheckResponse, TaskPrecheckIssue  # shared schema module

        targets = targets or []
        pipeline = self._scheduler.get_pipeline(pipeline_name)
        template = self._get_pipeline_template(pipeline_name)
        resolved_collector = self._resolve_collector_name(collector_name, pipeline, template)
        required_fields = self._required_target_fields(resolved_collector)
        issues: list[TaskPrecheckIssue] = []
        credential_status: dict[str, str] = {}
        data_source_status: dict[str, str] = {}

        if not name.strip():
            issues.append(self._issue("error", "missing_task_name", "name", "Task name is required."))
        if not pipeline_name.strip():
            issues.append(self._issue("error", "missing_pipeline", "pipeline_name", "Pipeline is required."))
        elif pipeline is None and template is None:
            issues.append(
                self._issue("error", "unknown_pipeline", "pipeline_name", f"Pipeline not found: {pipeline_name}")
            )
        if not resolved_collector:
            issues.append(
                self._issue("error", "collector_unresolved", "collector_name", "Collector cannot be inferred from pipeline.")
            )
        else:
            data_source_status[resolved_collector] = "available"

        if not targets:
            issues.append(self._issue("error", "missing_targets", "targets", "At least one target is required."))
        for index, target in enumerate(targets):
            issues.extend(self._validate_target(index, target, resolved_collector))

        issues.extend(self._credential_checks(resolved_collector, credential_status))

        has_error = any(issue.level == "error" for issue in issues)
        has_warning = any(issue.level == "warning" for issue in issues)
        status = "error" if has_error else "warning" if has_warning else "ok"
        return TaskPrecheckResponse(
            status=status,
            can_submit=not has_error,
            pipeline_name=pipeline_name,
            collector_name=resolved_collector,
            required_fields=required_fields,
            issues=issues,
            credential_status=credential_status,
            data_source_status=data_source_status,
        )

    # ------------------------------------------------------------------
    # Pipeline helpers
    # ------------------------------------------------------------------

    def get_pipeline(self, name: str):
        return self._scheduler.get_pipeline(name)

    def get_all_pipelines(self):
        return self._scheduler.get_all_pipelines()

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _issue(level: str, code: str, field: str, message: str):
        from src.schemas.tasks import TaskPrecheckIssue
        return TaskPrecheckIssue(level=level, code=code, field=field, message=message)

    @staticmethod
    def _get_pipeline_template(pipeline_name: str) -> dict[str, Any] | None:
        if not pipeline_name:
            return None
        from src.core.pipeline_templates import PIPELINE_TEMPLATES
        return next((item for item in PIPELINE_TEMPLATES if item.get("id") == pipeline_name), None)

    @staticmethod
    def _resolve_collector_name(explicit: str, pipeline: Any | None, template: dict[str, Any] | None) -> str:
        if explicit:
            return explicit
        if pipeline is not None:
            collector_step = next(
                (step for step in pipeline.steps if step.step_type.value == "collector"),
                None,
            )
            if collector_step is not None:
                return str(collector_step.component_name)
        for step in (template or {}).get("steps", []):
            if isinstance(step, dict) and step.get("type") == "collector":
                return str(step.get("name") or "")
        return ""

    @staticmethod
    def _required_target_fields(collector_name: str) -> list[str]:
        return {
            "steam": ["target.name", "target.params.app_id (recommended)"],
            "steam_discussions": ["target.params.app_id or target.params.forum_url"],
            "taptap": ["target.params.app_id or target.params.url"],
            "gtrends": ["target.name"],
            "monitor": ["target.params.app_id", "target.params.twitch_name (optional)", "target.params.siteurl (optional)"],
            "qimai": ["target.params.app_id"],
            "official_site": ["target.params.official_url"],
        }.get(collector_name, ["target.name or target.params"])

    @staticmethod
    def _validate_target(index: int, target: dict[str, Any], collector_name: str) -> list:
        from src.schemas.tasks import TaskPrecheckIssue

        issues: list[TaskPrecheckIssue] = []
        field = f"targets[{index}]"
        name = str(target.get("name") or "").strip()
        params = target.get("params", {}) if isinstance(target.get("params"), dict) else {}

        if collector_name == "steam":
            if not name and not str(params.get("app_id") or "").strip():
                issues.append(TaskPrecheckIssue(level="error", code="missing_steam_target", field=field, message="Steam target needs a game name or app_id."))
            elif not str(params.get("app_id") or "").strip():
                issues.append(TaskPrecheckIssue(level="warning", code="missing_steam_app_id", field=field, message="Steam app_id is recommended to avoid wrong game matches."))
        elif collector_name == "steam_discussions":
            if not str(params.get("app_id") or params.get("forum_url") or "").strip():
                issues.append(TaskPrecheckIssue(level="error", code="missing_discussion_target", field=field, message="Steam discussions need app_id or forum_url."))
        elif collector_name == "taptap":
            if not str(params.get("app_id") or params.get("url") or "").strip():
                issues.append(TaskPrecheckIssue(level="error", code="missing_taptap_target", field=field, message="TapTap target needs app_id or url."))
        elif collector_name == "gtrends":
            if not name:
                issues.append(TaskPrecheckIssue(level="error", code="missing_keyword", field=field, message="Google Trends target needs a keyword name."))
        elif collector_name == "monitor":
            if not str(params.get("app_id") or "").strip():
                issues.append(TaskPrecheckIssue(level="error", code="missing_monitor_app_id", field=field, message="Monitor target requires app_id (twitch_name and siteurl are optional supplements)."))
        elif collector_name == "qimai":
            if not str(params.get("app_id") or "").strip():
                issues.append(TaskPrecheckIssue(level="error", code="missing_qimai_app_id", field=field, message="Qimai target needs app_id."))
        elif collector_name == "official_site":
            if not str(params.get("official_url") or "").strip():
                issues.append(TaskPrecheckIssue(level="error", code="missing_official_url", field=field, message="Official site target needs official_url."))
        elif not name and not params:
            issues.append(TaskPrecheckIssue(level="warning", code="empty_target", field=field, message="Target has no name or params."))
        return issues

    @staticmethod
    def _credential_checks(collector_name: str, credential_status: dict[str, str]) -> list:
        from src.schemas.tasks import TaskPrecheckIssue

        issues: list[TaskPrecheckIssue] = []
        if collector_name == "steam":
            steam_key = str(get_config("steam.api_key", "") or "").strip()
            credential_status["steam.api_key"] = "configured" if steam_key else "missing"
            if not steam_key:
                issues.append(
                    TaskPrecheckIssue(level="warning", code="missing_steam_api_key", field="steam.api_key", message="Steam API Key is missing; official Steam APIs may be unavailable.")
                )
            if bool(get_config("steam.steamdb.enabled", False)) and bool(get_config("steam.steamdb.cdp_enabled", False)):
                credential_status["steam.steamdb.browser_session"] = "requires_login_session"
                issues.append(TaskPrecheckIssue(level="warning", code="steamdb_login_session", field="steam.steamdb", message="SteamDB may require a logged-in browser session."))
        elif collector_name in {"taptap", "official_site", "qimai"}:
            playwright_available = importlib.util.find_spec("playwright") is not None
            credential_status["playwright"] = "available" if playwright_available else "missing"
            if not playwright_available:
                issues.append(TaskPrecheckIssue(level="warning", code="missing_playwright", field="playwright", message="Playwright is not importable; browser-backed collection may fail."))
        else:
            credential_status["credentials"] = "not_required"
        return issues
