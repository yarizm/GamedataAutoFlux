"""Task service — shared business logic for task CRUD, precheck, and pipeline operations."""

from __future__ import annotations

import importlib.util
from typing import Any
from urllib.parse import urlsplit

from src.core.config import get as get_config
from src.core.collector_metadata import (
    CollectorMetadata,
    TargetValidationRule,
    build_collector_recovery_info,
    get_collector_metadata,
)
from src.core.diagnostics import build_collector_session_diagnostics
from src.core.sensitive import redact_sensitive_text
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
        config = config or {}

        precheck = self.precheck(
            name=name,
            pipeline_name=pipeline_name,
            collector_name=collector_name,
            targets=targets,
            config=config,
        )
        if not precheck.can_submit:
            raise ValueError(f"Task precheck failed: {self._format_precheck_errors(precheck)}")

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
    ):
        """Reusable precheck logic usable by both API route and Agent tools."""
        from src.schemas.tasks import (
            TaskPrecheckResponse,
            TaskPrecheckIssue,
        )  # shared schema module

        targets = targets or []
        pipeline = self._scheduler.get_pipeline(pipeline_name)
        template = self._get_pipeline_template(pipeline_name)
        resolved_collector = self._resolve_collector_name(collector_name, pipeline, template)
        collector_metadata = get_collector_metadata(resolved_collector)
        session_diagnostics = (
            build_collector_session_diagnostics(resolved_collector) if resolved_collector else {}
        )
        required_fields = self._required_target_fields(resolved_collector, collector_metadata)
        issues: list[TaskPrecheckIssue] = []
        credential_status: dict[str, str] = {}
        data_source_status: dict[str, str] = {}

        if not name.strip():
            issues.append(
                self._issue("error", "missing_task_name", "name", "Task name is required.")
            )
        if not pipeline_name.strip():
            issues.append(
                self._issue("error", "missing_pipeline", "pipeline_name", "Pipeline is required.")
            )
        elif pipeline is None and template is None:
            issues.append(
                self._issue(
                    "error",
                    "unknown_pipeline",
                    "pipeline_name",
                    f"Pipeline not found: {pipeline_name}",
                )
            )
        if not resolved_collector:
            issues.append(
                self._issue(
                    "error",
                    "collector_unresolved",
                    "collector_name",
                    "Collector cannot be inferred from pipeline.",
                )
            )
        else:
            data_source_status[resolved_collector] = "available"

        if not targets:
            issues.append(
                self._issue(
                    "error", "missing_targets", "targets", "At least one target is required."
                )
            )
        for index, target in enumerate(targets):
            issues.extend(
                self._validate_target(
                    index,
                    target,
                    resolved_collector,
                    collector_metadata=collector_metadata,
                )
            )

        issues.extend(
            self._credential_checks(
                resolved_collector,
                credential_status,
                collector_metadata=collector_metadata,
            )
        )
        issues.extend(
            self._collector_config_checks(
                resolved_collector,
                pipeline,
                template,
                collector_metadata=collector_metadata,
            )
        )
        issues.extend(self._session_diagnostic_issues(session_diagnostics))

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
            collector_metadata=(
                collector_metadata.model_dump(mode="json") if collector_metadata is not None else {}
            ),
            session_diagnostics=session_diagnostics,
            recovery=build_collector_recovery_info(resolved_collector) if resolved_collector else {},
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
        return build_collector_recovery_info(
            task.collector_name,
            latest_checkpoint=latest_payload,
        )

    def get_task_collector_metadata(self, task_id: str) -> dict[str, Any] | None:
        task = self._scheduler.get_task(task_id)
        if task is None:
            return None
        metadata = get_collector_metadata(task.collector_name)
        return metadata.model_dump(mode="json") if metadata is not None else {}

    def get_task_session_diagnostics(self, task_id: str) -> dict[str, Any] | None:
        task = self._scheduler.get_task(task_id)
        if task is None:
            return None
        return build_collector_session_diagnostics(task.collector_name)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _issue(level: str, code: str, field: str, message: str):
        from src.schemas.tasks import TaskPrecheckIssue

        return TaskPrecheckIssue(level=level, code=code, field=field, message=message)

    @staticmethod
    def _format_precheck_errors(precheck: Any) -> str:
        errors = [issue for issue in precheck.issues if issue.level == "error"]
        if not errors:
            return "Task input is not submittable."
        return "; ".join(
            f"{issue.code} at {issue.field}: {issue.message}" for issue in errors
        )

    @staticmethod
    def _get_pipeline_template(pipeline_name: str) -> dict[str, Any] | None:
        if not pipeline_name:
            return None
        from src.core.pipeline_templates import PIPELINE_TEMPLATES

        return next((item for item in PIPELINE_TEMPLATES if item.get("id") == pipeline_name), None)

    @staticmethod
    def _resolve_collector_name(
        explicit: str, pipeline: Any | None, template: dict[str, Any] | None
    ) -> str:
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
    def _resolve_collector_config(
        collector_name: str,
        pipeline: Any | None,
        template: dict[str, Any] | None,
    ) -> dict[str, Any]:
        if not collector_name:
            return {}
        if pipeline is not None:
            collector_step = next(
                (
                    step
                    for step in pipeline.steps
                    if step.step_type.value == "collector"
                    and step.component_name == collector_name
                ),
                None,
            )
            if collector_step is not None and isinstance(collector_step.config, dict):
                return dict(collector_step.config)
        for step in (template or {}).get("steps", []):
            if (
                isinstance(step, dict)
                and step.get("type") == "collector"
                and str(step.get("name") or "") == collector_name
            ):
                config = step.get("config", {})
                return dict(config) if isinstance(config, dict) else {}
        return {}

    @staticmethod
    def _required_target_fields(
        collector_name: str,
        collector_metadata: CollectorMetadata | None = None,
    ) -> list[str]:
        if collector_metadata is not None:
            return list(collector_metadata.target_schema.required_fields)
        return {
            "steam": ["target.name", "target.params.app_id (recommended)"],
            "steam_discussions": ["target.params.app_id or target.params.forum_url"],
            "taptap": ["target.params.app_id or target.params.url"],
            "gtrends": ["target.name"],
            "monitor": [
                "target.params.app_id or target.params.siteurl",
                "target.params.twitch_name (optional)",
            ],
            "qimai": ["target.params.app_id"],
            "official_site": ["target.params.official_url"],
            "dynamic_playwright": ["target.name"],
        }.get(collector_name, ["target.name or target.params"])

    @staticmethod
    def _validate_target(
        index: int,
        target: dict[str, Any],
        collector_name: str,
        *,
        collector_metadata: CollectorMetadata | None = None,
    ) -> list:
        from src.schemas.tasks import TaskPrecheckIssue

        issues: list[TaskPrecheckIssue] = []
        field = f"targets[{index}]"
        name = str(target.get("name") or "").strip()
        params = target.get("params", {}) if isinstance(target.get("params"), dict) else {}

        if collector_metadata is not None:
            for rule in collector_metadata.target_schema.rules:
                if rule.skip_if_error and any(issue.level == "error" for issue in issues):
                    continue
                if not TaskService._target_rule_passes(target, rule):
                    issues.append(
                        TaskPrecheckIssue(
                            level=rule.level,
                            code=rule.code,
                            field=(rule.field or field).format(index=index),
                            message=rule.message,
                        )
                    )
            return issues

        if collector_name == "steam":
            if not name and not str(params.get("app_id") or "").strip():
                issues.append(
                    TaskPrecheckIssue(
                        level="error",
                        code="missing_steam_target",
                        field=field,
                        message="Steam target needs a game name or app_id.",
                    )
                )
            elif not str(params.get("app_id") or "").strip():
                issues.append(
                    TaskPrecheckIssue(
                        level="warning",
                        code="missing_steam_app_id",
                        field=field,
                        message="Steam app_id is recommended to avoid wrong game matches.",
                    )
                )
        elif collector_name == "steam_discussions":
            if not str(params.get("app_id") or params.get("forum_url") or "").strip():
                issues.append(
                    TaskPrecheckIssue(
                        level="error",
                        code="missing_discussion_target",
                        field=field,
                        message="Steam discussions need app_id or forum_url.",
                    )
                )
        elif collector_name == "taptap":
            if not str(params.get("app_id") or params.get("url") or "").strip():
                issues.append(
                    TaskPrecheckIssue(
                        level="error",
                        code="missing_taptap_target",
                        field=field,
                        message="TapTap target needs app_id or url.",
                    )
                )
        elif collector_name == "gtrends":
            if not name:
                issues.append(
                    TaskPrecheckIssue(
                        level="error",
                        code="missing_keyword",
                        field=field,
                        message="Google Trends target needs a keyword name.",
                    )
                )
        elif collector_name == "monitor":
            if not str(params.get("app_id") or params.get("siteurl") or "").strip():
                issues.append(
                    TaskPrecheckIssue(
                        level="error",
                        code="missing_monitor_app_id",
                        field=field,
                        message="Monitor target requires app_id or siteurl.",
                    )
                )
        elif collector_name == "qimai":
            if not str(params.get("app_id") or "").strip():
                issues.append(
                    TaskPrecheckIssue(
                        level="error",
                        code="missing_qimai_app_id",
                        field=field,
                        message="Qimai target needs app_id.",
                    )
                )
        elif collector_name == "official_site":
            if not str(params.get("official_url") or "").strip():
                issues.append(
                    TaskPrecheckIssue(
                        level="error",
                        code="missing_official_url",
                        field=field,
                        message="Official site target needs official_url.",
                    )
                )
        elif collector_name == "dynamic_playwright":
            if not name:
                issues.append(
                    TaskPrecheckIssue(
                        level="warning",
                        code="missing_target_name",
                        field=field,
                        message="Dynamic Playwright target should have a game name.",
                    )
                )
        elif not name and not params:
            issues.append(
                TaskPrecheckIssue(
                    level="warning",
                    code="empty_target",
                    field=field,
                    message="Target has no name or params.",
                )
            )
        return issues

    @staticmethod
    def _target_rule_passes(target: dict[str, Any], rule: TargetValidationRule) -> bool:
        present = [TaskService._target_field_present(target, field) for field in rule.fields]
        if rule.mode == "all":
            return all(present)
        return any(present)

    @staticmethod
    def _target_field_present(target: dict[str, Any], field_path: str) -> bool:
        if field_path == "target.name":
            value = target.get("name")
        elif field_path == "target.target_type":
            value = target.get("target_type")
        elif field_path.startswith("target.params."):
            params = target.get("params", {}) if isinstance(target.get("params"), dict) else {}
            value = params.get(field_path.removeprefix("target.params."))
        else:
            value = None
        return str(value or "").strip() != ""

    @staticmethod
    def _collector_config_checks(
        collector_name: str,
        pipeline: Any | None,
        template: dict[str, Any] | None,
        *,
        collector_metadata: CollectorMetadata | None = None,
    ) -> list:
        from src.schemas.tasks import TaskPrecheckIssue

        if collector_metadata is None or not collector_metadata.config_schema:
            return []

        schema = collector_metadata.config_schema
        config = TaskService._resolve_collector_config(collector_name, pipeline, template)
        field_prefix = f"pipeline.steps.collector[{collector_name}].config"
        issues: list[TaskPrecheckIssue] = []

        if schema.get("type") == "object" and not isinstance(config, dict):
            return [
                TaskPrecheckIssue(
                    level="error",
                    code="invalid_collector_config",
                    field=field_prefix,
                    message=f"{collector_name} collector config must be an object.",
                )
            ]

        for key in schema.get("required", []) or []:
            if key not in config or config.get(key) in (None, ""):
                issues.append(
                    TaskPrecheckIssue(
                        level="error",
                        code="missing_collector_config",
                        field=f"{field_prefix}.{key}",
                        message=f"{collector_name} collector config requires {key}.",
                    )
                )

        properties = schema.get("properties", {})
        if not isinstance(properties, dict):
            return issues

        for key, prop_schema in properties.items():
            if key not in config or not isinstance(prop_schema, dict):
                continue
            value = config.get(key)
            issue = TaskService._collector_config_property_issue(
                collector_name,
                f"{field_prefix}.{key}",
                value,
                prop_schema,
            )
            if issue is not None:
                issues.append(issue)
        if collector_name == "dynamic_playwright" and not any(
            issue.level == "error" for issue in issues
        ):
            safety_issue = TaskService._dynamic_playwright_config_safety_issue(
                config,
                field_prefix=field_prefix,
            )
            if safety_issue is not None:
                issues.append(safety_issue)
        return issues

    @staticmethod
    def _collector_config_property_issue(
        collector_name: str,
        field: str,
        value: Any,
        schema: dict[str, Any],
    ):
        from src.schemas.tasks import TaskPrecheckIssue

        expected_type = str(schema.get("type") or "").strip()
        if expected_type and not TaskService._schema_type_matches(value, expected_type):
            return TaskPrecheckIssue(
                level="error",
                code="invalid_collector_config_type",
                field=field,
                message=f"{collector_name} config value must be {expected_type}.",
            )

        if expected_type in {"number", "integer"} and "minimum" in schema:
            try:
                numeric_value = int(value) if expected_type == "integer" else float(value)
                minimum = float(schema["minimum"])
            except (TypeError, ValueError):
                return TaskPrecheckIssue(
                    level="error",
                    code="invalid_collector_config_type",
                    field=field,
                    message=f"{collector_name} config value must be {expected_type}.",
                )
            if numeric_value < minimum:
                return TaskPrecheckIssue(
                    level="error",
                    code="invalid_collector_config_minimum",
                    field=field,
                    message=f"{collector_name} config value must be >= {schema['minimum']}.",
                )

        if schema.get("format") == "uri" and isinstance(value, str):
            parsed = urlsplit(value)
            if not parsed.scheme or not parsed.netloc:
                return TaskPrecheckIssue(
                    level="error",
                    code="invalid_collector_config_uri",
                    field=field,
                    message=f"{collector_name} config value must be an absolute URI.",
                )
        return None

    @staticmethod
    def _dynamic_playwright_config_safety_issue(config: dict[str, Any], *, field_prefix: str):
        from fastapi import HTTPException
        from src.schemas.tasks import TaskPrecheckIssue
        from src.web.safety import validate_dynamic_playwright_config

        try:
            validate_dynamic_playwright_config(config)
        except HTTPException as exc:
            return TaskPrecheckIssue(
                level="error",
                code="unsafe_dynamic_playwright_config",
                field=f"{field_prefix}.url",
                message=str(exc.detail or "dynamic_playwright config is unsafe."),
            )
        return None

    @staticmethod
    def _schema_type_matches(value: Any, expected_type: str) -> bool:
        if expected_type == "number":
            return isinstance(value, (int, float)) and not isinstance(value, bool)
        if expected_type == "integer":
            return isinstance(value, int) and not isinstance(value, bool)
        if expected_type == "string":
            return isinstance(value, str)
        if expected_type == "object":
            return isinstance(value, dict)
        if expected_type == "array":
            return isinstance(value, list)
        if expected_type == "boolean":
            return isinstance(value, bool)
        return True

    @staticmethod
    def _credential_checks(
        collector_name: str,
        credential_status: dict[str, str],
        *,
        collector_metadata: CollectorMetadata | None = None,
    ) -> list:
        from src.schemas.tasks import TaskPrecheckIssue

        if collector_metadata is not None:
            return TaskService._credential_checks_from_metadata(
                collector_metadata,
                credential_status,
            )

        issues: list[TaskPrecheckIssue] = []
        if collector_name == "steam":
            steam_key = str(get_config("steam.api_key", "") or "").strip()
            credential_status["steam.api_key"] = "configured" if steam_key else "missing"
            if not steam_key:
                issues.append(
                    TaskPrecheckIssue(
                        level="warning",
                        code="missing_steam_api_key",
                        field="steam.api_key",
                        message="Steam API Key is missing; official Steam APIs may be unavailable.",
                    )
                )
            if bool(get_config("steam.steamdb.enabled", False)) and bool(
                get_config("steam.steamdb.cdp_enabled", False)
            ):
                credential_status["steam.steamdb.browser_session"] = "requires_login_session"
                issues.append(
                    TaskPrecheckIssue(
                        level="warning",
                        code="steamdb_login_session",
                        field="steam.steamdb",
                        message="SteamDB may require a logged-in browser session.",
                    )
                )
        elif collector_name in {"taptap", "official_site", "qimai", "dynamic_playwright"}:
            playwright_available = importlib.util.find_spec("playwright") is not None
            credential_status["playwright"] = "available" if playwright_available else "missing"
            if not playwright_available:
                issues.append(
                    TaskPrecheckIssue(
                        level="warning",
                        code="missing_playwright",
                        field="playwright",
                        message="Playwright is not importable; browser-backed collection may fail.",
                    )
                )
        else:
            credential_status["credentials"] = "not_required"
        return issues

    @staticmethod
    def _credential_checks_from_metadata(
        collector_metadata: CollectorMetadata,
        credential_status: dict[str, str],
    ) -> list:
        from src.schemas.tasks import TaskPrecheckIssue

        issues: list[TaskPrecheckIssue] = []
        profiles = set(collector_metadata.credential_profiles)
        if not profiles:
            credential_status["credentials"] = "not_required"
            return issues

        if "steam_api_key" in profiles:
            steam_key = str(get_config("steam.api_key", "") or "").strip()
            credential_status["steam.api_key"] = "configured" if steam_key else "missing"
            if not steam_key:
                issues.append(
                    TaskPrecheckIssue(
                        level="warning",
                        code="missing_steam_api_key",
                        field="steam.api_key",
                        message="Steam API Key is missing; official Steam APIs may be unavailable.",
                    )
                )

        if "steamdb_optional_browser_session" in profiles and bool(
            get_config("steam.steamdb.enabled", False)
        ) and bool(get_config("steam.steamdb.cdp_enabled", False)):
            credential_status["steam.steamdb.browser_session"] = "requires_login_session"
            issues.append(
                TaskPrecheckIssue(
                    level="warning",
                    code="steamdb_login_session",
                    field="steam.steamdb",
                    message="SteamDB may require a logged-in browser session.",
                )
            )

        if "playwright_runtime" in profiles:
            playwright_available = importlib.util.find_spec("playwright") is not None
            credential_status["playwright"] = "available" if playwright_available else "missing"
            if not playwright_available:
                issues.append(
                    TaskPrecheckIssue(
                        level="warning",
                        code="missing_playwright",
                        field="playwright",
                        message="Playwright is not importable; browser-backed collection may fail.",
                    )
                )

        if "local_browser_profile" in profiles:
            credential_status["browser_profile"] = "local_profile_required"

        return issues

    @staticmethod
    def _session_diagnostic_issues(session_diagnostics: dict[str, Any]) -> list:
        from src.schemas.tasks import TaskPrecheckIssue

        if not session_diagnostics:
            return []

        issues: list[TaskPrecheckIssue] = []
        for check in session_diagnostics.get("checks", []) or []:
            if not isinstance(check, dict):
                continue
            name = str(check.get("name") or "session_diagnostic")
            if name.startswith("dependency:"):
                continue
            status = str(check.get("status") or "").strip().lower()
            if status not in {"warning", "error"}:
                continue
            issue_level = "error" if status == "error" else "warning"
            issues.append(
                TaskPrecheckIssue(
                    level=issue_level,
                    code=name.replace(":", "_"),
                    field="session",
                    message=str(check.get("message") or "Collector session diagnostic warning."),
                )
            )
        return issues
