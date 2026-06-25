"""Task creation precheck business logic."""

from __future__ import annotations

import importlib.util
from typing import Any
from urllib.parse import urlsplit

from src.core.collector_metadata import (
    CollectorMetadata,
    TargetValidationRule,
    build_collector_recovery_info,
    collector_metadata_payload,
    get_collector_metadata,
)
from src.core.config import get as get_config
from src.core.diagnostics import build_collector_session_diagnostics
from src.core.diagnostics import build_session_readiness_summary
from src.schemas.tasks import TaskPrecheckIssue, TaskPrecheckResponse


class TaskPrecheckService:
    """Validates task creation inputs against pipelines, collectors, and sessions."""

    def __init__(self, scheduler) -> None:
        self._scheduler = scheduler

    def precheck(
        self,
        name: str,
        pipeline_name: str,
        collector_name: str = "",
        targets: list[dict[str, Any]] | None = None,
        config: dict[str, Any] | None = None,
    ) -> TaskPrecheckResponse:
        """Reusable precheck logic usable by both API routes and Agent tools."""
        targets = targets or []
        context = self._build_precheck_context(
            pipeline_name=pipeline_name,
            collector_name=collector_name,
        )
        issues = self._collect_precheck_issues(
            name=name,
            pipeline_name=pipeline_name,
            targets=targets,
            context=context,
        )
        status = self._precheck_status(issues)
        return TaskPrecheckResponse(
            status=status,
            can_submit=status != "error",
            pipeline_name=pipeline_name,
            collector_name=context["resolved_collector"],
            required_fields=context["required_fields"],
            issues=issues,
            credential_status=context["credential_status"],
            data_source_status=context["data_source_status"],
            collector_metadata=context["collector_metadata_payload"],
            session_diagnostics=context["session_diagnostics"],
            session_readiness=context["session_readiness"],
            recovery=context["recovery"],
        )

    @staticmethod
    def format_errors(precheck: Any) -> str:
        errors = [issue for issue in precheck.issues if issue.level == "error"]
        if not errors:
            return "Task input is not submittable."
        return "; ".join(
            f"{issue.code} at {issue.field}: {issue.message}" for issue in errors
        )

    def _build_precheck_context(
        self,
        *,
        pipeline_name: str,
        collector_name: str,
    ) -> dict[str, Any]:
        pipeline = self._scheduler.get_pipeline(pipeline_name)
        template = self._get_pipeline_template(pipeline_name)
        resolved_collector = self._resolve_collector_name(collector_name, pipeline, template)
        collector_metadata = get_collector_metadata(resolved_collector)
        session_diagnostics = (
            build_collector_session_diagnostics(resolved_collector) if resolved_collector else {}
        )
        session_readiness = build_session_readiness_summary(session_diagnostics)
        return {
            "pipeline": pipeline,
            "template": template,
            "resolved_collector": resolved_collector,
            "collector_metadata": collector_metadata,
            "collector_metadata_payload": (
                collector_metadata_payload(resolved_collector) if collector_metadata is not None else {}
            ),
            "session_diagnostics": session_diagnostics,
            "session_readiness": session_readiness,
            "required_fields": self._required_target_fields(
                resolved_collector,
                collector_metadata,
            ),
            "credential_status": {},
            "data_source_status": {},
            "recovery": (
                build_collector_recovery_info(resolved_collector) if resolved_collector else {}
            ),
        }

    def _collect_precheck_issues(
        self,
        *,
        name: str,
        pipeline_name: str,
        targets: list[dict[str, Any]],
        context: dict[str, Any],
    ) -> list[TaskPrecheckIssue]:
        issues = self._build_basic_precheck_issues(
            name=name,
            pipeline_name=pipeline_name,
            targets=targets,
            context=context,
        )
        issues.extend(self._build_target_precheck_issues(targets=targets, context=context))
        issues.extend(
            self._credential_checks(
                context["resolved_collector"],
                context["credential_status"],
                collector_metadata=context["collector_metadata"],
            )
        )
        issues.extend(
            self._collector_config_checks(
                context["resolved_collector"],
                context["pipeline"],
                context["template"],
                collector_metadata=context["collector_metadata"],
            )
        )
        readiness_issue = self._session_readiness_issue(context["session_readiness"])
        if readiness_issue is not None:
            issues.append(readiness_issue)
        issues.extend(self._session_diagnostic_issues(context["session_diagnostics"]))
        return issues

    def _build_basic_precheck_issues(
        self,
        *,
        name: str,
        pipeline_name: str,
        targets: list[dict[str, Any]],
        context: dict[str, Any],
    ) -> list[TaskPrecheckIssue]:
        issues: list[TaskPrecheckIssue] = []

        if not name.strip():
            issues.append(
                self._issue("error", "missing_task_name", "name", "Task name is required.")
            )
        if not pipeline_name.strip():
            issues.append(
                self._issue("error", "missing_pipeline", "pipeline_name", "Pipeline is required.")
            )
        elif context["pipeline"] is None and context["template"] is None:
            issues.append(
                self._issue(
                    "error",
                    "unknown_pipeline",
                    "pipeline_name",
                    f"Pipeline not found: {pipeline_name}",
                )
            )
        if not context["resolved_collector"]:
            issues.append(
                self._issue(
                    "error",
                    "collector_unresolved",
                    "collector_name",
                    "Collector cannot be inferred from pipeline.",
                )
            )
        else:
            context["data_source_status"][context["resolved_collector"]] = "available"

        if not targets:
            issues.append(
                self._issue(
                    "error", "missing_targets", "targets", "At least one target is required."
                )
            )
        return issues

    def _build_target_precheck_issues(
        self,
        *,
        targets: list[dict[str, Any]],
        context: dict[str, Any],
    ) -> list[TaskPrecheckIssue]:
        issues: list[TaskPrecheckIssue] = []
        for index, target in enumerate(targets):
            issues.extend(
                self._validate_target(
                    index,
                    target,
                    context["resolved_collector"],
                    collector_metadata=context["collector_metadata"],
                )
            )
        return issues

    @staticmethod
    def _precheck_status(issues: list[TaskPrecheckIssue]) -> str:
        has_error = any(issue.level == "error" for issue in issues)
        has_warning = any(issue.level == "warning" for issue in issues)
        return "error" if has_error else "warning" if has_warning else "ok"

    @staticmethod
    def _issue(level: str, code: str, field: str, message: str) -> TaskPrecheckIssue:
        return TaskPrecheckIssue(level=level, code=code, field=field, message=message)

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
    ) -> list[TaskPrecheckIssue]:
        issues: list[TaskPrecheckIssue] = []
        field = f"targets[{index}]"
        name = str(target.get("name") or "").strip()
        params = target.get("params", {}) if isinstance(target.get("params"), dict) else {}

        if collector_metadata is not None:
            for rule in collector_metadata.target_schema.rules:
                if rule.skip_if_error and any(issue.level == "error" for issue in issues):
                    continue
                if not TaskPrecheckService._target_rule_passes(target, rule):
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
        present = [TaskPrecheckService._target_field_present(target, field) for field in rule.fields]
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
    ) -> list[TaskPrecheckIssue]:
        if collector_metadata is None or not collector_metadata.config_schema:
            return []

        schema = collector_metadata.config_schema
        config = TaskPrecheckService._resolve_collector_config(
            collector_name,
            pipeline,
            template,
        )
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
            issue = TaskPrecheckService._collector_config_property_issue(
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
            safety_issue = TaskPrecheckService._dynamic_playwright_config_safety_issue(
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
    ) -> TaskPrecheckIssue | None:
        expected_type = str(schema.get("type") or "").strip()
        if expected_type and not TaskPrecheckService._schema_type_matches(value, expected_type):
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
    def _dynamic_playwright_config_safety_issue(
        config: dict[str, Any],
        *,
        field_prefix: str,
    ) -> TaskPrecheckIssue | None:
        from fastapi import HTTPException
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
    ) -> list[TaskPrecheckIssue]:
        if collector_metadata is not None:
            return TaskPrecheckService._credential_checks_from_metadata(
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
    ) -> list[TaskPrecheckIssue]:
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
    def _session_diagnostic_issues(
        session_diagnostics: dict[str, Any],
    ) -> list[TaskPrecheckIssue]:
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

    @staticmethod
    def _session_readiness_issue(
        session_readiness: dict[str, Any],
    ) -> TaskPrecheckIssue | None:
        if not session_readiness:
            return None
        level = str(session_readiness.get("precheck_status") or "").strip().lower()
        if level not in {"warning", "error"}:
            return None
        code = "session_blocked" if level == "error" else "session_attention_required"
        return TaskPrecheckIssue(
            level=level,
            code=code,
            field="session",
            message=str(
                session_readiness.get("summary")
                or "Collector session needs attention before task submission."
            ),
        )
