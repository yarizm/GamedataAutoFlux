"""Task creation precheck business logic."""

from __future__ import annotations

import importlib.util
import re
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
from src.core.dag_validate import (
    collector_uses_from_upstream,
    validate_pipeline_collector_upstream,
)
from src.core.diagnostics import build_collector_session_diagnostics
from src.core.diagnostics import build_session_readiness_summary
from src.schemas.tasks import CollectorReadiness, TaskPrecheckIssue, TaskPrecheckResponse

_APP_ID_RE = re.compile(r"^\d+$")


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
        *,
        deep: bool = False,
    ) -> TaskPrecheckResponse:
        """Static precheck. For deep probes use ``precheck_async``."""
        if deep:
            # Sync path cannot run async probes; callers should use precheck_async.
            # Fall through as static-only for safety.
            pass
        return self._precheck_static(
            name=name,
            pipeline_name=pipeline_name,
            collector_name=collector_name,
            targets=targets,
            config=config,
            deep=False,
            probe_report={},
            extra_issues=None,
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
    ) -> TaskPrecheckResponse:
        """Static precheck plus optional deep probes (``deep`` or config default)."""
        if deep is None:
            deep = bool(get_config("precheck.deep_default", False))
        extra_issues: list[TaskPrecheckIssue] = []
        probe_report: dict[str, Any] = {}
        if deep:
            # Build context first to know collectors (cheap).
            context = self._build_precheck_context(
                pipeline_name=pipeline_name,
                collector_name=collector_name,
            )
            collector_ids = list(context.get("resolved_collectors") or [])
            if collector_ids:
                from src.core.collector_probes import (
                    build_probe_report,
                    merge_probe_issues,
                    run_collector_probes,
                )

                probe_results = await run_collector_probes(
                    collector_ids,
                    targets=targets or [],
                )
                probe_report = build_probe_report(probe_results)
                for raw in merge_probe_issues(probe_results=probe_results):
                    extra_issues.append(TaskPrecheckIssue(**raw))

        return self._precheck_static(
            name=name,
            pipeline_name=pipeline_name,
            collector_name=collector_name,
            targets=targets,
            config=config,
            deep=bool(deep),
            probe_report=probe_report,
            extra_issues=extra_issues,
        )

    def _precheck_static(
        self,
        *,
        name: str,
        pipeline_name: str,
        collector_name: str,
        targets: list[dict[str, Any]] | None,
        config: dict[str, Any] | None,
        deep: bool,
        probe_report: dict[str, Any],
        extra_issues: list[TaskPrecheckIssue] | None,
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
        if extra_issues:
            issues.extend(extra_issues)
        status = self._precheck_status(issues)
        collectors_readiness = self._build_collectors_readiness(context, issues)
        return TaskPrecheckResponse(
            status=status,
            can_submit=status != "error",
            pipeline_name=pipeline_name,
            collector_name=context["resolved_collector"],
            collectors=list(context["resolved_collectors"]),
            collectors_readiness=collectors_readiness,
            required_fields=context["required_fields"],
            issues=issues,
            credential_status=context["credential_status"],
            data_source_status=context["data_source_status"],
            collector_metadata=context["collector_metadata_payload"],
            session_diagnostics=context["session_diagnostics"],
            session_diagnostics_by_collector=context["session_diagnostics_by_collector"],
            session_readiness=context["session_readiness"],
            session_readiness_by_collector=context["session_readiness_by_collector"],
            recovery=context["recovery"],
            deep=deep,
            probe_report=probe_report or {},
        )

    @staticmethod
    def format_errors(precheck: Any) -> str:
        errors = [issue for issue in precheck.issues if issue.level == "error"]
        if not errors:
            return "Task input is not submittable."
        return "; ".join(f"{issue.code} at {issue.field}: {issue.message}" for issue in errors)

    def _build_precheck_context(
        self,
        *,
        pipeline_name: str,
        collector_name: str,
    ) -> dict[str, Any]:
        pipeline = self._scheduler.get_pipeline(pipeline_name)
        template = self._get_pipeline_template(pipeline_name)
        collector_entries = self._list_collector_entries(pipeline, template)
        resolved_collectors = self._resolve_collectors(
            collector_name, pipeline, template, collector_entries
        )
        resolved_collector = resolved_collectors[0] if resolved_collectors else ""
        collector_metadata = get_collector_metadata(resolved_collector)

        session_diagnostics_by_collector: dict[str, Any] = {}
        session_readiness_by_collector: dict[str, Any] = {}
        for cid in resolved_collectors:
            diagnostics = build_collector_session_diagnostics(cid)
            session_diagnostics_by_collector[cid] = diagnostics
            session_readiness_by_collector[cid] = build_session_readiness_summary(diagnostics)

        session_diagnostics = (
            session_diagnostics_by_collector.get(resolved_collector, {})
            if resolved_collector
            else {}
        )
        session_readiness = (
            session_readiness_by_collector.get(resolved_collector, {})
            if resolved_collector
            else {}
        )

        from_upstream_by_collector = {
            name: collector_uses_from_upstream(config) for name, config in collector_entries
        }
        # Collectors not in entries (explicit-only) treated as root.
        for cid in resolved_collectors:
            from_upstream_by_collector.setdefault(cid, False)

        root_collectors = [
            cid for cid in resolved_collectors if not from_upstream_by_collector.get(cid, False)
        ]
        # Target validation targets root collectors; fallback to primary.
        target_collector = root_collectors[0] if root_collectors else resolved_collector
        target_metadata = get_collector_metadata(target_collector)

        return {
            "pipeline": pipeline,
            "template": template,
            "collector_entries": collector_entries,
            "resolved_collectors": resolved_collectors,
            "resolved_collector": resolved_collector,
            "from_upstream_by_collector": from_upstream_by_collector,
            "root_collectors": root_collectors,
            "target_collector": target_collector,
            "collector_metadata": collector_metadata,
            "target_collector_metadata": target_metadata,
            "collector_metadata_payload": (
                collector_metadata_payload(resolved_collector)
                if collector_metadata is not None
                else {}
            ),
            "session_diagnostics": session_diagnostics,
            "session_diagnostics_by_collector": session_diagnostics_by_collector,
            "session_readiness": session_readiness,
            "session_readiness_by_collector": session_readiness_by_collector,
            "required_fields": self._required_target_fields(
                target_collector,
                target_metadata,
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
        issues.extend(self._build_graph_upstream_issues(context=context))

        for cid in context["resolved_collectors"]:
            metadata = get_collector_metadata(cid)
            context["data_source_status"][cid] = "available"
            issues.extend(
                self._credential_checks(
                    cid,
                    context["credential_status"],
                    collector_metadata=metadata,
                )
            )
            issues.extend(
                self._collector_config_checks(
                    cid,
                    context["pipeline"],
                    context["template"],
                    collector_metadata=metadata,
                )
            )
            readiness = context["session_readiness_by_collector"].get(cid) or {}
            readiness_issue = self._session_readiness_issue(readiness, collector_id=cid)
            if readiness_issue is not None:
                issues.append(readiness_issue)
            issues.extend(
                self._session_diagnostic_issues(
                    context["session_diagnostics_by_collector"].get(cid) or {},
                    collector_id=cid,
                )
            )

        issues.extend(self._execution_backend_issues())
        issues.extend(
            self._worker_capability_issues(
                context.get("resolved_collectors") or [],
                context.get("session_readiness_by_collector") or {},
            )
        )
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
                self._issue(
                    "error",
                    "missing_task_name",
                    "name",
                    "Task name is required.",
                    category="config",
                )
            )
        if not pipeline_name.strip():
            issues.append(
                self._issue(
                    "error",
                    "missing_pipeline",
                    "pipeline_name",
                    "Pipeline is required.",
                    category="config",
                )
            )
        elif context["pipeline"] is None and context["template"] is None:
            issues.append(
                self._issue(
                    "error",
                    "unknown_pipeline",
                    "pipeline_name",
                    f"Pipeline not found: {pipeline_name}",
                    category="config",
                )
            )
        if not context["resolved_collector"]:
            issues.append(
                self._issue(
                    "error",
                    "collector_unresolved",
                    "collector_name",
                    "Collector cannot be inferred from pipeline.",
                    category="config",
                )
            )

        if not targets:
            root_collectors = context.get("root_collectors") or []
            all_from_upstream = bool(context.get("resolved_collectors")) and not root_collectors
            if all_from_upstream:
                # Chained collectors take targets from upstream at runtime.
                pass
            else:
                issues.append(
                    self._issue(
                        "error",
                        "missing_targets",
                        "targets",
                        "At least one target is required.",
                        category="target",
                    )
                )
        return issues

    def _build_target_precheck_issues(
        self,
        *,
        targets: list[dict[str, Any]],
        context: dict[str, Any],
    ) -> list[TaskPrecheckIssue]:
        if not targets:
            return []
        issues: list[TaskPrecheckIssue] = []
        # Validate task targets against root collector(s). Primary root first;
        # if multiple roots share the same target shape, use the first.
        root_collectors = context.get("root_collectors") or []
        target_collector = context.get("target_collector") or context["resolved_collector"]
        if not target_collector:
            return issues

        metadata = context.get("target_collector_metadata") or get_collector_metadata(
            target_collector
        )
        for index, target in enumerate(targets):
            issues.extend(
                self._validate_target(
                    index,
                    target,
                    target_collector,
                    collector_metadata=metadata,
                )
            )

        # When multiple root collectors exist, warn that targets only validated
        # against the primary root schema.
        if len(root_collectors) > 1:
            issues.append(
                self._issue(
                    "warning",
                    "multi_root_target_schema",
                    "targets",
                    (
                        "Pipeline has multiple root collectors; targets were validated "
                        f"against '{target_collector}' only."
                    ),
                    collector_id=target_collector,
                    category="target",
                    suggested_action="Ensure targets satisfy every root collector schema.",
                )
            )
        return issues

    def _build_graph_upstream_issues(self, *, context: dict[str, Any]) -> list[TaskPrecheckIssue]:
        raw_issues = validate_pipeline_collector_upstream(context.get("collector_entries") or [])
        return [
            TaskPrecheckIssue(
                level=str(item.get("level") or "warning"),
                code=str(item.get("code") or "graph_issue"),
                field=str(item.get("field") or "pipeline"),
                message=str(item.get("message") or "Graph validation issue."),
                collector_id=str(item.get("collector_id") or ""),
                category=str(item.get("category") or "graph"),
                suggested_action=str(item.get("suggested_action") or ""),
            )
            for item in raw_issues
        ]

    def _build_collectors_readiness(
        self,
        context: dict[str, Any],
        issues: list[TaskPrecheckIssue],
    ) -> list[CollectorReadiness]:
        readiness_list: list[CollectorReadiness] = []
        from_upstream_map = context.get("from_upstream_by_collector") or {}
        for cid in context.get("resolved_collectors") or []:
            related = [
                issue
                for issue in issues
                if issue.collector_id == cid
                or (
                    not issue.collector_id
                    and cid == context.get("resolved_collector")
                    and issue.category in {"target", "config"}
                )
            ]
            # Also attribute unscoped credential issues containing collector-less codes
            # that were emitted with collector_id set — already covered.
            error_count = sum(1 for i in related if i.level == "error")
            warning_count = sum(1 for i in related if i.level == "warning")
            if error_count:
                status = "error"
            elif warning_count:
                status = "warning"
            else:
                status = "ok"
            session = context.get("session_readiness_by_collector", {}).get(cid) or {}
            uses_upstream = bool(from_upstream_map.get(cid, False))
            readiness_list.append(
                CollectorReadiness(
                    collector_id=cid,
                    status=status,
                    requires_session=bool(session.get("required")),
                    session_precheck_status=str(session.get("precheck_status") or "ok"),
                    is_root=not uses_upstream,
                    from_upstream=uses_upstream,
                    issue_count=len(related),
                    error_count=error_count,
                    warning_count=warning_count,
                )
            )
        return readiness_list

    def _execution_backend_issues(self) -> list[TaskPrecheckIssue]:
        backend = str(get_config("scheduler.execution_backend", "in_process") or "in_process")
        backend = backend.strip().lower()
        if backend != "worker_claim":
            return []
        # Static check only: warn that tasks need online workers at runtime.
        return [
            self._issue(
                "warning",
                "worker_claim_backend",
                "scheduler.execution_backend",
                (
                    "Scheduler uses worker_claim; ensure at least one online worker "
                    "with required capabilities before relying on task completion."
                ),
                category="runtime",
                suggested_action="Start a worker agent or switch to in_process for local runs.",
            )
        ]

    def _worker_capability_issues(
        self,
        collector_ids: list[str],
        session_readiness_by_collector: dict[str, Any],
    ) -> list[TaskPrecheckIssue]:
        """When worker_claim is enabled, warn if no online worker matches required caps."""
        backend = str(get_config("scheduler.execution_backend", "in_process") or "in_process")
        if backend.strip().lower() != "worker_claim":
            return []

        required: set[str] = set()
        for cid in collector_ids:
            readiness = session_readiness_by_collector.get(cid) or {}
            for cap in readiness.get("required_worker_capabilities") or []:
                if str(cap).strip():
                    required.add(str(cap).strip())
            # Also pull from metadata helper
            try:
                from src.core.collector_metadata import required_worker_capabilities

                required |= set(required_worker_capabilities(cid))
            except Exception:
                pass

        workers = self._list_online_workers_sync()
        if workers is None:
            return []  # registry unavailable — backend warning already covers it
        if not workers:
            return [
                self._issue(
                    "error",
                    "no_online_workers",
                    "workers",
                    "execution_backend is worker_claim but no online workers are registered.",
                    category="runtime",
                    suggested_action="Start at least one worker agent.",
                )
            ]
        if not required:
            return []

        capable = []
        for worker in workers:
            caps = set(str(c) for c in (worker.get("capabilities") or []))
            if required.issubset(caps):
                capable.append(worker)
        if capable:
            return []
        return [
            self._issue(
                "warning",
                "no_matching_worker_capabilities",
                "workers",
                (
                    "No online worker declares all required capabilities: "
                    + ", ".join(sorted(required))
                ),
                category="runtime",
                suggested_action="Start a worker with the required session/capability tags.",
            )
        ]

    def _list_online_workers_sync(self) -> list[dict[str, Any]] | None:
        """Best-effort snapshot of online workers; None if registry not available."""
        try:
            from src.web.app import get_worker_registry
        except Exception:
            return None
        registry = get_worker_registry()
        if registry is None:
            return None
        list_fn = getattr(registry, "list_workers", None)
        if not callable(list_fn):
            return None
        try:
            import asyncio
            import inspect

            result = list_fn(stale_after_seconds=120)
            if inspect.isawaitable(result):
                try:
                    loop = asyncio.get_running_loop()
                except RuntimeError:
                    loop = None
                if loop and loop.is_running():
                    # Cannot block; skip live worker enumeration in async context.
                    return None
                result = asyncio.run(result)
            workers = result or []
        except Exception:
            return None

        online: list[dict[str, Any]] = []
        for worker in workers:
            if hasattr(worker, "to_public_payload"):
                payload = worker.to_public_payload()
            elif isinstance(worker, dict):
                payload = worker
            else:
                payload = {
                    "status": getattr(worker, "status", None),
                    "capabilities": getattr(worker, "capabilities", []) or [],
                }
            status = str(payload.get("status") or "").lower()
            if status in {"online", "idle", "busy", "draining"}:
                online.append(payload)
        return online

    @staticmethod
    def _precheck_status(issues: list[TaskPrecheckIssue]) -> str:
        has_error = any(issue.level == "error" for issue in issues)
        has_warning = any(issue.level == "warning" for issue in issues)
        return "error" if has_error else "warning" if has_warning else "ok"

    @staticmethod
    def _issue(
        level: str,
        code: str,
        field: str,
        message: str,
        *,
        collector_id: str = "",
        category: str = "",
        suggested_action: str = "",
    ) -> TaskPrecheckIssue:
        return TaskPrecheckIssue(
            level=level,
            code=code,
            field=field,
            message=message,
            collector_id=collector_id,
            category=category,
            suggested_action=suggested_action,
        )

    @staticmethod
    def _get_pipeline_template(pipeline_name: str) -> dict[str, Any] | None:
        if not pipeline_name:
            return None
        from src.core.pipeline_templates import PIPELINE_TEMPLATES

        return next((item for item in PIPELINE_TEMPLATES if item.get("id") == pipeline_name), None)

    @staticmethod
    def _list_collector_entries(
        pipeline: Any | None,
        template: dict[str, Any] | None,
    ) -> list[tuple[str, dict[str, Any]]]:
        """Return ordered (collector_name, config) pairs from pipeline or template."""
        entries: list[tuple[str, dict[str, Any]]] = []
        seen: set[str] = set()

        if pipeline is not None:
            for step in pipeline.steps:
                if getattr(step, "step_type", None) is None:
                    continue
                if step.step_type.value != "collector":
                    continue
                name = str(step.component_name or "").strip()
                if not name or name in seen:
                    continue
                cfg = dict(step.config) if isinstance(step.config, dict) else {}
                entries.append((name, cfg))
                seen.add(name)
            return entries

        for step in (template or {}).get("steps", []):
            if not isinstance(step, dict) or step.get("type") != "collector":
                continue
            name = str(step.get("name") or "").strip()
            if not name or name in seen:
                continue
            cfg = step.get("config", {})
            entries.append((name, dict(cfg) if isinstance(cfg, dict) else {}))
            seen.add(name)
        return entries

    @staticmethod
    def _resolve_collectors(
        explicit: str,
        pipeline: Any | None,
        template: dict[str, Any] | None,
        collector_entries: list[tuple[str, dict[str, Any]]] | None = None,
    ) -> list[str]:
        entries = collector_entries
        if entries is None:
            entries = TaskPrecheckService._list_collector_entries(pipeline, template)
        names = [name for name, _ in entries]
        if explicit:
            explicit = explicit.strip()
            if explicit in names:
                # Keep full pipeline collectors; put explicit first for primary.
                return [explicit] + [n for n in names if n != explicit]
            return [explicit] + names
        return names

    @staticmethod
    def _resolve_collector_name(
        explicit: str, pipeline: Any | None, template: dict[str, Any] | None
    ) -> str:
        collectors = TaskPrecheckService._resolve_collectors(explicit, pipeline, template)
        return collectors[0] if collectors else ""

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
                    if step.step_type.value == "collector" and step.component_name == collector_name
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
                            collector_id=collector_name,
                            category="target",
                        )
                    )
            issues.extend(
                TaskPrecheckService._validate_target_formats(
                    index, target, collector_name, has_presence_error=any(
                        i.level == "error" for i in issues
                    )
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
                        collector_id=collector_name,
                        category="target",
                    )
                )
            elif not str(params.get("app_id") or "").strip():
                issues.append(
                    TaskPrecheckIssue(
                        level="warning",
                        code="missing_steam_app_id",
                        field=field,
                        message="Steam app_id is recommended to avoid wrong game matches.",
                        collector_id=collector_name,
                        category="target",
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
                        collector_id=collector_name,
                        category="target",
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
                        collector_id=collector_name,
                        category="target",
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
                        collector_id=collector_name,
                        category="target",
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
                        collector_id=collector_name,
                        category="target",
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
                        collector_id=collector_name,
                        category="target",
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
                        collector_id=collector_name,
                        category="target",
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
                        collector_id=collector_name,
                        category="target",
                    )
                )
        elif not name and not params:
            issues.append(
                TaskPrecheckIssue(
                    level="warning",
                    code="empty_target",
                    field=field,
                    message="Target has no name or params.",
                    collector_id=collector_name,
                    category="target",
                )
            )

        issues.extend(
            TaskPrecheckService._validate_target_formats(
                index,
                target,
                collector_name,
                has_presence_error=any(i.level == "error" for i in issues),
            )
        )
        return issues

    @staticmethod
    def _validate_target_formats(
        index: int,
        target: dict[str, Any],
        collector_name: str,
        *,
        has_presence_error: bool,
    ) -> list[TaskPrecheckIssue]:
        """Static format checks (no network). Skip when presence already failed."""
        if has_presence_error:
            return []
        issues: list[TaskPrecheckIssue] = []
        params = target.get("params", {}) if isinstance(target.get("params"), dict) else {}
        field_base = f"targets[{index}]"

        app_id = str(params.get("app_id") or "").strip()
        if app_id and collector_name in {
            "steam",
            "steam_discussions",
            "taptap",
            "qimai",
            "monitor",
        }:
            if not _APP_ID_RE.match(app_id):
                issues.append(
                    TaskPrecheckIssue(
                        level="warning",
                        code="invalid_app_id_format",
                        field=f"{field_base}.params.app_id",
                        message=f"app_id should be numeric, got '{app_id}'.",
                        collector_id=collector_name,
                        category="target",
                        suggested_action="Use a numeric platform app id.",
                    )
                )

        for url_key in ("official_url", "url", "forum_url", "siteurl"):
            # siteurl for monitor may be a slug, not a URL — skip format for siteurl
            if url_key == "siteurl":
                continue
            raw = str(params.get(url_key) or "").strip()
            if not raw:
                continue
            if collector_name == "official_site" and url_key == "official_url":
                parsed = urlsplit(raw)
                if not parsed.scheme or not parsed.netloc:
                    issues.append(
                        TaskPrecheckIssue(
                            level="error",
                            code="invalid_official_url",
                            field=f"{field_base}.params.official_url",
                            message="official_url must be an absolute URL (http/https).",
                            collector_id=collector_name,
                            category="target",
                            suggested_action="Provide a full URL including scheme.",
                        )
                    )

        if collector_name in {"youtube_profiles", "youtube_comments"}:
            for url_key in ("video_url", "channel_url"):
                raw = str(params.get(url_key) or "").strip()
                if not raw or not raw.startswith("http"):
                    continue
                host = (urlsplit(raw).hostname or "").lower()
                if host.endswith("youtube.com") or host == "youtu.be":
                    continue
                issues.append(
                    TaskPrecheckIssue(
                        level="warning",
                        code="invalid_youtube_url_host",
                        field=f"{field_base}.params.{url_key}",
                        message=f"{url_key} does not look like a YouTube URL.",
                        collector_id=collector_name,
                        category="target",
                        suggested_action="Use a youtube.com or youtu.be URL.",
                    )
                )

        return issues

    @staticmethod
    def _target_rule_passes(target: dict[str, Any], rule: TargetValidationRule) -> bool:
        present = [
            TaskPrecheckService._target_field_present(target, field) for field in rule.fields
        ]
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
                    collector_id=collector_name,
                    category="config",
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
                        collector_id=collector_name,
                        category="config",
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
                collector_id=collector_name,
                category="config",
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
                    collector_id=collector_name,
                    category="config",
                )
            if numeric_value < minimum:
                return TaskPrecheckIssue(
                    level="error",
                    code="invalid_collector_config_minimum",
                    field=field,
                    message=f"{collector_name} config value must be >= {schema['minimum']}.",
                    collector_id=collector_name,
                    category="config",
                )

        if schema.get("format") == "uri" and isinstance(value, str):
            parsed = urlsplit(value)
            if not parsed.scheme or not parsed.netloc:
                return TaskPrecheckIssue(
                    level="error",
                    code="invalid_collector_config_uri",
                    field=field,
                    message=f"{collector_name} config value must be an absolute URI.",
                    collector_id=collector_name,
                    category="config",
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
                collector_id="dynamic_playwright",
                category="config",
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
                        collector_id=collector_name,
                        category="credential",
                        suggested_action="Set steam.api_key in settings or .env.",
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
                        collector_id=collector_name,
                        category="credential",
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
                        collector_id=collector_name,
                        category="credential",
                        suggested_action="pip install playwright && playwright install chromium",
                    )
                )
        else:
            credential_status.setdefault("credentials", "not_required")
        return issues

    @staticmethod
    def _credential_checks_from_metadata(
        collector_metadata: CollectorMetadata,
        credential_status: dict[str, str],
    ) -> list[TaskPrecheckIssue]:
        issues: list[TaskPrecheckIssue] = []
        collector_name = collector_metadata.collector_id
        profiles = set(collector_metadata.credential_profiles)
        if not profiles:
            credential_status.setdefault("credentials", "not_required")
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
                        collector_id=collector_name,
                        category="credential",
                        suggested_action="Set steam.api_key in settings or .env.",
                    )
                )

        if (
            "steamdb_optional_browser_session" in profiles
            and bool(get_config("steam.steamdb.enabled", False))
            and bool(get_config("steam.steamdb.cdp_enabled", False))
        ):
            credential_status["steam.steamdb.browser_session"] = "requires_login_session"
            issues.append(
                TaskPrecheckIssue(
                    level="warning",
                    code="steamdb_login_session",
                    field="steam.steamdb",
                    message="SteamDB may require a logged-in browser session.",
                    collector_id=collector_name,
                    category="credential",
                )
            )

        if "youtube_api_key" in profiles:
            raw_keys = get_config("youtube.api_keys", []) or []
            if isinstance(raw_keys, str):
                raw_keys = [raw_keys]
            youtube_keys = [
                str(key).strip()
                for key in raw_keys
                if str(key).strip() and not str(key).strip().startswith("${")
            ]
            credential_status["youtube.api_keys"] = (
                "configured" if youtube_keys else "missing"
            )
            if not youtube_keys:
                issues.append(
                    TaskPrecheckIssue(
                        level="error",
                        code="missing_youtube_api_key",
                        field="youtube.api_keys",
                        message=(
                            "YouTube API keys are missing; configure youtube.api_keys "
                            "before running YouTube collectors."
                        ),
                        collector_id=collector_name,
                        category="credential",
                        suggested_action="Configure youtube.api_keys in settings.yaml / .env.",
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
                        collector_id=collector_name,
                        category="credential",
                        suggested_action="pip install playwright && playwright install chromium",
                    )
                )

        if "local_browser_profile" in profiles:
            credential_status["browser_profile"] = "local_profile_required"

        return issues

    @staticmethod
    def _session_diagnostic_issues(
        session_diagnostics: dict[str, Any],
        *,
        collector_id: str = "",
    ) -> list[TaskPrecheckIssue]:
        if not session_diagnostics:
            return []

        cid = collector_id or str(session_diagnostics.get("collector_id") or "")
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
                    collector_id=cid,
                    category="session",
                )
            )
        return issues

    @staticmethod
    def _session_readiness_issue(
        session_readiness: dict[str, Any],
        *,
        collector_id: str = "",
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
            collector_id=collector_id,
            category="session",
            suggested_action=str(session_readiness.get("recommended_action") or ""),
        )
