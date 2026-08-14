"""Find persisted and live objects that depend on plugin collectors."""

from __future__ import annotations

from typing import Any, Iterable

from src.plugin_manager.models import PluginReference


class PluginReferenceScanError(RuntimeError):
    """Fail closed when dependency references cannot be inspected."""

    code = "PLUGIN_REFERENCE_SCAN_FAILED"


class PluginReferenceScanner:
    """Scan Pipeline, DAG, Cron, and active task references."""

    def __init__(self, *, scheduler: Any = None, dag_repository: Any = None) -> None:
        self.scheduler = scheduler
        self.dag_repository = dag_repository

    async def scan(self, collectors: Iterable[str]) -> list[PluginReference]:
        owned = {str(item).strip() for item in collectors if str(item).strip()}
        if not owned:
            return []

        references: list[PluginReference] = []
        pipeline_collectors: dict[str, set[str]] = {}
        try:
            pipelines = (
                list(self.scheduler.get_all_pipelines())
                if self.scheduler is not None
                else []
            )
            for pipeline in pipelines:
                matches = {
                    str(getattr(step, "component_name", "") or "")
                    for step in getattr(pipeline, "steps", ())
                    if str(
                        getattr(
                            getattr(step, "step_type", None),
                            "value",
                            getattr(step, "step_type", ""),
                        )
                    )
                    == "collector"
                    and str(getattr(step, "component_name", "") or "") in owned
                }
                if not matches:
                    continue
                name = str(getattr(pipeline, "name", "") or "")
                pipeline_collectors.setdefault(name, set()).update(matches)
                for collector in sorted(matches):
                    references.append(
                        PluginReference("pipeline", name, name, collector)
                    )

            if self.dag_repository is not None:
                dags = await self.dag_repository.list_all()
                for dag in dags:
                    matches = {
                        str(getattr(node, "component", "") or "")
                        for node in getattr(dag, "nodes", ())
                        if getattr(node, "type", "") == "collector"
                        and str(getattr(node, "component", "") or "") in owned
                    }
                    if not matches:
                        continue
                    name = str(getattr(dag, "name", "") or "")
                    pipeline_collectors.setdefault(name, set()).update(matches)
                    for collector in sorted(matches):
                        references.append(PluginReference("dag", name, name, collector))

            cron_jobs = (
                list(self.scheduler.list_cron_jobs())
                if self.scheduler is not None
                else []
            )
            for job in cron_jobs:
                template = job.get("task_template")
                template = template if isinstance(template, dict) else {}
                direct = str(template.get("collector_name") or "")
                pipeline_name = str(job.get("pipeline_name") or "")
                matches = set(pipeline_collectors.get(pipeline_name, set()))
                if direct in owned:
                    matches.add(direct)
                for collector in sorted(matches):
                    reference_id = str(job.get("id") or job.get("name") or "")
                    references.append(
                        PluginReference(
                            "cron",
                            reference_id,
                            str(job.get("name") or reference_id),
                            collector,
                            "enabled" if job.get("enabled", True) else "disabled",
                        )
                    )

            tasks = (
                list(self.scheduler.get_all_tasks())
                if self.scheduler is not None
                else []
            )
            protected_states = {"pending", "running", "retrying"}
            for task in tasks:
                raw_state = getattr(task, "status", "")
                state = str(getattr(raw_state, "value", raw_state) or "")
                if state not in protected_states:
                    continue
                pipeline_name = str(getattr(task, "pipeline_name", "") or "")
                direct = str(getattr(task, "collector_name", "") or "")
                matches = set(pipeline_collectors.get(pipeline_name, set()))
                if direct in owned:
                    matches.add(direct)
                for collector in sorted(matches):
                    references.append(
                        PluginReference(
                            "task",
                            str(getattr(task, "id", "") or ""),
                            str(getattr(task, "name", "") or ""),
                            collector,
                            state,
                        )
                    )
        except Exception as exc:
            raise PluginReferenceScanError(
                f"Unable to verify plugin references: {exc}"
            ) from exc

        unique: dict[tuple[str, str, str], PluginReference] = {}
        for reference in references:
            unique[(reference.kind, reference.reference_id, reference.collector)] = reference
        return sorted(
            unique.values(),
            key=lambda item: (item.kind, item.name, item.collector),
        )
