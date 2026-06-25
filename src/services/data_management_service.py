"""Shared data-management operations for API routes and future tool reuse."""

from __future__ import annotations

import uuid
from collections.abc import Awaitable, Callable
from typing import Any

from fastapi import HTTPException

from src.core.sensitive import redact_sensitive, redact_sensitive_text
from src.core.task import Task
from src.services.session_inventory_sync import sync_session_inventory_best_effort
from src.services.task_precheck_service import TaskPrecheckService
from src.storage.base import BaseStorage, StorageRecord

RecordSummaryFn = Callable[[StorageRecord], Any | None]
RecordGroupFn = Callable[[Any], dict[str, str]]
TaskMatchesCategoryFn = Callable[[Task, set[str], set[str], set[str]], bool]
DataGroupMatchesFn = Callable[[Any, set[str], set[str]], bool]
LoadSourceRecordsFn = Callable[[int], Awaitable[list[StorageRecord]]]
LoadRecordFn = Callable[[str], Awaitable[StorageRecord]]
GetStorageFn = Callable[[], BaseStorage]
BuildRefreshTaskFn = Callable[..., Task]
RecordExporterFn = Callable[[StorageRecord], dict[str, Any]]


class DataManagementService:
    """Encapsulates destructive and refresh-oriented data operations."""

    def __init__(
        self,
        *,
        get_storage: GetStorageFn,
        load_source_records: LoadSourceRecordsFn,
        load_record: LoadRecordFn,
        record_summary: RecordSummaryFn,
        record_group: RecordGroupFn,
        task_matches_category: TaskMatchesCategoryFn,
        data_group_matches: DataGroupMatchesFn,
        build_refresh_task: BuildRefreshTaskFn,
        export_record_payload: RecordExporterFn,
    ) -> None:
        self._get_storage = get_storage
        self._load_source_records = load_source_records
        self._load_record = load_record
        self._record_summary = record_summary
        self._record_group = record_group
        self._task_matches_category = task_matches_category
        self._data_group_matches = data_group_matches
        self._build_refresh_task = build_refresh_task
        self._export_record_payload = export_record_payload

    async def update_record_metadata(self, record_key: str, req: Any) -> None:
        store = self._get_storage()
        await store.initialize()
        try:
            record = await store.load(record_key)
            if record is None:
                raise HTTPException(404, f"Data record not found: {record_key}")
            metadata = dict(record.metadata or {})
            if req.group_id is not None:
                metadata["group_id"] = req.group_id.strip()
            if req.group_name is not None:
                metadata["group_name"] = req.group_name.strip()
            if req.display_name is not None:
                metadata["display_name"] = req.display_name.strip()
            if req.notes is not None:
                metadata["notes"] = req.notes.strip()
            if req.task_name is not None:
                source_task = dict(metadata.get("source_task") or {})
                source_task["task_name"] = req.task_name.strip()
                metadata["source_task"] = source_task
            tags = record.tags
            if req.tags is not None:
                tags = [str(tag).strip() for tag in req.tags if str(tag).strip()]
            updated = StorageRecord(
                key=record.key,
                data=record.data,
                metadata=metadata,
                stored_at=record.stored_at,
                source=record.source,
                tags=tags,
            )
            await store.save(updated)
        finally:
            await store.close()

    async def delete_record(self, record_key: str) -> dict[str, str]:
        store = self._get_storage()
        await store.initialize()
        try:
            record = await store.load(record_key)
            if record is None:
                raise HTTPException(404, f"Data record not found: {record_key}")
            await store.delete(record_key)
        finally:
            await store.close()
        return {"message": f"Data record deleted: {record_key}"}

    async def batch_delete_records(self, keys: list[str]) -> dict[str, Any]:
        deleted_keys: list[str] = []
        failed_keys: list[dict[str, str]] = []
        store = self._get_storage()
        await store.initialize()
        try:
            for key in keys:
                try:
                    await store.delete(key)
                    deleted_keys.append(key)
                except Exception as exc:
                    failed_keys.append({"key": key, "error": redact_sensitive_text(str(exc))})
        finally:
            await store.close()
        return {
            "message": f"Deleted {len(deleted_keys)} records, {len(failed_keys)} failed",
            "deleted_keys": deleted_keys,
            "failed_keys": failed_keys,
        }

    async def batch_export_records(self, keys: list[str]) -> dict[str, Any]:
        exported: list[dict[str, Any]] = []
        store = self._get_storage()
        await store.initialize()
        try:
            for key in keys:
                record = await store.load(key)
                if record and record.data:
                    exported.append(self._export_record_payload(record))
        finally:
            await store.close()
        return {"count": len(exported), "records": exported}

    async def delete_data_category(
        self,
        *,
        scheduler: Any,
        report_generator: Any,
        game_key: str = "",
        group_id: str = "",
    ) -> dict[str, Any]:
        if not game_key and not group_id:
            raise HTTPException(400, "Missing game_key or group_id")

        matched_records: list[StorageRecord] = []
        matched_summaries: list[Any] = []
        for record in await self._load_source_records(limit=100000):
            summary = self._record_summary(record)
            if not summary:
                continue
            if group_id and summary.group_id != group_id:
                continue
            if game_key and summary.game_key != game_key:
                continue
            matched_records.append(record)
            matched_summaries.append(summary)

        if not matched_records:
            label = group_id or game_key
            raise HTTPException(404, f"Data category not found: {label}")

        record_keys = {record.key for record in matched_records}
        task_ids = {summary.task_id for summary in matched_summaries if summary.task_id}
        group_ids = {summary.group_id for summary in matched_summaries if summary.group_id}
        group_names = {summary.group_name for summary in matched_summaries if summary.group_name}

        await self._ensure_related_tasks_are_not_running(
            scheduler=scheduler,
            task_ids=task_ids,
            group_ids=group_ids,
            group_names=group_names,
        )
        records_deleted = await self._delete_local_records(record_keys)
        tasks_deleted = await self._delete_related_tasks(
            scheduler=scheduler,
            task_ids=task_ids,
            group_ids=group_ids,
            group_names=group_names,
        )
        cron_deleted = self._delete_related_cron_jobs(
            scheduler=scheduler,
            group_ids=group_ids,
            group_names=group_names,
        )
        reports_deleted = await self._delete_related_reports(
            report_generator=report_generator,
            record_keys=record_keys,
            task_ids=task_ids,
            group_ids=group_ids,
            group_names=group_names,
            game_key=game_key,
        )
        return {
            "message": "Data category deleted",
            "game_key": game_key,
            "group_id": group_id or next(iter(group_ids), ""),
            "records_deleted": records_deleted,
            "tasks_deleted": tasks_deleted,
            "cron_jobs_deleted": cron_deleted,
            "reports_deleted": reports_deleted,
        }

    async def submit_refresh_task(
        self,
        *,
        task_service: Any,
        record_key: str,
        rolling_window: bool,
    ) -> dict[str, str]:
        record = await self._load_record(record_key)
        task = self._build_refresh_task(
            record,
            refresh_kind="manual",
            rolling_window=rolling_window,
        )
        try:
            created = await task_service.create(
                name=task.name,
                description=task.description,
                pipeline_name=task.pipeline_name,
                collector_name=task.collector_name,
                targets=[target.model_dump() for target in task.targets],
                config=task.config,
            )
        except ValueError as exc:
            raise HTTPException(400, redact_sensitive_text(str(exc)))
        return {"message": "Refresh task submitted", "task_id": created.id}

    async def create_refresh_schedule(
        self,
        *,
        scheduler: Any,
        task_service: Any,
        record_key: str,
        req: Any,
        safe_filename: Callable[[str], str],
    ) -> dict[str, str]:
        record = await self._load_record(record_key)
        job_id = req.name or f"refresh_{safe_filename(record_key)}_{uuid.uuid4().hex[:6]}"
        task = self._build_refresh_task(
            record,
            refresh_kind="scheduled",
            rolling_window=req.rolling_window,
            scheduled_job_id=job_id,
        )
        task_template = {
            "description": task.description,
            "collector_name": task.collector_name,
            "targets": [target.model_dump() for target in task.targets],
            "config": task.config,
        }
        precheck = task_service.precheck(
            name=task.name,
            pipeline_name=task.pipeline_name,
            collector_name=task.collector_name,
            targets=task_template["targets"],
            config=task.config,
        )
        if not precheck.can_submit:
            raise HTTPException(
                400,
                redact_sensitive_text(TaskPrecheckService.format_errors(precheck)),
            )
        if (
            getattr(task_service, "sync_from_diagnostics", None) is not None
            and precheck.session_diagnostics
        ):
            await sync_session_inventory_best_effort(
                registry=task_service,
                diagnostics=precheck.session_diagnostics,
                context="data_refresh_schedule",
                collector_id=str(precheck.session_diagnostics.get("collector_id") or ""),
            )
        try:
            scheduler.add_cron_job(
                name=job_id,
                pipeline_name=task.pipeline_name,
                cron_expr=req.cron_expr,
                task_template=task_template,
            )
        except ValueError as exc:
            raise HTTPException(400, redact_sensitive_text(str(exc)))
        return {"message": "Refresh schedule created", "job_id": job_id}

    async def _delete_local_records(self, record_keys: set[str]) -> int:
        store = self._get_storage()
        await store.initialize()
        try:
            deleted = 0
            for key in record_keys:
                if await store.delete(key):
                    deleted += 1
            return deleted
        finally:
            await store.close()

    async def _ensure_related_tasks_are_not_running(
        self,
        *,
        scheduler: Any,
        task_ids: set[str],
        group_ids: set[str],
        group_names: set[str],
    ) -> None:
        running = [
            task.id
            for task in scheduler.get_all_tasks()
            if self._task_matches_category(task, task_ids, group_ids, group_names)
            and not task.is_terminal
        ]
        if running:
            raise HTTPException(
                409, f"Cannot delete category while related tasks are running: {', '.join(running)}"
            )

    async def _delete_related_tasks(
        self,
        *,
        scheduler: Any,
        task_ids: set[str],
        group_ids: set[str],
        group_names: set[str],
    ) -> int:
        related_ids = [
            task.id
            for task in scheduler.get_all_tasks()
            if self._task_matches_category(task, task_ids, group_ids, group_names)
        ]
        deleted = 0
        for task_id in related_ids:
            if await scheduler.delete_task(task_id):
                deleted += 1
        return deleted

    def _delete_related_cron_jobs(
        self,
        *,
        scheduler: Any,
        group_ids: set[str],
        group_names: set[str],
    ) -> int:
        deleted = 0
        for job in scheduler.list_cron_jobs():
            template = job.get("task_template", {}) if isinstance(job, dict) else {}
            config = template.get("config", {}) if isinstance(template, dict) else {}
            if self._data_group_matches(config.get("data_group", {}), group_ids, group_names):
                if scheduler.remove_cron_job(str(job.get("name") or job.get("id") or "")):
                    deleted += 1
        return deleted

    async def _delete_related_reports(
        self,
        *,
        report_generator: Any,
        record_keys: set[str],
        task_ids: set[str],
        group_ids: set[str],
        group_names: set[str],
        game_key: str,
    ) -> int:
        deleted = 0
        for summary in await report_generator.list_reports(limit=100000):
            report = await report_generator.get_report(summary.id)
            if report is None:
                continue
            metadata = report.metadata if isinstance(report.metadata, dict) else {}
            selected_keys = metadata.get("selected_record_keys", [])
            if not isinstance(selected_keys, list):
                selected_keys = []
            matches = (
                bool(record_keys.intersection(str(key) for key in selected_keys))
                or str(metadata.get("task_id") or "") in task_ids
                or str(metadata.get("group_id") or "") in group_ids
                or str(metadata.get("group_name") or "") in group_names
                or (game_key and report.data_source == game_key)
                or (report.data_source in group_ids)
            )
            if matches and await report_generator.delete_report(summary.id):
                deleted += 1
        return deleted


def export_record_payload(record: StorageRecord) -> dict[str, Any]:
    item: dict[str, Any] = {
        "key": record.key,
        "source": record.source,
        "stored_at": record.stored_at.isoformat() if record.stored_at else None,
    }
    if isinstance(record.data, dict):
        item["data"] = redact_sensitive(record.data)
    else:
        item["data"] = redact_sensitive_text(str(record.data))
    return item
