"""Stored data browsing API routes."""

from __future__ import annotations

import contextlib
import copy
import json
import uuid
from collections import defaultdict
from typing import Annotated, Any, AsyncIterator

from fastapi import APIRouter, HTTPException, Path, Query, Body
from fastapi.responses import Response
from pydantic import BaseModel, Field

from src.core.task import Task, TaskTarget
from src.services._utils import (
    build_record_summary,
    compute_record_completeness,
    extract_record_identity,
    max_iso,
    record_group,
    roll_time_params,
)
from src.storage.base import StorageRecord
from src.storage.local_store import LocalStorage
from src.storage.vector_store import VectorStorage
from src.web.safety import require_explicit_confirmation

router = APIRouter(tags=["data"])


@contextlib.asynccontextmanager
async def _local_store() -> AsyncIterator[LocalStorage]:
    """Shared async context manager for LocalStorage connections."""
    store = LocalStorage()
    await store.initialize()
    try:
        yield store
    finally:
        await store.close()


class DataSourceSummary(BaseModel):
    name: str
    collector: str
    count: int = 0
    latest_stored_at: str | None = None


class DataGameSummary(BaseModel):
    game_key: str
    game_name: str
    app_id: str | None = None
    total_records: int = 0
    latest_stored_at: str | None = None
    group_id: str = ""
    group_name: str = ""
    sources: list[DataSourceSummary] = Field(default_factory=list)


class DataRecordSummary(BaseModel):
    key: str
    game_key: str
    game_name: str
    app_id: str | None = None
    data_source: str
    collector: str
    source: str
    stored_at: str
    group_id: str = ""
    group_name: str = ""
    display_name: str = ""
    task_id: str = ""
    task_name: str = ""
    refresh_parent_key: str = ""
    refresh_series_id: str = ""
    refresh_kind: str = ""
    summary: dict[str, Any] = Field(default_factory=dict)
    completeness: str = "full"


class DataRecordDetail(DataRecordSummary):
    metadata: dict[str, Any] = Field(default_factory=dict)
    data: Any = None


class DataRecordPage(BaseModel):
    items: list[DataRecordSummary] = Field(default_factory=list)
    total: int = 0
    page: int = 1
    page_size: int = 50
    has_more: bool = False


class DataGroupSummary(BaseModel):
    group_id: str
    group_name: str
    count: int
    latest_stored_at: str | None = None


class DeleteDataCategoryResponse(BaseModel):
    message: str
    game_key: str = ""
    group_id: str = ""
    records_deleted: int = 0
    vector_records_deleted: int = 0
    tasks_deleted: int = 0
    cron_jobs_deleted: int = 0
    reports_deleted: int = 0


class UpdateRecordRequest(BaseModel):
    group_id: str | None = None
    group_name: str | None = None
    display_name: str | None = None
    notes: str | None = None
    tags: list[str] | None = None
    task_name: str | None = None


class RefreshRecordRequest(BaseModel):
    rolling_window: bool = True


class CreateRefreshScheduleRequest(BaseModel):
    name: str | None = None
    cron_expr: str = Field(..., description="Five-field cron expression")
    rolling_window: bool = True


class BatchRecordRequest(BaseModel):
    keys: list[str] = Field(..., min_length=1, max_length=500, description="Record keys to operate on")
    confirm: bool = Field(default=False, description="Must be true for destructive operations")


@router.get("/data/games", response_model=list[DataGameSummary])
async def list_data_games(
    limit: Annotated[int, Query(description="Maximum source records to scan")] = 1000,
):
    records = await _load_source_records(limit=limit)
    grouped: dict[str, dict[str, Any]] = {}

    for record in records:
        identity = extract_record_identity(record)
        if not identity:
            continue
        group = record_group(record)
        grouped_key = f"group:{group['group_id']}" if group.get("group_id") else identity["game_key"]
        game = grouped.setdefault(
            grouped_key,
            {
                "game_key": grouped_key,
                "game_name": group.get("group_name") or identity["game_name"],
                "app_id": identity.get("app_id"),
                "total_records": 0,
                "latest_stored_at": None,
                "group_id": group.get("group_id", ""),
                "group_name": group.get("group_name", ""),
                "sources": defaultdict(lambda: {"name": "", "collector": "", "count": 0, "latest_stored_at": None}),
            },
        )
        game["total_records"] += 1
        game["latest_stored_at"] = max_iso(game["latest_stored_at"], record.stored_at.isoformat())
        if group.get("group_id"):
            game["app_id"] = _merge_app_id(game.get("app_id"), identity.get("app_id"))
        elif identity.get("app_id") and not game.get("app_id"):
            game["app_id"] = identity["app_id"]

        source_bucket = game["sources"][identity["data_source"]]
        source_bucket["name"] = identity["data_source"]
        source_bucket["collector"] = identity["collector"]
        source_bucket["count"] += 1
        source_bucket["latest_stored_at"] = max_iso(source_bucket["latest_stored_at"], record.stored_at.isoformat())

    response: list[DataGameSummary] = []
    for game in grouped.values():
        sources = [
            DataSourceSummary(**source)
            for source in sorted(game["sources"].values(), key=lambda item: item.get("latest_stored_at") or "", reverse=True)
        ]
        response.append(DataGameSummary(**{**game, "sources": sources}))

    response.sort(key=lambda item: item.latest_stored_at or "", reverse=True)
    return response


@router.get("/data/groups", response_model=list[DataGroupSummary])
async def list_data_groups(
    limit: Annotated[int, Query(description="Maximum source records to scan")] = 1000,
):
    groups: dict[str, dict[str, Any]] = {}
    for record in await _load_source_records(limit=limit):
        group = record_group(record)
        if not group.get("group_id"):
            continue
        bucket = groups.setdefault(
            group["group_id"],
            {
                "group_id": group["group_id"],
                "group_name": group.get("group_name") or group["group_id"],
                "count": 0,
                "latest_stored_at": None,
            },
        )
        bucket["count"] += 1
        bucket["latest_stored_at"] = max_iso(bucket["latest_stored_at"], record.stored_at.isoformat())
    return [
        DataGroupSummary(**item)
        for item in sorted(groups.values(), key=lambda item: item.get("latest_stored_at") or "", reverse=True)
    ]


@router.get("/data/records", response_model=DataRecordPage)
async def list_data_records(
    q: Annotated[str, Query(description="Optional key/source search text")] = "",
    source: Annotated[str, Query(description="Optional exact storage source filter")] = "",
    collector: Annotated[str, Query(description="Optional collector filter")] = "",
    game_name: Annotated[str, Query(description="Optional game name filter")] = "",
    app_id: Annotated[str, Query(description="Optional app_id filter")] = "",
    group_id: Annotated[str, Query(description="Optional group_id filter")] = "",
    task_id: Annotated[str, Query(description="Optional task_id filter")] = "",
    page: Annotated[int, Query(ge=1, description="Page number, starting from 1")] = 1,
    page_size: Annotated[int, Query(ge=1, le=200, description="Records per page")] = 50,
    sort_order: Annotated[str, Query(pattern="^(asc|desc)$", description="stored_at order")] = "desc",
):
    query_text = f"source:{source.strip()}" if source.strip() else (q.strip() or "key:")
    offset = (page - 1) * page_size
    store = LocalStorage()
    await store.initialize()
    try:
        result = await store.query(
            query_text,
            limit=page_size,
            offset=offset,
            order=sort_order,
            collector=collector.strip(),
            game_name=game_name.strip(),
            app_id=app_id.strip(),
            group_id=group_id.strip(),
            task_id=task_id.strip(),
        )
    finally:
        await store.close()

    summaries = [summary for record in result.records if (summary := _record_summary(record))]
    return DataRecordPage(
        items=summaries,
        total=result.total,
        page=page,
        page_size=page_size,
        has_more=offset + len(result.records) < result.total,
    )


@router.delete("/data/groups/{group_id}", response_model=DeleteDataCategoryResponse)
async def delete_data_group(
    group_id: Annotated[str, Path(description="Data group id")],
    confirm: Annotated[bool, Query(description="Must be true for destructive delete")] = False,
):
    require_explicit_confirmation(confirm, "data group deletion")
    return await _delete_data_category(group_id=group_id.strip())


@router.get("/data/search", response_model=list[DataRecordSummary])
async def search_data_records(
    q: Annotated[str, Query(description="Search text for key, task id/name, game or group")],
    limit: Annotated[int, Query(description="Maximum source records to scan")] = 1000,
):
    needle = q.strip().lower()
    if not needle:
        return []
    results: list[DataRecordSummary] = []
    for record in await _load_source_records(limit=limit):
        summary = _record_summary(record)
        if not summary:
            continue
        haystack = " ".join(
            str(value)
            for value in (
                summary.key,
                summary.game_name,
                summary.app_id,
                summary.data_source,
                summary.group_id,
                summary.group_name,
                summary.task_id,
                summary.task_name,
                record.source,
            )
            if value
        ).lower()
        if needle in haystack:
            results.append(summary)
    results.sort(key=lambda item: item.stored_at, reverse=True)
    return results


@router.get("/data/games/{game_key}/records", response_model=list[DataRecordSummary])
async def list_game_records(
    game_key: Annotated[str, Path(description="Game grouping key")],
    source: Annotated[str | None, Query(description="Optional data source filter")] = None,
    limit: Annotated[int, Query(description="Maximum source records to scan")] = 1000,
):
    summaries: list[DataRecordSummary] = []
    for record in await _load_source_records(limit=limit):
        summary = _record_summary(record)
        if not summary or summary.game_key != game_key:
            continue
        if source and summary.data_source != source:
            continue
        summaries.append(summary)

    summaries.sort(key=lambda item: item.stored_at, reverse=True)
    return summaries


@router.delete("/data/games/{game_key}", response_model=DeleteDataCategoryResponse)
async def delete_data_game(
    game_key: Annotated[str, Path(description="Game grouping key")],
    confirm: Annotated[bool, Query(description="Must be true for destructive delete")] = False,
):
    require_explicit_confirmation(confirm, "data category deletion")
    return await _delete_data_category(game_key=game_key.strip())


@router.get("/data/records/{record_key}", response_model=DataRecordDetail)
async def get_data_record(record_key: Annotated[str, Path(description="Storage record key")]):
    record = await _load_record(record_key)
    summary = _record_summary(record)
    if not summary:
        raise HTTPException(404, f"Data record is not browsable: {record_key}")
    return DataRecordDetail(**summary.model_dump(), metadata=record.metadata, data=record.data)


@router.patch("/data/records/{record_key}", response_model=DataRecordDetail)
async def update_data_record(
    record_key: Annotated[str, Path(description="Storage record key")],
    req: Annotated[UpdateRecordRequest, Body(description="Editable metadata fields")],
):
    async with _local_store() as store:
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
    return await get_data_record(record_key)


@router.delete("/data/records/{record_key}")
async def delete_data_record(
    record_key: Annotated[str, Path(description="Storage record key")],
    confirm: Annotated[bool, Query(description="Must be true for destructive delete")] = False,
):
    require_explicit_confirmation(confirm, "data record deletion")
    async with _local_store() as store:
        record = await store.load(record_key)
        if record is None:
            raise HTTPException(404, f"Data record not found: {record_key}")
        await store.delete(record_key)
    return {"message": f"Data record deleted: {record_key}"}


@router.post("/data/records/batch-delete")
async def batch_delete_records(req: BatchRecordRequest):
    require_explicit_confirmation(req.confirm, "batch data record deletion")
    deleted_keys: list[str] = []
    failed_keys: list[dict[str, str]] = []
    async with _local_store() as store:
        for key in req.keys:
            try:
                await store.delete(key)
                deleted_keys.append(key)
            except Exception as e:
                failed_keys.append({"key": key, "error": str(e)})
    return {
        "message": f"Deleted {len(deleted_keys)} records, {len(failed_keys)} failed",
        "deleted_keys": deleted_keys,
        "failed_keys": failed_keys,
    }


@router.post("/data/records/batch-export")
async def batch_export_records(req: BatchRecordRequest):
    async with _local_store() as store:
        records: list[dict[str, Any]] = []
        for key in req.keys:
            record = await store.load(key)
            if record and record.data:
                item: dict[str, Any] = {
                    "key": record.key,
                    "source": record.source,
                    "stored_at": record.stored_at.isoformat() if record.stored_at else None,
                }
                if isinstance(record.data, dict):
                    item["data"] = record.data
                else:
                    item["data"] = str(record.data)
                records.append(item)
    return {"count": len(records), "records": records}


async def _delete_data_category(*, game_key: str = "", group_id: str = "") -> DeleteDataCategoryResponse:
    if not game_key and not group_id:
        raise HTTPException(400, "Missing game_key or group_id")

    matched_records: list[StorageRecord] = []
    matched_summaries: list[DataRecordSummary] = []
    for record in await _load_source_records(limit=100000):
        summary = _record_summary(record)
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

    await _ensure_related_tasks_are_not_running(task_ids=task_ids, group_ids=group_ids, group_names=group_names)

    records_deleted = await _delete_local_records(record_keys)
    vector_deleted = await _delete_vector_records(record_keys=record_keys, game_key=game_key, group_ids=group_ids, group_names=group_names)
    tasks_deleted = await _delete_related_tasks(task_ids=task_ids, group_ids=group_ids, group_names=group_names)
    cron_deleted = _delete_related_cron_jobs(group_ids=group_ids, group_names=group_names)
    reports_deleted = await _delete_related_reports(
        record_keys=record_keys,
        task_ids=task_ids,
        group_ids=group_ids,
        group_names=group_names,
        game_key=game_key,
    )

    return DeleteDataCategoryResponse(
        message="Data category deleted",
        game_key=game_key,
        group_id=group_id or next(iter(group_ids), ""),
        records_deleted=records_deleted,
        vector_records_deleted=vector_deleted,
        tasks_deleted=tasks_deleted,
        cron_jobs_deleted=cron_deleted,
        reports_deleted=reports_deleted,
    )


async def _delete_local_records(record_keys: set[str]) -> int:
    store = LocalStorage()
    await store.initialize()
    deleted = 0
    try:
        for key in record_keys:
            if await store.delete(key):
                deleted += 1
    finally:
        await store.close()
    return deleted


async def _delete_vector_records(
    *,
    record_keys: set[str],
    game_key: str,
    group_ids: set[str],
    group_names: set[str],
) -> int:
    vector = VectorStorage()
    await vector.initialize()
    deleted = 0
    deleted_keys: set[str] = set()
    try:
        for key in record_keys:
            if await vector.delete(key):
                deleted += 1
                deleted_keys.add(key)

        for key in await vector.list_keys(limit=100000):
            if key in deleted_keys:
                continue
            record = await vector.load(key)
            if record is None:
                continue
            if _record_matches_category(record, game_key=game_key, group_ids=group_ids, group_names=group_names):
                if await vector.delete(key):
                    deleted += 1
                    deleted_keys.add(key)
    finally:
        await vector.close()
    return deleted


async def _ensure_related_tasks_are_not_running(
    *,
    task_ids: set[str],
    group_ids: set[str],
    group_names: set[str],
) -> None:
    from src.web.app import scheduler

    running = [
        task.id
        for task in scheduler.get_all_tasks()
        if _task_matches_category(task, task_ids=task_ids, group_ids=group_ids, group_names=group_names)
        and not task.is_terminal
    ]
    if running:
        raise HTTPException(409, f"Cannot delete category while related tasks are running: {', '.join(running)}")


async def _delete_related_tasks(
    *,
    task_ids: set[str],
    group_ids: set[str],
    group_names: set[str],
) -> int:
    from src.web.app import scheduler

    related_ids = [
        task.id
        for task in scheduler.get_all_tasks()
        if _task_matches_category(task, task_ids=task_ids, group_ids=group_ids, group_names=group_names)
    ]
    deleted = 0
    for task_id in related_ids:
        if await scheduler.delete_task(task_id):
            deleted += 1
    return deleted


def _delete_related_cron_jobs(*, group_ids: set[str], group_names: set[str]) -> int:
    from src.web.app import scheduler

    deleted = 0
    for job in scheduler.list_cron_jobs():
        template = job.get("task_template", {}) if isinstance(job, dict) else {}
        config = template.get("config", {}) if isinstance(template, dict) else {}
        if _data_group_matches(config.get("data_group", {}), group_ids=group_ids, group_names=group_names):
            if scheduler.remove_cron_job(str(job.get("name") or job.get("id") or "")):
                deleted += 1
    return deleted


async def _delete_related_reports(
    *,
    record_keys: set[str],
    task_ids: set[str],
    group_ids: set[str],
    group_names: set[str],
    game_key: str,
) -> int:
    from src.web.app import report_generator

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


def _record_matches_category(
    record: StorageRecord,
    *,
    game_key: str,
    group_ids: set[str],
    group_names: set[str],
) -> bool:
    summary = _record_summary(record)
    if not summary:
        group = record_group(record)
        return bool(
            (group.get("group_id") and group["group_id"] in group_ids)
            or (group.get("group_name") and group["group_name"] in group_names)
        )
    return bool(
        (game_key and summary.game_key == game_key)
        or (summary.group_id and summary.group_id in group_ids)
        or (summary.group_name and summary.group_name in group_names)
    )


def _task_matches_category(
    task: Task,
    *,
    task_ids: set[str],
    group_ids: set[str],
    group_names: set[str],
) -> bool:
    if task.id in task_ids:
        return True
    return _data_group_matches(task.config.get("data_group", {}), group_ids=group_ids, group_names=group_names)


def _data_group_matches(value: Any, *, group_ids: set[str], group_names: set[str]) -> bool:
    if not isinstance(value, dict):
        return False
    current_id = str(value.get("id") or value.get("group_id") or "").strip()
    current_name = str(value.get("name") or value.get("group_name") or "").strip()
    return bool(
        (current_id and current_id in group_ids)
        or (current_name and current_name in group_names)
    )


@router.post("/data/records/{record_key}/refresh")
async def refresh_data_record(
    record_key: Annotated[str, Path(description="Storage record key")],
    req: Annotated[RefreshRecordRequest, Body(description="Refresh options")] = RefreshRecordRequest(),
):
    from src.web.app import scheduler

    record = await _load_record(record_key)
    task = _build_refresh_task(record, refresh_kind="manual", rolling_window=req.rolling_window)
    try:
        await scheduler.submit(task, pipeline_name=task.pipeline_name)
    except (ValueError, RuntimeError) as exc:
        raise HTTPException(400, str(exc))
    return {"message": "Refresh task submitted", "task_id": task.id}


@router.post("/data/records/{record_key}/refresh-schedules")
async def create_record_refresh_schedule(
    record_key: Annotated[str, Path(description="Storage record key")],
    req: Annotated[CreateRefreshScheduleRequest, Body(description="Refresh schedule")],
):
    from src.web.app import scheduler

    record = await _load_record(record_key)
    job_id = req.name or f"refresh_{_safe_filename(record_key)}_{uuid.uuid4().hex[:6]}"
    task = _build_refresh_task(
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
    try:
        scheduler.add_cron_job(
            name=job_id,
            pipeline_name=task.pipeline_name,
            cron_expr=req.cron_expr,
            task_template=task_template,
        )
    except ValueError as exc:
        raise HTTPException(400, str(exc))
    return {"message": "Refresh schedule created", "job_id": job_id}


@router.get("/data/records/{record_key}/download")
async def download_data_record(record_key: Annotated[str, Path(description="Storage record key")]):
    record = await _load_record(record_key)
    payload = {
        "key": record.key,
        "source": record.source,
        "metadata": record.metadata,
        "stored_at": record.stored_at.isoformat(),
        "data": record.data,
    }
    filename = f"{_safe_filename(record.key)}.json"
    return Response(
        content=json.dumps(payload, ensure_ascii=False, indent=2, default=str),
        media_type="application/json",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


async def _load_source_records(limit: int = 1000) -> list[StorageRecord]:
    store = LocalStorage()
    await store.initialize()
    try:
        result = await store.query("key:", limit=limit)
        return result.records
    finally:
        await store.close()


async def _load_record(key: str) -> StorageRecord:
    store = LocalStorage()
    await store.initialize()
    try:
        record = await store.load(key)
    finally:
        await store.close()
    if record is None:
        raise HTTPException(404, f"Data record not found: {key}")
    return record


def _record_summary(record: StorageRecord) -> DataRecordSummary | None:
    identity = extract_record_identity(record)
    if not identity:
        return None
    group = record_group(record)
    source_task = record.metadata.get("source_task", {}) if isinstance(record.metadata, dict) else {}
    if not isinstance(source_task, dict):
        source_task = {}
    grouped_key = f"group:{group['group_id']}" if group.get("group_id") else identity["game_key"]
    return DataRecordSummary(
        key=record.key,
        game_key=grouped_key,
        game_name=identity["game_name"],
        app_id=identity.get("app_id"),
        data_source=identity["data_source"],
        collector=identity["collector"],
        source=record.source,
        stored_at=record.stored_at.isoformat(),
        group_id=group.get("group_id", ""),
        group_name=group.get("group_name", ""),
        display_name=str(record.metadata.get("display_name", "") or ""),
        task_id=str(source_task.get("task_id", "") or ""),
        task_name=str(source_task.get("task_name", "") or ""),
        refresh_parent_key=str(record.metadata.get("refresh_parent_key", "") or ""),
        refresh_series_id=str(record.metadata.get("refresh_series_id", "") or ""),
        refresh_kind=str(record.metadata.get("refresh_kind", "") or ""),
        summary=build_record_summary(record.data),
        completeness=compute_record_completeness(record),
    )


# record_group, extract_record_identity, etc. are now in src.services._utils


# build_record_summary, compute_record_completeness in src.services._utils

def _build_refresh_task(
    record: StorageRecord,
    *,
    refresh_kind: str,
    rolling_window: bool,
    scheduled_job_id: str = "",
) -> Task:
    source_task = record.metadata.get("source_task", {}) if isinstance(record.metadata, dict) else {}
    if not isinstance(source_task, dict) or not source_task.get("pipeline_name"):
        raise HTTPException(400, "This record has no stored source task parameters")

    target_params = copy.deepcopy(source_task.get("target_params", {}))
    if not isinstance(target_params, dict):
        target_params = {}
    if rolling_window:
        roll_time_params(target_params)

    group = record_group(record)
    config = copy.deepcopy(source_task.get("task_config", {}))
    if not isinstance(config, dict):
        config = {}
    if group.get("group_id") or group.get("group_name"):
        config["data_group"] = {
            "id": group.get("group_id", ""),
            "name": group.get("group_name", ""),
        }

    parent_key = str(record.metadata.get("refresh_parent_key") or record.key)
    series_id = str(record.metadata.get("refresh_series_id") or uuid.uuid4().hex[:12])
    config["refresh"] = {
        "refresh_parent_key": parent_key,
        "refresh_series_id": series_id,
        "refresh_run_id": uuid.uuid4().hex[:12],
        "refresh_kind": refresh_kind,
        "scheduled_job_id": scheduled_job_id,
        "rolling_window": rolling_window,
    }

    return Task(
        name=f"Refresh {source_task.get('task_name') or record.key}",
        description=f"Refresh from data record {record.key}",
        pipeline_name=str(source_task["pipeline_name"]),
        collector_name=str(source_task.get("collector_name", "")),
        targets=[
            TaskTarget(
                name=str(source_task.get("target") or record.metadata.get("target") or record.key),
                target_type=str(source_task.get("target_type") or "game"),
                params=target_params,
            )
        ],
        config=config,
    )


# All utility functions (roll_time_params, parse_date_prefix, replace_date_prefix,
# detect_collector, source_label, nested_get, first_str, normalize_key, max_iso)
# are now imported from src.services._utils


def _merge_app_id(current: str | None, incoming: str | None) -> str:
    values: list[str] = []
    for raw in (current, incoming):
        for item in str(raw or "").split(","):
            cleaned = item.strip()
            if cleaned and cleaned != "-" and cleaned not in values:
                values.append(cleaned)
    return ", ".join(values)


def _safe_filename(value: str) -> str:
    return "".join(ch if ch.isalnum() or ch in ("-", "_") else "_" for ch in value)
