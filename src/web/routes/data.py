"""Stored data browsing API routes."""

from __future__ import annotations

import contextlib
import copy
import json
import uuid
from typing import Annotated, Any, AsyncIterator

from fastapi import APIRouter, HTTPException, Path, Query, Body
from fastapi.responses import Response
from pydantic import BaseModel, Field

from src.core.sensitive import redact_sensitive, redact_sensitive_text
from src.core.task import Task, TaskTarget
from src.services.data_browser_service import DataBrowserService
from src.services.data_management_service import (
    DataManagementService,
    export_record_payload,
)
from src.services._utils import (
    build_record_summary,
    compute_record_completeness,
    extract_record_identity,
    filter_records_by_data_source,
    filter_source_data_records,
    max_iso,
    normalize_key,
    normalize_source_token,
    record_group,
    record_source_values,
    roll_time_params,
)
from src.storage.base import StorageRecord
from src.storage.base import BaseStorage
from src.storage.factory import get_storage
from src.web.safety import require_explicit_confirmation

router = APIRouter(tags=["data"])

_SOURCE_FILTER_SCAN_PAGE_SIZE = 1000


def _get_data_browser() -> DataBrowserService:
    return DataBrowserService(
        record_summary=lambda record: _record_summary(record),
        record_source_match_kind=lambda record, source_filter: _record_source_match_kind(
            record, source_filter
        ),
        extract_record_identity=extract_record_identity,
        record_group=record_group,
        normalize_key=normalize_key,
        redact_text=redact_sensitive_text,
        max_iso=max_iso,
        filter_records_by_data_source=filter_records_by_data_source,
        merge_app_id=_merge_app_id,
        source_filter_scan_page_size=_SOURCE_FILTER_SCAN_PAGE_SIZE,
    )


def _get_data_management_service() -> DataManagementService:
    return DataManagementService(
        get_storage=get_storage,
        load_source_records=_load_source_records,
        load_record=_load_record,
        record_summary=lambda record: _record_summary(record),
        record_group=record_group,
        task_matches_category=lambda task, task_ids, group_ids, group_names: _task_matches_category(
            task, task_ids=task_ids, group_ids=group_ids, group_names=group_names
        ),
        data_group_matches=lambda value, group_ids, group_names: _data_group_matches(
            value, group_ids=group_ids, group_names=group_names
        ),
        build_refresh_task=_build_refresh_task,
        export_record_payload=export_record_payload,
    )


@contextlib.asynccontextmanager
async def _local_store() -> AsyncIterator[BaseStorage]:
    """Shared async context manager for LocalStorage connections."""
    store = get_storage()
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
    keys: list[str] = Field(
        ..., min_length=1, max_length=500, description="Record keys to operate on"
    )
    confirm: bool = Field(default=False, description="Must be true for destructive operations")


@router.get("/data/games", response_model=list[DataGameSummary])
async def list_data_games(
    limit: Annotated[
        int, Query(ge=1, le=5000, description="Maximum source records to scan")
    ] = 1000,
):
    records = await _load_source_records(limit=limit)
    response: list[DataGameSummary] = []
    for game in _get_data_browser().list_games(records):
        sources = [DataSourceSummary(**source) for source in game["sources"]]
        response.append(DataGameSummary(**{**game, "sources": sources}))
    return response


@router.get("/data/groups", response_model=list[DataGroupSummary])
async def list_data_groups(
    limit: Annotated[
        int, Query(ge=1, le=5000, description="Maximum source records to scan")
    ] = 1000,
):
    groups = _get_data_browser().list_groups(await _load_source_records(limit=limit))
    return [DataGroupSummary(**item) for item in groups]


@router.get("/data/records", response_model=DataRecordPage)
async def list_data_records(
    q: Annotated[str, Query(description="Optional key/source search text")] = "",
    source: Annotated[str, Query(description="Optional data source filter")] = "",
    collector: Annotated[str, Query(description="Optional collector filter")] = "",
    game_name: Annotated[str, Query(description="Optional game name filter")] = "",
    app_id: Annotated[str, Query(description="Optional app_id filter")] = "",
    group_id: Annotated[str, Query(description="Optional group_id filter")] = "",
    task_id: Annotated[str, Query(description="Optional task_id filter")] = "",
    page: Annotated[int, Query(ge=1, description="Page number, starting from 1")] = 1,
    page_size: Annotated[int, Query(ge=1, le=200, description="Records per page")] = 50,
    sort_order: Annotated[
        str, Query(pattern="^(asc|desc)$", description="stored_at order")
    ] = "desc",
):
    source_filter = source.strip()
    query_text = q.strip() or "key:"
    store = get_storage()
    await store.initialize()
    try:
        filter_kwargs = {
            "collector": collector.strip(),
            "game_name": game_name.strip(),
            "app_id": app_id.strip(),
            "group_id": group_id.strip(),
            "task_id": task_id.strip(),
        }
        page_payload = await _get_data_browser().list_record_page(
            store,
            query_text=query_text,
            source_filter=source_filter,
            page=page,
            page_size=page_size,
            sort_order=sort_order,
            filter_kwargs=filter_kwargs,
        )
        return DataRecordPage(
            items=page_payload["items"],
            total=page_payload["total"],
            page=page_payload["page"],
            page_size=page_payload["page_size"],
            has_more=page_payload["has_more"],
        )
    finally:
        await store.close()


def _record_source_match_kind(record: StorageRecord, source_filter: str) -> str:
    needle = normalize_source_token(source_filter)
    if not needle:
        return "exact" if filter_source_data_records([record]) else ""

    if not filter_source_data_records([record]):
        return ""

    candidate_tokens = {
        normalize_source_token(value)
        for value in record_source_values(record)
        if str(value or "").strip()
    }
    if needle in candidate_tokens:
        return "exact"
    if len(needle) >= 6 and any(needle in token or token in needle for token in candidate_tokens):
        return "relaxed"
    return ""


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
    limit: Annotated[
        int, Query(ge=1, le=5000, description="Maximum source records to scan")
    ] = 1000,
):
    return _get_data_browser().search_records(await _load_source_records(limit=limit), q)


@router.get("/data/games/{game_key}/records", response_model=DataRecordPage)
async def list_game_records(
    game_key: Annotated[str, Path(description="Game grouping key")],
    source: Annotated[str | None, Query(description="Optional data source filter")] = None,
    page: Annotated[int, Query(ge=1, description="Page number, starting from 1")] = 1,
    page_size: Annotated[int, Query(ge=1, le=200, description="Records per page")] = 50,
    sort_order: Annotated[
        str, Query(pattern="^(asc|desc)$", description="stored_at order")
    ] = "desc",
    limit: Annotated[
        int, Query(ge=1, le=5000, description="Maximum source records to scan")
    ] = 2000,
):
    page_payload = _get_data_browser().list_game_record_page(
        await _load_source_records(limit=limit),
        game_key=game_key,
        source=source,
        page=page,
        page_size=page_size,
        sort_order=sort_order,
    )
    return DataRecordPage(
        items=page_payload["items"],
        total=page_payload["total"],
        page=page_payload["page"],
        page_size=page_payload["page_size"],
        has_more=page_payload["has_more"],
    )


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
        raise HTTPException(
            404, f"Data record is not browsable: {redact_sensitive_text(record_key)}"
        )
    return _record_detail_response(record, summary)


@router.patch("/data/records/{record_key}", response_model=DataRecordDetail)
async def update_data_record(
    record_key: Annotated[str, Path(description="Storage record key")],
    req: Annotated[UpdateRecordRequest, Body(description="Editable metadata fields")],
):
    await _get_data_management_service().update_record_metadata(record_key, req)
    return await get_data_record(record_key)


@router.delete("/data/records/{record_key}")
async def delete_data_record(
    record_key: Annotated[str, Path(description="Storage record key")],
    confirm: Annotated[bool, Query(description="Must be true for destructive delete")] = False,
):
    require_explicit_confirmation(confirm, "data record deletion")
    return await _get_data_management_service().delete_record(record_key)


@router.post("/data/records/batch-delete")
async def batch_delete_records(req: BatchRecordRequest):
    require_explicit_confirmation(req.confirm, "batch data record deletion")
    return await _get_data_management_service().batch_delete_records(req.keys)


@router.post("/data/records/batch-export")
async def batch_export_records(req: BatchRecordRequest):
    return await _get_data_management_service().batch_export_records(req.keys)


async def _delete_data_category(
    *, game_key: str = "", group_id: str = ""
) -> DeleteDataCategoryResponse:
    from src.web.app import report_generator, scheduler

    payload = await _get_data_management_service().delete_data_category(
        scheduler=scheduler,
        report_generator=report_generator,
        game_key=game_key,
        group_id=group_id,
    )
    return DeleteDataCategoryResponse(**payload)


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
    return _data_group_matches(
        task.config.get("data_group", {}), group_ids=group_ids, group_names=group_names
    )


def _data_group_matches(value: Any, *, group_ids: set[str], group_names: set[str]) -> bool:
    if not isinstance(value, dict):
        return False
    current_id = str(value.get("id") or value.get("group_id") or "").strip()
    current_name = str(value.get("name") or value.get("group_name") or "").strip()
    return bool(
        (current_id and current_id in group_ids) or (current_name and current_name in group_names)
    )


@router.post("/data/records/{record_key}/refresh")
async def refresh_data_record(
    record_key: Annotated[str, Path(description="Storage record key")],
    req: Annotated[
        RefreshRecordRequest, Body(description="Refresh options")
    ] = RefreshRecordRequest(),
):
    from src.web.app import get_task_service

    return await _get_data_management_service().submit_refresh_task(
        task_service=get_task_service(),
        record_key=record_key,
        rolling_window=req.rolling_window,
    )


@router.post("/data/records/{record_key}/refresh-schedules")
async def create_record_refresh_schedule(
    record_key: Annotated[str, Path(description="Storage record key")],
    req: Annotated[CreateRefreshScheduleRequest, Body(description="Refresh schedule")],
):
    from src.web.app import get_task_service, scheduler

    return await _get_data_management_service().create_refresh_schedule(
        scheduler=scheduler,
        task_service=get_task_service(),
        record_key=record_key,
        req=req,
        safe_filename=_safe_filename,
    )


@router.get("/data/records/{record_key}/download")
async def download_data_record(record_key: Annotated[str, Path(description="Storage record key")]):
    record = await _load_record(record_key)
    payload = {
        "key": record.key,
        "source": record.source,
        "metadata": redact_sensitive(record.metadata),
        "stored_at": record.stored_at.isoformat(),
        "data": redact_sensitive(record.data),
    }
    filename = f"{_safe_filename(record.key)}.json"
    return Response(
        content=json.dumps(payload, ensure_ascii=False, indent=2, default=str),
        media_type="application/json",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


async def _load_source_records(limit: int = 1000) -> list[StorageRecord]:
    store = get_storage()
    await store.initialize()
    try:
        result = await store.query("key:", limit=limit)
        return result.records
    finally:
        await store.close()


async def _load_record(key: str) -> StorageRecord:
    store = get_storage()
    await store.initialize()
    try:
        record = await store.load(key)
        if record is None:
            raise HTTPException(404, f"Data record not found: {key}")
        return record
    finally:
        await store.close()


def _record_summary(record: StorageRecord) -> DataRecordSummary | None:
    identity = extract_record_identity(record)
    if not identity:
        return None
    group = record_group(record)
    source_task = (
        record.metadata.get("source_task", {}) if isinstance(record.metadata, dict) else {}
    )
    if not isinstance(source_task, dict):
        source_task = {}
    grouped_key = f"group:{group['group_id']}" if group.get("group_id") else identity["game_key"]
    safe_game_name = redact_sensitive_text(identity["game_name"])
    safe_app_id = redact_sensitive_text(identity.get("app_id") or "")
    safe_group_id = redact_sensitive_text(group.get("group_id", ""))
    if safe_group_id:
        grouped_key = f"group:{safe_group_id}"
    elif safe_game_name:
        grouped_key = f"name:{normalize_key(safe_game_name)}"
    else:
        grouped_key = f"app:{safe_app_id}"
    task_id = str(source_task.get("task_id") or record.metadata.get("task_id") or "")
    task_name = str(source_task.get("task_name") or record.metadata.get("task_name") or "")
    return DataRecordSummary(
        key=record.key,
        game_key=grouped_key,
        game_name=safe_game_name,
        app_id=safe_app_id or None,
        data_source=redact_sensitive_text(identity["data_source"]),
        collector=redact_sensitive_text(identity["collector"]),
        source=redact_sensitive_text(record.source),
        stored_at=record.stored_at.isoformat(),
        group_id=redact_sensitive_text(group.get("group_id", "")),
        group_name=redact_sensitive_text(group.get("group_name", "")),
        display_name=redact_sensitive_text(str(record.metadata.get("display_name", "") or "")),
        task_id=redact_sensitive_text(task_id),
        task_name=redact_sensitive_text(task_name),
        refresh_parent_key=redact_sensitive_text(
            str(record.metadata.get("refresh_parent_key", "") or "")
        ),
        refresh_series_id=redact_sensitive_text(
            str(record.metadata.get("refresh_series_id", "") or "")
        ),
        refresh_kind=redact_sensitive_text(str(record.metadata.get("refresh_kind", "") or "")),
        summary=redact_sensitive(build_record_summary(record.data)),
        completeness=compute_record_completeness(record),
    )


def _record_detail_response(record: StorageRecord, summary: DataRecordSummary) -> DataRecordDetail:
    return DataRecordDetail(
        **summary.model_dump(),
        metadata=redact_sensitive(record.metadata),
        data=redact_sensitive(record.data),
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
    source_task = (
        record.metadata.get("source_task", {}) if isinstance(record.metadata, dict) else {}
    )
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
