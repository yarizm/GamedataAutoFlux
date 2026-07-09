"""YouTube data export API — independent XLSX export, bypasses the game-industry report system."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any

from fastapi import APIRouter, HTTPException
from openpyxl import Workbook
from pydantic import BaseModel, Field

from src.storage.base import BaseStorage, StorageRecord
from src.storage.factory import get_storage

router = APIRouter(tags=["youtube_export"])

EXPORT_DIR = Path(__file__).resolve().parent.parent.parent.parent / "tmp"
EXPORT_SCAN_PAGE_SIZE = 5000
SUPPORTED_COLLECTORS = {"youtube_profiles", "youtube_comments"}


class ExportRequest(BaseModel):
    collector: str = Field(..., description="youtube_profiles or youtube_comments")
    task_ids: list[str] = Field(..., description="Filter by task IDs")
    format: str = Field(default="xlsx", description="Export format")


class ExportResponse(BaseModel):
    download_url: str
    record_count: int


@router.post("/data/export/youtube", response_model=ExportResponse)
async def export_youtube(req: ExportRequest):
    """Export YouTube collection data as XLSX file."""
    collector = req.collector.strip()
    if collector not in SUPPORTED_COLLECTORS:
        raise HTTPException(400, f"Unsupported collector: {req.collector}")
    if req.format.strip().lower() != "xlsx":
        raise HTTPException(400, f"Unsupported export format: {req.format}")

    storage = get_storage()
    await storage.initialize()
    try:
        records = await _load_youtube_records(
            storage,
            collector=collector,
            task_ids={str(task_id).strip() for task_id in req.task_ids if str(task_id).strip()},
        )
    finally:
        await storage.close()

    if not records:
        raise HTTPException(404, "No matching records found")

    EXPORT_DIR.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"export_{req.collector}_{timestamp}.xlsx"
    filepath = EXPORT_DIR / filename

    wb = Workbook()

    if collector == "youtube_profiles":
        _export_profiles(wb, records)
    elif collector == "youtube_comments":
        _export_comments(wb, records)

    wb.save(str(filepath))

    return ExportResponse(
        download_url=f"/api/files/export/{filename}",
        record_count=len(records),
    )


async def _load_youtube_records(
    storage: BaseStorage,
    *,
    collector: str,
    task_ids: set[str],
) -> list[StorageRecord]:
    records: list[StorageRecord] = []
    seen: set[str] = set()
    offset = 0

    while True:
        page = await storage.query(
            "key:",
            limit=EXPORT_SCAN_PAGE_SIZE,
            offset=offset,
            order="asc",
        )
        if not page.records:
            break

        for record in page.records:
            if record.key in seen:
                continue
            if _record_matches_export(record, collector=collector, task_ids=task_ids):
                records.append(record)
                seen.add(record.key)

        offset += len(page.records)
        if len(page.records) < EXPORT_SCAN_PAGE_SIZE:
            break
        if page.total and offset >= page.total:
            break

    return records


def _record_matches_export(
    record: StorageRecord,
    *,
    collector: str,
    task_ids: set[str],
) -> bool:
    if collector not in _record_collector_values(record):
        return False
    if not task_ids:
        return True
    return bool(task_ids & _record_task_values(record))


def _record_collector_values(record: StorageRecord) -> set[str]:
    data = _record_data(record)
    metadata = _record_metadata(record)
    source_task = _dict_value(metadata.get("source_task"))
    return {
        value
        for value in (
            str(record.source or "").strip(),
            str(data.get("collector") or "").strip(),
            str(metadata.get("collector") or "").strip(),
            str(source_task.get("collector_name") or "").strip(),
        )
        if value
    }


def _record_task_values(record: StorageRecord) -> set[str]:
    metadata = _record_metadata(record)
    source_task = _dict_value(metadata.get("source_task"))
    return {
        value
        for value in (
            str(metadata.get("task_id") or "").strip(),
            str(source_task.get("task_id") or "").strip(),
            _record_key_task_id(record.key),
        )
        if value
    }


def _record_key_task_id(key: str) -> str:
    return str(key or "").split(":", 1)[0].strip()


def _record_data(record: StorageRecord) -> dict[str, Any]:
    return record.data if isinstance(record.data, dict) else {}


def _record_metadata(record: StorageRecord) -> dict[str, Any]:
    return record.metadata if isinstance(record.metadata, dict) else {}


def _dict_value(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _export_profiles(wb: Workbook, records) -> None:
    ws = wb.active
    ws.title = "博主信息"
    headers = ["输入链接", "博主名称", "频道ID", "频道URL", "粉丝数", "简介", "解析方式", "状态"]
    ws.append(headers)
    for record in records:
        data = record.data if isinstance(record.data, dict) else {}
        ws.append([
            data.get("input_url", ""),
            data.get("author_name", ""),
            data.get("channel_id", ""),
            data.get("channel_url", ""),
            data.get("subscriber_count", ""),
            data.get("description", ""),
            data.get("resolution_method", ""),
            data.get("resolution_status", ""),
        ])


def _export_comments(wb: Workbook, records) -> None:
    ws_video = wb.active
    ws_video.title = "视频信息"
    video_headers = [
        "视频链接", "标题", "频道名", "频道链接", "发布时间", "视频类型",
        "直播状态", "视频时长", "播放量", "点赞数", "评论数", "简介",
    ]
    ws_video.append(video_headers)

    ws_comment = wb.create_sheet("评论信息")
    comment_headers = ["视频链接", "评论点赞量", "评论内容", "评论发布时间"]
    ws_comment.append(comment_headers)

    for record in records:
        data = record.data if isinstance(record.data, dict) else {}
        ws_video.append([
            data.get("video_url", ""),
            data.get("title", ""),
            data.get("channel_name", ""),
            data.get("channel_url", ""),
            data.get("published_at", ""),
            data.get("video_type", ""),
            data.get("live_status", ""),
            data.get("duration", ""),
            data.get("view_count", ""),
            data.get("like_count", ""),
            data.get("comment_count", ""),
            data.get("description", ""),
        ])
        for comment in data.get("comments", []) or []:
            ws_comment.append([
                data.get("video_url", ""),
                str(comment.get("like_count", "")),
                comment.get("text", ""),
                comment.get("published_at", ""),
            ])
