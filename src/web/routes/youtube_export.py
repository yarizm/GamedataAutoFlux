"""YouTube data export API — independent XLSX export, bypasses the game-industry report system."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from uuid import uuid4

from fastapi import APIRouter, HTTPException
from fastapi.responses import FileResponse
from openpyxl import Workbook
from openpyxl.worksheet.worksheet import Worksheet
from pydantic import BaseModel, Field

from src.storage.base import BaseStorage, StorageRecord
from src.storage.factory import get_storage

router = APIRouter(tags=["youtube_export"])

# 导出文件单独放一个目录，不和 tmp/ 根目录下的日志、脚本等临时文件混放。
# 下载接口只认「这个目录下的直接子文件 + .xlsx 后缀」，不再维护文件名模板的正则拷贝。
EXPORT_DIR = Path(__file__).resolve().parents[3] / "tmp" / "exports" / "youtube"
EXPORT_SCAN_PAGE_SIZE = 5000
SUPPORTED_COLLECTORS = {"youtube_profiles", "youtube_comments"}
XLSX_MEDIA_TYPE = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
# 下载文件名里出现任何一个就直接 404：路径分隔符、上级目录、
# 冒号（Windows 盘符如 "D:x.xlsx" 会让 EXPORT_DIR / name 跳出目录，也是 ADS 语法）、NUL。
_UNSAFE_FILENAME_PARTS = ("/", "\\", "..", ":", "\x00")
# 随机后缀撞上已有文件时换名重试的次数（独占创建保证不会覆盖）。
_FILENAME_ATTEMPTS = 3


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

    wb = Workbook()

    if collector == "youtube_profiles":
        _export_profiles(wb, records)
    elif collector == "youtube_comments":
        _export_comments(wb, records)

    filename = _save_export_workbook(wb, collector)

    return ExportResponse(
        download_url=f"/api/files/export/{filename}",
        record_count=len(records),
    )


@router.get("/files/export/{filename}")
async def download_youtube_export(filename: str):
    """下载 YouTube 导出文件：只放行 EXPORT_DIR 下的直接子文件且后缀为 .xlsx。"""
    filepath = _resolve_export_file(filename)
    if filepath is None:
        raise HTTPException(404, "Export file not found")
    return FileResponse(filepath, filename=filename, media_type=XLSX_MEDIA_TYPE)


def _save_export_workbook(wb: Workbook, collector: str) -> str:
    """把工作簿写成一个新的导出文件，返回文件名。

    文件名 = collector + 秒级时间戳 + 8 位随机后缀，同一秒的多次导出不会同名；
    再用独占创建（"xb"）打开，万一后缀撞上已有文件也只会换名重试，绝不覆盖别人的导出。
    """
    EXPORT_DIR.mkdir(parents=True, exist_ok=True)
    for _ in range(_FILENAME_ATTEMPTS):
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        filename = f"export_{collector}_{timestamp}_{uuid4().hex[:8]}.xlsx"
        try:
            fh = (EXPORT_DIR / filename).open("xb")
        except FileExistsError:
            continue
        with fh:
            wb.save(fh)
        return filename
    raise RuntimeError(f"连续 {_FILENAME_ATTEMPTS} 次生成的导出文件名都已存在")


def _resolve_export_file(filename: str) -> Path | None:
    """把下载请求里的文件名解析成 EXPORT_DIR 下的 .xlsx 文件；不合法或不存在返回 None。"""
    if not filename or any(part in filename for part in _UNSAFE_FILENAME_PARTS):
        return None
    if Path(filename).suffix != ".xlsx":
        return None
    export_dir = EXPORT_DIR.resolve()
    filepath = (export_dir / filename).resolve()
    # resolve() 会跟随符号链接：指向目录外的链接在这里被挡掉。
    if not filepath.is_relative_to(export_dir) or not filepath.is_file():
        return None
    return filepath


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


def _active_worksheet(wb: Workbook) -> Worksheet:
    ws = wb.active
    if not isinstance(ws, Worksheet):
        raise RuntimeError("Workbook 缺少可写的活动工作表")
    return ws


def _export_profiles(wb: Workbook, records) -> None:
    ws = _active_worksheet(wb)
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
    ws_video = _active_worksheet(wb)
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
