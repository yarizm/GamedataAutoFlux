"""报告生成 API 路由。"""

from __future__ import annotations

import json
import uuid
from typing import Annotated, Any
from datetime import datetime
from pathlib import Path as FilePath

from fastapi import APIRouter, HTTPException, Query, Path, Body, File, UploadFile
from fastapi.responses import FileResponse
from pydantic import BaseModel, Field

from src.reporting.generator import GeneratedReport, ReportSummary
from src.reporting.report_templates import list_report_templates, normalize_collector
from src.storage.base import StorageRecord
from src.storage.local_store import LocalStorage

router = APIRouter(tags=["reports"])


# ==================== 请求/响应模型 ====================

class GenerateReportRequest(BaseModel):
    """生成报告请求"""
    prompt: str = Field(..., description="提示词")
    data_source: str = Field(default="", description="数据源标识")
    template: str = Field(default="default", description="报告模板")
    params: dict[str, Any] = Field(default_factory=dict, description="额外参数")
    record_keys: list[str] = Field(default_factory=list, description="指定用于报告的原始 JSON 记录 key")


class UpdateReportRequest(BaseModel):
    title: str | None = None
    prompt: str | None = None
    data_source: str | None = None
    template: str | None = None
    notes: str | None = None


class ReportResponse(BaseModel):
    """报告响应"""
    id: str
    title: str
    content: str
    generated_at: str
    prompt: str
    data_source: str
    template: str
    matched_records: int
    metadata: dict[str, Any] = Field(default_factory=dict)


class ReportSummaryResponse(BaseModel):
    id: str
    title: str
    generated_at: str
    prompt: str
    data_source: str
    template: str
    matched_records: int


class UploadedJsonResponse(BaseModel):
    key: str
    filename: str
    collector: str
    game_name: str
    app_id: str | None = None


# ==================== 路由 ====================

@router.post("/reports/generate", response_model=ReportResponse)
async def generate_report(
    req: Annotated[GenerateReportRequest, Body(description="报告生成配置")]
):
    """生成分析报告并写入历史。"""
    from src.web.app import report_generator
    records = await _load_selected_records(req.record_keys) if req.record_keys else None

    report = await report_generator.generate(
        prompt=req.prompt,
        data_source=req.data_source,
        template=req.template,
        params=req.params,
        records=records,
        metadata={"selected_record_keys": req.record_keys} if req.record_keys else None,
    )
    return _to_report_response(report)


@router.get("/reports/templates")
async def get_report_templates():
    """列出可用于 Excel 报告的固定模板。"""
    return list_report_templates()


@router.post("/reports/upload-json", response_model=list[UploadedJsonResponse])
async def upload_report_json(
    files: Annotated[list[UploadFile], File(description="JSON data source files")],
):
    """上传 JSON 数据源并导入本地数据列表，返回可用于报告的 record keys。"""
    if not files:
        raise HTTPException(400, "No files uploaded")

    store = LocalStorage()
    await store.initialize()
    responses: list[UploadedJsonResponse] = []
    try:
        for index, file in enumerate(files, start=1):
            raw = await file.read()
            try:
                payload = json.loads(raw.decode("utf-8-sig"))
            except Exception as exc:
                raise HTTPException(400, f"Invalid JSON file {file.filename}: {exc}") from exc
            if not isinstance(payload, dict):
                raise HTTPException(400, f"JSON file {file.filename} must contain an object")

            data = payload.get("data") if _looks_like_download_wrapper(payload) else payload
            if not isinstance(data, dict):
                raise HTTPException(400, f"JSON file {file.filename} does not contain object data")

            collector = normalize_collector(_infer_collector(data, payload))
            game_name = _infer_game_name(data, payload) or "Uploaded JSON"
            app_id = _infer_app_id(data)
            key = f"upload:{datetime.now().strftime('%Y%m%d%H%M%S')}:{uuid.uuid4().hex[:8]}:{index}"
            await store.save(
                StorageRecord(
                    key=key,
                    data=data,
                    metadata={
                        "kind": "uploaded_json_source",
                        "collector": collector,
                        "target": game_name,
                        "app_id": app_id or "",
                        "uploaded_filename": file.filename or "",
                    },
                    source="upload",
                    tags=["uploaded_json", collector, game_name],
                )
            )
            responses.append(
                UploadedJsonResponse(
                    key=key,
                    filename=file.filename or key,
                    collector=collector,
                    game_name=game_name,
                    app_id=app_id,
                )
            )
    finally:
        await store.close()

    return responses


@router.get("/reports", response_model=list[ReportSummaryResponse])
async def list_reports(
    limit: Annotated[int, Query(description="返回数量限制")] = 20
):
    """获取历史报告列表。"""
    from src.web.app import report_generator

    reports = await report_generator.list_reports(limit=limit)
    return [_to_summary_response(report) for report in reports]


@router.get("/reports/group-records")
async def list_group_records_for_report(
    group_id: Annotated[str, Query(description="Data group id")],
    source: Annotated[str | None, Query(description="Optional data source label")] = None,
    limit: Annotated[int, Query(description="Maximum source records to scan")] = 1000,
):
    from src.web.routes.data import _load_source_records, _record_summary

    records = []
    for record in await _load_source_records(limit=limit):
        summary = _record_summary(record)
        if not summary or summary.group_id != group_id:
            continue
        if source and summary.data_source != source:
            continue
        records.append(summary.model_dump())
    records.sort(key=lambda item: item.get("stored_at") or "", reverse=True)
    return records


@router.get("/reports/{report_id}", response_model=ReportResponse)
async def get_report(
    report_id: Annotated[str, Path(description="报告 ID")]
):
    """获取单个历史报告。"""
    from src.web.app import report_generator

    report = await report_generator.get_report(report_id)
    if report is None:
        raise HTTPException(404, f"报告不存在: {report_id}")
    return _to_report_response(report)


@router.patch("/reports/{report_id}", response_model=ReportResponse)
async def update_report(
    report_id: Annotated[str, Path(description="Report ID")],
    req: Annotated[UpdateReportRequest, Body(description="Editable report fields")],
):
    from src.web.app import report_generator

    metadata = {"notes": req.notes} if req.notes is not None else None
    report = await report_generator.update_report(
        report_id,
        title=req.title,
        prompt=req.prompt,
        data_source=req.data_source,
        template=req.template,
        metadata=metadata,
    )
    if report is None:
        raise HTTPException(404, f"Report not found: {report_id}")
    return _to_report_response(report)


@router.delete("/reports/{report_id}")
async def delete_report(
    report_id: Annotated[str, Path(description="Report ID")],
):
    from src.web.app import report_generator

    if not await report_generator.delete_report(report_id):
        raise HTTPException(404, f"Report not found: {report_id}")
    return {"message": f"Report deleted: {report_id}"}


@router.post("/reports/generate-excel", response_model=ReportResponse)
async def generate_excel_report(
    req: Annotated[GenerateReportRequest, Body(description="Excel 报告生成配置")]
):
    """生成 Excel 格式的分析报告。"""
    from src.web.app import report_generator
    records = await _load_selected_records(req.record_keys) if req.record_keys else None

    report = await report_generator.generate_excel(
        prompt=req.prompt,
        data_source=req.data_source,
        template=req.template,
        params=req.params,
        records=records,
        metadata={"selected_record_keys": req.record_keys} if req.record_keys else None,
    )
    return _to_report_response(report)


@router.get("/reports/{report_id}/download")
async def download_report(
    report_id: Annotated[str, Path(description="报告 ID")]
):
    """下载报告的 Excel 文件。"""
    from src.web.app import report_generator

    report = await report_generator.get_report(report_id)
    if report is None:
        raise HTTPException(404, f"报告不存在: {report_id}")

    excel_path = report.excel_path if hasattr(report, "excel_path") else None
    if not excel_path:
        # 尝试从 metadata 中获取
        excel_path = report.metadata.get("excel_path") if isinstance(report.metadata, dict) else None

    if not excel_path or not FilePath(excel_path).exists():
        raise HTTPException(404, f"该报告没有对应的 Excel 文件: {report_id}")

    filename = f"report_{report_id}.xlsx"
    return FileResponse(
        path=excel_path,
        filename=filename,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )


def _to_report_response(report: GeneratedReport) -> ReportResponse:
    return ReportResponse(
        id=report.id,
        title=report.title,
        content=report.content,
        generated_at=report.generated_at.isoformat(),
        prompt=report.prompt,
        data_source=report.data_source,
        template=report.template,
        matched_records=report.matched_records,
        metadata=report.metadata,
    )


def _to_summary_response(report: ReportSummary) -> ReportSummaryResponse:
    return ReportSummaryResponse(
        id=report.id,
        title=report.title,
        generated_at=report.generated_at.isoformat(),
        prompt=report.prompt,
        data_source=report.data_source,
        template=report.template,
        matched_records=report.matched_records,
    )


async def _load_selected_records(record_keys: list[str]):
    store = LocalStorage()
    await store.initialize()
    try:
        records = []
        for key in record_keys:
            record = await store.load(key)
            if record is None:
                raise HTTPException(404, f"原始数据记录不存在: {key}")
            records.append(record)
        return records
    finally:
        await store.close()


def _looks_like_download_wrapper(payload: dict[str, Any]) -> bool:
    return "data" in payload and any(key in payload for key in ("key", "metadata", "stored_at", "source"))


def _infer_collector(data: dict[str, Any], payload: dict[str, Any]) -> str:
    metadata = payload.get("metadata", {}) if isinstance(payload.get("metadata"), dict) else {}
    source_meta = data.get("source_meta", {}) if isinstance(data.get("source_meta"), dict) else {}
    content = data.get("content", {}) if isinstance(data.get("content"), dict) else {}

    for value in (
        data.get("collector"),
        content.get("collector"),
        source_meta.get("collector"),
        metadata.get("collector"),
    ):
        if value:
            return str(value)

    if "discussions" in data:
        return "steam_discussions"
    if "steamdb" in data or "steam_api" in data:
        return "steam"
    if "trend_history" in data:
        return "gtrends"
    if "events" in data or "event_history" in data:
        return "events"
    if "monitor_metrics" in data or "metrics" in data:
        return "monitor"
    if "reviews_summary" in data or "availability" in data or "game" in data:
        return "taptap"
    return "unknown"


def _infer_game_name(data: dict[str, Any], payload: dict[str, Any]) -> str:
    metadata = payload.get("metadata", {}) if isinstance(payload.get("metadata"), dict) else {}
    snapshot = data.get("snapshot", {}) if isinstance(data.get("snapshot"), dict) else {}
    content = data.get("content", {}) if isinstance(data.get("content"), dict) else {}
    content_snapshot = content.get("snapshot", {}) if isinstance(content.get("snapshot"), dict) else {}
    game = data.get("game", {}) if isinstance(data.get("game"), dict) else {}

    for value in (
        data.get("game_name"),
        snapshot.get("name"),
        content.get("game_name"),
        content_snapshot.get("name"),
        game.get("title"),
        data.get("keyword"),
        metadata.get("target"),
    ):
        if value:
            return str(value)
    return ""


def _infer_app_id(data: dict[str, Any]) -> str | None:
    snapshot = data.get("snapshot", {}) if isinstance(data.get("snapshot"), dict) else {}
    source_meta = data.get("source_meta", {}) if isinstance(data.get("source_meta"), dict) else {}
    content = data.get("content", {}) if isinstance(data.get("content"), dict) else {}
    game = data.get("game", {}) if isinstance(data.get("game"), dict) else {}

    for value in (
        data.get("app_id"),
        snapshot.get("app_id"),
        source_meta.get("app_id"),
        content.get("app_id"),
        game.get("app_id"),
        game.get("id"),
    ):
        if value not in (None, ""):
            return str(value)
    return None
