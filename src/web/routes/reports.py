"""报告生成 API 路由。"""

from __future__ import annotations

from typing import Annotated, Any
from pathlib import Path as FilePath

from fastapi import APIRouter, HTTPException, Query, Path, Body
from fastapi.responses import FileResponse
from pydantic import BaseModel, Field

from src.reporting.generator import GeneratedReport, ReportSummary

router = APIRouter(tags=["reports"])


# ==================== 请求/响应模型 ====================

class GenerateReportRequest(BaseModel):
    """生成报告请求"""
    prompt: str = Field(..., description="提示词")
    data_source: str = Field(default="", description="数据源标识")
    template: str = Field(default="default", description="报告模板")
    params: dict[str, Any] = Field(default_factory=dict, description="额外参数")


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


# ==================== 路由 ====================

@router.post("/reports/generate", response_model=ReportResponse)
async def generate_report(
    req: Annotated[GenerateReportRequest, Body(description="报告生成配置")]
):
    """生成分析报告并写入历史。"""
    from src.web.app import report_generator

    report = await report_generator.generate(
        prompt=req.prompt,
        data_source=req.data_source,
        template=req.template,
        params=req.params,
    )
    return _to_report_response(report)


@router.get("/reports", response_model=list[ReportSummaryResponse])
async def list_reports(
    limit: Annotated[int, Query(description="返回数量限制")] = 20
):
    """获取历史报告列表。"""
    from src.web.app import report_generator

    reports = await report_generator.list_reports(limit=limit)
    return [_to_summary_response(report) for report in reports]


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


@router.post("/reports/generate-excel", response_model=ReportResponse)
async def generate_excel_report(
    req: Annotated[GenerateReportRequest, Body(description="Excel 报告生成配置")]
):
    """生成 Excel 格式的分析报告。"""
    from src.web.app import report_generator

    report = await report_generator.generate_excel(
        prompt=req.prompt,
        data_source=req.data_source,
        template=req.template,
        params=req.params,
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

    if not excel_path or not Path(excel_path).exists():
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
