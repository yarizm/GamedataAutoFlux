"""报告预检服务：数据选择、加载与模板覆盖度检查。

从 Web 路由下沉的业务逻辑，供 `/api/reports/precheck`、报告生成路径与
Agent 预检工具共用。Web 层只做 HTTP 语义包装（异常 → HTTPException、
响应模型）；本模块面向领域，不依赖 FastAPI。
"""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field

from src.core.exceptions import DomainError, ValidationError
from src.reporting.data_extractor import extract_from_records
from src.reporting.report_templates import validate_template_sources
from src.services._utils import (
    coerce_record_limit,
    filter_records_by_data_source,
    filter_source_data_records,
    is_report_history_record,
    source_label,
)
from src.storage.base import StorageRecord
from src.storage.factory import get_storage


class RecordKeyNotFoundError(DomainError):
    """指定的原始数据记录不存在。"""


class ReportHistoryOnlySelectionError(ValidationError):
    """选中的 key 全部是报告历史记录，不是可用的源数据。"""


class ReportPrecheck(BaseModel):
    """报告预检结果（Web 响应与 Agent payload 共用同一结构）。"""

    status: str
    message: str
    selected_records: int
    usable_records: int
    template: str
    known_template: bool = False
    required_collectors: list[str] = Field(default_factory=list)
    available_collectors: list[str] = Field(default_factory=list)
    missing_collectors: list[str] = Field(default_factory=list)
    source_counts: dict[str, int] = Field(default_factory=dict)
    recommendations: list[str] = Field(default_factory=list)


async def load_selected_records(record_keys: list[str]) -> list[StorageRecord]:
    """按 key 精确加载源数据记录（报告历史记录被剔除）。"""
    store = get_storage()
    await store.initialize()
    try:
        records = []
        for key in record_keys:
            record = await store.load(key)
            if record is not None and is_report_history_record(record):
                continue
            if record is None:
                raise RecordKeyNotFoundError(f"原始数据记录不存在: {key}")
            records.append(record)
        if record_keys and not records:
            raise ReportHistoryOnlySelectionError(
                "Selected keys only contain generated report history. "
                "Select source data records instead."
            )
        return records
    finally:
        await store.close()


def selected_record_metadata(
    requested_keys: list[str],
    records: list[StorageRecord] | None,
) -> dict[str, Any] | None:
    """生成路径的报告 metadata：选中 key 与被剔除（报告历史）key。"""
    if not requested_keys:
        return None
    selected_keys = [record.key for record in records or []]
    selected_set = set(selected_keys)
    metadata: dict[str, Any] = {"selected_record_keys": selected_keys}
    excluded_keys = [key for key in requested_keys if key not in selected_set]
    if excluded_keys:
        metadata["excluded_report_record_keys"] = excluded_keys
    return metadata


async def load_report_precheck_records(
    *,
    record_keys: list[str] | None = None,
    data_source: str = "",
    params: dict[str, Any] | None = None,
) -> list[StorageRecord]:
    """预检数据加载：显式 key 优先，否则按数据源扫描。"""
    if record_keys:
        return await load_selected_records(record_keys)

    params = params or {}
    limit = coerce_record_limit(params.get("limit"), default=100)
    store = get_storage()
    await store.initialize()
    try:
        if data_source:
            result = await store.query(f"source:{data_source}", limit=limit)
            source_records = filter_source_data_records(result.records)
            if source_records:
                return source_records

            scan_limit = coerce_record_limit(limit * 20, default=500, maximum=5000)
            candidates_by_key: dict[str, StorageRecord] = {}
            for query in (data_source, "key:"):
                result = await store.query(query, limit=scan_limit)
                for record in result.records:
                    candidates_by_key[record.key] = record
            return filter_records_by_data_source(
                list(candidates_by_key.values()),
                data_source,
            )[:limit]

        scan_limit = coerce_record_limit(limit * 20, default=500, maximum=5000)
        result = await store.query("key:", limit=scan_limit)
        return filter_source_data_records(result.records)[:limit]
    finally:
        await store.close()


def build_report_precheck(template: str, records: list[StorageRecord]) -> ReportPrecheck:
    """模板覆盖度检查：可用记录 → 提取 → 校验必需 collector 是否齐备。"""
    usable_records = [record for record in records if isinstance(record.data, dict)]
    if not usable_records:
        validation = validate_template_sources(template, {})
        missing = list(validation.get("missing_collectors") or [])
        return ReportPrecheck(
            status="empty",
            message="No usable JSON records found for this report.",
            selected_records=len(records),
            usable_records=0,
            template=str(validation.get("template") or template),
            known_template=bool(validation.get("known_template", False)),
            required_collectors=list(validation.get("required_collectors") or []),
            missing_collectors=missing,
            recommendations=[
                "Select records from Data Browser or upload JSON files before generating.",
                *[
                    f"Add {source_label(collector)} data before generating for better report coverage."
                    for collector in missing
                ],
            ],
        )

    extracted = extract_from_records(
        [record.data for record in usable_records],
        record_keys=[record.key for record in usable_records],
        metadata_list=[record.metadata for record in usable_records],
    )
    validation = validate_template_sources(template, extracted.source_coverage)
    missing = list(validation.get("missing_collectors") or [])
    status = "complete" if not missing else "partial"
    recommendations = [
        f"Add {source_label(collector)} data before generating for better report coverage."
        for collector in missing
    ]
    message = (
        "Report data coverage is complete."
        if status == "complete"
        else "Report can be generated, but some expected data sources are missing."
    )
    return ReportPrecheck(
        status=status,
        message=message,
        selected_records=len(records),
        usable_records=len(usable_records),
        template=str(validation.get("template") or template),
        known_template=bool(validation.get("known_template", False)),
        required_collectors=list(validation.get("required_collectors") or []),
        available_collectors=list(validation.get("available_collectors") or []),
        missing_collectors=missing,
        source_counts=dict(validation.get("source_counts") or {}),
        recommendations=recommendations,
    )
