"""报告生成与预检服务 — 供路由和 Agent 工具统一调用"""

from __future__ import annotations

from typing import Any

from src.reporting.data_extractor import extract_from_records
from src.reporting.report_templates import validate_template_sources, normalize_collector
from src.services._utils import source_label


class ReportService:
    """封装报告生成、预检、模板查询等可复用逻辑"""

    def __init__(self, report_generator=None, data_service=None):
        from src.services.data_service import DataService

        self._data = data_service or DataService()
        # 延迟导入避免循环依赖
        self._generator = report_generator

    def _get_generator(self):
        if self._generator is None:
            from src.web.app import report_generator
            self._generator = report_generator
        return self._generator

    # ---- 预检 ----

    async def precheck(
        self,
        template: str,
        record_keys: list[str] | None = None,
        data_source: str = "",
    ) -> dict[str, Any]:
        """报告生成前数据完整性预检"""
        if record_keys:
            records = await self._data.load_records_by_keys(record_keys)
        elif data_source:
            result = await self._data.query_records(f"source:{data_source}", limit=500)
            records = list(result.records)
        else:
            records = await self._data.load_source_records(limit=500)

        usable = [r for r in records if isinstance(r.data, dict)]
        if not usable:
            validation = validate_template_sources(template, {})
            missing = list(validation.get("missing_collectors") or [])
            return {
                "status": "empty",
                "message": "No usable JSON records found for this report.",
                "selected_records": len(records),
                "usable_records": 0,
                "template": str(validation.get("template") or template),
                "known_template": bool(validation.get("known_template", False)),
                "required_collectors": list(validation.get("required_collectors") or []),
                "available_collectors": [],
                "missing_collectors": missing,
                "source_counts": {},
                "recommendations": [
                    f"Add {source_label(c)} data before generating for better report coverage."
                    for c in missing
                ] if missing else [
                    "Select records from Data Browser or upload JSON files before generating."
                ],
            }

        extracted = extract_from_records([r.data for r in usable])
        validation = validate_template_sources(template, extracted.source_coverage)
        missing = list(validation.get("missing_collectors") or [])
        status = "complete" if not missing else "partial"
        recommendations = (
            [f"Add {source_label(c)} data before generating for better report coverage."
             for c in missing]
            if missing
            else []
        )
        message = (
            "Report data coverage is complete."
            if status == "complete"
            else f"Missing {len(missing)} data source(s), report may be incomplete."
        )

        return {
            "status": status,
            "message": message,
            "selected_records": len(records),
            "usable_records": len(usable),
            "template": str(validation.get("template") or template),
            "known_template": bool(validation.get("known_template", False)),
            "required_collectors": list(validation.get("required_collectors") or []),
            "available_collectors": list(validation.get("available_collectors") or []),
            "missing_collectors": missing,
            "source_counts": dict(validation.get("source_counts") or {}),
            "recommendations": recommendations,
        }

    # ---- 报告生成 ----

    async def generate(self, prompt: str, data_source: str = "",
                       template: str = "general_game",
                       record_keys: list[str] | None = None):
        """生成文本报告"""
        records = await self._data.load_records_by_keys(record_keys or [])
        gen = self._get_generator()
        return gen.generate(prompt=prompt, data_source=data_source,
                           template=template, records=records)

    async def generate_excel(self, prompt: str, data_source: str = "",
                             template: str = "general_game",
                             record_keys: list[str] | None = None):
        """生成 Excel 报告"""
        records = await self._data.load_records_by_keys(record_keys or [])
        gen = self._get_generator()
        return gen.generate_excel(prompt=prompt, data_source=data_source,
                                  template=template, records=records)

    # ---- 报告管理 ----

    def list_reports(self, limit: int = 50):
        return self._get_generator().list_reports(limit=limit)

    def get_report(self, report_id: str):
        return self._get_generator().get_report(report_id)

    def update_report(self, report_id: str, notes: str | None = None):
        return self._get_generator().update_report(report_id, notes=notes)

    def delete_report(self, report_id: str):
        return self._get_generator().delete_report(report_id)

    # ---- JSON 上传与数据发现 ----

    @staticmethod
    def looks_like_download_wrapper(payload: dict[str, Any]) -> bool:
        return "data" in payload and any(
            key in payload for key in ("key", "metadata", "stored_at", "source")
        )

    @staticmethod
    def infer_collector(data: dict[str, Any], payload: dict[str, Any]) -> str:
        metadata = payload.get("metadata", {}) if isinstance(payload.get("metadata"), dict) else {}
        source_meta = data.get("source_meta", {}) if isinstance(data.get("source_meta"), dict) else {}
        content = data.get("content", {}) if isinstance(data.get("content"), dict) else {}
        for value in (
            data.get("collector"),
            content.get("collector"),
            source_meta.get("collector"),
            metadata.get("collector"),
        ):
            if isinstance(value, str) and value.strip():
                return normalize_collector(value.strip())
        if "discussions" in data:
            return "steam_discussions"
        if "steamdb" in data or "news" in data:
            return "steam"
        if "reviews_summary" in data or "availability" in data:
            return "taptap"
        if "trend_history" in data:
            return "gtrends"
        if "events" in data or "event_history" in data:
            return "events"
        if "monitor_metrics" in data or "metrics" in data:
            return "monitor"
        return "unknown"

    @staticmethod
    def infer_game_name(data: dict[str, Any], payload: dict[str, Any]) -> str:
        metadata = payload.get("metadata", {}) if isinstance(payload.get("metadata"), dict) else {}
        source_meta = data.get("source_meta", {}) if isinstance(data.get("source_meta"), dict) else {}
        content = data.get("content", {}) if isinstance(data.get("content"), dict) else {}
        for value in (
            source_meta.get("game_name"),
            metadata.get("game_name"),
            content.get("game_name"),
            payload.get("game_name"),
            data.get("game_name"),
        ):
            if isinstance(value, str) and value.strip():
                return value.strip()
        return "Unknown"

    @staticmethod
    def infer_app_id(data: dict[str, Any]) -> str:
        metadata = data.get("metadata", {}) if isinstance(data.get("metadata"), dict) else {}
        source_meta = data.get("source_meta", {}) if isinstance(data.get("source_meta"), dict) else {}
        content = data.get("content", {}) if isinstance(data.get("content"), dict) else {}
        for value in (
            source_meta.get("app_id"),
            metadata.get("app_id"),
            content.get("app_id"),
            data.get("app_id"),
        ):
            if value:
                return str(value)
        return ""
