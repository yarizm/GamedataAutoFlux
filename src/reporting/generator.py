"""
Report generator.

It loads stored source records, renders Markdown or Excel reports, and saves
report history for the API and WebUI.
"""

from __future__ import annotations

import asyncio
import json
import re
import time
import uuid
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import httpx
from loguru import logger
from pydantic import BaseModel, Field

from src.core.config import get as get_config
from src.core.config import get_root_dir
from src.core.sensitive import redact_sensitive, redact_sensitive_text
from src.storage.base import StorageRecord
from src.storage.factory import get_storage
from src.reporting.data_extractor import ExtractedData, extract_from_records
from src.reporting.excel_exporter import export_to_excel
from src.reporting.report_templates import get_report_template, validate_template_sources
from src.services._utils import (
    build_record_summary,
    compute_record_completeness,
    coerce_record_limit,
    derive_collection_target_context,
    extract_record_identity,
    filter_records_by_data_source,
    filter_source_data_records,
    is_report_history_record,
)


class ReportSummary(BaseModel):
    """Report summary used by list views."""

    id: str
    title: str
    prompt: str
    data_source: str
    template: str
    generated_at: datetime
    matched_records: int = 0
    metadata: dict[str, Any] = Field(default_factory=dict)


class GeneratedReport(ReportSummary):
    """Full generated report."""

    content: str
    excel_path: str | None = Field(default=None, description="Excel report file path")


@dataclass(frozen=True)
class _PreparedReportData:
    records: list[StorageRecord]
    metadata: dict[str, Any] | None
    usable_records: list[StorageRecord]
    extracted: ExtractedData
    template_validation: dict[str, Any]


@dataclass(frozen=True)
class _LlmRequestConfig:
    provider: str
    provider_label: str
    api_key: str
    base_url: str
    model: str
    temperature: float
    max_tokens: int
    timeout: float
    retry_count: int
    retry_delay: float


def get_reports_dir() -> Path:
    """Return the configured report output directory as an absolute path."""
    configured = str(get_config("storage.reports_dir", "data/excel_reports") or "").strip()
    report_dir = get_root_dir() / "data" / "excel_reports" if not configured else Path(configured)
    if not report_dir.is_absolute():
        report_dir = get_root_dir() / report_dir
    report_dir.mkdir(parents=True, exist_ok=True)
    return report_dir.resolve()


class ReportGenerator:
    """Report generation and history management."""

    def __init__(
        self,
        source_storage_config: dict[str, Any] | None = None,
        report_storage_config: dict[str, Any] | None = None,
    ):
        self._source_storage_config = source_storage_config or {}
        self._report_storage_config = {
            "db_name": "reports.db",
            "json_dir": "reports",
            **(report_storage_config or {}),
        }

        self._llm_provider = get_config("llm.provider", "stub")

    async def generate(
        self,
        prompt: str,
        data_source: str = "",
        template: str = "default",
        provider: str = "",
        params: dict[str, Any] | None = None,
        records: list[StorageRecord] | None = None,
        metadata: dict[str, Any] | None = None,
        custom_prompt: str = "",
    ) -> GeneratedReport:
        self._select_llm_provider(provider)
        params = params or {}
        progress_id = str(params.get("progress_id") or "")
        logger.info(
            "[Report] generate start template={} data_source={} records={} progress_id={}",
            template,
            data_source or "",
            len(records) if records is not None else "auto",
            progress_id or "-",
        )
        await _emit_report_progress(progress_id, "started", 0.05, "Report generation started")
        prepared = await self._prepare_report_data(
            prompt=prompt,
            data_source=data_source,
            template=template,
            params=params,
            records=records,
            metadata=metadata,
            progress_id=progress_id,
        )

        await _emit_report_progress(progress_id, "llm", 0.42, "Calling LLM for report analysis")
        content = await self._render_report(
            self._build_template_prompt(
                prompt,
                template,
                prepared.template_validation,
                custom_prompt=custom_prompt,
            ),
            data_source,
            template,
            prepared.records,
        )
        await _emit_report_progress(progress_id, "llm_done", 0.76, "LLM analysis completed")

        report = GeneratedReport(
            id=uuid.uuid4().hex[:12],
            title=self._build_title(prompt, data_source, template),
            prompt=prompt,
            data_source=data_source,
            template=template,
            generated_at=datetime.now(),
            matched_records=len(prepared.records),
            content=content,
            metadata=self._build_generated_report_metadata(
                prompt=prompt,
                data_source=data_source,
                template=template,
                prepared=prepared,
                extra=prepared.metadata,
                report_format="markdown",
            ),
        )

        await self._save_report(report)
        logger.info(
            "[Report] generate complete report_id={} matched_records={}",
            report.id,
            report.matched_records,
        )
        await _emit_report_progress(
            progress_id, "completed", 1.0, "Report generated", report_id=report.id
        )
        return report

    async def generate_excel(
        self,
        prompt: str,
        data_source: str = "",
        template: str = "default",
        provider: str = "",
        params: dict[str, Any] | None = None,
        records: list[StorageRecord] | None = None,
        metadata: dict[str, Any] | None = None,
        custom_prompt: str = "",
    ) -> GeneratedReport:
        """
        Generate an Excel report.

        Flow: load records, extract structured fields, optionally ask the LLM,
        then write the .xlsx file.
        """
        self._select_llm_provider(provider)
        params = params or {}
        progress_id = str(params.get("progress_id") or "")
        logger.info(
            "[Report] generate_excel start template={} data_source={} records={} progress_id={}",
            template,
            data_source or "",
            len(records) if records is not None else "auto",
            progress_id or "-",
        )
        await _emit_report_progress(progress_id, "started", 0.05, "Report generation started")
        prepared = await self._prepare_report_data(
            prompt=prompt,
            data_source=data_source,
            template=template,
            params=params,
            records=records,
            metadata=metadata,
            progress_id=progress_id,
            emit_extract_progress=True,
        )

        # Optional LLM narrative.
        llm_content = await self._render_optional_excel_llm_content(
            prompt=prompt,
            data_source=data_source,
            template=template,
            custom_prompt=custom_prompt,
            prepared=prepared,
            params=params,
            progress_id=progress_id,
        )

        # Write Excel output.
        await _emit_report_progress(progress_id, "exporting", 0.78, "Writing Excel report")
        report_id = uuid.uuid4().hex[:12]
        title = self._build_title(prompt, data_source, template)
        excel_path = self._build_excel_report_path(report_id)

        export_to_excel(
            data=prepared.extracted,
            output_path=excel_path,
            title=title,
            llm_content=llm_content,
            template_id=template,
            template_validation=prepared.template_validation,
        )

        report = GeneratedReport(
            id=report_id,
            title=title,
            prompt=prompt,
            data_source=data_source,
            template=template,
            generated_at=datetime.now(),
            matched_records=len(prepared.records),
            content=llm_content or "Report generated as an Excel file",
            excel_path=str(excel_path),
            metadata=self._build_generated_report_metadata(
                prompt=prompt,
                data_source=data_source,
                template=template,
                prepared=prepared,
                extra={
                    **(prepared.metadata or {}),
                    "sheets": self._build_excel_sheet_counts(prepared.extracted),
                },
                report_format="excel",
            ),
        )

        await self._save_report(report)
        logger.info(
            "[Report] generate_excel complete report_id={} matched_records={} excel_path={}",
            report.id,
            report.matched_records,
            report.excel_path or "",
        )
        await _emit_report_progress(
            progress_id, "completed", 1.0, "Report generated", report_id=report.id
        )
        return report

    async def list_reports(self, limit: int = 20) -> list[ReportSummary]:
        store = get_storage()
        await store.initialize()
        try:
            result = await store.query("key:report:", limit=limit)
            reports = [self._summary_from_record(record) for record in result.records]
            reports.sort(key=lambda item: item.generated_at, reverse=True)
            return reports
        finally:
            await store.close()

    async def get_report(self, report_id: str) -> GeneratedReport | None:
        store = get_storage()
        await store.initialize()
        try:
            record = await store.load(f"report:{report_id}")
            if not record or not isinstance(record.data, dict):
                return None
            return GeneratedReport.model_validate(record.data)
        finally:
            await store.close()

    async def update_report(
        self,
        report_id: str,
        *,
        title: str | None = None,
        prompt: str | None = None,
        data_source: str | None = None,
        template: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> GeneratedReport | None:
        report = await self.get_report(report_id)
        if report is None:
            return None
        update_data: dict[str, Any] = {}
        if title is not None:
            update_data["title"] = title
        if prompt is not None:
            update_data["prompt"] = prompt
        if data_source is not None:
            update_data["data_source"] = data_source
        if template is not None:
            update_data["template"] = template
        if metadata:
            update_data["metadata"] = {**report.metadata, **metadata}
        updated = report.model_copy(update=update_data)
        await self._save_report(updated)
        return updated

    async def delete_report(self, report_id: str) -> bool:
        report = await self.get_report(report_id)
        if report is None:
            return False
        excel_path = report.excel_path or report.metadata.get("excel_path")
        path = _deletable_report_file(excel_path)
        if path and path.exists():
            try:
                path.unlink()
            except Exception as exc:
                logger.warning(
                    "[Report] failed to delete Excel file {}: {}",
                    _safe_context_text(path),
                    _safe_context_text(exc),
                )
        store = get_storage()
        await store.initialize()
        try:
            await store.delete(f"report:{report_id}")
        finally:
            await store.close()
        return True

    async def _save_report(self, report: GeneratedReport) -> None:
        store = get_storage()
        await store.initialize()
        try:
            await store.save(
                StorageRecord(
                    key=f"report:{report.id}",
                    data=report.model_dump(mode="json"),
                    metadata={
                        "kind": "report",
                        "template": report.template,
                        "data_source": report.data_source,
                    },
                    source="reporting",
                    tags=["report", report.template],
                )
            )
        finally:
            await store.close()

    def _select_llm_provider(self, provider: str) -> None:
        self._llm_provider = provider or get_config("llm.provider", "stub")

    async def _prepare_report_data(
        self,
        *,
        prompt: str,
        data_source: str,
        template: str,
        params: dict[str, Any],
        records: list[StorageRecord] | None,
        metadata: dict[str, Any] | None,
        progress_id: str,
        emit_extract_progress: bool = False,
    ) -> _PreparedReportData:
        resolved_records, resolved_metadata = await self._resolve_report_records(
            prompt=prompt,
            data_source=data_source,
            params=params,
            records=records,
            metadata=metadata,
            progress_id=progress_id,
        )
        logger.info(
            "[Report] records loaded count={} template={}",
            len(resolved_records),
            template,
        )

        usable_records = [record for record in resolved_records if record.data is not None]
        if emit_extract_progress:
            await _emit_report_progress(
                progress_id,
                "extracting",
                0.22,
                f"Parsing {len(usable_records)} records",
            )
        extracted = self._extract_report_data(usable_records)
        template_validation = validate_template_sources(template, extracted.source_coverage)
        self._log_extracted_report_data(extracted)
        return _PreparedReportData(
            records=resolved_records,
            metadata=resolved_metadata,
            usable_records=usable_records,
            extracted=extracted,
            template_validation=template_validation,
        )

    async def _resolve_report_records(
        self,
        *,
        prompt: str,
        data_source: str,
        params: dict[str, Any],
        records: list[StorageRecord] | None,
        metadata: dict[str, Any] | None,
        progress_id: str,
    ) -> tuple[list[StorageRecord], dict[str, Any] | None]:
        if records is not None:
            return _prepare_explicit_source_records(records, metadata)

        await _emit_report_progress(progress_id, "loading_records", 0.12, "Loading report records")
        loaded_records = await self._load_source_records(
            prompt=prompt,
            data_source=data_source,
            params=params,
        )
        return loaded_records, metadata

    def _extract_report_data(self, usable_records: list[StorageRecord]) -> ExtractedData:
        return extract_from_records(
            [record.data for record in usable_records],
            record_keys=[record.key for record in usable_records],
            metadata_list=[record.metadata for record in usable_records],
        )

    def _log_extracted_report_data(self, extracted: ExtractedData) -> None:
        logger.info(
            "[Report] extracted coverage={} overview={} steam_peaks={} google={} monitor={} events={} discussions={}",
            extracted.source_coverage,
            len(extracted.overview),
            len(extracted.steam_player_peaks),
            len(extracted.google_trends),
            len(extracted.monitor_metrics),
            len(extracted.events),
            len(extracted.community_discussions),
        )

    def _build_generated_report_metadata(
        self,
        *,
        prompt: str,
        data_source: str,
        template: str,
        prepared: _PreparedReportData,
        extra: dict[str, Any] | None,
        report_format: str,
    ) -> dict[str, Any]:
        return _build_report_metadata(
            provider=self._llm_provider,
            template=template,
            source_query=data_source or prompt,
            records=prepared.records,
            usable_records=prepared.usable_records,
            source_coverage=prepared.extracted.source_coverage,
            template_validation=prepared.template_validation,
            target_context=derive_collection_target_context(
                prepared.records,
                prompt=prompt,
                data_source=data_source,
            ),
            extra=extra,
            report_format=report_format,
        )

    async def _render_optional_excel_llm_content(
        self,
        *,
        prompt: str,
        data_source: str,
        template: str,
        custom_prompt: str,
        prepared: _PreparedReportData,
        params: dict[str, Any],
        progress_id: str,
    ) -> str | None:
        if not params.get("include_llm_analysis", True):
            return None

        try:
            await _emit_report_progress(progress_id, "llm", 0.42, "Calling LLM for report analysis")
            llm_content = await self._render_report(
                self._build_template_prompt(
                    prompt,
                    template,
                    prepared.template_validation,
                    custom_prompt=custom_prompt,
                ),
                data_source,
                template,
                prepared.records,
            )
            await _emit_report_progress(progress_id, "llm_done", 0.68, "LLM analysis completed")
            return llm_content
        except Exception as exc:
            safe_error = _safe_context_text(exc)
            logger.warning(
                "[Report] LLM analysis failed, fallback to template report: {}",
                safe_error,
            )
            await _emit_report_progress(
                progress_id,
                "llm_failed",
                0.62,
                f"LLM failed; falling back to template report: {safe_error}",
            )
            return None

    def _build_excel_report_path(self, report_id: str) -> Path:
        return get_reports_dir() / f"report_{report_id}.xlsx"

    def _build_excel_sheet_counts(self, extracted: ExtractedData) -> dict[str, int]:
        return {
            "overview": len(extracted.overview),
            "reviews": len(extracted.reviews),
            "trends": len(extracted.trends),
            "related_queries": len(extracted.related_queries),
            "steam_player_peaks": len(extracted.steam_player_peaks),
            "steam_monthly_peaks": len(extracted.steam_monthly_peaks),
            "google_trends": len(extracted.google_trends),
            "monitor_metrics": len(extracted.monitor_metrics),
            "events": len(extracted.events),
            "community_discussions": len(extracted.community_discussions),
            "raw_appendices": len(extracted.raw_sources),
        }

    async def _load_source_records(
        self,
        prompt: str,
        data_source: str,
        params: dict[str, Any],
    ) -> list[StorageRecord]:
        limit = coerce_record_limit(params.get("limit"), default=5)

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

            keywords = self._extract_keywords(prompt)
            for keyword in keywords:
                result = await store.query(keyword, limit=limit)
                source_records = filter_source_data_records(result.records)
                if source_records:
                    return source_records

            scan_limit = coerce_record_limit(limit * 20, default=500, maximum=5000)
            return filter_source_data_records(
                (await store.query("key:", limit=scan_limit)).records
            )[:limit]
        finally:
            await store.close()

    def _extract_keywords(self, prompt: str) -> list[str]:
        cleaned = re.sub(r"[^\w\u4e00-\u9fff]+", " ", prompt.lower())
        tokens = [token for token in cleaned.split() if len(token) >= 2]
        seen: set[str] = set()
        ordered_tokens: list[str] = []
        for token in tokens:
            if token not in seen:
                seen.add(token)
                ordered_tokens.append(token)
        return ordered_tokens[:5]

    def _build_title(self, prompt: str, data_source: str, template: str) -> str:
        prefix = data_source or "Combined data"
        subject = prompt.strip()[:24] or "report"
        return f"{prefix} {template} report: {subject}"

    def _build_template_prompt(
        self,
        prompt: str,
        template: str,
        template_validation: dict[str, Any],
        custom_prompt: str = "",
    ) -> str:
        if template == "auto":
            available = template_validation.get("available_collectors") or []
            avail_text = ", ".join(available) if available else "none"
            instruction = (
                f"{prompt}\n\n"
                f"Report template: Auto Exploration\n"
                f"Available data sources: {avail_text}\n"
                "Analyze strictly from the provided JSON records. Dynamically structure the report "
                "based ONLY on the available data sources. Create appropriate chapters for the data found, "
                "and ignore missing sources without mentioning them.\n"
            )
            if custom_prompt:
                instruction += f"\nAdditional constraints/focus: {custom_prompt}\n"
            return instruction

        template_def = get_report_template(template)
        if template_def is None:
            return f"{prompt}\n\n{custom_prompt}" if custom_prompt else prompt

        missing = template_validation.get("missing_collectors") or []
        missing_text = ", ".join(missing) if missing else "none"
        base_prompt = (
            f"{prompt}\n\n"
            f"Report template: {template_def.name}\n"
            f"Template requirements: {template_def.prompt_instruction}\n"
            f"Missing data sources: {missing_text}\n"
            "Analyze strictly from the provided JSON records. If a source is missing, "
            "state the gap instead of inventing data."
        )
        if custom_prompt:
            base_prompt += f"\n\nAdditional constraints/focus: {custom_prompt}"
        return base_prompt

    async def _render_report(
        self,
        prompt: str,
        data_source: str,
        template: str,
        records: list[StorageRecord],
    ) -> str:
        provider_config = get_config(f"llm.{self._llm_provider}", {})
        if isinstance(provider_config, dict) and provider_config.get("model"):
            try:
                return await self._render_report_with_openai_compatible(
                    provider=self._llm_provider,
                    prompt=prompt,
                    data_source=data_source,
                    template=template,
                    records=records,
                )
            except Exception as exc:
                fallback_enabled = bool(
                    get_config(f"llm.{self._llm_provider}.fallback_to_stub", True)
                )
                if not fallback_enabled:
                    raise
                provider_label = self.provider_label(self._llm_provider)
                safe_error = _safe_context_text(exc)
                return self._render_stub_report(
                    prompt=prompt,
                    data_source=data_source,
                    template=template,
                    records=records,
                    extra_note=(
                        f"{provider_label} request failed; fell back to template report: "
                        f"{safe_error}"
                    ),
                )
        return self._render_stub_report(prompt, data_source, template, records)

    def _render_stub_report(
        self,
        prompt: str,
        data_source: str,
        template: str,
        records: list[StorageRecord],
        extra_note: str | None = None,
    ) -> str:
        lines = [
            f"# {self._build_title(prompt, data_source, template)}",
            "",
            "## Task Input",
            f"- Prompt: {prompt}",
            f"- Data source filter: {data_source or 'not specified'}",
            f"- Template: {template}",
            f"- Matched records: {len(records)}",
            "",
        ]

        if extra_note:
            lines.extend(["## Generation Note", extra_note, ""])

        if not records:
            lines.extend(
                [
                    "## Data Conclusion",
                    "No historical records were found for this report.",
                    "Run a collection task first, then generate the report again.",
                ]
            )
            return "\n".join(lines)

        lines.extend(
            [
                "## Data Overview",
                *self._render_record_overview(records),
                "",
                "## Initial Observations",
                *self._render_observations(records),
            ]
        )

        if template == "brief":
            lines.extend(
                [
                    "",
                    "## Suggestions",
                    "Review the latest 1-2 records first and decide whether more collection is needed.",
                ]
            )
        else:
            lines.extend(
                [
                    "",
                    "## Suggested Actions",
                    "1. Add scheduled collection for high-volatility or high-interest targets.",
                    "2. Put targets with clear review or announcement changes into the review pool.",
                    "3. Add more time slices and discussion text for deeper analysis.",
                ]
            )

        return "\n".join(lines)

    async def _render_report_with_openai_compatible(
        self,
        provider: str,
        prompt: str,
        data_source: str,
        template: str,
        records: list[StorageRecord],
    ) -> str:
        config = self._load_openai_compatible_request_config(provider)
        payload, headers, request_stats = self._build_openai_compatible_request(
            config=config,
            prompt=prompt,
            data_source=data_source,
            template=template,
            records=records,
        )
        data = await self._request_openai_compatible_completion(
            config=config,
            payload=payload,
            headers=headers,
            request_stats=request_stats,
            record_count=len(records),
            template=template,
        )
        return self._extract_openai_compatible_content(data, config.provider_label)

    def _load_openai_compatible_request_config(self, provider: str) -> _LlmRequestConfig:
        provider_label = self.provider_label(provider)
        api_key = get_config(f"llm.{provider}.api_key", "")
        if provider == "local" and not api_key:
            api_key = "local"
        if not api_key or api_key.startswith("${"):
            raise ValueError(f"llm.{provider}.api_key is not configured")

        default_base_urls = {
            "qwen": "https://dashscope.aliyuncs.com/compatible-mode/v1",
            "deepseek": "https://api.deepseek.com",
            "openai": "https://api.openai.com/v1",
            "local": "http://localhost:11434/v1",
        }
        default_models = {
            "qwen": "qwen-max",
            "deepseek": "deepseek-chat",
            "openai": "gpt-4o-mini",
            "local": "qwen2.5",
        }
        base_url = get_config(
            f"llm.{provider}.base_url",
            default_base_urls.get(provider, ""),
        ).rstrip("/")
        if not base_url:
            raise ValueError(f"llm.{provider}.base_url is not configured")
        model = get_config(f"llm.{provider}.model", default_models.get(provider, ""))
        if not model:
            raise ValueError(f"llm.{provider}.model is not configured")
        return _LlmRequestConfig(
            provider=provider,
            provider_label=provider_label,
            api_key=api_key,
            base_url=base_url,
            model=model,
            temperature=float(get_config(f"llm.{provider}.temperature", 0.3)),
            max_tokens=int(get_config(f"llm.{provider}.max_tokens", 2000)),
            timeout=float(get_config(f"llm.{provider}.timeout", 180 if provider == "qwen" else 90)),
            retry_count=int(get_config(f"llm.{provider}.retry_count", 2)),
            retry_delay=float(get_config(f"llm.{provider}.retry_delay", 2.0)),
        )

    def _build_openai_compatible_request(
        self,
        *,
        config: _LlmRequestConfig,
        prompt: str,
        data_source: str,
        template: str,
        records: list[StorageRecord],
    ) -> tuple[dict[str, Any], dict[str, str], str]:
        context = self._build_record_context(records, provider=config.provider)
        system_prompt = self._build_report_system_prompt(template=template)
        user_prompt = (
            f"User request: {prompt}\n"
            f"Data source filter: {data_source or 'not specified'}\n\n"
            "Below are the matched data records. Generate a Chinese Markdown report "
            "based only on these records. If information is insufficient, state the "
            "gap explicitly and do not invent data.\n\n"
            f"{context}"
        )
        payload = {
            "model": config.model,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            "temperature": config.temperature,
            "max_tokens": config.max_tokens,
            "stream": False,
        }
        headers = {
            "Authorization": f"Bearer {config.api_key}",
            "Content-Type": "application/json",
            "Connection": "close",
        }
        request_stats = f"context_chars={len(context)}, user_prompt_chars={len(user_prompt)}"
        return payload, headers, request_stats

    async def _request_openai_compatible_completion(
        self,
        *,
        config: _LlmRequestConfig,
        payload: dict[str, Any],
        headers: dict[str, str],
        request_stats: str,
        record_count: int,
        template: str,
    ) -> dict[str, Any]:
        attempts = max(1, config.retry_count + 1)
        logger.info(
            "[Report][LLM] request provider={} model={} template={} records={} {} attempts={}",
            config.provider_label,
            config.model,
            template,
            record_count,
            request_stats,
            attempts,
        )
        data: dict[str, Any] | None = None
        last_error: Exception | None = None
        limits = httpx.Limits(max_connections=5, max_keepalive_connections=0)
        for attempt in range(1, attempts + 1):
            attempt_started_at = time.monotonic()
            try:
                logger.info(
                    "[Report][LLM] attempt {}/{} provider={} model={}",
                    attempt,
                    attempts,
                    config.provider_label,
                    config.model,
                )
                request_timeout = httpx.Timeout(
                    connect=20.0,
                    read=config.timeout,
                    write=60.0,
                    pool=20.0,
                )
                async with httpx.AsyncClient(timeout=request_timeout, limits=limits) as client:
                    response = await client.post(
                        f"{config.base_url}/chat/completions",
                        headers=headers,
                        json=payload,
                    )
                elapsed = time.monotonic() - attempt_started_at
                if response.is_error:
                    try:
                        error_payload = response.json()
                    except Exception:
                        error_payload = response.text
                    if response.status_code in {429, 500, 502, 503, 504} and attempt < attempts:
                        logger.warning(
                            "[Report][LLM] retryable status={} attempt={}/{} {}",
                            response.status_code,
                            attempt,
                            attempts,
                            request_stats,
                        )
                        await asyncio.sleep(config.retry_delay * attempt)
                        continue
                    raise ValueError(
                        f"{config.provider_label} report request failed: status={response.status_code}, "
                        f"{request_stats}, body={redact_sensitive(error_payload)}"
                    )
                data = response.json()
                logger.info(
                    "[Report][LLM] response received provider={} attempt={} elapsed={:.2f}s",
                    config.provider_label,
                    attempt,
                    elapsed,
                )
                break
            except httpx.TransportError as exc:
                last_error = exc
                elapsed = time.monotonic() - attempt_started_at
                if attempt < attempts:
                    logger.warning(
                        "[Report][LLM] transport error attempt={}/{} type={} repr={} elapsed={:.2f}s read_timeout={} {}",
                        attempt,
                        attempts,
                        exc.__class__.__name__,
                        _safe_context_text(repr(exc)),
                        elapsed,
                        config.timeout,
                        request_stats,
                    )
                    await asyncio.sleep(config.retry_delay * attempt)
                    continue
                raise ValueError(
                    f"{config.provider_label} report request failed after {attempt} attempts: "
                    f"{exc.__class__.__name__}: {_safe_context_text(repr(exc))}; "
                    f"elapsed={elapsed:.2f}s; "
                    f"read_timeout={config.timeout}; {request_stats}"
                ) from exc

        if data is None:
            raise ValueError(
                f"{config.provider_label} report request failed: {_safe_context_text(last_error)}; "
                f"{request_stats}"
            )
        return data

    def _extract_openai_compatible_content(
        self,
        data: dict[str, Any],
        provider_label: str,
    ) -> str:
        choices = data.get("choices") or []
        if not choices:
            raise ValueError(f"{provider_label} returned empty choices")

        message = choices[0].get("message") or {}
        content = message.get("content", "").strip()
        if not content:
            raise ValueError(f"{provider_label} returned empty content")
        return content

    @staticmethod
    def provider_label(provider: str) -> str:
        labels = {
            "qwen": "Qwen",
            "deepseek": "DeepSeek",
            "openai": "OpenAI",
            "local": "Local",
            "sense": "SenseNova",
        }
        return labels.get(provider, provider)

    @staticmethod
    def get_providers() -> list[dict]:
        """从配置读取所有可用的 LLM provider 列表（不依赖 AgentService）"""
        llm_config = get_config("llm", {}) or {}
        providers: list[dict] = []
        for key, cfg in llm_config.items():
            if key == "provider" or not isinstance(cfg, dict):
                continue
            model = cfg.get("model")
            if model:
                providers.append(
                    {
                        "key": key,
                        "label": ReportGenerator.provider_label(key),
                        "model": model,
                    }
                )
        return providers

    def _build_record_context(self, records: list[StorageRecord], provider: str = "") -> str:
        if not records:
            return "No records were matched."

        max_chars = int(get_config(f"llm.{provider}.max_input_chars", 22000) if provider else 22000)
        sections: list[str] = [_build_context_overview(records)]
        for index, record in enumerate(_select_context_records(records, max_records=12), start=1):
            snapshot = _compact_value(self._extract_snapshot(record.data))
            compact_data = _compact_record_data(record.data)
            sections.append(
                "\n".join(
                    [
                        f"### Record {index}",
                        f"- key: {_safe_context_text(record.key)}",
                        f"- source: {record.source or 'unknown'}",
                        f"- metadata: {_compact_metadata(record.metadata)}",
                        f"- snapshot: {_safe_json(snapshot, max_chars=1000)}",
                        f"- compact_data: {_safe_json(compact_data, max_chars=3500)}",
                    ]
                )
            )
        context = "\n\n".join(sections)
        if len(context) > max_chars:
            return (
                context[:max_chars] + "\n\n[TRUNCATED: LLM input context was capped before request]"
            )
        return context

    def _build_report_system_prompt(self, template: str) -> str:
        base = (
            "You are a game industry data analyst. "
            "Generate a Chinese Markdown report strictly from the provided records. "
            "Do not invent missing data. "
            "Prioritize conclusions, evidence, and recommended actions. "
        )
        if template == "brief":
            return base + "Keep the report concise and within four sections."
        return base + "Include overview, findings, risks or uncertainty, and suggested actions."

    def _render_record_overview(self, records: list[StorageRecord]) -> list[str]:
        lines: list[str] = []
        for index, record in enumerate(records[:5], start=1):
            snapshot = self._extract_snapshot(record.data)
            name = snapshot.get("name") or snapshot.get("game_name") or record.key
            details: list[str] = []
            if snapshot.get("current_players") not in (None, ""):
                details.append(f"current_players {snapshot['current_players']}")
            if snapshot.get("total_reviews") not in (None, ""):
                details.append(f"total_reviews {snapshot['total_reviews']}")
            if snapshot.get("review_score"):
                details.append(f"review_score {snapshot['review_score']}")
            if snapshot.get("price") not in (None, ""):
                details.append(f"price {snapshot['price']}")
            suffix = ", ".join(details) if details else "no key snapshot"
            lines.append(f"{index}. {name}, source {record.source or 'unknown'}, {suffix}")
        return lines

    def _render_observations(self, records: list[StorageRecord]) -> list[str]:
        snapshots = [self._extract_snapshot(record.data) for record in records]
        player_values = [
            value
            for value in (snapshot.get("current_players") for snapshot in snapshots)
            if isinstance(value, (int, float))
        ]
        review_values = [
            value
            for value in (snapshot.get("total_reviews") for snapshot in snapshots)
            if isinstance(value, (int, float))
        ]

        observations = [
            f"1. This report covers {len(records)} stored records and is suitable for a quick review.",
            f"2. Highest online player value: {max(player_values)}."
            if player_values
            else "2. No stable online player metric was found.",
            f"3. Highest review count: {max(review_values)}."
            if review_values
            else "3. No stable review count metric was found.",
            "4. For stronger conclusions, add more time slices and discussion text.",
        ]
        return observations

    def _extract_snapshot(self, data: Any) -> dict[str, Any]:
        if isinstance(data, dict):
            content = data.get("content")
            if isinstance(content, dict):
                snapshot = content.get("snapshot")
                if isinstance(snapshot, dict):
                    return {**content, **snapshot}
                return content
            snapshot = data.get("snapshot")
            if isinstance(snapshot, dict):
                return snapshot
            return data
        return {}

    def _summary_from_record(self, record: StorageRecord) -> ReportSummary:
        payload = record.data if isinstance(record.data, dict) else {}
        return ReportSummary.model_validate(payload)


def _compact_metadata(metadata: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(metadata, dict):
        return {}
    metadata = redact_sensitive(metadata)
    source_task = (
        metadata.get("source_task") if isinstance(metadata.get("source_task"), dict) else {}
    )
    return {
        key: value
        for key, value in {
            "collector": metadata.get("collector"),
            "target": metadata.get("target"),
            "group_id": metadata.get("group_id"),
            "group_name": metadata.get("group_name"),
            "task_id": source_task.get("task_id") if isinstance(source_task, dict) else "",
            "task_name": source_task.get("task_name") if isinstance(source_task, dict) else "",
        }.items()
        if value not in (None, "")
    }


def _deletable_report_file(excel_path: Any) -> Path | None:
    if not excel_path:
        return None
    try:
        resolved = Path(str(excel_path)).resolve()
    except (OSError, RuntimeError, ValueError) as exc:
        logger.warning("[Report] invalid Excel path on delete: {}", _safe_context_text(exc))
        return None
    allowed_dir = get_reports_dir()
    if not resolved.is_relative_to(allowed_dir):
        logger.warning(
            "[Report] skip deleting Excel file outside reports dir: {}",
            _safe_context_text(resolved),
        )
        return None
    return resolved


def _build_report_metadata(
    *,
    provider: str,
    template: str,
    source_query: str,
    records: list[StorageRecord],
    usable_records: list[StorageRecord],
    source_coverage: dict[str, int],
    template_validation: dict[str, Any],
    target_context: dict[str, Any],
    extra: dict[str, Any] | None,
    report_format: str,
) -> dict[str, Any]:
    completeness_counts: dict[str, int] = {}
    for record in records:
        completeness = compute_record_completeness(record)
        completeness_counts[completeness] = completeness_counts.get(completeness, 0) + 1

    metadata: dict[str, Any] = {
        "provider": provider,
        "template": template,
        "source_query": source_query,
        "format": report_format,
        **(extra or {}),
    }
    metadata.update(
        {
            "source_record_count": len(records),
            "usable_record_count": len(usable_records),
            "source_record_keys": [record.key for record in records],
            "usable_record_keys": [record.key for record in usable_records],
            "empty_record_keys": [record.key for record in records if record.data is None],
            "source_coverage": dict(source_coverage or {}),
            "record_completeness": completeness_counts,
            "template_validation": template_validation,
            "target_context": target_context,
        }
    )
    freshness = _build_source_freshness_metadata(records)
    if freshness:
        metadata["source_freshness"] = freshness
    return redact_sensitive(metadata)


def _build_source_freshness_metadata(records: list[StorageRecord]) -> dict[str, Any]:
    stored_times = [
        record.stored_at
        for record in records
        if isinstance(getattr(record, "stored_at", None), datetime)
    ]
    if not stored_times:
        return {}
    oldest = min(stored_times)
    newest = max(stored_times)
    now = datetime.now(tz=oldest.tzinfo) if oldest.tzinfo else datetime.now()
    max_age_seconds = max(0, int((now - oldest).total_seconds()))
    newest_age_seconds = max(0, int((now - newest).total_seconds()))
    return {
        "oldest_record_at": oldest.isoformat(),
        "newest_record_at": newest.isoformat(),
        "max_age_days": max_age_seconds // 86400,
        "newest_age_days": newest_age_seconds // 86400,
        "warning_days": _coerce_positive_int(
            get_config("reporting.freshness_warning_days", 30),
            default=30,
        ),
    }


def _coerce_positive_int(value: Any, *, default: int) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return default
    return parsed if parsed > 0 else default


def _prepare_explicit_source_records(
    records: list[StorageRecord],
    metadata: dict[str, Any] | None,
) -> tuple[list[StorageRecord], dict[str, Any] | None]:
    if not records:
        return records, metadata

    source_records = filter_source_data_records(records)
    excluded_keys = [record.key for record in records if is_report_history_record(record)]
    if records and not source_records:
        raise ValueError(
            "Selected records only contain generated report history. "
            "Select source data records instead."
        )
    if not excluded_keys:
        return records, metadata

    next_metadata = dict(metadata or {})
    next_metadata["selected_record_keys"] = [record.key for record in source_records]
    previous_excluded = [
        str(key)
        for key in next_metadata.get("excluded_report_record_keys", [])
        if str(key or "").strip()
    ]
    next_metadata["excluded_report_record_keys"] = [
        *previous_excluded,
        *(key for key in excluded_keys if key not in previous_excluded),
    ]
    return source_records, next_metadata


def _build_context_overview(records: list[StorageRecord]) -> str:
    source_counts: dict[str, int] = {}
    completeness_counts: dict[str, int] = {}
    games: dict[str, set[str]] = {}
    key_summaries: list[str] = []

    for record in records:
        identity = extract_record_identity(record)
        source = (
            identity.get("collector")
            if identity
            else record.source or _compact_metadata(record.metadata).get("collector") or "unknown"
        )
        source_counts[source] = source_counts.get(source, 0) + 1
        completeness = compute_record_completeness(record)
        completeness_counts[completeness] = completeness_counts.get(completeness, 0) + 1

        if identity:
            game_name = identity.get("game_name") or "Unknown"
            games.setdefault(game_name, set()).add(identity.get("data_source") or source)

        summary = build_record_summary(record.data)
        if summary and len(key_summaries) < 8:
            short_summary = ", ".join(f"{key}={value}" for key, value in list(summary.items())[:4])
            key_summaries.append(f"- {_safe_context_text(record.key)}: {short_summary}")

    game_lines = [
        f"- {name}: {', '.join(sorted(sources))}"
        for name, sources in list(sorted(games.items()))[:8]
    ]
    source_line = (
        ", ".join(f"{name}={count}" for name, count in sorted(source_counts.items())) or "unknown"
    )
    completeness_line = (
        ", ".join(f"{name}={count}" for name, count in sorted(completeness_counts.items()))
        or "unknown"
    )

    return "\n".join(
        [
            "### Dataset Coverage",
            f"- total_records: {len(records)}",
            f"- sources: {source_line}",
            f"- completeness: {completeness_line}",
            "- games:",
            *(game_lines or ["- Unknown"]),
            "- key_metric_samples:",
            *(key_summaries or ["- No compact key metrics detected."]),
        ]
    )


def _select_context_records(
    records: list[StorageRecord],
    *,
    max_records: int,
) -> list[StorageRecord]:
    """Select detailed context records while preserving source coverage."""
    if len(records) <= max_records:
        return records

    selected: list[StorageRecord] = []
    selected_keys: set[str] = set()
    source_order: list[str] = []
    best_by_source: dict[str, tuple[int, int, StorageRecord]] = {}

    for index, record in enumerate(records):
        source = _record_context_source(record)
        if source not in best_by_source:
            source_order.append(source)
            best_by_source[source] = (_context_record_score(record), index, record)
            continue

        current_score, current_index, _ = best_by_source[source]
        score = _context_record_score(record)
        if (score, -index) > (current_score, -current_index):
            best_by_source[source] = (score, index, record)

    for source in source_order:
        _, _, record = best_by_source[source]
        _append_selected_context_record(record, selected, selected_keys)
        if len(selected) >= max_records:
            return selected

    for record in records:
        _append_selected_context_record(record, selected, selected_keys)
        if len(selected) >= max_records:
            break
    return selected


def _append_selected_context_record(
    record: StorageRecord,
    selected: list[StorageRecord],
    selected_keys: set[str],
) -> None:
    key = str(record.key or id(record))
    if key in selected_keys:
        return
    selected.append(record)
    selected_keys.add(key)


def _record_context_source(record: StorageRecord) -> str:
    identity = extract_record_identity(record)
    if identity and identity.get("collector"):
        return str(identity["collector"])
    metadata = _compact_metadata(record.metadata)
    return str(record.source or metadata.get("collector") or "unknown")


def _context_record_score(record: StorageRecord) -> int:
    completeness_rank = {"full": 30, "partial": 20, "empty": 0}
    score = completeness_rank.get(compute_record_completeness(record), 10)
    summary = build_record_summary(record.data)
    score += min(len(summary), 10)
    return score


def _compact_record_data(data: Any) -> Any:
    if not isinstance(data, dict):
        return _compact_value(data)

    compact: dict[str, Any] = {}
    for key in (
        "collector",
        "game_name",
        "app_id",
        "keyword",
        "snapshot",
        "source_meta",
        "steamdb",
        "steam_api",
        "trend_history",
        "related_queries",
        "monitor_metrics",
        "discussions",
        "events",
        "event_history",
        "reviews",
        "game",
        "updates",
    ):
        if key in data:
            compact[key] = _compact_value(data[key], key_hint=key)
    return compact or _compact_value(data)


def _compact_value(value: Any, *, key_hint: str = "", depth: int = 0) -> Any:
    if depth > 5:
        return _truncate_text(_safe_json(value, max_chars=500), 500)
    if isinstance(value, str):
        return _truncate_text(value, _string_limit_for_key(key_hint))
    if isinstance(value, (int, float, bool)) or value is None:
        return value
    if isinstance(value, list):
        limit = _list_limit_for_key(key_hint)
        return [_compact_value(item, key_hint=key_hint, depth=depth + 1) for item in value[:limit]]
    if isinstance(value, dict):
        compact: dict[str, Any] = {}
        for index, (key, child) in enumerate(value.items()):
            if index >= 80:
                compact["__truncated_keys__"] = len(value) - 80
                break
            compact[str(key)] = _compact_value(child, key_hint=str(key), depth=depth + 1)
        return compact
    return _truncate_text(str(value), 500)


def _list_limit_for_key(key: str) -> int:
    lowered = key.lower()
    if lowered in {"daily_rows", "trend_history", "records"}:
        return 60
    if lowered in {"topics", "posts", "items", "reviews", "news", "events", "updates"}:
        return 12
    if lowered in {"related_queries", "top", "rising"}:
        return 30
    return 25


def _string_limit_for_key(key: str) -> int:
    lowered = key.lower()
    if lowered in {"content", "contents", "summary", "review", "text", "body"}:
        return 500
    return 200


def _truncate_text(text: str, max_chars: int) -> str:
    if len(text) <= max_chars:
        return text
    return text[:max_chars] + "...[truncated]"


def _safe_context_text(value: Any) -> str:
    return redact_sensitive_text(str(value or ""))


def _safe_json(value: Any, *, max_chars: int) -> str:
    text = json.dumps(
        redact_sensitive(value), ensure_ascii=False, default=str, separators=(",", ":")
    )
    return _truncate_text(text, max_chars)


async def _emit_report_progress(
    progress_id: str,
    stage: str,
    progress: float,
    message: str,
    **extra: Any,
) -> None:
    if not progress_id:
        return
    safe_message = _safe_context_text(message)
    safe_extra = redact_sensitive(extra)
    logger.info(
        "[Report][Progress] id={} stage={} progress={} message={}",
        _safe_context_text(progress_id),
        _safe_context_text(stage),
        progress,
        safe_message,
    )
    try:
        from src.web.routes.ws import manager

        await manager.broadcast(
            {
                "type": "report_progress",
                "progress_id": progress_id,
                "stage": stage,
                "progress": progress,
                "message": safe_message,
                **safe_extra,
            }
        )
    except Exception as exc:
        logger.debug("[Report][Progress] broadcast failed: {}", _safe_context_text(exc))
