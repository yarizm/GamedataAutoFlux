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
from datetime import datetime
from typing import Any

import httpx
from loguru import logger
from pydantic import BaseModel, Field

from src.core.config import get as get_config
from src.core.config import get_data_dir
from src.storage.base import StorageRecord
from src.storage.local_store import LocalStorage
from src.storage.vector_store import VectorStorage
from src.reporting.data_extractor import extract_from_records
from src.reporting.excel_exporter import export_to_excel
from src.reporting.report_templates import get_report_template, validate_template_sources


class ReportSummary(BaseModel):
    """Report summary used by list views."""

    id: str
    title: str
    prompt: str
    data_source: str
    template: str
    generated_at: datetime
    matched_records: int = 0


class GeneratedReport(ReportSummary):
    """Full generated report."""

    content: str
    metadata: dict[str, Any] = Field(default_factory=dict)
    excel_path: str | None = Field(default=None, description="Excel report file path")


class ReportGenerator:
    """Report generation and history management."""

    def __init__(
        self,
        source_storage_config: dict[str, Any] | None = None,
        report_storage_config: dict[str, Any] | None = None,
        vector_storage_config: dict[str, Any] | None = None,
    ):
        self._source_storage_config = source_storage_config or {}
        self._report_storage_config = {
            "db_name": "reports.db",
            "json_dir": "reports",
            **(report_storage_config or {}),
        }
        self._vector_storage_config = vector_storage_config or {
            "provider": get_config("vector_store.provider", "local"),
            "db_name": get_config("vector_store.local.db_name", "vector_store.db"),
            "json_dir": get_config("vector_store.local.json_dir", "vector_records"),
        }
        self._llm_provider = get_config("llm.provider", "stub")

    async def generate(
        self,
        prompt: str,
        data_source: str = "",
        template: str = "default",
        params: dict[str, Any] | None = None,
        records: list[StorageRecord] | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> GeneratedReport:
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
        if records is None:
            await _emit_report_progress(progress_id, "loading_records", 0.12, "Loading report records")
            records = await self._load_source_records(prompt=prompt, data_source=data_source, params=params)
        logger.info("[Report] records loaded count={} template={}", len(records), template)

        await _emit_report_progress(progress_id, "llm", 0.42, "Calling LLM for report analysis")
        content = await self._render_report(prompt, data_source, template, records)
        await _emit_report_progress(progress_id, "llm_done", 0.76, "LLM analysis completed")

        report = GeneratedReport(
            id=uuid.uuid4().hex[:12],
            title=self._build_title(prompt, data_source, template),
            prompt=prompt,
            data_source=data_source,
            template=template,
            generated_at=datetime.now(),
            matched_records=len(records),
            content=content,
            metadata={
                "provider": self._llm_provider,
                "template": template,
                "source_query": data_source or prompt,
                **(metadata or {}),
            },
        )

        await self._save_report(report)
        logger.info(
            "[Report] generate complete report_id={} matched_records={}",
            report.id,
            report.matched_records,
        )
        await _emit_report_progress(progress_id, "completed", 1.0, "Report generated", report_id=report.id)
        return report

    async def generate_excel(
        self,
        prompt: str,
        data_source: str = "",
        template: str = "default",
        params: dict[str, Any] | None = None,
        records: list[StorageRecord] | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> GeneratedReport:
        """
        Generate an Excel report.

        Flow: load records, extract structured fields, optionally ask the LLM,
        then write the .xlsx file.
        """
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
        if records is None:
            await _emit_report_progress(progress_id, "loading_records", 0.12, "Loading report records")
            records = await self._load_source_records(prompt=prompt, data_source=data_source, params=params)
        logger.info("[Report] records loaded count={} template={}", len(records), template)

        usable_records = [r for r in records if r.data is not None]
        await _emit_report_progress(progress_id, "extracting", 0.22, f"Parsing {len(usable_records)} records")
        raw_data_list = [r.data for r in usable_records]
        extracted = extract_from_records(
            raw_data_list,
            record_keys=[r.key for r in usable_records],
            metadata_list=[r.metadata for r in usable_records],
        )
        template_validation = validate_template_sources(template, extracted.source_coverage)
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

        # Optional LLM narrative.
        llm_content = None
        if params.get("include_llm_analysis", True):
            try:
                await _emit_report_progress(progress_id, "llm", 0.42, "Calling LLM for report analysis")
                llm_content = await self._render_report(
                    self._build_template_prompt(prompt, template, template_validation),
                    data_source,
                    template,
                    records,
                )
                await _emit_report_progress(progress_id, "llm_done", 0.68, "LLM analysis completed")
            except Exception as exc:
                logger.warning("[Report] LLM analysis failed, fallback to template report: {}", exc)
                await _emit_report_progress(progress_id, "llm_failed", 0.62, f"LLM failed; falling back to template report: {exc}")

        # Write Excel output.
        await _emit_report_progress(progress_id, "exporting", 0.78, "Writing Excel report")
        report_id = uuid.uuid4().hex[:12]
        title = self._build_title(prompt, data_source, template)
        excel_dir = get_data_dir() / "excel_reports"
        excel_path = excel_dir / f"report_{report_id}.xlsx"

        export_to_excel(
            data=extracted,
            output_path=excel_path,
            title=title,
            llm_content=llm_content,
            template_id=template,
            template_validation=template_validation,
        )

        report = GeneratedReport(
            id=report_id,
            title=title,
            prompt=prompt,
            data_source=data_source,
            template=template,
            generated_at=datetime.now(),
            matched_records=len(records),
            content=llm_content or "Report generated as an Excel file",
            excel_path=str(excel_path),
            metadata={
                "provider": self._llm_provider,
                "template": template,
                "source_query": data_source or prompt,
                "format": "excel",
                "sheets": {
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
                },
                "template_validation": template_validation,
                **(metadata or {}),
            },
        )

        await self._save_report(report)
        logger.info(
            "[Report] generate_excel complete report_id={} matched_records={} excel_path={}",
            report.id,
            report.matched_records,
            report.excel_path or "",
        )
        await _emit_report_progress(progress_id, "completed", 1.0, "Report generated", report_id=report.id)
        return report

    async def list_reports(self, limit: int = 20) -> list[ReportSummary]:
        store = LocalStorage(self._report_storage_config)
        await store.initialize()
        try:
            result = await store.query("key:report:", limit=limit)
            reports = [self._summary_from_record(record) for record in result.records]
            reports.sort(key=lambda item: item.generated_at, reverse=True)
            return reports
        finally:
            await store.close()

    async def get_report(self, report_id: str) -> GeneratedReport | None:
        store = LocalStorage(self._report_storage_config)
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
        if excel_path:
            try:
                from pathlib import Path

                path = Path(excel_path)
                if path.exists():
                    path.unlink()
            except Exception:
                pass
        store = LocalStorage(self._report_storage_config)
        await store.initialize()
        try:
            await store.delete(f"report:{report_id}")
        finally:
            await store.close()
        return True

    async def _save_report(self, report: GeneratedReport) -> None:
        store = LocalStorage(self._report_storage_config)
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

    async def _load_source_records(
        self,
        prompt: str,
        data_source: str,
        params: dict[str, Any],
    ) -> list[StorageRecord]:
        limit = int(params.get("limit", 5))
        use_vector = params.get("use_vector", True)

        if use_vector:
            vector_records = await self._load_vector_records(prompt=prompt, limit=limit)
            if vector_records:
                return vector_records

        store = LocalStorage(self._source_storage_config)
        await store.initialize()
        try:
            if data_source:
                result = await store.query(f"source:{data_source}", limit=limit)
                return result.records

            keywords = self._extract_keywords(prompt)
            for keyword in keywords:
                result = await store.query(keyword, limit=limit)
                if result.records:
                    return result.records

            return (await store.query("key:", limit=limit)).records
        finally:
            await store.close()

    async def _load_vector_records(self, prompt: str, limit: int) -> list[StorageRecord]:
        store = VectorStorage(self._vector_storage_config)
        await store.initialize()
        try:
            result = await store.query(prompt, limit=limit)
            return result.records
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
    ) -> str:
        template_def = get_report_template(template)
        if template_def is None:
            return prompt

        missing = template_validation.get("missing_collectors") or []
        missing_text = ", ".join(missing) if missing else "none"
        return (
            f"{prompt}\n\n"
            f"Report template: {template_def.name}\n"
            f"Template requirements: {template_def.prompt_instruction}\n"
            f"Missing data sources: {missing_text}\n"
            "Analyze strictly from the provided JSON records. If a source is missing, "
            "state the gap instead of inventing data."
        )

    async def _render_report(
        self,
        prompt: str,
        data_source: str,
        template: str,
        records: list[StorageRecord],
    ) -> str:
        if self._llm_provider in {"deepseek", "qwen"}:
            try:
                return await self._render_report_with_openai_compatible(
                    provider=self._llm_provider,
                    prompt=prompt,
                    data_source=data_source,
                    template=template,
                    records=records,
                )
            except Exception as exc:
                fallback_enabled = bool(get_config(f"llm.{self._llm_provider}.fallback_to_stub", True))
                if not fallback_enabled:
                    raise
                provider_label = "Qwen" if self._llm_provider == "qwen" else "DeepSeek"
                return self._render_stub_report(
                    prompt=prompt,
                    data_source=data_source,
                    template=template,
                    records=records,
                    extra_note=f"{provider_label} request failed; fell back to template report: {exc}",
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
            lines.extend(["", "## Suggestions", "Review the latest 1-2 records first and decide whether more collection is needed."])
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
        provider_label = "Qwen" if provider == "qwen" else provider
        api_key = get_config(f"llm.{provider}.api_key", "")
        if not api_key or api_key.startswith("${"):
            raise ValueError(f"llm.{provider}.api_key is not configured")

        default_base_url = (
            "https://dashscope.aliyuncs.com/compatible-mode/v1"
            if provider == "qwen"
            else "https://api.deepseek.com"
        )
        default_model = "qwen-max" if provider == "qwen" else "deepseek-chat"
        base_url = get_config(f"llm.{provider}.base_url", default_base_url).rstrip("/")
        model = get_config(f"llm.{provider}.model", default_model)
        temperature = float(get_config(f"llm.{provider}.temperature", 0.3))
        max_tokens = int(get_config(f"llm.{provider}.max_tokens", 2000))
        timeout = float(get_config(f"llm.{provider}.timeout", 180 if provider == "qwen" else 90))
        retry_count = int(get_config(f"llm.{provider}.retry_count", 2))
        retry_delay = float(get_config(f"llm.{provider}.retry_delay", 2.0))

        context = self._build_record_context(records, provider=provider)
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
            "model": model,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            "temperature": temperature,
            "max_tokens": max_tokens,
            "stream": False,
        }

        headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
            "Connection": "close",
        }

        request_stats = f"context_chars={len(context)}, user_prompt_chars={len(user_prompt)}"
        attempts = max(1, retry_count + 1)
        logger.info(
            "[Report][LLM] request provider={} model={} template={} records={} {} attempts={}",
            provider_label,
            model,
            template,
            len(records),
            request_stats,
            attempts,
        )
        data = None
        last_error: Exception | None = None
        limits = httpx.Limits(max_connections=5, max_keepalive_connections=0)
        for attempt in range(1, attempts + 1):
            try:
                logger.info("[Report][LLM] attempt {}/{} provider={} model={}", attempt, attempts, provider_label, model)
                attempt_started_at = time.monotonic()
                request_timeout = httpx.Timeout(connect=20.0, read=timeout, write=60.0, pool=20.0)
                async with httpx.AsyncClient(timeout=request_timeout, limits=limits) as client:
                    response = await client.post(f"{base_url}/chat/completions", headers=headers, json=payload)
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
                        await asyncio.sleep(retry_delay * attempt)
                        continue
                    raise ValueError(
                        f"{provider_label} report request failed: status={response.status_code}, "
                        f"{request_stats}, body={error_payload}"
                    )
                data = response.json()
                logger.info("[Report][LLM] response received provider={} attempt={} elapsed={:.2f}s", provider_label, attempt, elapsed)
                break
            except httpx.TransportError as exc:
                last_error = exc
                elapsed = time.monotonic() - attempt_started_at if "attempt_started_at" in locals() else 0.0
                if attempt < attempts:
                    logger.warning(
                        "[Report][LLM] transport error attempt={}/{} type={} repr={} elapsed={:.2f}s read_timeout={} {}",
                        attempt,
                        attempts,
                        exc.__class__.__name__,
                        repr(exc),
                        elapsed,
                        timeout,
                        request_stats,
                    )
                    await asyncio.sleep(retry_delay * attempt)
                    continue
                raise ValueError(
                    f"{provider_label} report request failed after {attempt} attempts: "
                    f"{exc.__class__.__name__}: {repr(exc)}; elapsed={elapsed:.2f}s; "
                    f"read_timeout={timeout}; {request_stats}"
                ) from exc

        if data is None:
            raise ValueError(f"{provider_label} report request failed: {last_error}; {request_stats}")

        choices = data.get("choices") or []
        if not choices:
            raise ValueError(f"{provider_label} returned empty choices")

        message = choices[0].get("message") or {}
        content = message.get("content", "").strip()
        if not content:
            raise ValueError(f"{provider_label} returned empty content")
        return content

    def _build_record_context(self, records: list[StorageRecord], provider: str = "") -> str:
        if not records:
            return "No records were matched."

        max_chars = int(get_config(f"llm.{provider}.max_input_chars", 22000) if provider else 22000)
        sections: list[str] = []
        for index, record in enumerate(records[:12], start=1):
            snapshot = _compact_value(self._extract_snapshot(record.data))
            compact_data = _compact_record_data(record.data)
            sections.append(
                "\n".join(
                    [
                        f"### Record {index}",
                        f"- key: {record.key}",
                        f"- source: {record.source or 'unknown'}",
                        f"- metadata: {_compact_metadata(record.metadata)}",
                        f"- snapshot: {_safe_json(snapshot, max_chars=1000)}",
                        f"- compact_data: {_safe_json(compact_data, max_chars=3500)}",
                    ]
                )
            )
        context = "\n\n".join(sections)
        if len(context) > max_chars:
            return context[:max_chars] + "\n\n[TRUNCATED: LLM input context was capped before request]"
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
            value for value in (snapshot.get("current_players") for snapshot in snapshots)
            if isinstance(value, (int, float))
        ]
        review_values = [
            value for value in (snapshot.get("total_reviews") for snapshot in snapshots)
            if isinstance(value, (int, float))
        ]

        observations = [
            f"1. This report covers {len(records)} stored records and is suitable for a quick review.",
            f"2. Highest online player value: {max(player_values)}." if player_values else "2. No stable online player metric was found.",
            f"3. Highest review count: {max(review_values)}." if review_values else "3. No stable review count metric was found.",
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
    source_task = metadata.get("source_task") if isinstance(metadata.get("source_task"), dict) else {}
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


def _safe_json(value: Any, *, max_chars: int) -> str:
    text = json.dumps(value, ensure_ascii=False, default=str, separators=(",", ":"))
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
    logger.info("[Report][Progress] id={} stage={} progress={} message={}", progress_id, stage, progress, message)
    try:
        from src.web.routes.ws import manager

        await manager.broadcast(
            {
                "type": "report_progress",
                "progress_id": progress_id,
                "stage": stage,
                "progress": progress,
                "message": message,
                **extra,
            }
        )
    except Exception as exc:
        logger.debug("[Report][Progress] broadcast failed: {}", exc)
