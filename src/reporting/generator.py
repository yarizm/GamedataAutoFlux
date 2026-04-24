"""
报告生成器。

当前提供一个可落地的本地实现:
  - 从本地存储检索相关数据
  - 使用模板组装结构化 Markdown 报告
  - 保存报告历史，供 API 与 WebUI 查询

后续如接入真实 LLM，仅需替换 _render_report 内容生成逻辑。
"""

from __future__ import annotations

import re
import uuid
from datetime import datetime
from typing import Any

import httpx
from pydantic import BaseModel, Field

from src.core.config import get as get_config
from src.core.config import get_data_dir
from src.storage.base import StorageRecord
from src.storage.local_store import LocalStorage
from src.storage.vector_store import VectorStorage
from src.reporting.data_extractor import extract_from_records
from src.reporting.excel_exporter import export_to_excel


class ReportSummary(BaseModel):
    """报告摘要，用于列表展示。"""

    id: str
    title: str
    prompt: str
    data_source: str
    template: str
    generated_at: datetime
    matched_records: int = 0


class GeneratedReport(ReportSummary):
    """完整报告对象。"""

    content: str
    metadata: dict[str, Any] = Field(default_factory=dict)
    excel_path: str | None = Field(default=None, description="Excel 报告文件路径")


class ReportGenerator:
    """报告生成与历史管理。"""

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
        records = records or await self._load_source_records(prompt=prompt, data_source=data_source, params=params)

        report = GeneratedReport(
            id=uuid.uuid4().hex[:12],
            title=self._build_title(prompt, data_source, template),
            prompt=prompt,
            data_source=data_source,
            template=template,
            generated_at=datetime.now(),
            matched_records=len(records),
            content=await self._render_report(prompt, data_source, template, records),
            metadata={
                "provider": self._llm_provider,
                "template": template,
                "source_query": data_source or prompt,
                **(metadata or {}),
            },
        )

        await self._save_report(report)
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
        生成 Excel 格式的报告。

        流程: 加载数据 → 提取结构化字段 → 可选 LLM 分析 → 写入 .xlsx
        """
        params = params or {}
        records = records or await self._load_source_records(prompt=prompt, data_source=data_source, params=params)

        # 提取结构化数据
        raw_data_list = [r.data for r in records if r.data is not None]
        extracted = extract_from_records(raw_data_list)

        # 可选: LLM 文字分析
        llm_content = None
        if params.get("include_llm_analysis", True):
            try:
                llm_content = await self._render_report(prompt, data_source, template, records)
            except Exception as exc:
                from loguru import logger
                logger.warning(f"LLM 分析生成失败，Excel 中将不包含 AI 分析: {exc}")

        # 生成 Excel
        report_id = uuid.uuid4().hex[:12]
        title = self._build_title(prompt, data_source, template)
        excel_dir = get_data_dir() / "excel_reports"
        excel_path = excel_dir / f"report_{report_id}.xlsx"

        export_to_excel(
            data=extracted,
            output_path=excel_path,
            title=title,
            llm_content=llm_content,
        )

        report = GeneratedReport(
            id=report_id,
            title=title,
            prompt=prompt,
            data_source=data_source,
            template=template,
            generated_at=datetime.now(),
            matched_records=len(records),
            content=llm_content or "报告已生成为 Excel 文件",
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
                },
                **(metadata or {}),
            },
        )

        await self._save_report(report)
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
        prefix = data_source or "综合数据"
        subject = prompt.strip()[:24] or "报告"
        return f"{prefix} {template}报告: {subject}"

    async def _render_report(
        self,
        prompt: str,
        data_source: str,
        template: str,
        records: list[StorageRecord],
    ) -> str:
        if self._llm_provider == "deepseek":
            try:
                return await self._render_report_with_deepseek(prompt, data_source, template, records)
            except Exception as exc:
                fallback_enabled = bool(get_config("llm.deepseek.fallback_to_stub", True))
                if not fallback_enabled:
                    raise
                return self._render_stub_report(
                    prompt=prompt,
                    data_source=data_source,
                    template=template,
                    records=records,
                    extra_note=f"DeepSeek 调用失败，已回退到模板报告：{exc}",
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
            "## 任务输入",
            f"- 提示词: {prompt}",
            f"- 数据源过滤: {data_source or '未指定'}",
            f"- 模板: {template}",
            f"- 匹配记录数: {len(records)}",
            "",
        ]

        if extra_note:
            lines.extend(["## 生成备注", extra_note, ""])

        if not records:
            lines.extend(
                [
                    "## 数据结论",
                    "当前没有检索到可用于生成分析的历史记录。",
                    "建议先执行采集任务并落库，再重新生成报告。",
                ]
            )
            return "\n".join(lines)

        lines.extend(
            [
                "## 数据概览",
                *self._render_record_overview(records),
                "",
                "## 初步观察",
                *self._render_observations(records),
            ]
        )

        if template == "brief":
            lines.extend(["", "## 建议", "优先复查最新 1-2 条记录，确认是否需要追加采集。"])
        else:
            lines.extend(
                [
                    "",
                    "## 建议动作",
                    "1. 对高波动或高热度目标追加定时采集。",
                    "2. 对评论或公告明显变化的目标进入复盘池。",
                    "3. 如需更细的研判，可继续补充更多时间切片和评论文本。",
                ]
            )

        return "\n".join(lines)

    async def _render_report_with_deepseek(
        self,
        prompt: str,
        data_source: str,
        template: str,
        records: list[StorageRecord],
    ) -> str:
        api_key = get_config("llm.deepseek.api_key", "")
        if not api_key or api_key.startswith("${"):
            raise ValueError("llm.deepseek.api_key 未配置")

        base_url = get_config("llm.deepseek.base_url", "https://api.deepseek.com").rstrip("/")
        model = get_config("llm.deepseek.model", "deepseek-chat")
        temperature = float(get_config("llm.deepseek.temperature", 0.3))
        max_tokens = int(get_config("llm.deepseek.max_tokens", 1200))
        timeout = float(get_config("llm.deepseek.timeout", 60))

        context = self._build_record_context(records)
        system_prompt = self._build_deepseek_system_prompt(template=template)
        user_prompt = (
            f"用户需求：{prompt}\n"
            f"数据源过滤：{data_source or '未指定'}\n\n"
            f"下面是检索到的数据记录，请基于这些记录生成一份中文 Markdown 报告。\n"
            f"如果信息不足，请明确指出不足，不要编造。\n\n"
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
        }

        async with httpx.AsyncClient(timeout=httpx.Timeout(timeout)) as client:
            response = await client.post(f"{base_url}/chat/completions", headers=headers, json=payload)
            if response.is_error:
                try:
                    error_payload = response.json()
                except Exception:
                    error_payload = response.text
                raise ValueError(
                    f"DeepSeek report request failed: status={response.status_code}, body={error_payload}"
                )
            data = response.json()

        choices = data.get("choices") or []
        if not choices:
            raise ValueError("DeepSeek 返回为空 choices")

        message = choices[0].get("message") or {}
        content = message.get("content", "").strip()
        if not content:
            raise ValueError("DeepSeek 返回空内容")
        return content

    def _build_record_context(self, records: list[StorageRecord]) -> str:
        if not records:
            return "没有检索到任何记录。"

        sections: list[str] = []
        for index, record in enumerate(records[:8], start=1):
            snapshot = self._extract_snapshot(record.data)
            sections.append(
                "\n".join(
                    [
                        f"### 记录 {index}",
                        f"- key: {record.key}",
                        f"- source: {record.source or 'unknown'}",
                        f"- snapshot: {snapshot}",
                        f"- raw_data: {record.data}",
                    ]
                )
            )
        return "\n\n".join(sections)

    def _build_deepseek_system_prompt(self, template: str) -> str:
        base = (
            "你是游戏行业数据分析师。"
            "请严格基于提供的记录生成中文 Markdown 报告。"
            "不要编造不存在的数据。"
            "优先输出结论、证据和建议。"
        )
        if template == "brief":
            return base + "报告尽量简洁，控制在 4 个小节以内。"
        return base + "报告建议包含概览、发现、风险/不确定性、建议动作。"

    def _render_record_overview(self, records: list[StorageRecord]) -> list[str]:
        lines: list[str] = []
        for index, record in enumerate(records[:5], start=1):
            snapshot = self._extract_snapshot(record.data)
            name = snapshot.get("name") or snapshot.get("game_name") or record.key
            details: list[str] = []
            if snapshot.get("current_players") not in (None, ""):
                details.append(f"当前在线 {snapshot['current_players']}")
            if snapshot.get("total_reviews") not in (None, ""):
                details.append(f"评论总量 {snapshot['total_reviews']}")
            if snapshot.get("review_score"):
                details.append(f"口碑 {snapshot['review_score']}")
            if snapshot.get("price") not in (None, ""):
                details.append(f"价格 {snapshot['price']}")
            suffix = "，".join(details) if details else "暂无关键快照"
            lines.append(f"{index}. {name}，来源 {record.source or 'unknown'}，{suffix}")
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
            f"1. 当前报告覆盖 {len(records)} 条已落库记录，适合做快速盘点，不适合做长期趋势判断。",
            f"2. 最高在线值为 {max(player_values)}。" if player_values else "2. 本批数据未包含稳定的在线人数指标。",
            f"3. 最高评论量为 {max(review_values)}。" if review_values else "3. 本批数据未包含稳定的评论量指标。",
            "4. 如需更强结论，建议补充更多时间切片数据，并将报告生成切换到真实 LLM/Embedding。"
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
