"""
报告生成与查询工具
"""
import json
from typing import Type
from langchain_core.tools import BaseTool
from pydantic import BaseModel
from loguru import logger

from src.agent.schemas import (
    GenerateReportInput,
    GetReportContentInput,
)
from src.agent.tools.data import _list_available_games
from src.services._utils import extract_record_identity

def _extract_prompt_keywords(prompt: str) -> list[str]:
    import re
    stop_words = {
        "帮我", "生成", "报告", "一个", "一份", "的", "了", "是", "在", "和", "请", "要", "需要",
        "分析", "综合", "全面", "关于", "对于", "这个", "include", "report", "generate",
        "for", "the", "a", "an",
    }
    split_pattern = re.compile(
        r"[，。！？、；：（）\s]+"
        r"|请对|进行|包括|并提|要求|帮我|生成|分析|综合|全面|完整|详细"
        r"|[a-zA-Z]{2,}"
    )
    raw_parts = re.findall(r"[一-鿿]{2,}", prompt)
    keywords = []
    for part in raw_parts:
        sub_parts = split_pattern.split(part)
        for sub in sub_parts:
            sub = sub.strip()
            if len(sub) >= 2 and sub not in stop_words:
                keywords.append(sub)
    eng_tokens = re.findall(r"[a-zA-Z0-9]{2,}", prompt.lower())
    for token in eng_tokens:
        if token not in stop_words:
            keywords.append(token)
    seen = set()
    result = []
    for kw in keywords:
        if kw not in seen:
            seen.add(kw)
            result.append(kw)
    return result[:5]


def _filter_records_by_keywords(records: list, keywords: list[str]) -> list:
    """只保留 game_name 与任一关键词匹配的记录（双向子串匹配）"""
    matched = []
    for record in records:
        identity = extract_record_identity(record)
        if not identity:
            continue
        game_name = identity.get("game_name", "").lower()
        for kw in keywords:
            kw_lower = kw.lower()
            if kw_lower in game_name or game_name in kw_lower:
                matched.append(record)
                break
    return matched

class GenerateReportTool(BaseTool):
    name: str = "generate_report"
    description: str = (
        "生成数据分析报告（Excel 格式）。"
        "需要 prompt(分析提示词)、data_source(数据源标签) 或 record_keys(指定记录)。"
        "template 可选: general_game / steam_game / taptap_game"
    )
    args_schema: Type[BaseModel] = GenerateReportInput

    async def _arun(
        self,
        prompt: str,
        data_source: str = "",
        template: str = "general_game",
        record_keys: list[str] | None = None,
    ) -> str:
        from src.web.app import report_generator
        from src.storage.local_store import LocalStorage

        record_keys = record_keys or []
        records = None
        metadata = None

        store = LocalStorage()
        await store.initialize()
        try:
            if record_keys:
                records = []
                for key in record_keys:
                    record = await store.load(key)
                    if record is None:
                        return json.dumps(
                            {"success": False, "error": f"数据记录不存在: {key}"},
                            ensure_ascii=False,
                        )
                    records.append(record)
                metadata = {"selected_record_keys": record_keys}
            else:
                keywords = _extract_prompt_keywords(prompt)
                
                # OPTIMIZATION: If we have keywords, query the storage directly with the first keyword to reduce memory load
                # LocalStorage query supports searching by 'query' which matches key or source
                if keywords:
                    primary_keyword = keywords[0]
                    # We pass the keyword to `query` method to reduce the result set at SQLite level
                    result = await store.query(primary_keyword, limit=500)
                else:
                    result = await store.query("key:", limit=2000)
                    
                all_records = result.records
                if not all_records:
                    return json.dumps(
                        {"success": False, "error": "系统中没有找到相关数据记录，请先采集数据"},
                        ensure_ascii=False,
                    )

                if keywords:
                    matched = _filter_records_by_keywords(all_records, keywords)
                    if matched:
                        records = matched
                    else:
                        return json.dumps(
                            {
                                "success": False,
                                "error": (
                                    f"未找到与 '{' '.join(keywords)}' 相关的数据记录。"
                                    f"请检查游戏名称是否正确，或先执行采集任务。"
                                    f"当前查询到的游戏: {_list_available_games(all_records)}"
                                ),
                            },
                            ensure_ascii=False,
                        )
                else:
                    records = all_records

                metadata = {"selected_record_keys": [r.key for r in records]}
        finally:
            await store.close()

        try:
            result = await report_generator.generate_excel(
                prompt=prompt,
                data_source=data_source or "",
                template=template,
                records=records,
                metadata=metadata,
            )
            response = {
                "success": True,
                "report_id": result.id,
                "title": result.title,
                "matched_records": len(records),
            }
            if result.content:
                content = result.content
                if len(content) > 4000:
                    content = content[:4000] + "\n\n...(报告过长已截断，完整内容见 Excel 文件)"
                response["content"] = content
            return json.dumps(response, ensure_ascii=False)
        except Exception as e:
            logger.error(f"Agent 生成报告失败: {e}")
            return json.dumps({"success": False, "error": str(e)}, ensure_ascii=False)

    def _run(self, **kwargs) -> str:
        raise NotImplementedError("Use _arun")


class GetReportContentTool(BaseTool):
    name: str = "get_report_content"
    description: str = (
        "获取已生成报告的完整内容。需要 report_id。当用户要求查看报告详情、分析结果时使用此工具。"
    )
    args_schema: Type[BaseModel] = GetReportContentInput

    async def _arun(self, report_id: str) -> str:
        from src.web.app import report_generator

        try:
            report = await report_generator.get_report(report_id)
            if report is None:
                return json.dumps(
                    {"success": False, "error": f"报告不存在: {report_id}"},
                    ensure_ascii=False,
                )
            return json.dumps(
                {
                    "success": True,
                    "report_id": report.id,
                    "title": report.title,
                    "content": report.content,
                    "excel_path": report.excel_path,
                    "matched_records": report.matched_records,
                },
                ensure_ascii=False,
            )
        except Exception as e:
            logger.error(f"获取报告内容失败: {e}")
            return json.dumps({"success": False, "error": str(e)}, ensure_ascii=False)

    def _run(self, **kwargs) -> str:
        raise NotImplementedError("Use _arun")
