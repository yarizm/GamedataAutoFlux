"""
数据浏览相关工具
"""

from typing import Type
from langchain_core.tools import BaseTool
from pydantic import BaseModel

from src.agent.schemas import (
    ListDataGamesInput,
    ReviewCollectionResultsInput,
    SearchDataInput,
    CollectionReviewResult,
    CollectionReviewIssue,
)
from src.agent.tools.utils import _format_result, _safe_json
from src.services._utils import extract_record_identity, compute_record_completeness


def _list_available_games(records: list) -> str:
    """列出 records 中可用的游戏名（去重，最多 10 个）"""
    games: dict[str, str] = {}
    for record in records:
        identity = extract_record_identity(record)
        if not identity:
            continue
        name = identity.get("game_name", "")
        if name and name not in games:
            games[name] = identity.get("collector", "")
    names = list(games.keys())[:10]
    if len(games) > 10:
        return ", ".join(names) + f" 等{len(games)}款"
    return ", ".join(names) if names else "无"


class ListDataGamesTool(BaseTool):
    name: str = "list_data_games"
    description: str = "浏览已采集数据的游戏分类列表"
    args_schema: Type[BaseModel] = ListDataGamesInput

    async def _arun(self, limit: int = 50) -> str:
        from src.storage.factory import get_storage

        store = get_storage()
        await store.initialize()
        try:
            result = await store.query("key:", limit=limit)
            games: dict[str, list[str]] = {}
            for record in result.records:
                identity = extract_record_identity(record)
                if not identity:
                    continue
                name = identity["game_name"]
                src = identity["data_source"]
                if name not in games:
                    games[name] = []
                if src not in games[name]:
                    games[name].append(src)

            items = [{"game": g, "sources": s} for g, s in sorted(games.items())[:limit]]
            if not items:
                return _format_result(
                    "empty",
                    "系统中暂无已采集的数据",
                    suggestion="使用 create_task 创建采集任务来获取数据",
                )
            return _format_result(
                "ok",
                f"共 {len(games)} 个游戏分类，展示前 {len(items)} 个",
                items,
                record_count=len(items),
                suggestion="使用 search_data 按关键词搜索，或使用 generate_report 生成报告",
            )
        finally:
            await store.close()

    def _run(self, **kwargs) -> str:
        raise NotImplementedError("Use _arun")


class SearchDataTool(BaseTool):
    name: str = "search_data"
    description: str = "搜索已采集的数据，按关键词匹配"
    args_schema: Type[BaseModel] = SearchDataInput

    async def _arun(self, query: str, limit: int = 20) -> str:
        from src.storage.factory import get_storage

        store = get_storage()
        await store.initialize()
        try:
            result = await store.query(query, limit=limit)
            summaries = []
            for record in result.records[:limit]:
                identity = extract_record_identity(record)
                summaries.append(
                    {
                        "key": record.key,
                        "source": record.source,
                        "game": identity.get("game_name", "") if identity else "",
                        "app_id": identity.get("app_id", "") if identity else "",
                        "stored_at": str(record.stored_at) if record.stored_at else "",
                    }
                )
            if not summaries:
                return _format_result(
                    "empty",
                    f"未找到匹配 '{query}' 的数据记录",
                    suggestion="尝试使用 list_data_games 查看可用游戏，或先执行采集任务",
                )
            games = list({s["game"] for s in summaries if s["game"]})
            return _format_result(
                "ok",
                f"找到 {result.total} 条记录（展示前 {len(summaries)} 条），涉及游戏: {', '.join(games[:5])}",
                summaries,
                record_count=result.total,
                suggestion="使用 generate_report 为这些数据生成分析报告",
            )
        finally:
            await store.close()

    def _run(self, **kwargs) -> str:
        raise NotImplementedError("Use _arun")


class ReviewCollectionResultsTool(BaseTool):
    name: str = "review_collection_results"
    description: str = (
        "审查已完成采集任务的结果完整性和正确性。"
        "检查数据是否存在、关键字段是否缺失、标识符是否匹配。"
        "如果 auto_retry=true 且发现问题，会自动创建修正后的重试任务。"
    )
    args_schema: Type[BaseModel] = ReviewCollectionResultsInput

    async def _arun(self, task_id: str, auto_retry: bool = False) -> str:
        from src.web.app import get_task_service
        from src.storage.factory import get_storage
        from src.core.task import TaskStatus

        task = get_task_service().get_task(task_id)
        if task is None:
            return _format_result("error", f"任务不存在: {task_id}")

        store = get_storage()
        await store.initialize()
        try:
            issues: list[CollectionReviewIssue] = []
            records: list = []

            if task.status in (TaskStatus.FAILED, TaskStatus.CANCELLED):
                issues.append(
                    CollectionReviewIssue(
                        level="error",
                        category="task_failed",
                        message=f"任务状态: {task.status.value}，错误: {task.error or '未知'}",
                    )
                )

            result = await store.query(query=f"key:{task.id}", limit=50)
            records = result.records

            if not records:
                issues.append(
                    CollectionReviewIssue(
                        level="error",
                        category="empty_result",
                        message="任务完成但未找到存储的数据记录",
                    )
                )
            else:
                for record in records:
                    completeness = compute_record_completeness(record) or "unknown"
                    if completeness == "empty":
                        issues.append(
                            CollectionReviewIssue(
                                level="warning",
                                category="empty_data",
                                message=f"记录 {record.key} 数据为空",
                            )
                        )
                    elif completeness == "partial":
                        issues.append(
                            CollectionReviewIssue(
                                level="info",
                                category="partial_data",
                                message=f"记录 {record.key} 数据部分完整",
                            )
                        )

            error_count = sum(1 for i in issues if i.level == "error")
            warning_count = sum(1 for i in issues if i.level == "warning")

            if error_count == 0 and warning_count == 0:
                completeness = "full"
            elif error_count == 0:
                completeness = "partial"
            else:
                completeness = "empty"

            suggestions: list[str] = []
            if error_count > 0:
                suggestions.append("使用 search_game_identifiers 重新发现正确的标识符后创建新任务")
            if warning_count > 0:
                suggestions.append("部分数据不完整，可考虑调整采集参数重新采集")

            review = CollectionReviewResult(
                task_id=task_id,
                task_name=task.name,
                completeness=completeness,
                issues=issues,
                suggestions=suggestions,
                record_count=len(records),
            )
            return _safe_json(review.model_dump(mode="json", exclude_none=True))
        finally:
            await store.close()

    def _run(self, **kwargs) -> str:
        raise NotImplementedError("Use _arun")


class GetDataRecordContentInput(BaseModel):
    record_key: str


class GetDataRecordContentTool(BaseTool):
    name: str = "get_data_record_content"
    description: str = "获取指定数据记录(key)的完整详细内容。当从 search_data 查到关键记录并需要查看具体文本、指标等详细数据时使用此工具。"
    args_schema: Type[BaseModel] = GetDataRecordContentInput

    async def _arun(self, record_key: str) -> str:
        from src.storage.factory import get_storage

        store = get_storage()
        await store.initialize()
        try:
            record = await store.load(record_key)
            if not record:
                return _format_result("error", f"未找到数据记录: {record_key}")

            return _format_result(
                "ok",
                f"记录 {record_key} 详情内容",
                record.data,
                record_count=1,
                max_data_length=15000,
            )
        finally:
            await store.close()

    def _run(self, **kwargs) -> str:
        raise NotImplementedError("Use _arun")
