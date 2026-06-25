"""
数据浏览相关工具
"""

from typing import Any, Type
from langchain_core.tools import BaseTool
from pydantic import BaseModel

from src.agent.schemas import (
    ListDataGamesInput,
    ReviewCollectionResultsInput,
    SearchDataInput,
    CollectionReviewResult,
    CollectionReviewIssue,
)
from src.agent.tools.utils import _format_result, _safe_error_text, _safe_json
from src.core.sensitive import redact_sensitive
from src.services.data_browser_service import DataBrowserService
from src.services._utils import (
    coerce_record_limit,
    compute_record_completeness,
    extract_record_identity,
    filter_source_data_records,
    is_report_history_record,
    max_iso,
    normalize_key,
    record_group,
)


def _source_scan_limit(limit: int) -> int:
    return coerce_record_limit(limit * 20, default=500, maximum=5000)


def _get_data_browser() -> DataBrowserService:
    return DataBrowserService(
        record_summary=lambda record: None,
        record_source_match_kind=lambda record, source_filter: "",
        extract_record_identity=extract_record_identity,
        record_group=record_group,
        normalize_key=normalize_key,
        redact_text=lambda value: str(value or ""),
        max_iso=max_iso,
        filter_records_by_data_source=lambda records, source: records,
        merge_app_id=lambda current, incoming: str(incoming or current or ""),
    )


def _task_targets_payload(task: Any) -> list[dict[str, Any]]:
    targets: list[dict[str, Any]] = []
    for target in getattr(task, "targets", []) or []:
        if hasattr(target, "model_dump"):
            targets.append(target.model_dump(mode="json"))
        elif isinstance(target, dict):
            targets.append(dict(target))
    return targets


def _task_collection_summary(task: Any) -> dict[str, Any] | None:
    result_summary = getattr(task, "result_summary", None)
    if isinstance(result_summary, dict):
        collection_summary = result_summary.get("collection_summary")
        if isinstance(collection_summary, dict):
            return collection_summary

    result = getattr(task, "result", None)
    collection_summary = getattr(result, "collection_summary", None)
    if isinstance(collection_summary, dict):
        return collection_summary
    return None


def _failed_retry_targets_payload(task: Any) -> list[dict[str, Any]]:
    collection_summary = _task_collection_summary(task)
    if not isinstance(collection_summary, dict):
        return []

    failed_targets = collection_summary.get("failed_targets")
    if not isinstance(failed_targets, list):
        return []

    failed_keys = {
        _retry_target_match_key(item) for item in failed_targets if isinstance(item, dict)
    }
    failed_keys.discard(None)
    if not failed_keys:
        return []

    targets = _task_targets_payload(task)
    selected = [target for target in targets if _retry_target_match_key(target) in failed_keys]
    if selected:
        return selected

    failed_names = {
        str(item.get("target") or "").strip()
        for item in failed_targets
        if isinstance(item, dict) and str(item.get("target") or "").strip()
    }
    return [target for target in targets if str(target.get("name") or "").strip() in failed_names]


def _redacted_retry_target_labels(targets: list[dict[str, Any]]) -> list[str]:
    labels: list[str] = []
    for target in targets:
        if not _target_params_have_redacted_placeholder(target):
            continue
        name = str(target.get("name") or target.get("target") or "unknown").strip()
        target_type = str(target.get("target_type") or "default").strip() or "default"
        labels.append(f"{name} ({target_type})")
    return labels


def _target_params_have_redacted_placeholder(target: dict[str, Any]) -> bool:
    params = target.get("params")
    if not isinstance(params, dict):
        params = target.get("target_params")
    return _contains_redacted_placeholder(params if isinstance(params, dict) else {})


def _contains_redacted_placeholder(value: Any) -> bool:
    if isinstance(value, dict):
        return any(_contains_redacted_placeholder(item) for item in value.values())
    if isinstance(value, list):
        return any(_contains_redacted_placeholder(item) for item in value)
    if isinstance(value, str):
        return "[REDACTED" in value.upper()
    return False


def _retry_target_match_key(target: dict[str, Any]) -> tuple[str, str, str] | None:
    name = str(target.get("target") or target.get("name") or "").strip()
    if not name:
        return None
    target_type = str(target.get("target_type") or "default").strip() or "default"
    params = target.get("target_params")
    if not isinstance(params, dict):
        params = target.get("params")
    safe_params = redact_sensitive(params if isinstance(params, dict) else {})
    return name, target_type, _stable_json_key(safe_params)


def _stable_json_key(value: Any) -> str:
    import json

    return json.dumps(value, ensure_ascii=False, sort_keys=True, default=str)


def _collection_failure_issues(
    collection_summary: dict[str, Any] | None,
) -> list[CollectionReviewIssue]:
    if not isinstance(collection_summary, dict):
        return []

    failed_targets = collection_summary.get("failed_targets")
    if not isinstance(failed_targets, list):
        return []

    issues: list[CollectionReviewIssue] = []
    for item in failed_targets[:20]:
        if not isinstance(item, dict):
            continue
        target = str(item.get("target") or "unknown")
        error_code = str(item.get("error_code") or "unknown")
        error = str(item.get("error") or "unknown error")
        retry = item.get("retry")
        retry_note = ""
        if isinstance(retry, dict):
            retry_attempts = _safe_int(retry.get("retry_attempts"))
            attempts = _safe_int(retry.get("attempts"), default=1)
            max_attempts = _safe_int(retry.get("max_attempts"), default=attempts)
            if retry_attempts > 0:
                retry_note = f" Attempts {attempts}/{max_attempts}, retries {retry_attempts}."
            retry_note += _last_retry_failure_note(retry)
        suggestion = str(item.get("suggestion") or "").strip()
        suggestion_note = f" Suggestion: {suggestion}" if suggestion else ""
        issues.append(
            CollectionReviewIssue(
                level="error",
                category="collector_failure",
                message=(
                    f"Collector failed for target {target}: [{error_code}] {error}."
                    f"{retry_note}{suggestion_note}"
                ),
            )
        )

    omitted = _safe_int(collection_summary.get("failed_targets_omitted"))
    if omitted > 0:
        issues.append(
            CollectionReviewIssue(
                level="info",
                category="collector_failure_omitted",
                message=f"{omitted} additional failed collection target(s) were omitted.",
            )
        )
    return issues


def _last_retry_failure_note(retry: dict[str, Any]) -> str:
    last_retry_error = _safe_error_text(retry.get("last_retry_error") or "").strip()
    last_retry_error_code = _safe_error_text(retry.get("last_retry_error_code") or "").strip()
    if last_retry_error and last_retry_error_code:
        return f" Last retry failed with [{last_retry_error_code}] {last_retry_error}."
    if last_retry_error:
        return f" Last retry failed: {last_retry_error}."
    if last_retry_error_code:
        return f" Last retry error code: {last_retry_error_code}."
    return ""


def _safe_int(value: Any, *, default: int = 0) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return default
    return parsed if parsed >= 0 else default


async def _create_review_retry_task(task_service: Any, task: Any) -> tuple[Any | None, str, str]:
    retry_targets = _failed_retry_targets_payload(task)
    targeted_retry = bool(retry_targets)
    targets = retry_targets or _task_targets_payload(task)
    if not getattr(task, "pipeline_name", ""):
        return (
            None,
            "Cannot auto retry because the original task has no pipeline_name.",
            "retry_failed",
        )
    if not targets:
        return None, "Cannot auto retry because the original task has no targets.", "retry_failed"

    blocked_labels = _redacted_retry_target_labels(targets)
    if blocked_labels:
        visible = ", ".join(blocked_labels[:5])
        omitted = len(blocked_labels) - 5
        omitted_note = f", and {omitted} more" if omitted > 0 else ""
        return (
            None,
            (
                "Cannot auto retry because selected target params contain redacted "
                f"sensitive values for {visible}{omitted_note}. Recreate the task "
                "with the original credentials or identifiers."
            ),
            "retry_blocked_redacted_params",
        )

    retry_name = f"{task.name} ({'targeted ' if targeted_retry else ''}review retry)"
    collector_name = getattr(task, "collector_name", "")

    precheck = task_service.precheck(
        name=retry_name,
        pipeline_name=task.pipeline_name,
        collector_name=collector_name,
        targets=targets,
    )
    if not getattr(precheck, "can_submit", False):
        issues = "; ".join(
            f"{getattr(issue, 'field', '')}: {getattr(issue, 'message', '')}"
            for issue in getattr(precheck, "issues", [])
        )
        return None, f"Auto retry precheck failed: {issues or 'unknown issue'}", "retry_failed"

    retry_task = await task_service.create(
        name=retry_name,
        pipeline_name=task.pipeline_name,
        collector_name=collector_name,
        targets=targets,
        config=dict(getattr(task, "config", {}) or {}),
        description=(
            f"Auto {'targeted ' if targeted_retry else ''}retry created by collection "
            f"review for task {task.id}."
        ),
    )
    return retry_task, "", ""


def _record_matches_task(record: Any, task_id: str) -> bool:
    metadata = record.metadata if isinstance(getattr(record, "metadata", None), dict) else {}
    source_task = metadata.get("source_task", {})
    if not isinstance(source_task, dict):
        source_task = {}
    return (
        str(getattr(record, "key", "")).startswith(task_id)
        or str(metadata.get("task_id") or "") == task_id
        or str(source_task.get("task_id") or "") == task_id
    )


async def _load_task_records(store: Any, task_id: str, limit: int = 50) -> list[Any]:
    records_by_key: dict[str, Any] = {}

    result = await store.query(query=f"key:{task_id}", limit=limit)
    for record in result.records:
        records_by_key[record.key] = record

    if len(records_by_key) < limit:
        result = await store.query(query="key:", limit=limit, task_id=task_id)
        for record in result.records:
            if _record_matches_task(record, task_id):
                records_by_key[record.key] = record

    return filter_source_data_records(list(records_by_key.values()))[:limit]


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


def _review_record_summary(record: Any, completeness: str) -> dict[str, Any]:
    identity = extract_record_identity(record) or {}
    return {
        "key": getattr(record, "key", ""),
        "source": getattr(record, "source", ""),
        "collector": identity.get("collector", ""),
        "data_source": identity.get("data_source", ""),
        "game": identity.get("game_name", ""),
        "app_id": identity.get("app_id", ""),
        "completeness": completeness,
        "stored_at": str(getattr(record, "stored_at", "") or ""),
    }


class ListDataGamesTool(BaseTool):
    name: str = "list_data_games"
    description: str = "浏览已采集数据的游戏分类列表"
    args_schema: Type[BaseModel] = ListDataGamesInput

    async def _arun(self, limit: int = 50) -> str:
        from src.storage.factory import get_storage

        limit = coerce_record_limit(limit, default=50, maximum=500)
        store = get_storage()
        await store.initialize()
        try:
            result = await store.query("key:", limit=_source_scan_limit(limit))
            items = _get_data_browser().list_game_overview(
                filter_source_data_records(result.records),
                limit=limit,
            )
            games = items
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

        limit = coerce_record_limit(limit, default=20, maximum=500)
        store = get_storage()
        await store.initialize()
        try:
            result = await store.query(query, limit=_source_scan_limit(limit))
            source_records = filter_source_data_records(result.records)
            search_payload = _get_data_browser().search_record_overview(
                source_records,
                query=query,
                limit=limit,
            )
            result.total = search_payload["total"]
            summaries = search_payload["items"]
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

        task_service = get_task_service()
        task = task_service.get_task(task_id)
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

            collection_summary = _task_collection_summary(task)
            issues.extend(_collection_failure_issues(collection_summary))

            records = await _load_task_records(store, task.id, limit=50)
            record_summaries: list[dict[str, Any]] = []
            source_coverage: dict[str, int] = {}
            completeness_counts: dict[str, int] = {}

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
                    completeness_counts[completeness] = completeness_counts.get(completeness, 0) + 1
                    summary = _review_record_summary(record, completeness)
                    record_summaries.append(summary)
                    source = summary.get("collector") or summary.get("source") or "unknown"
                    source_coverage[str(source)] = source_coverage.get(str(source), 0) + 1
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

            retry_created: bool | None = None
            retry_task_id: str | None = None
            retry_task_name: str | None = None
            retry_error: str | None = None
            retry_issue_category = "retry_failed"
            if auto_retry:
                retry_created = False
                if error_count > 0 or warning_count > 0:
                    try:
                        (
                            retry_task,
                            retry_error,
                            retry_issue_category,
                        ) = await _create_review_retry_task(task_service, task)
                    except Exception as exc:
                        retry_task = None
                        retry_error = f"Auto retry creation failed: {_safe_error_text(exc)}"
                        retry_issue_category = "retry_failed"

                    if retry_task is not None:
                        retry_created = True
                        retry_task_id = getattr(retry_task, "id", "")
                        retry_task_name = getattr(retry_task, "name", "")
                        suggestions.append(f"已自动创建重试任务: {retry_task_id}")
                    elif retry_error:
                        issues.append(
                            CollectionReviewIssue(
                                level="warning",
                                category=retry_issue_category or "retry_failed",
                                message=retry_error,
                            )
                        )
                        suggestions.append(retry_error)
                else:
                    suggestions.append("审查未发现 error/warning，未创建重试任务")

            review = CollectionReviewResult(
                task_id=task_id,
                task_name=task.name,
                completeness=completeness,
                issues=issues,
                suggestions=suggestions,
                record_count=len(records),
                record_summaries=record_summaries,
                source_coverage=source_coverage,
                completeness_counts=completeness_counts,
                collection_summary=collection_summary,
                retry_created=retry_created,
                retry_task_id=retry_task_id,
                retry_task_name=retry_task_name,
                retry_error=retry_error,
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
                return _format_result("error", "Data record not found.")

            if is_report_history_record(record):
                return _format_result(
                    "warning",
                    "This record is generated report history, not source collection data.",
                    {
                        "key": record.key,
                        "source": record.source,
                        "metadata": record.metadata,
                    },
                    record_count=0,
                    suggestion="Use get_report_content for report history, or search_data for source records.",
                )

            return _format_result(
                "ok",
                f"记录 {record_key} 详情内容",
                {
                    "key": record.key,
                    "source": record.source,
                    "stored_at": record.stored_at,
                    "metadata": record.metadata,
                    "data": record.data,
                },
                record_count=1,
                max_data_length=15000,
            )
        finally:
            await store.close()

    def _run(self, **kwargs) -> str:
        raise NotImplementedError("Use _arun")
