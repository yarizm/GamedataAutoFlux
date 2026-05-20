"""Agent 工具定义 —— 封装 Scheduler / ReportGenerator / LocalStorage 的内部调用"""

import json
from pathlib import Path
from typing import Any, ClassVar, Type

from langchain_core.tools import BaseTool
from loguru import logger
from pydantic import BaseModel

from src.agent.schemas import (
    CancelTaskInput,
    CollectionReviewResult,
    CollectionReviewIssue,
    CreateCronJobInput,
    CreatePipelineInput,
    CreateTaskInput,
    DeleteCronJobInput,
    DeletePipelineInput,
    GenerateReportInput,
    GetReportContentInput,
    GetTaskDetailInput,
    IdentifierConfidence,
    ListDataGamesInput,
    ListTasksInput,
    ResolveSteamAppIdInput,
    ReviewCollectionResultsInput,
    SearchDataInput,
    SearchGameIdentifiersInput,
    VerifyGameIdentifierInput,
    VerifySteamAppIdInput,
    CreateDynamicPipelineInput,
)
from src.services._utils import extract_record_identity, compute_record_completeness


def _safe_json(obj: Any) -> str:
    """序列化为 JSON 字符串，处理 Pydantic 模型与 datetime"""
    if hasattr(obj, "model_dump"):
        obj = obj.model_dump(mode="json")
    elif isinstance(obj, list):
        obj = [
            item.model_dump(mode="json") if hasattr(item, "model_dump") else item for item in obj
        ]
    return json.dumps(obj, ensure_ascii=False, default=str)


def _format_result(
    status: str,
    summary: str,
    data: Any = None,
    *,
    record_count: int | None = None,
    warnings: list[str] | None = None,
    suggestion: str = "",
    max_data_length: int = 4000,
) -> str:
    """构建结构化的工具返回结果，提供 summary/data/suggestion 三层可读性"""
    result: dict[str, Any] = {
        "status": status,
        "summary": summary,
    }
    if record_count is not None:
        result["record_count"] = record_count
    if warnings:
        result["warnings"] = warnings
    if suggestion:
        result["suggestion"] = suggestion

    if data is not None:
        serialized = _safe_json(data)
        if len(serialized) > max_data_length:
            result["data_truncated"] = True
            result["summary"] = summary + "（数据量过大，已截断，请进一步查询）"
        else:
            result["data"] = data

    return json.dumps(result, ensure_ascii=False, default=str)


# ==================== 任务相关工具 ====================


class ListTasksTool(BaseTool):
    name: str = "list_tasks"
    description: str = (
        "获取任务列表，可按状态过滤。"
        "status 可选值: pending / running / success / failed / cancelled"
    )
    args_schema: Type[BaseModel] = ListTasksInput

    async def _arun(self, status: str | None = None) -> str:
        from src.web.app import get_task_service

        try:
            tasks = get_task_service().list_tasks(status)
        except ValueError:
            return _format_result(
                "error",
                f"无效的状态: {status}",
                suggestion="status 可选: pending / running / success / failed / cancelled",
            )

        summaries = [t.to_summary() for t in tasks[:50]]
        status_counts = {}
        for t in tasks:
            s = t.status.value if hasattr(t.status, "value") else str(t.status)
            status_counts[s] = status_counts.get(s, 0) + 1
        count_desc = ", ".join(f"{v} {k}" for k, v in sorted(status_counts.items()))
        return _format_result(
            "ok",
            f"共 {len(tasks)} 个任务（{count_desc}），展示最近 {len(summaries)} 个",
            summaries,
            record_count=len(summaries),
            suggestion="使用 get_task_detail 查看任意任务详情",
        )

    def _run(self, status: str | None = None) -> str:
        raise NotImplementedError("Use _arun")


class GetTaskDetailTool(BaseTool):
    name: str = "get_task_detail"
    description: str = "获取单个任务的详细信息，包括步骤日志和结果摘要"
    args_schema: Type[BaseModel] = GetTaskDetailInput

    async def _arun(self, task_id: str) -> str:
        from src.web.app import get_task_service

        task = get_task_service().get_task(task_id)
        if not task:
            return _format_result(
                "error", f"任务不存在: {task_id}", suggestion="使用 list_tasks 查看所有任务"
            )
        payload = task.to_storage_payload()
        status = payload.get("status", "unknown")
        return _format_result(
            "ok",
            f"任务 '{payload.get('name', task_id)}' 当前状态: {status}",
            payload,
            record_count=1,
            suggestion="使用 generate_report 为此任务的数据生成报告" if status == "success" else "",
        )

    def _run(self, task_id: str) -> str:
        raise NotImplementedError("Use _arun")


class CreateTaskTool(BaseTool):
    name: str = "create_task"
    description: str = (
        "创建并提交一个新的数据采集任务。"
        "需要指定任务名称(name)、Pipeline 模板 ID(pipeline_name)和采集目标(targets)。"
        'targets 格式: [{"name": "游戏名", "target_type": "game", "params": {"app_id": 123}}]。'
        "config 可选，支持 report.enabled / data_group 等配置。"
    )
    args_schema: Type[BaseModel] = CreateTaskInput

    async def _arun(
        self,
        name: str,
        pipeline_name: str,
        targets: list[dict] | None = None,
        collector_name: str = "",
        config: dict | None = None,
    ) -> str:
        from src.web.app import get_task_service

        targets = targets or []
        config = config or {}

        ts = get_task_service()

        # 自动发现缺失的平台标识符
        targets = await _auto_fill_identifiers(targets, pipeline_name)

        # 提交前预校验
        precheck = ts.precheck(
            name=name,
            pipeline_name=pipeline_name,
            collector_name=collector_name,
            targets=targets,
        )
        if not precheck.can_submit:
            issues_desc = "; ".join(f"[{i.level}] {i.field}: {i.message}" for i in precheck.issues)
            return _format_result(
                "error",
                f"任务创建预校验失败: {issues_desc}",
                [
                    {"level": i.level, "code": i.code, "field": i.field, "message": i.message}
                    for i in precheck.issues
                ],
                warnings=[i.message for i in precheck.issues if i.level == "warning"],
                suggestion="请补充必填字段后重试。必填字段: " + ", ".join(precheck.required_fields),
            )

        try:
            task = await ts.create(
                name=name,
                pipeline_name=pipeline_name,
                collector_name=collector_name,
                targets=targets,
                config=config,
            )
            response = {
                "success": True,
                "task_id": task.id,
                "task_name": name,
                "pipeline": pipeline_name,
            }
            warnings = [i.message for i in precheck.issues if i.level == "warning"]
            return _format_result(
                "ok",
                f"任务 '{name}' 已创建并提交，task_id: {task.id}",
                response,
                record_count=1,
                warnings=warnings if warnings else None,
                suggestion="使用 list_tasks 查看任务状态，或使用 get_task_detail 查看详情",
            )
        except Exception as e:
            from src.core.errors import classify_exception, error_summary

            code = classify_exception(e)
            logger.error(f"Agent 创建任务失败: [{code.value}] {e}")
            return _format_result(
                "error",
                f"任务提交失败: {e}",
                error_summary(code, str(e)),
                suggestion=error_summary(code)["suggestion"],
            )

    def _run(self, **kwargs) -> str:
        raise NotImplementedError("Use _arun")


class CancelTaskTool(BaseTool):
    name: str = "cancel_task"
    description: str = "取消一个正在运行或等待中的任务"
    args_schema: Type[BaseModel] = CancelTaskInput

    async def _arun(self, task_id: str) -> str:
        from src.web.app import get_task_service

        ok = await get_task_service().cancel(task_id)
        if ok:
            return _format_result("ok", f"任务已取消: {task_id}")
        return _format_result("error", f"取消失败（任务可能已结束或不存在）: {task_id}")

    def _run(self, task_id: str) -> str:
        raise NotImplementedError("Use _arun")


# ==================== Pipeline 相关工具 ====================


class ListPipelineTemplatesTool(BaseTool):
    name: str = "list_pipeline_templates"
    description: str = "获取所有可用的 Pipeline 模板列表，包含模板 ID、名称、描述和步骤配置"

    async def _arun(self) -> str:
        from src.core.pipeline_templates import PIPELINE_TEMPLATES

        # 只返回摘要信息，不包含完整 steps
        summaries = [
            {"id": t["id"], "name": t["name"], "description": t["description"]}
            for t in PIPELINE_TEMPLATES
        ]
        return _safe_json(summaries)

    def _run(self) -> str:
        raise NotImplementedError("Use _arun")


class ListPipelinesTool(BaseTool):
    name: str = "list_pipelines"
    description: str = "获取已保存的自定义 Pipeline 列表"

    async def _arun(self) -> str:
        from src.web.app import scheduler

        pipelines = scheduler.get_all_pipelines()
        summaries = [{"name": p.name, "steps": len(p.steps)} for p in pipelines]
        return _safe_json(summaries)

    def _run(self) -> str:
        raise NotImplementedError("Use _arun")


class CreatePipelineTool(BaseTool):
    name: str = "create_pipeline"
    description: str = (
        "创建自定义 Pipeline。"
        'steps 格式: [{"type": "collector/processor/storage", "name": "组件名", "config": {}}]。'
        "可用组件用 list_pipeline_templates 查看。"
    )
    args_schema: Type[BaseModel] = CreatePipelineInput

    async def _arun(self, name: str, steps: list[dict]) -> str:
        from src.core.pipeline import Pipeline, StepType
        from src.web.app import scheduler

        pipeline = Pipeline(name)
        for step in steps:
            step_type = StepType(step["type"])
            step_name = step["name"]
            step_config = step.get("config", {})
            if step_type == StepType.COLLECTOR:
                pipeline.add_collector(step_name, config=step_config)
            elif step_type == StepType.PROCESSOR:
                pipeline.add_processor(step_name, config=step_config)
            elif step_type == StepType.STORAGE:
                pipeline.add_storage(step_name, config=step_config)

        try:
            await scheduler.save_pipeline(pipeline)
            return _format_result(
                "ok",
                f"Pipeline '{name}' 已创建，包含 {len(steps)} 个步骤",
                {"name": name, "steps_count": len(steps)},
                record_count=1,
            )
        except Exception as e:
            return _format_result("error", f"创建 Pipeline 失败: {e}")

    def _run(self, **kwargs) -> str:
        raise NotImplementedError("Use _arun")


class DeletePipelineTool(BaseTool):
    name: str = "delete_pipeline"
    description: str = "删除一个已保存的 Pipeline"
    args_schema: Type[BaseModel] = DeletePipelineInput

    async def _arun(self, name: str, confirm: bool = False) -> str:
        from src.web.app import scheduler

        if not confirm:
            return _format_result(
                "warning", "高风险操作已取消", suggestion="确认后重新调用并传入 confirm=true"
            )
        ok = await scheduler.delete_pipeline(name)
        if ok:
            return _format_result("ok", f"Pipeline '{name}' 已删除")
        return _format_result("error", f"删除失败，Pipeline '{name}' 不存在")

    def _run(self, name: str, confirm: bool = False) -> str:
        raise NotImplementedError("Use _arun")


class CreateDynamicPipelineTool(BaseTool):
    name: str = "create_dynamic_pipeline"
    description: str = (
        "创建一个基于 Playwright 的动态网页数据采集 Pipeline。\n"
        "该工具会自动配置 Playwright 采集器 (dynamic_playwright) -> 降噪清洗处理器 (cleaner) "
        "-> 本地 SQLite 存储 (local) -> 向量数据库存储 (vector)。\n"
        "专为需要通过执行 JS 脚本进行交互或自定义提取的动态网页（如单页应用、需要复杂交互的页面）设计。\n"
        "生成的 Pipeline 可以直接通过 create_task 来运行。"
    )
    args_schema: Type[BaseModel] = CreateDynamicPipelineInput

    async def _arun(
        self,
        pipeline_name: str,
        url: str,
        wait_strategy_type: str = "networkidle",
        wait_strategy_selector: str | None = None,
        js_script: str = "",
    ) -> str:
        from src.core.pipeline import Pipeline
        from src.web.app import scheduler

        # 1. 组装 Playwright 采集器配置
        collector_config = {
            "url": url,
            "extraction_mode": "js_evaluate",
            "js_script": js_script,
            "wait_strategy": {
                "type": wait_strategy_type,
                "timeout_ms": 10000,
            },
        }
        if wait_strategy_type == "selector" and wait_strategy_selector:
            collector_config["wait_strategy"]["selector"] = wait_strategy_selector

        # 2. 构建 Pipeline 结构
        pipeline = (
            Pipeline(pipeline_name)
            .add_collector("dynamic_playwright", config=collector_config)
            .add_processor("cleaner")
            .add_storage("local")
            .add_storage("vector")
        )

        try:
            await scheduler.save_pipeline(pipeline)
            return _format_result(
                "ok",
                f"动态 Pipeline '{pipeline_name}' 已成功创建并保存！\n"
                f"流程配置: Playwright 采集器 -> cleaner 处理器 -> local/vector 存储。\n"
                f"你可以直接使用 create_task 创建任务，将 pipeline_name 设置为 '{pipeline_name}' 来执行采集。",
                {"pipeline_name": pipeline_name, "steps_count": 4},
                record_count=1,
            )
        except Exception as e:
            return _format_result("error", f"创建动态 Pipeline 失败: {e}")

    def _run(self, **kwargs) -> str:
        raise NotImplementedError("Use _arun")



# ==================== 定时任务相关工具 ====================


class ListCronJobsTool(BaseTool):
    name: str = "list_cron_jobs"
    description: str = "获取所有定时任务的列表"

    async def _arun(self) -> str:
        from src.web.app import scheduler

        jobs = scheduler.list_cron_jobs()
        return _safe_json(jobs)

    def _run(self) -> str:
        raise NotImplementedError("Use _arun")


class CreateCronJobTool(BaseTool):
    name: str = "create_cron_job"
    description: str = (
        "创建定时采集任务。"
        "cron_expr 是5段式 cron 表达式，如 '0 8 * * *' 表示每天上午8点。"
        "task_template 可包含 name, targets, config 等。"
    )
    args_schema: Type[BaseModel] = CreateCronJobInput

    async def _arun(
        self,
        name: str,
        pipeline_name: str,
        cron_expr: str,
        task_template: dict | None = None,
        confirm: bool = False,
    ) -> str:
        from src.web.app import scheduler

        if not confirm:
            return _format_result(
                "warning", "高风险操作已取消", suggestion="确认后重新调用并传入 confirm=true"
            )
        try:
            job_id = scheduler.add_cron_job(
                name=name,
                pipeline_name=pipeline_name,
                cron_expr=cron_expr,
                task_template=task_template,
            )
            return _format_result(
                "ok",
                f"定时任务 '{name}' 已创建",
                {"job_id": job_id, "name": name, "cron": cron_expr},
                record_count=1,
            )
        except Exception as e:
            return _format_result("error", f"创建定时任务失败: {e}")

    def _run(self, **kwargs) -> str:
        raise NotImplementedError("Use _arun")


class DeleteCronJobTool(BaseTool):
    name: str = "delete_cron_job"
    description: str = "删除一个定时任务"
    args_schema: Type[BaseModel] = DeleteCronJobInput

    async def _arun(self, name: str, confirm: bool = False) -> str:
        from src.web.app import scheduler

        if not confirm:
            return _format_result(
                "warning", "高风险操作已取消", suggestion="确认后重新调用并传入 confirm=true"
            )
        ok = scheduler.remove_cron_job(name)
        if ok:
            return _format_result("ok", f"定时任务 '{name}' 已删除")
        return _format_result("error", f"删除失败，定时任务 '{name}' 不存在")

    def _run(self, name: str, confirm: bool = False) -> str:
        raise NotImplementedError("Use _arun")


# ==================== 数据浏览相关工具 ====================

# extract_record_identity, nested_get, first_str are now in src.services._utils


def _extract_prompt_keywords(prompt: str) -> list[str]:
    """从 prompt 中提取有意义的搜索关键词（过滤掉常见的语气词/助词）"""
    import re

    stop_words = {
        "帮我",
        "生成",
        "报告",
        "一个",
        "一份",
        "的",
        "了",
        "是",
        "在",
        "和",
        "请",
        "要",
        "需要",
        "分析",
        "综合",
        "全面",
        "关于",
        "对于",
        "这个",
        "include",
        "report",
        "generate",
        "for",
        "the",
        "a",
        "an",
    }
    split_pattern = re.compile(
        r"[，。！？、；：（）\s"
        r"请对|进行|包括|并提|要求|帮我|生成|分析|综合|全面|完整|详细"
        r"]+|[a-zA-Z]{2,}"
    )
    # 提取中文片段（2字以上）
    raw_parts = re.findall(r"[一-鿿]{2,}", prompt)
    keywords = []
    for part in raw_parts:
        sub_parts = split_pattern.split(part)
        for sub in sub_parts:
            sub = sub.strip()
            if len(sub) >= 2 and sub not in stop_words:
                keywords.append(sub)
    # 提取英文/数字关键词（2字符以上）
    eng_tokens = re.findall(r"[a-zA-Z0-9]{2,}", prompt.lower())
    for token in eng_tokens:
        if token not in stop_words:
            keywords.append(token)
    # 去重并保持顺序
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
            # 双向匹配：关键词包含游戏名，或游戏名包含关键词
            if kw_lower in game_name or game_name in kw_lower:
                matched.append(record)
                break
    return matched


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
        from src.storage.local_store import LocalStorage

        store = LocalStorage()
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
        from src.storage.local_store import LocalStorage

        store = LocalStorage()
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


# ==================== 报告相关工具 ====================


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
                # 未指定 record_keys 时，从 prompt 提取关键词过滤数据
                result = await store.query("key:", limit=2000)
                all_records = result.records
                if not all_records:
                    return json.dumps(
                        {"success": False, "error": "系统中没有数据记录，请先采集数据"},
                        ensure_ascii=False,
                    )

                # 按 prompt 中的关键词过滤记录（匹配 game_name）
                keywords = _extract_prompt_keywords(prompt)
                if keywords:
                    matched = _filter_records_by_keywords(all_records, keywords)
                    if matched:
                        records = matched
                    else:
                        # 无匹配时返回提示，避免生成无关数据报告
                        return json.dumps(
                            {
                                "success": False,
                                "error": (
                                    f"未找到与 '{' '.join(keywords)}' 相关的数据记录。"
                                    f"请检查游戏名称是否正确，或先执行采集任务。"
                                    f"当前可用的游戏: {_list_available_games(all_records)}"
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
            # 附带报告正文内容，避免 Agent 额外调用工具获取
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


# ==================== Steam App ID 搜索工具 ====================


class ResolveSteamAppIdTool(BaseTool):
    name: str = "resolve_steam_app_id"
    description: str = (
        "按游戏名称搜索 Steam App ID。支持中文或英文游戏名，返回精确或模糊匹配结果。"
        "创建 Steam 采集任务前必须使用此工具获取正确的 app_id，不要凭记忆猜测。"
    )
    args_schema: Type[BaseModel] = ResolveSteamAppIdInput

    # Steam 官方公开搜索 API（免费，无需 API Key），每页最多 50 条
    STORE_SEARCH_URL: ClassVar[str] = "https://store.steampowered.com/api/storesearch/"
    COMMUNITY_SEARCH_URL: ClassVar[str] = "https://steamcommunity.com/actions/SearchApps/"

    async def _arun(self, game_name: str) -> str:
        import httpx

        all_items: list[dict] = []
        seen_ids: set[int] = set()

        def add_items(items: list[dict], key_id: str = "app_id"):
            for item in items:
                app_id = item.get(key_id, 0)
                name = item.get("name", "")
                if not app_id or not name:
                    continue
                if app_id in seen_ids:
                    continue
                seen_ids.add(app_id)
                all_items.append({"app_id": app_id, "name": name})

        async with httpx.AsyncClient(
            timeout=10,
            follow_redirects=True,
            headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"},
        ) as client:
            # 方案 1: Community SearchApps（快速、干净，支持英文名和混合名）
            community_items = await self._search_community(client, game_name)
            add_items(community_items, key_id="appid")

            # 方案 2: Store search English（最全面的英文游戏搜索）
            store_en = await self._search_store(client, game_name, language="english", cc="us")
            add_items(store_en, key_id="id")

            # 方案 3: Store search 简体中文（纯中文名搜索）
            store_cn = await self._search_store(client, game_name, language="schinese", cc="cn")
            add_items(store_cn, key_id="id")

        if all_items:
            return _safe_json({"found": True, "source": "steam_api", "results": all_items[:10]})

        # 方案 4: 本地缓存兜底
        cache_file = Path("data/steam_app_list.json")
        if cache_file.exists():
            cache_result = self._search_cache(cache_file, game_name)
            if cache_result:
                return cache_result
            return json.dumps(
                {
                    "found": False,
                    "source": "cache",
                    "message": f"所有在线 API 及本地缓存均未找到 '{game_name}'，请尝试英文名或手动提供 app_id",
                },
                ensure_ascii=False,
            )

        return json.dumps(
            {
                "found": False,
                "error": "所有 Steam 搜索 API 均不可用且本地缓存不存在。"
                "请尝试英文名搜索，或手动提供 app_id。",
            },
            ensure_ascii=False,
        )

    async def _search_community(self, client, game_name: str) -> list[dict]:
        """Steam Community SearchApps —— 快速、免费、无需 API Key"""
        try:
            resp = await client.get(f"{self.COMMUNITY_SEARCH_URL}{game_name}")
            resp.raise_for_status()
            data = resp.json()
            if isinstance(data, list):
                return data
        except Exception:
            pass
        return []

    async def _search_store(self, client, game_name: str, language: str, cc: str) -> list[dict]:
        """Steam 公开商店搜索 API"""
        try:
            resp = await client.get(
                self.STORE_SEARCH_URL,
                params={"term": game_name, "l": language, "cc": cc},
            )
            resp.raise_for_status()
            data = resp.json()
            return data.get("items", [])
        except Exception:
            return []

    @staticmethod
    def _search_cache(cache_file: Path, game_name: str) -> str | None:
        """在本地缓存文件中搜索"""
        try:
            data = json.loads(cache_file.read_text(encoding="utf-8"))
            apps = data if isinstance(data, list) else data.get("apps", [])
        except Exception:
            return None

        name_lower = game_name.lower().strip()
        exact: list[dict] = []
        fuzzy: list[dict] = []

        for app in apps:
            app_name = app.get("name", "")
            if not app_name:
                continue
            raw_app_id = app.get("appid") or app.get("app_id", 0)
            if app_name.lower().strip() == name_lower:
                exact.append({"app_id": raw_app_id, "name": app_name})
            elif name_lower in app_name.lower():
                fuzzy.append({"app_id": raw_app_id, "name": app_name})
                if len(fuzzy) >= 30:
                    break

        results = exact + fuzzy
        if not results:
            return None

        seen: set[int] = set()
        filtered: list[dict] = []
        for item in results:
            if item["app_id"] in seen:
                continue
            seen.add(item["app_id"])
            filtered.append(item)

        return _safe_json({"found": True, "source": "cache", "results": filtered[:10]})

    def _run(self, **kwargs) -> str:
        raise NotImplementedError("Use _arun")


class VerifySteamAppIdTool(BaseTool):
    name: str = "verify_steam_app_id"
    description: str = (
        "通过 Steam Store API 验证一个 App ID 是否有效，返回游戏名称。"
        "用于确认 resolve_steam_app_id 返回的 app_id 是否正确。"
    )
    args_schema: Type[BaseModel] = VerifySteamAppIdInput

    async def _arun(self, app_id: int) -> str:
        import httpx

        try:
            async with httpx.AsyncClient(
                timeout=10,
                follow_redirects=True,
                headers={
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
                },
            ) as client:
                resp = await client.get(
                    "https://store.steampowered.com/api/appdetails",
                    params={"appids": str(app_id)},
                )
                resp.raise_for_status()
                data = resp.json()
                entry = data.get(str(app_id), {})
                if entry.get("success"):
                    name = entry["data"].get("name", "")
                    return json.dumps(
                        {"valid": True, "app_id": app_id, "name": name}, ensure_ascii=False
                    )
                return json.dumps({"valid": False, "app_id": app_id}, ensure_ascii=False)
        except Exception as e:
            return json.dumps(
                {"valid": False, "app_id": app_id, "error": str(e)}, ensure_ascii=False
            )

    def _run(self, **kwargs) -> str:
        raise NotImplementedError("Use _arun")


# ==================== 系统概览工具 ====================


class GetSystemStatsTool(BaseTool):
    name: str = "get_system_stats"
    description: str = "获取系统概览统计信息：任务总数、运行中数量、定时任务数等"

    async def _arun(self) -> str:
        from src.web.app import scheduler

        stats = scheduler.get_stats()
        total = stats.get("total_tasks", 0)
        running = stats.get("running_tasks", 0)
        return _format_result(
            "ok",
            f"系统概览: {total} 个任务记录，{running} 个运行中",
            stats,
            record_count=total,
            suggestion="使用 list_tasks 查看任务列表，使用 list_data_games 浏览数据",
        )

    def _run(self) -> str:
        raise NotImplementedError("Use _arun")


# ==================== 游戏标识符自动发现工具 ====================


class SearchGameIdentifiersTool(BaseTool):
    name: str = "search_game_identifiers"
    description: str = (
        "给定游戏名称，自动搜索所有平台的标识符（Steam App ID, TapTap ID, "
        "Qimai App ID, Monitor siteurl, 官网 URL 等）。"
        "返回结构化结果，包含每个平台的置信度评分（high/medium/low）和可用候选项。"
        "创建采集任务前如果缺少平台标识符，应优先调用此工具。"
    )
    args_schema: Type[BaseModel] = SearchGameIdentifiersInput

    async def _arun(self, game_name: str, platforms: list[str] | None = None) -> str:
        from src.services.game_resolver import GameIdentifierResolver

        resolver = GameIdentifierResolver()
        try:
            result = await resolver.resolve_all(game_name, platforms)
            data = result.model_dump(mode="json", exclude_none=True)
            high = result.high_confidence()
            missing = [
                p
                for p in ("steam", "taptap", "qimai", "monitor", "official_site")
                if getattr(result, p, None) is None
            ]
            return _format_result(
                "ok",
                f"已搜索 '{game_name}' 的平台标识符: {len(high)} 个高置信度, {len(missing)} 个未找到",
                data,
                record_count=len(result.found_platforms()),
                suggestion=(
                    f"高置信度平台: {', '.join(high)}。可直接创建采集任务。"
                    if high
                    else "部分平台置信度较低，建议向用户确认后创建任务"
                ),
            )
        except Exception as e:
            return _format_result("error", f"搜索标识符失败: {e}")
        finally:
            await resolver.teardown()

    def _run(self, **kwargs) -> str:
        raise NotImplementedError("Use _arun")


class VerifyGameIdentifierTool(BaseTool):
    name: str = "verify_game_identifier"
    description: str = (
        "验证单个平台的标识符是否有效并对应预期的游戏名称。"
        "用于确认 search_game_identifiers 返回的标识符是否正确。"
    )
    args_schema: Type[BaseModel] = VerifyGameIdentifierInput

    async def _arun(self, platform: str, identifier: str, game_name: str) -> str:
        from src.services.game_resolver import GameIdentifierResolver

        resolver = GameIdentifierResolver()
        try:
            await resolver.setup()
            result = await resolver.verify_identifier(platform, identifier, game_name)
            return json.dumps(result, ensure_ascii=False)
        except Exception as e:
            return json.dumps({"valid": False, "error": str(e)}, ensure_ascii=False)
        finally:
            await resolver.teardown()

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
        from src.storage.local_store import LocalStorage
        from src.core.task import TaskStatus

        task = get_task_service().get_task(task_id)
        if task is None:
            return _format_result("error", f"任务不存在: {task_id}")

        store = LocalStorage()
        await store.initialize()
        try:
            issues: list[CollectionReviewIssue] = []
            records: list = []

            # 检查任务状态
            if task.status in (TaskStatus.FAILED, TaskStatus.CANCELLED):
                issues.append(
                    CollectionReviewIssue(
                        level="error",
                        category="task_failed",
                        message=f"任务状态: {task.status.value}，错误: {task.error or '未知'}",
                    )
                )

            # 查询关联的数据记录
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

            # 汇总
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


# ==================== 工具列表 ====================


async def _auto_fill_identifiers(targets: list[dict], pipeline_name: str) -> list[dict]:
    """在创建任务前自动发现缺失的平台标识符（仅 HIGH 置信度时自动填充）。

    如果 Playwright 不可用（如 Windows SelectorEventLoop 限制），静默跳过。
    """
    from src.services.game_resolver import GameIdentifierResolver

    # 快速检查：pipeline 类型是否可能需要自动填充
    needs_resolve = any(
        pipeline_name.startswith(prefix)
        for prefix in ("steam", "taptap", "monitor", "qimai", "official_site")
    )
    if not needs_resolve:
        return targets

    resolver = GameIdentifierResolver()
    try:
        await resolver.setup()
    except (NotImplementedError, RuntimeError, OSError) as e:
        logger.warning(f"标识符自动填充跳过 (Playwright/浏览器不可用): {e}")
        return targets

    try:
        for target in targets:
            params = dict(target.get("params", {}) or {})
            name = str(target.get("name", "") or "").strip()
            if not name:
                continue

            if pipeline_name.startswith("steam") and not params.get("app_id"):
                result = await resolver.resolve_steam(name)
                if result and result.confidence == IdentifierConfidence.HIGH:
                    params["app_id"] = int(result.identifier)
                    target["params"] = params

            elif (
                pipeline_name.startswith("taptap")
                and not params.get("app_id")
                and not params.get("url")
            ):
                result = await resolver.resolve_taptap(name)
                if result and result.confidence == IdentifierConfidence.HIGH:
                    params["app_id"] = result.identifier
                    target["params"] = params

            elif pipeline_name.startswith("monitor") and not params.get("siteurl"):
                result = await resolver.resolve_monitor_name(name)
                if result and result.confidence == IdentifierConfidence.HIGH:
                    params["siteurl"] = result.identifier
                    target["params"] = params

            elif (
                pipeline_name.startswith("qimai")
                and not params.get("app_id")
                and not params.get("qimai_app_id")
            ):
                result = await resolver.resolve_qimai(name)
                if result and result.confidence == IdentifierConfidence.HIGH:
                    params["qimai_app_id"] = result.identifier
                    target["params"] = params

            elif pipeline_name.startswith("official_site") and not params.get("official_url"):
                result = await resolver.resolve_official_site(name)
                if result and result.confidence == IdentifierConfidence.HIGH:
                    params["official_url"] = result.identifier
                    target["params"] = params
    finally:
        await resolver.teardown()
    return targets


ALL_TOOLS: list[BaseTool] = [
    ResolveSteamAppIdTool(),
    VerifySteamAppIdTool(),
    ListTasksTool(),
    GetTaskDetailTool(),
    CreateTaskTool(),
    CancelTaskTool(),
    ListPipelineTemplatesTool(),
    ListPipelinesTool(),
    CreatePipelineTool(),
    DeletePipelineTool(),
    CreateDynamicPipelineTool(),
    ListCronJobsTool(),
    CreateCronJobTool(),
    DeleteCronJobTool(),
    ListDataGamesTool(),
    SearchDataTool(),
    GenerateReportTool(),
    GetReportContentTool(),
    GetSystemStatsTool(),
    SearchGameIdentifiersTool(),
    VerifyGameIdentifierTool(),
    ReviewCollectionResultsTool(),
]
