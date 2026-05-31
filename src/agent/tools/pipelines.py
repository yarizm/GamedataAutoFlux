"""
Pipeline 管理相关工具
"""

from typing import Type
from langchain_core.tools import BaseTool
from pydantic import BaseModel

from src.agent.schemas import (
    CreateDynamicPipelineInput,
    CreatePipelineInput,
    DeletePipelineInput,
)
from src.agent.tools.utils import _format_result, _safe_json


class ListPipelineTemplatesTool(BaseTool):
    name: str = "list_pipeline_templates"
    description: str = "获取所有可用的 Pipeline 模板列表，包含模板 ID、名称、描述和步骤配置"

    async def _arun(self) -> str:
        from src.core.pipeline_templates import PIPELINE_TEMPLATES

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
        "\n\n采集器 config 支持 mode 参数："
        "- fast: 纯代码提取（默认，零 LLM 成本）"
        "- smart: LLM 辅助提取/验证（适应复杂网站）"
        "- auto: 先试 fast，失败自动降级 smart"
        '示例: {"mode": "auto", "official_url": "https://..."}'
    )
    args_schema: Type[BaseModel] = CreatePipelineInput

    async def _arun(self, name: str, steps: list[dict]) -> str:
        from src.core.pipeline import Pipeline, StepType
        from src.web.app import scheduler
        from src.web.safety import validate_dynamic_playwright_config

        pipeline = Pipeline(name)
        for step in steps:
            step_type = StepType(step["type"])
            step_name = step["name"]
            step_config = step.get("config", {})
            if step_type == StepType.COLLECTOR:
                if step_name == "dynamic_playwright":
                    try:
                        validate_dynamic_playwright_config(step_config)
                    except Exception as e:
                        return _format_result("error", f"动态 Pipeline 配置不安全: {e}")
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
        "-> 数据库存储 (sqlalchemy) -> 向量数据库存储 (vector)。\n"
        "专为需要通过执行 JS 脚本进行交互或自定义提取的动态网页（如单页应用、需要复杂交互的页面）设计。\n"
        "生成的 Pipeline 可以直接通过 create_task 来运行。"
        "\n\n采集器 config 支持 mode 参数：fast/smart/auto。"
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
        from src.web.safety import validate_dynamic_playwright_config

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

        try:
            validate_dynamic_playwright_config(collector_config)
        except Exception as e:
            return _format_result("error", f"动态 Pipeline 配置不安全: {e}")

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
