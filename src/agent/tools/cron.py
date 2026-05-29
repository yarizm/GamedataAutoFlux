"""
定时任务相关工具
"""

from typing import Type
from langchain_core.tools import BaseTool
from pydantic import BaseModel

from src.agent.schemas import (
    CreateCronJobInput,
    DeleteCronJobInput,
)
from src.agent.tools.utils import _format_result, _safe_json


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
