"""Pipeline 业务门面（修复清单第三批：拆薄 Scheduler）。

Scheduler 退守为执行引擎（调度、并发、生命周期）；Pipeline 的注册、
持久化、查询、解析与删除等业务操作收敛到本服务。Phase 1 实现委托
Scheduler 既有内部，调用方从此只面向本服务——后续把实现体整体迁出
Scheduler 时零改动。
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Callable

if TYPE_CHECKING:
    from src.core.pipeline import Pipeline
    from src.core.scheduler import Scheduler


class PipelineService:
    def __init__(self, get_scheduler: "Callable[[], Scheduler]") -> None:
        # 动态解析：容器/测试可随时替换 scheduler 实例（引擎细节）
        self._get_scheduler = get_scheduler

    def register_pipeline(self, pipeline: "Pipeline") -> None:
        """注册 Pipeline 配置（启动前内存注册）。"""
        return self._get_scheduler().register_pipeline(pipeline)

    async def save_pipeline(self, pipeline: "Pipeline") -> None:
        """保存 Pipeline 并持久化。"""
        return await self._get_scheduler().save_pipeline(pipeline)

    async def resolve_pipeline(self, name: str) -> "Pipeline | None":
        """按名解析：内存注册表优先，回落持久化 graph/pipeline 投影。"""
        return await self._get_scheduler().resolve_pipeline(name)

    def get_pipeline(self, name: str) -> "Pipeline | None":
        return self._get_scheduler().get_pipeline(name)

    def get_all_pipelines(self) -> "list[Pipeline]":
        return self._get_scheduler().get_all_pipelines()

    async def delete_pipeline(self, name: str) -> bool:
        return await self._get_scheduler().delete_pipeline(name)
