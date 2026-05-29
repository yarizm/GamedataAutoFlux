"""
Pipeline 配置持久化仓储抽象层

将 Pipeline 快照的存储/查询与 Scheduler 解耦。
InMemoryPipelineRepository 可用于测试。
"""

from __future__ import annotations

from abc import ABC, abstractmethod

from src.core.pipeline import Pipeline


class PipelineRepository(ABC):
    """Pipeline 配置仓储接口"""

    @abstractmethod
    async def save(self, pipeline: Pipeline) -> None:
        """保存或更新 Pipeline 快照"""
        ...

    @abstractmethod
    async def load(self, name: str) -> Pipeline | None:
        """按名称加载 Pipeline，不存在返回 None"""
        ...

    @abstractmethod
    async def delete(self, name: str) -> bool:
        """删除 Pipeline，返回是否成功"""
        ...

    @abstractmethod
    async def list_all(self) -> list[Pipeline]:
        """列出所有 Pipeline"""
        ...


class InMemoryPipelineRepository(PipelineRepository):
    """内存 Pipeline 仓储，供测试使用"""

    def __init__(self) -> None:
        self._pipelines: dict[str, Pipeline] = {}

    async def save(self, pipeline: Pipeline) -> None:
        self._pipelines[pipeline.name] = Pipeline.from_config(pipeline.to_config())

    async def load(self, name: str) -> Pipeline | None:
        return self._pipelines.get(name)

    async def delete(self, name: str) -> bool:
        return self._pipelines.pop(name, None) is not None

    async def list_all(self) -> list[Pipeline]:
        return list(self._pipelines.values())
