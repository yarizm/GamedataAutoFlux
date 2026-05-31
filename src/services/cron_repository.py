"""
Cron 任务配置持久化仓储抽象层

将 CronJob 配置的存储/查询与 Scheduler 解耦。
InMemoryCronRepository 可用于测试。
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any


@dataclass
class CronJobConfig:
    """Cron 定时任务配置"""

    name: str
    pipeline_name: str
    cron_expr: str
    task_template: dict[str, Any] = field(default_factory=dict)


class CronRepository(ABC):
    """Cron 任务配置仓储接口"""

    @abstractmethod
    async def save(self, job: CronJobConfig) -> None:
        """保存或更新 Cron 任务配置"""
        ...

    @abstractmethod
    async def load(self, name: str) -> CronJobConfig | None:
        """按名称加载配置，不存在返回 None"""
        ...

    @abstractmethod
    async def delete(self, name: str) -> bool:
        """删除配置，返回是否成功"""
        ...

    @abstractmethod
    async def list_all(self) -> list[CronJobConfig]:
        """列出所有配置"""
        ...


class InMemoryCronRepository(CronRepository):
    """内存 Cron 仓储，供测试使用"""

    def __init__(self) -> None:
        self._jobs: dict[str, CronJobConfig] = {}

    async def save(self, job: CronJobConfig) -> None:
        self._jobs[job.name] = CronJobConfig(
            name=job.name,
            pipeline_name=job.pipeline_name,
            cron_expr=job.cron_expr,
            task_template=dict(job.task_template),
        )

    async def load(self, name: str) -> CronJobConfig | None:
        job = self._jobs.get(name)
        if not job:
            return None
        import copy
        return copy.deepcopy(job)

    async def delete(self, name: str) -> bool:
        return self._jobs.pop(name, None) is not None

    async def list_all(self) -> list[CronJobConfig]:
        import copy
        return [copy.deepcopy(job) for job in self._jobs.values()]
