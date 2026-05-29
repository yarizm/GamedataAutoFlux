"""
任务持久化仓储抽象层

将任务对象的存储/查询与 Scheduler 的持久化逻辑解耦。
InMemoryTaskRepository 可用于测试。
"""

from __future__ import annotations

from abc import ABC, abstractmethod

from src.core.task import Task, TaskStatus


class TaskRepository(ABC):
    """任务持久化仓储接口"""

    @abstractmethod
    async def save(self, task: Task) -> None:
        """保存或更新任务"""
        ...

    @abstractmethod
    async def load(self, task_id: str) -> Task | None:
        """按 ID 加载任务，不存在返回 None"""
        ...

    @abstractmethod
    async def delete(self, task_id: str) -> bool:
        """删除任务，返回是否成功"""
        ...

    @abstractmethod
    async def query(self, limit: int = 100, offset: int = 0) -> list[Task]:
        """查询所有任务（分页）"""
        ...

    @abstractmethod
    async def query_by_status(self, status: TaskStatus, limit: int = 100) -> list[Task]:
        """按状态查询任务"""
        ...

    @abstractmethod
    async def list_keys(self, prefix: str = "", limit: int = 100) -> list[str]:
        """列出任务键（可按前缀过滤）"""
        ...

    @abstractmethod
    async def count_by_status(self) -> dict[str, int]:
        """按状态统计任务数量"""
        ...


class InMemoryTaskRepository(TaskRepository):
    """内存任务仓储，供测试使用"""

    def __init__(self) -> None:
        self._tasks: dict[str, Task] = {}

    async def save(self, task: Task) -> None:
        self._tasks[task.id] = task.model_copy(deep=True)

    async def load(self, task_id: str) -> Task | None:
        task = self._tasks.get(task_id)
        return task.model_copy(deep=True) if task else None

    async def delete(self, task_id: str) -> bool:
        return self._tasks.pop(task_id, None) is not None

    async def query(self, limit: int = 100, offset: int = 0) -> list[Task]:
        tasks = sorted(self._tasks.values(), key=lambda t: t.created_at, reverse=True)
        return [t.model_copy(deep=True) for t in tasks[offset : offset + limit]]

    async def query_by_status(self, status: TaskStatus, limit: int = 100) -> list[Task]:
        tasks = [t for t in self._tasks.values() if t.status == status]
        tasks.sort(key=lambda t: t.created_at, reverse=True)
        return [t.model_copy(deep=True) for t in tasks[:limit]]

    async def list_keys(self, prefix: str = "", limit: int = 100) -> list[str]:
        keys = [f"task:{t.id}" for t in self._tasks.values()]
        if prefix:
            keys = [k for k in keys if k.startswith(prefix)]
        return keys[:limit]

    async def count_by_status(self) -> dict[str, int]:
        counts: dict[str, int] = {}
        for task in self._tasks.values():
            status = task.status.value
            counts[status] = counts.get(status, 0) + 1
        return counts
