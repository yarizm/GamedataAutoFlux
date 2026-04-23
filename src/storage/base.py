"""
存储层抽象基类

存储层负责持久化采集和处理后的数据。
支持结构化（SQLite）和非结构化（JSON + 向量数据库）两种模式。

通过 @registry.register("storage", "name") 装饰器注册到系统。
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field


class StorageRecord(BaseModel):
    """存储记录"""
    key: str = Field(..., description="记录键")
    data: Any = Field(..., description="数据内容")
    metadata: dict[str, Any] = Field(default_factory=dict, description="元数据")
    stored_at: datetime = Field(default_factory=datetime.now, description="存储时间")
    source: str = Field(default="", description="数据来源")
    tags: list[str] = Field(default_factory=list, description="标签（用于检索）")


class QueryResult(BaseModel):
    """查询结果"""
    records: list[StorageRecord] = Field(default_factory=list, description="匹配的记录")
    total: int = Field(default=0, description="总匹配数")
    query: str = Field(default="", description="查询条件")


class BaseStorage(ABC):
    """
    存储层抽象基类。

    子类实现示例:
        @registry.register("storage", "sqlite")
        class SQLiteStorage(BaseStorage):
            async def save(self, record):
                await self.db.execute(...)

            async def load(self, key):
                row = await self.db.fetchone(...)
                return StorageRecord(...)
    """

    def __init__(self, config: dict[str, Any] | None = None):
        self.config = config or {}

    async def initialize(self) -> None:
        """
        初始化存储（如创建表、连接数据库）。
        默认 no-op，子类按需覆盖。
        """
        pass

    @abstractmethod
    async def save(self, record: StorageRecord) -> None:
        """
        保存一条记录。

        Args:
            record: 要保存的记录
        """
        ...

    async def save_batch(self, records: list[StorageRecord]) -> None:
        """
        批量保存。默认逐条调用 save()，子类可覆盖以优化。

        Args:
            records: 记录列表
        """
        for record in records:
            await self.save(record)

    @abstractmethod
    async def load(self, key: str) -> StorageRecord | None:
        """
        按键加载记录。

        Args:
            key: 记录键

        Returns:
            记录或 None
        """
        ...

    @abstractmethod
    async def query(self, query: str, limit: int = 10, **kwargs: Any) -> QueryResult:
        """
        查询记录。

        Args:
            query: 查询条件（可以是关键词、SQL 片段或向量查询）
            limit: 最大返回数
            **kwargs: 额外参数

        Returns:
            QueryResult 查询结果
        """
        ...

    async def delete(self, key: str) -> bool:
        """
        删除记录。

        Args:
            key: 记录键

        Returns:
            是否成功
        """
        return False

    async def list_keys(self, prefix: str = "", limit: int = 100) -> list[str]:
        """
        列出所有键。

        Args:
            prefix: 键前缀过滤
            limit: 最大返回数

        Returns:
            键列表
        """
        return []

    async def close(self) -> None:
        """关闭存储连接。默认 no-op。"""
        pass

    async def __aenter__(self):
        await self.initialize()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()
        return False
