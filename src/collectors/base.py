"""
数据采集器抽象基类

所有数据采集器必须继承 BaseCollector 并实现其抽象方法。
通过 @registry.register("collector", "name") 装饰器注册到系统。

生命周期:
    setup() → collect() [可多次调用] → teardown()
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import datetime
from enum import Enum
from typing import Any

from pydantic import BaseModel, ConfigDict, Field


class CollectTarget(BaseModel):
    """采集目标"""
    model_config = ConfigDict(extra="allow")

    name: str = Field(..., description="目标名称，如游戏名")
    target_type: str = Field(default="default", description="目标类型")
    params: dict[str, Any] = Field(default_factory=dict, description="额外参数")


class CollectResult(BaseModel):
    """采集结果"""
    target: CollectTarget = Field(..., description="对应的采集目标")
    data: Any = Field(default=None, description="采集到的数据")
    metadata: dict[str, Any] = Field(default_factory=dict, description="元数据")
    collected_at: datetime = Field(default_factory=datetime.now, description="采集时间")
    success: bool = Field(default=True, description="是否成功")
    error: str | None = Field(default=None, description="错误信息")
    raw_data: Any = Field(default=None, description="原始数据（用于调试）")


class BaseCollector(ABC):
    """
    数据采集器抽象基类。

    子类实现示例:
        @registry.register("collector", "steam")
        class SteamCollector(BaseCollector):
            async def setup(self, config):
                self.session = httpx.AsyncClient(...)

            async def collect(self, target):
                resp = await self.session.get(...)
                return CollectResult(target=target, data=resp.json())

            async def teardown(self):
                await self.session.aclose()
    """

    def __init__(self, config: dict[str, Any] | None = None):
        self.config = config or {}
        self._is_setup = False

    async def setup(self, config: dict[str, Any] | None = None) -> None:
        """
        初始化采集器（如建立连接、登录等）。
        默认实现为 no-op，子类按需覆盖。

        Args:
            config: 运行时配置，会合并到 self.config
        """
        if config:
            self.config.update(config)
        self._is_setup = True

    @abstractmethod
    async def collect(self, target: CollectTarget) -> CollectResult:
        """
        执行数据采集。

        Args:
            target: 采集目标

        Returns:
            CollectResult 包含采集到的数据
        """
        ...

    async def collect_batch(self, targets: list[CollectTarget]) -> list[CollectResult]:
        """
        批量采集。默认逐个调用 collect()，子类可覆盖以实现并发。

        Args:
            targets: 采集目标列表

        Returns:
            结果列表
        """
        results = []
        for target in targets:
            try:
                result = await self.collect(target)
                results.append(result)
            except Exception as e:
                results.append(
                    CollectResult(
                        target=target,
                        success=False,
                        error=str(e),
                    )
                )
        return results

    async def teardown(self) -> None:
        """
        清理资源（如关闭连接）。
        默认实现为 no-op，子类按需覆盖。
        """
        self._is_setup = False

    def validate_config(self, config: dict[str, Any] | None = None) -> bool:
        """
        校验配置是否满足采集器需求。
        默认返回 True，子类按需覆盖。

        Args:
            config: 要校验的配置

        Returns:
            是否合法
        """
        return True

    async def __aenter__(self):
        await self.setup()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.teardown()
        return False
