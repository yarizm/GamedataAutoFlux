"""
数据处理器抽象基类

处理器负责将采集到的原始数据转换为结构化/可存储的形式。
处理器可以链式组合，前一个处理器的输出作为下一个的输入。

通过 @registry.register("processor", "name") 装饰器注册到系统。
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field


class ProcessInput(BaseModel):
    """处理器输入"""
    data: Any = Field(..., description="待处理数据")
    metadata: dict[str, Any] = Field(default_factory=dict, description="元数据")
    source: str = Field(default="unknown", description="数据来源标识")


class ProcessOutput(BaseModel):
    """处理器输出"""
    data: Any = Field(..., description="处理后的数据")
    metadata: dict[str, Any] = Field(default_factory=dict, description="处理后的元数据")
    processor_name: str = Field(default="", description="处理器名称")
    processed_at: datetime = Field(default_factory=datetime.now, description="处理时间")
    success: bool = Field(default=True, description="是否成功")
    error: str | None = Field(default=None, description="错误信息")


class BaseProcessor(ABC):
    """
    数据处理器抽象基类。

    子类实现示例:
        @registry.register("processor", "cleaner")
        class DataCleaner(BaseProcessor):
            async def process(self, input_data):
                cleaned = self._clean(input_data.data)
                return ProcessOutput(data=cleaned, processor_name="cleaner")
    """

    def __init__(self, config: dict[str, Any] | None = None):
        self.config = config or {}

    @abstractmethod
    async def process(self, input_data: ProcessInput) -> ProcessOutput:
        """
        处理数据。

        Args:
            input_data: 处理器输入

        Returns:
            ProcessOutput 处理后的结果
        """
        ...

    async def process_batch(self, inputs: list[ProcessInput]) -> list[ProcessOutput]:
        """
        批量处理。默认逐个调用 process()，子类可覆盖以优化。

        Args:
            inputs: 输入列表

        Returns:
            结果列表
        """
        results = []
        for input_data in inputs:
            try:
                result = await self.process(input_data)
                results.append(result)
            except Exception as e:
                results.append(
                    ProcessOutput(
                        data=None,
                        processor_name=self.__class__.__name__,
                        success=False,
                        error=str(e),
                    )
                )
        return results
