"""
数据清洗处理器（示例骨架）

演示如何通过继承 BaseProcessor 并使用 @register 注册到系统。
负责清洗和标准化采集到的原始数据。
"""

from __future__ import annotations

from typing import Any

from loguru import logger

from src.core.registry import registry
from src.processors.base import BaseProcessor, ProcessInput, ProcessOutput


@registry.register("processor", "cleaner")
class DataCleaner(BaseProcessor):
    """
    数据清洗处理器。

    职责:
      - 去除无效/空数据
      - 字段标准化
      - 添加处理元数据
    """

    async def process(self, input_data: ProcessInput) -> ProcessOutput:
        """清洗数据"""
        logger.debug(f"[Cleaner] 处理来自 {input_data.source} 的数据")

        data = input_data.data

        if data is None:
            return ProcessOutput(
                data=None,
                processor_name="cleaner",
                success=False,
                error="输入数据为空",
            )

        # 如果是字典类型，执行标准化清洗
        if isinstance(data, dict):
            cleaned = self._clean_dict(data)
        elif isinstance(data, list):
            cleaned = [self._clean_dict(item) if isinstance(item, dict) else item for item in data]
        else:
            cleaned = data

        return ProcessOutput(
            data=cleaned,
            metadata={
                **input_data.metadata,
                "cleaned": True,
                "cleaner_version": "1.0",
            },
            processor_name="cleaner",
            success=True,
        )

    def _clean_dict(self, data: dict[str, Any]) -> dict[str, Any]:
        """清洗字典数据"""
        cleaned = {}
        for key, value in data.items():
            # 跳过 None 值
            if value is None:
                continue
            # 字符串去首尾空白
            if isinstance(value, str):
                value = value.strip()
                if not value:
                    continue
            cleaned[key] = value
        return cleaned
