"""
数据清洗处理器。

在落库之前进行深度的清洗和瘦身，抛弃用于调试的巨大 HTML 结构（例如 raw_snapshots），
并对过长的列表或文本进行截断，保证落库 JSON 具有高信息密度，解决数据库膨胀问题。
"""

from __future__ import annotations

import re
from typing import Any

from loguru import logger

from src.core.registry import registry
from src.processors.base import BaseProcessor, ProcessInput, ProcessOutput


@registry.register("processor", "cleaner")
class DataCleaner(BaseProcessor):
    """
    深度数据清洗处理器。

    职责:
      - 剔除无效/空数据
      - 深度瘦身：删除 raw_snapshots, 大量 HTML 片段
      - 截断过长的数组（如保留前 100 条评论）
      - 移除过长的无用文本
    """

    def __init__(self, config: dict[str, Any] | None = None):
        super().__init__(config)
        self.max_reviews = self.config.get("max_reviews", 100)
        self.max_news = self.config.get("max_news", 50)
        self.max_text_len = self.config.get("max_text_len", 1000)

    async def process(self, input_data: ProcessInput) -> ProcessOutput:
        """清洗数据"""
        logger.debug(f"[Cleaner] 正在深度清洗来自 {input_data.source} 的数据...")

        data = input_data.data

        if data is None:
            return ProcessOutput(
                data=None,
                processor_name="cleaner",
                success=False,
                error="输入数据为空",
            )

        # 深拷贝以防修改原始引用
        import copy
        data = copy.deepcopy(data)

        if isinstance(data, dict):
            cleaned = self._clean_record(data)
        elif isinstance(data, list):
            cleaned = [self._clean_record(item) if isinstance(item, dict) else item for item in data]
        else:
            cleaned = data

        return ProcessOutput(
            data=cleaned,
            metadata={
                **input_data.metadata,
                "cleaned": True,
                "cleaner_version": "2.0",
            },
            processor_name="cleaner",
            success=True,
        )

    def _clean_record(self, data: dict[str, Any]) -> dict[str, Any]:
        """清洗单条采集记录"""
        
        # 1. 直接剔除已知的高噪音字段
        noisy_keys = ["raw_snapshots", "html", "page_source", "raw_content"]
        for k in noisy_keys:
            data.pop(k, None)

        # 2. 如果包含旧版 content 嵌套，将其提权或内部清洗
        if "content" in data and isinstance(data["content"], dict):
            for k in noisy_keys:
                data["content"].pop(k, None)

        # 3. 截断评论列表 (Steam / TapTap)
        if "reviews" in data and isinstance(data["reviews"], dict):
            items = data["reviews"].get("items")
            if isinstance(items, list) and len(items) > self.max_reviews:
                data["reviews"]["items"] = items[:self.max_reviews]
                
        # 4. 截断新闻列表 (Steam)
        if "news" in data and isinstance(data["news"], dict):
            items = data["news"].get("items")
            if isinstance(items, list) and len(items) > self.max_news:
                data["news"]["items"] = items[:self.max_news]
                
        # 5. 递归处理所有字典层级，移除无用或过大的值
        return self._recursive_clean(data)

    def _recursive_clean(self, node: Any) -> Any:
        """递归清理整个 JSON 树，去除大段 HTML 和过长文本"""
        if isinstance(node, dict):
            cleaned_node = {}
            for key, value in node.items():
                if value is None:
                    continue
                # 清洗后的值如果变为空字典/空列表，也可酌情保留或丢弃
                cleaned_val = self._recursive_clean(value)
                cleaned_node[key] = cleaned_val
            return cleaned_node
            
        elif isinstance(node, list):
            # 对列表元素做清洗
            return [self._recursive_clean(item) for item in node if item is not None]
            
        elif isinstance(node, str):
            node = node.strip()
            if not node:
                return ""
            
            # 简单的 HTML 标签探测
            if "<html" in node.lower() or "<body" in node.lower() or "<div" in node.lower():
                # 如果是明显的一大坨 HTML 源码，直接替换为提示
                if len(node) > 500:
                    return "[HTML Content Removed by Cleaner]"
            
            # 如果文本特别长，做截断处理，防止占太多空间
            if len(node) > self.max_text_len:
                return node[:self.max_text_len] + "... [Truncated]"
                
            return node
            
        else:
            return node
