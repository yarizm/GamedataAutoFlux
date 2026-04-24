"""
Google Trends 数据采集器
"""

from __future__ import annotations

import asyncio
from typing import Any

from loguru import logger

# Monkeypatch urllib3.util.retry for pytrends compatibility with urllib3 v2+
import urllib3.util.retry
if not hasattr(urllib3.util.retry.Retry, 'DEFAULT_METHOD_WHITELIST'):
    _original_init = urllib3.util.retry.Retry.__init__
    def _patched_init(self, *args, **kwargs):
        if 'method_whitelist' in kwargs:
            kwargs['allowed_methods'] = kwargs.pop('method_whitelist')
        _original_init(self, *args, **kwargs)
    urllib3.util.retry.Retry.__init__ = _patched_init

import pytrends
from pytrends.request import TrendReq

from src.collectors.base import BaseCollector, CollectTarget, CollectResult
from src.core.registry import registry
from src.core.config import get_settings


@registry.register("collector", "gtrends")
class GoogleTrendsCollector(BaseCollector):
    """
    Google Trends 采集器。
    
    使用 pytrends 库获取指定关键词的搜索热度和相关搜索词。
    注意：频繁调用可能会触发 429 Too Many Requests，建议配置 proxies。
    """

    def __init__(self, config: dict[str, Any] | None = None):
        super().__init__(config)
        self._trend_req: TrendReq | None = None

    async def setup(self, config: dict[str, Any] | None = None) -> None:
        await super().setup(config)

        try:
            settings = get_settings()
            gtrends_cfg = settings.get("gtrends", {})
        except Exception:
            gtrends_cfg = {}

        self.hl = self.config.get("hl", gtrends_cfg.get("hl", "zh-CN"))
        self.geo = self.config.get("geo", gtrends_cfg.get("geo", ""))
        self.timeframe = self.config.get("timeframe", gtrends_cfg.get("timeframe", "today 12-m"))
        self.retries = int(self.config.get("retries", gtrends_cfg.get("retries", 2)))
        self.backoff_factor = float(self.config.get("backoff_factor", gtrends_cfg.get("backoff_factor", 0.5)))
        
        proxies = self.config.get("proxies", gtrends_cfg.get("proxies", []))
        
        # 初始化 TrendReq
        # 注意: pytrends 是同步请求，我们会用 asyncio.to_thread 包裹它
        self._trend_req = TrendReq(
            hl=self.hl, 
            tz=360, 
            timeout=(10, 25),
            proxies=proxies if proxies else [],
            retries=self.retries,
            backoff_factor=self.backoff_factor
        )
        logger.info(f"[GTrends] 初始化完成: hl={self.hl}, geo={self.geo}, timeframe={self.timeframe}, proxies={len(proxies)}")

    async def collect(self, target: CollectTarget) -> CollectResult:
        keyword = target.params.get("keyword") or target.name
        if not keyword:
            return CollectResult(
                target=target,
                success=False,
                error="未指定有效的关键词 (keyword)"
            )

        hl = target.params.get("hl", self.hl)
        geo = target.params.get("geo", self.geo)
        timeframe = target.params.get("timeframe", self.timeframe)
        
        logger.info(f"[GTrends] 开始采集: '{keyword}' (geo='{geo}', timeframe='{timeframe}')")

        try:
            # Pytrends 的网络请求是同步的，为了不阻塞事件循环，使用 to_thread
            data = await asyncio.to_thread(
                self._fetch_trends_data, 
                keyword=keyword, 
                geo=geo, 
                timeframe=timeframe,
                hl=hl
            )
            
            logger.info(f"[GTrends] 采集成功: '{keyword}'")
            return CollectResult(
                target=target,
                data={
                    "collector": "gtrends",
                    "game_name": target.name,
                    "keyword": keyword,
                    "geo": geo,
                    "timeframe": timeframe,
                    **data
                },
                metadata={
                    "collector": "gtrends",
                    "data_sources": ["pytrends"]
                },
                success=True
            )
        except Exception as e:
            error_msg = str(e)
            logger.error(f"[GTrends] 采集失败: '{keyword}' - {error_msg}")
            # 处理常见的 429 错误
            if "429" in error_msg or "Too Many Requests" in error_msg:
                error_msg = "Google Trends 触发人机验证 (429 Too Many Requests)，建议配置代理或降低请求频率。"
            
            return CollectResult(
                target=target,
                success=False,
                error=error_msg,
                metadata={
                    "collector": "gtrends",
                    "data_sources": ["pytrends(failed)"]
                }
            )

    def _fetch_trends_data(self, keyword: str, geo: str, timeframe: str, hl: str) -> dict[str, Any]:
        """
        同步调用 pytrends 获取数据。
        """
        if not self._trend_req:
            raise RuntimeError("GoogleTrendsCollector 未初始化")
            
        kw_list = [keyword]
        
        # 1. 构建 payload
        self._trend_req.build_payload(kw_list, cat=0, timeframe=timeframe, geo=geo, gprop='')

        # 2. 获取兴趣随时间变化的数据
        interest_df = self._trend_req.interest_over_time()
        trend_history = []
        if not interest_df.empty:
            # 移除 isPartial 列并转为 dict 列表
            if 'isPartial' in interest_df.columns:
                interest_df = interest_df.drop(columns=['isPartial'])
            
            # 重新索引并将日期转为字符串
            interest_df = interest_df.reset_index()
            for _, row in interest_df.iterrows():
                date_str = row['date'].strftime('%Y-%m-%d')
                value = row[keyword]
                trend_history.append({"date": date_str, "value": int(value)})

        # 3. 获取相关查询
        related_queries_dict = self._trend_req.related_queries()
        queries_data = related_queries_dict.get(keyword, {})
        
        top_queries = []
        rising_queries = []
        
        if queries_data:
            top_df = queries_data.get('top')
            if top_df is not None and not top_df.empty:
                top_queries = top_df.to_dict('records')
                
            rising_df = queries_data.get('rising')
            if rising_df is not None and not rising_df.empty:
                rising_queries = rising_df.to_dict('records')

        return {
            "trend_history": trend_history,
            "related_queries": {
                "top": top_queries,
                "rising": rising_queries
            },
            "snapshot": {
                "name": keyword,
                "latest_trend_value": trend_history[-1]["value"] if trend_history else None,
                "top_related_count": len(top_queries),
                "rising_related_count": len(rising_queries)
            }
        }
