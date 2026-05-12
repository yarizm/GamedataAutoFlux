"""
Google Trends 数据采集器

主策略：pytrends（直接 API 调用）
备用策略：Firecrawl（当 pytrends 被 429 限流时自动切换）
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

from pytrends.request import TrendReq

from src.collectors.base import BaseCollector, CollectTarget, CollectResult
from src.collectors.gtrends.firecrawl_fallback import GtrendsFirecrawlFallback
from src.core.config import get_settings
from src.core.errors import ErrorCode, classify_exception
from src.core.registry import registry


@registry.register("collector", "gtrends")
class GoogleTrendsCollector(BaseCollector):
    """Google Trends 采集器。

    pytrends 作为主策略，Firecrawl 作为 429 限流时的备用策略。
    """

    def __init__(self, config: dict[str, Any] | None = None):
        super().__init__(config)
        self._trend_req: TrendReq | None = None
        self._firecrawl: GtrendsFirecrawlFallback | None = None

    async def setup(self, config: dict[str, Any] | None = None) -> None:
        await super().setup(config)

        try:
            settings = get_settings()
            gtrends_cfg = settings.get("gtrends", {})
            firecrawl_cfg = settings.get("firecrawl", {})
        except Exception:
            gtrends_cfg = {}
            firecrawl_cfg = {}

        self.hl = self.config.get("hl", gtrends_cfg.get("hl", "zh-CN"))
        self.geo = self.config.get("geo", gtrends_cfg.get("geo", ""))
        self.timeframe = self.config.get("timeframe", gtrends_cfg.get("timeframe", "today 12-m"))
        self.retries = int(self.config.get("retries", gtrends_cfg.get("retries", 2)))
        self.backoff_factor = float(self.config.get("backoff_factor", gtrends_cfg.get("backoff_factor", 0.5)))

        proxies = self.config.get("proxies", gtrends_cfg.get("proxies", []))

        self._trend_req = TrendReq(
            hl=self.hl,
            tz=360,
            timeout=(10, 25),
            proxies=proxies if proxies else [],
            retries=self.retries,
            backoff_factor=self.backoff_factor
        )
        logger.info(
            f"[GTrends] pytrends ready: hl={self.hl}, geo={self.geo}, "
            f"timeframe={self.timeframe}, proxies={len(proxies)}"
        )

        # Firecrawl fallback
        fc_key = firecrawl_cfg.get("api_key", "")
        if fc_key and not fc_key.startswith("${"):
            self._firecrawl = GtrendsFirecrawlFallback(
                api_key=fc_key,
                timeout=int(firecrawl_cfg.get("timeout", 30)),
            )
            await self._firecrawl.setup()
            logger.info("[GTrends] Firecrawl fallback ready")
        else:
            self._firecrawl = None
            logger.debug("[GTrends] Firecrawl not configured (no api_key)")

    async def teardown(self) -> None:
        if self._firecrawl:
            await self._firecrawl.teardown()
            self._firecrawl = None
        self._trend_req = None
        await super().teardown()

    async def validate_config(self, config: dict[str, Any] | None = None) -> bool:
        cfg = {**self.config, **(config or {})}
        keyword = cfg.get("keyword", "")
        if not keyword:
            logger.warning("[GTrends] no keyword configured")
        return True

    async def collect(self, target: CollectTarget) -> CollectResult:
        keyword = target.params.get("keyword") or target.name
        if not keyword:
            return CollectResult(
                target=target,
                success=False,
                error="未指定有效的关键词 (keyword)",
                error_code=ErrorCode.empty_data.value,
            )

        hl = target.params.get("hl", self.hl)
        geo = target.params.get("geo", self.geo)
        timeframe = target.params.get("timeframe", self.timeframe)

        logger.info(f"[GTrends] 开始采集: '{keyword}' (geo='{geo}', timeframe='{timeframe}')")

        # ---- Primary: pytrends ----
        try:
            data = await asyncio.to_thread(
                self._fetch_trends_data,
                keyword=keyword,
                geo=geo,
                timeframe=timeframe,
                hl=hl,
            )
            logger.info(f"[GTrends] pytrends 采集成功: '{keyword}'")
            return CollectResult(
                target=target,
                data={
                    "collector": "gtrends",
                    "game_name": target.name,
                    "keyword": keyword,
                    "geo": geo,
                    "timeframe": timeframe,
                    **data,
                },
                metadata={"collector": "gtrends", "data_sources": ["pytrends"]},
                success=True,
            )
        except Exception as pytrends_err:
            error_msg = str(pytrends_err)
            _last_error = pytrends_err
            is_429 = "429" in error_msg or "Too Many Requests" in error_msg
            fallback_hint = (
                ", trying Firecrawl fallback" if self._firecrawl
                else ", no fallback configured"
            )
            logger.warning(
                f"[GTrends] pytrends failed ({'429' if is_429 else 'error'}): "
                f"{error_msg[:120]}{fallback_hint}"
            )

        # ---- Fallback: Firecrawl ----
        if self._firecrawl:
            logger.info(f"[GTrends] Switching to Firecrawl fallback for '{keyword}'")
            try:
                fc_data = await self._firecrawl.scrape(
                    keyword, hl=hl, geo=geo, timeframe=timeframe
                )
                if fc_data.get("error"):
                    logger.error(f"[GTrends] Firecrawl fallback failed: {fc_data['error']}")
                else:
                    logger.info(f"[GTrends] Firecrawl fallback succeeded: '{keyword}'")
                    return CollectResult(
                        target=target,
                        data={
                            "collector": "gtrends",
                            "game_name": target.name,
                            "keyword": keyword,
                            "geo": geo,
                            "timeframe": timeframe,
                            **fc_data,
                        },
                        metadata={"collector": "gtrends", "data_sources": ["firecrawl"]},
                        success=True,
                    )
            except Exception as fc_err:
                logger.error(f"[GTrends] Firecrawl fallback exception: {fc_err}")

        # ---- Both failed ----
        code = classify_exception(_last_error) if _last_error else ErrorCode.unknown
        if "429" in error_msg or "Too Many Requests" in error_msg:
            error_msg = (
                "Google Trends 触发人机验证 (429 Too Many Requests)。"
                "pytrends 和 Firecrawl 均失败，建议配置代理或等待后重试。"
            )

        return CollectResult(
            target=target,
            success=False,
            error=error_msg,
            error_code=code.value,
            metadata={"collector": "gtrends", "data_sources": ["pytrends(failed)", "firecrawl(failed)"]},
        )

    def _fetch_trends_data(
        self, keyword: str, geo: str, timeframe: str, hl: str
    ) -> dict[str, Any]:
        """同步调用 pytrends 获取数据。"""
        if not self._trend_req:
            raise RuntimeError("GoogleTrendsCollector 未初始化")

        kw_list = [keyword]
        self._trend_req.build_payload(kw_list, cat=0, timeframe=timeframe, geo=geo, gprop='')

        interest_df = self._trend_req.interest_over_time()
        trend_history: list[dict[str, Any]] = []
        if not interest_df.empty:
            if 'isPartial' in interest_df.columns:
                interest_df = interest_df.drop(columns=['isPartial'])
            interest_df = interest_df.reset_index()
            for _, row in interest_df.iterrows():
                date_str = row['date'].strftime('%Y-%m-%d')
                trend_history.append({"date": date_str, "value": int(row[keyword])})

        related_queries_dict = self._trend_req.related_queries()
        queries_data = related_queries_dict.get(keyword, {})
        top_queries: list[dict[str, Any]] = []
        rising_queries: list[dict[str, Any]] = []

        if queries_data:
            top_df = queries_data.get('top')
            if top_df is not None and not top_df.empty:
                top_queries = top_df.to_dict('records')
            rising_df = queries_data.get('rising')
            if rising_df is not None and not rising_df.empty:
                rising_queries = rising_df.to_dict('records')

        return {
            "trend_history": trend_history,
            "related_queries": {"top": top_queries, "rising": rising_queries},
            "snapshot": {
                "name": keyword,
                "latest_trend_value": trend_history[-1]["value"] if trend_history else None,
                "top_related_count": len(top_queries),
                "rising_related_count": len(rising_queries),
            },
        }
