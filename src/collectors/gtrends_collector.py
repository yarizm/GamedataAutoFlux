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

if not hasattr(urllib3.util.retry.Retry, "DEFAULT_METHOD_WHITELIST"):
    _original_init = urllib3.util.retry.Retry.__init__

    def _patched_init(self, *args, **kwargs):
        if "method_whitelist" in kwargs:
            kwargs["allowed_methods"] = kwargs.pop("method_whitelist")
        _original_init(self, *args, **kwargs)

    urllib3.util.retry.Retry.__init__ = _patched_init

from pytrends.request import TrendReq

from src.collectors.base import (
    BaseCollector,
    CollectResult,
    CollectTarget,
    _build_failure_metadata,
    _collection_error_message,
    _finalize_collect_result,
    _is_retryable_collect_error,
    _resolve_collect_retries,
    _resolve_collect_retry_delay,
    _resolve_collect_timeout,
    _sleep_before_retry,
)
from src.collectors.gtrends.firecrawl_fallback import GtrendsFirecrawlFallback
from src.core.config import get_settings
from src.core.errors import ErrorCode, classify_exception
from src.core.registry import registry
from src.core.sensitive import redact_sensitive, redact_sensitive_text


def _is_rate_limited_failure(result: CollectResult) -> bool:
    if result.success:
        return False
    if result.error_code == ErrorCode.rate_limited.value:
        return True
    error = result.error or ""
    return "429" in error or "Too Many Requests" in error


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
        self.backoff_factor = float(
            self.config.get("backoff_factor", gtrends_cfg.get("backoff_factor", 0.5))
        )

        proxies = self.config.get("proxies", gtrends_cfg.get("proxies", []))

        self._trend_req = TrendReq(
            hl=self.hl,
            tz=360,
            timeout=(10, 25),
            proxies=proxies if proxies else [],
            retries=self.retries,
            backoff_factor=self.backoff_factor,
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

    def validate_config(self, config: dict[str, Any] | None = None) -> bool:
        cfg = {**self.config, **(config or {})}
        keyword = cfg.get("keyword", "")
        if not keyword:
            logger.warning("[GTrends] no keyword configured")
        return True

    async def collect_batch(self, targets: list[CollectTarget]) -> list[CollectResult]:
        """批量采集，遇到 429 时进行指数退避等待，避免整个 batch 全军覆没"""
        recovery = _resolve_gtrends_recovery(self.config)
        results = []
        consecutive_429 = 0
        collect_timeout = _resolve_collect_timeout(self)
        collect_retries = _resolve_collect_retries(self)
        collect_retry_delay = _resolve_collect_retry_delay(self)
        max_attempts = collect_retries + 1

        targets = _apply_gtrends_recovery_targets(targets, recovery)
        if not targets:
            return []

        for offset, target in enumerate(targets):
            current_target_index = recovery["next_target_index"] + offset
            if consecutive_429 > 0:
                # 若之前遇到过 429，在下一个目标前先强制等待
                sleep_time = min(300, (2 ** consecutive_429) * 10)
                logger.warning(f"[GTrends] 主动退避 {sleep_time} 秒以应对 429 限流...")
                await asyncio.sleep(sleep_time)

            last_retry_error = ""
            last_retry_error_code = ""
            for attempt in range(1, max_attempts + 1):
                try:
                    if collect_timeout > 0:
                        result = await asyncio.wait_for(
                            self.collect(target),
                            timeout=collect_timeout,
                        )
                    else:
                        result = await self.collect(target)
                    result.metadata = {
                        **(result.metadata or {}),
                        **_gtrends_resume_metadata(
                            recovery,
                            target=target,
                            target_index=current_target_index,
                        ),
                    }
                    result = _finalize_collect_result(
                        self,
                        result,
                        collect_timeout=collect_timeout,
                        attempt=attempt,
                        max_attempts=max_attempts,
                        last_retry_error=last_retry_error,
                        last_retry_error_code=last_retry_error_code,
                    )
                    if _is_rate_limited_failure(result):
                        consecutive_429 += 1
                    else:
                        consecutive_429 = 0
                    if (
                        result.success
                        or attempt >= max_attempts
                        or not _is_retryable_collect_error(result.error_code or "")
                    ):
                        results.append(result)
                        break
                    last_retry_error = result.error or result.error_code or ""
                    last_retry_error_code = result.error_code or ""
                    await _sleep_before_retry(
                        self,
                        target,
                        attempt=attempt,
                        retry_delay=collect_retry_delay,
                        error=result.error or result.error_code or "",
                    )
                    continue
                except Exception as e:
                    error_msg = _collection_error_message(e, collect_timeout=collect_timeout)
                    code = classify_exception(Exception(error_msg))
                    if "429" in error_msg or "Too Many Requests" in error_msg:
                        consecutive_429 += 1
                    if attempt < max_attempts and _is_retryable_collect_error(code.value):
                        last_retry_error = error_msg
                        last_retry_error_code = code.value
                        await _sleep_before_retry(
                            self,
                            target,
                            attempt=attempt,
                            retry_delay=collect_retry_delay,
                            error=error_msg,
                        )
                        continue
                    failed = CollectResult(
                        target=target,
                        success=False,
                        error=error_msg,
                        error_code=code.value,
                        metadata=_build_failure_metadata(
                            self,
                            target,
                            code.value,
                            collect_timeout=collect_timeout,
                            attempt=attempt,
                            max_attempts=max_attempts,
                            last_retry_error=last_retry_error,
                            last_retry_error_code=last_retry_error_code,
                        ),
                    )
                    failed.metadata = {
                        **(failed.metadata or {}),
                        **_gtrends_resume_metadata(
                            recovery,
                            target=target,
                            target_index=current_target_index,
                        ),
                    }
                    results.append(failed)
                    break
        return results

    def _with_failure_metadata(
        self,
        result: CollectResult,
        *,
        collect_timeout: float = 0,
    ) -> CollectResult:
        result.metadata = redact_sensitive(result.metadata or {})
        if result.success:
            return result
        code = result.error_code or classify_exception(Exception(result.error or "")).value
        result.error_code = code
        if result.error:
            result.error = redact_sensitive_text(result.error)
        result.metadata = {
            **(result.metadata or {}),
            **_build_failure_metadata(self, result.target, code, collect_timeout=collect_timeout),
        }
        return result

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
                ", trying Firecrawl fallback" if self._firecrawl else ", no fallback configured"
            )
            logger.warning(
                f"[GTrends] pytrends failed ({'429' if is_429 else 'error'}): "
                f"{error_msg[:120]}{fallback_hint}"
            )

        # ---- Fallback: Firecrawl ----
        if self._firecrawl:
            logger.info(f"[GTrends] Switching to Firecrawl fallback for '{keyword}'")
            try:
                fc_data = await self._firecrawl.scrape(keyword, hl=hl, geo=geo, timeframe=timeframe)
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
            metadata={
                "collector": "gtrends",
                "data_sources": ["pytrends(failed)", "firecrawl(failed)"],
            },
        )

    def _fetch_trends_data(self, keyword: str, geo: str, timeframe: str, hl: str) -> dict[str, Any]:
        """同步调用 pytrends 获取数据。"""
        if not self._trend_req:
            raise RuntimeError("GoogleTrendsCollector 未初始化")

        kw_list = [keyword]
        self._trend_req.build_payload(kw_list, cat=0, timeframe=timeframe, geo=geo, gprop="")

        interest_df = self._trend_req.interest_over_time()
        trend_history: list[dict[str, Any]] = []
        if not interest_df.empty:
            if "isPartial" in interest_df.columns:
                interest_df = interest_df.drop(columns=["isPartial"])
            interest_df = interest_df.reset_index()
            for _, row in interest_df.iterrows():
                date_str = row["date"].strftime("%Y-%m-%d")
                trend_history.append({"date": date_str, "value": int(row[keyword])})

        related_queries_dict = self._trend_req.related_queries()
        queries_data = related_queries_dict.get(keyword, {})
        top_queries: list[dict[str, Any]] = []
        rising_queries: list[dict[str, Any]] = []

        if queries_data:
            top_df = queries_data.get("top")
            if top_df is not None and not top_df.empty:
                top_queries = top_df.to_dict("records")
            rising_df = queries_data.get("rising")
            if rising_df is not None and not rising_df.empty:
                rising_queries = rising_df.to_dict("records")

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


def _resolve_gtrends_recovery(config: dict[str, Any] | None) -> dict[str, Any]:
    payload = config.get("recovery_checkpoint") if isinstance(config, dict) else None
    if not isinstance(payload, dict):
        return {"next_target_index": 0, "target_order": [], "enabled": False}

    collect = payload.get("collect")
    if not isinstance(collect, dict) or not collect.get("enabled"):
        return {"next_target_index": 0, "target_order": [], "enabled": False}

    target_order = collect.get("target_order")
    normalized_order = [
        str(name).strip()
        for name in target_order or []
        if str(name or "").strip()
    ]
    next_target_index = _safe_resume_index(collect.get("next_target_index"))
    return {
        "enabled": True,
        "next_target_index": next_target_index,
        "target_order": normalized_order,
        "checkpoint_id": str(payload.get("checkpoint_id") or "").strip(),
        "recovery_level": str(payload.get("recovery_level") or "L0").strip().upper(),
    }


def _apply_gtrends_recovery_targets(
    targets: list[CollectTarget],
    recovery: dict[str, Any],
) -> list[CollectTarget]:
    if not recovery.get("enabled"):
        return list(targets)

    next_target_index = _safe_resume_index(recovery.get("next_target_index"))
    if next_target_index <= 0:
        return list(targets)
    if next_target_index >= len(targets):
        return []
    return list(targets[next_target_index:])


def _gtrends_resume_metadata(
    recovery: dict[str, Any],
    *,
    target: CollectTarget,
    target_index: int,
) -> dict[str, Any]:
    if not recovery.get("enabled"):
        return {}
    return {
        "resume": {
            "resumed": True,
            "checkpoint_id": recovery.get("checkpoint_id", ""),
            "recovery_level": recovery.get("recovery_level", "L0"),
            "target_index": max(0, int(target_index)),
            "target": redact_sensitive_text(target.name),
        }
    }


def _safe_resume_index(value: Any) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return 0
    return parsed if parsed >= 0 else 0
