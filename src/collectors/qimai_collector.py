"""
Qimai collector.

The collector uses Playwright with a persistent browser profile because Qimai
serves most useful data through dynamic `api.qimai.cn` requests that depend on
its logged-in web session and request signing.
"""

from __future__ import annotations

import asyncio
import os
import random
import re
import statistics
import sys
import time
from datetime import date, datetime, timedelta, timezone
from typing import Any, Iterable

from loguru import logger

from src.collectors.base import BaseCollector, CollectResult, CollectTarget
from src.core.config import get as get_config
from src.core.registry import registry

try:
    from playwright.async_api import Response as AsyncResponse
    from playwright.async_api import async_playwright
    from playwright.sync_api import Response as SyncResponse
    from playwright.sync_api import sync_playwright
except ImportError:  # pragma: no cover - dependency is optional at import time
    async_playwright = None
    sync_playwright = None
    AsyncResponse = Any
    SyncResponse = Any


class QimaiScrapeFailed(Exception):
    """Raised when Qimai scraping fails."""


@registry.register("collector", "qimai")
class QimaiCollector(BaseCollector):
    """
    Collect Qimai iOS App Store metrics.

    Target params:
      - qimai_app_id or app_id: Qimai/App Store app id.
      - country: defaults to cn.

    Requested output fields:
      - grossing_rank_cn
      - ios_grossing_rank_trend
      - appstore_rating_cn
      - appstore_review_trend
      - dau_avg_30d
      - dau_trend_90d
      - downloads_avg_30d
      - downloads_trend_90d
      - revenue_avg_30d
      - revenue_trend_90d
    """

    BASE_URL = "https://www.qimai.cn"

    def __init__(self, config: dict[str, Any] | None = None):
        super().__init__(config)
        self._headless = self.config.get("headless", get_config("qimai.headless", True))
        self._timeout = int(self.config.get("timeout", get_config("qimai.timeout", 45000)))
        self._delay = float(self.config.get("request_delay", get_config("qimai.request_delay", 8.0)))
        self._jitter = float(self.config.get("request_jitter", get_config("qimai.request_jitter", 4.0)))
        self._click_delay = float(self.config.get("click_delay", get_config("qimai.click_delay", 1.5)))
        self._scroll_delay = float(self.config.get("scroll_delay", get_config("qimai.scroll_delay", 2.0)))
        self._user_data_dir = self.config.get(
            "user_data_dir",
            get_config("qimai.user_data_dir", os.path.join(os.getcwd(), "data", "qimai_profile")),
        )
        self._max_api_payloads = int(self.config.get("max_api_payloads", get_config("qimai.max_api_payloads", 80)))

    async def setup(self, config: dict[str, Any] | None = None) -> None:
        await super().setup(config)
        logger.info(
            "[QimaiCollector] initialized user-data-dir={} delay={} jitter={} click_delay={} scroll_delay={}",
            self._user_data_dir,
            self._delay,
            self._jitter,
            self._click_delay,
            self._scroll_delay,
        )

    async def collect(self, target: CollectTarget) -> CollectResult:
        app_id = _normalize_app_id(target.params.get("qimai_app_id", target.params.get("app_id", "")))
        if not app_id:
            return CollectResult(
                target=target,
                success=False,
                error="Qimai requires 'qimai_app_id' or 'app_id'",
            )

        country = str(target.params.get("country") or "cn").lower()
        logger.info("[Qimai] collecting {} app_id={} country={}", target.name, app_id, country)

        try:
            if self._should_use_threaded_playwright():
                qimai_data = await asyncio.to_thread(self._scrape_sync, app_id, country)
            else:
                qimai_data = await self._scrape_async(app_id, country)
        except Exception as exc:
            logger.exception("[Qimai] collection failed")
            return CollectResult(
                target=target,
                success=False,
                error=f"Qimai collection failed: {exc}",
                metadata={"collector": "qimai"},
            )

        app_name = qimai_data.get("app_name") or target.name
        if _looks_like_navigation_text(app_name):
            app_name = target.name
        merged_data = {
            "collector": "qimai",
            "game_name": app_name,
            "app_id": app_id,
            "qimai": qimai_data,
            "snapshot": {
                "name": app_name,
                "review_score": qimai_data.get("rating", qimai_data.get("appstore_rating_cn", "")),
                "total_reviews": qimai_data.get("rating_count", 0),
                "free_rank": qimai_data.get("free_rank", ""),
                "grossing_rank": qimai_data.get("grossing_rank", qimai_data.get("grossing_rank_cn", "")),
                "grossing_rank_cn": qimai_data.get("grossing_rank_cn", ""),
                "appstore_rating_cn": qimai_data.get("appstore_rating_cn", ""),
                "dau_avg_30d": qimai_data.get("dau_avg_30d"),
                "downloads_avg_30d": qimai_data.get("downloads_avg_30d"),
                "revenue_avg_30d": qimai_data.get("revenue_avg_30d"),
            },
        }

        return CollectResult(
            target=target,
            data=merged_data,
            success=True,
            metadata={"collector": "qimai"},
        )

    def _should_use_threaded_playwright(self) -> bool:
        if sys.platform != "win32":
            return False
        loop_name = asyncio.get_running_loop().__class__.__name__
        return "Selector" in loop_name

    def _urls(self, app_id: str, country: str) -> list[tuple[str, str]]:
        return [
            ("baseinfo", f"{self.BASE_URL}/app/baseinfo/appid/{app_id}/country/{country}"),
            ("rank", f"{self.BASE_URL}/app/rank/appid/{app_id}/country/{country}"),
            ("comment", f"{self.BASE_URL}/app/comment/appid/{app_id}/country/{country}"),
            ("appstatus", f"{self.BASE_URL}/app/appstatus/appid/{app_id}/country/{country}"),
        ]

    async def _scrape_async(self, app_id: str, country: str) -> dict[str, Any]:
        if async_playwright is None:
            raise QimaiScrapeFailed("playwright is not installed")

        state = _QimaiCaptureState(app_id=app_id, country=country, max_payloads=self._max_api_payloads)

        async with async_playwright() as p:
            context = await p.chromium.launch_persistent_context(
                self._user_data_dir,
                headless=self._headless,
                args=["--disable-blink-features=AutomationControlled"],
                ignore_default_args=["--enable-automation"],
                viewport={"width": 1920, "height": 1080},
                user_agent=_desktop_user_agent(),
            )
            try:
                page = await context.new_page()
                await page.add_init_script(_stealth_script())
                capture_tasks: list[asyncio.Task[Any]] = []

                def on_response(response: Any) -> None:
                    capture_tasks.append(asyncio.create_task(state.capture_async(response)))

                page.on("response", on_response)

                for index, (page_name, url) in enumerate(self._urls(app_id, country)):
                    if index:
                        await self._polite_wait_async(f"before navigating {page_name}")
                    await page.goto(url, wait_until="domcontentloaded", timeout=self._timeout)
                    await self._configure_page_async(page, page_name)
                    await self._polite_wait_async(f"after configuring {page_name}")
                    await _safe_scroll_async(page)
                    await self._scroll_wait_async(page_name)
                    state.add_page_text(page_name, await page.locator("body").inner_text(timeout=5000))
                    state.add_page_url(page_name, page.url)
                await self._capture_pred_estimates_async(page, state, app_id, country)
                if country == "cn":
                    await self._capture_public_rank_async(page, state)
                if capture_tasks:
                    await asyncio.gather(*capture_tasks, return_exceptions=True)
            finally:
                await context.close()

        return state.build_result()

    def _scrape_sync(self, app_id: str, country: str) -> dict[str, Any]:
        if sync_playwright is None:
            raise QimaiScrapeFailed("playwright is not installed")

        state = _QimaiCaptureState(app_id=app_id, country=country, max_payloads=self._max_api_payloads)

        with sync_playwright() as p:
            context = p.chromium.launch_persistent_context(
                self._user_data_dir,
                headless=self._headless,
                args=["--disable-blink-features=AutomationControlled"],
                ignore_default_args=["--enable-automation"],
                viewport={"width": 1920, "height": 1080},
                user_agent=_desktop_user_agent(),
            )
            try:
                page = context.new_page()
                page.add_init_script(_stealth_script())
                page.on("response", state.capture_sync)

                for index, (page_name, url) in enumerate(self._urls(app_id, country)):
                    if index:
                        self._polite_wait_sync(f"before navigating {page_name}")
                    page.goto(url, wait_until="domcontentloaded", timeout=self._timeout)
                    self._configure_page_sync(page, page_name)
                    self._polite_wait_sync(f"after configuring {page_name}")
                    _safe_scroll_sync(page)
                    self._scroll_wait_sync(page_name)
                    state.add_page_text(page_name, page.locator("body").inner_text(timeout=5000))
                    state.add_page_url(page_name, page.url)
                self._capture_pred_estimates_sync(page, state, app_id, country)
                if country == "cn":
                    self._capture_public_rank_sync(page, state)
            finally:
                context.close()

        return state.build_result()

    async def _configure_page_async(self, page: Any, page_name: str) -> None:
        if page_name == "rank":
            for label in ("\u7545\u9500", "\u6309\u5929", "\u8fd1\u4e09\u4e2a\u6708"):
                await _click_text_async(page, label, self._click_delay, self._jitter)
        elif page_name in {"comment", "appstatus", "download", "income"}:
            for label in ("\u6309\u5929", "\u8fd1\u4e09\u4e2a\u6708"):
                await _click_text_async(page, label, self._click_delay, self._jitter)
        if page_name == "appstatus":
            for label in ("DAU", "\u65e5\u6d3b", "\u6d3b\u8dc3"):
                await _click_text_async(page, label, self._click_delay, self._jitter)

    def _configure_page_sync(self, page: Any, page_name: str) -> None:
        if page_name == "rank":
            for label in ("\u7545\u9500", "\u6309\u5929", "\u8fd1\u4e09\u4e2a\u6708"):
                _click_text_sync(page, label, self._click_delay, self._jitter)
        elif page_name in {"comment", "appstatus", "download", "income"}:
            for label in ("\u6309\u5929", "\u8fd1\u4e09\u4e2a\u6708"):
                _click_text_sync(page, label, self._click_delay, self._jitter)
        if page_name == "appstatus":
            for label in ("DAU", "\u65e5\u6d3b", "\u6d3b\u8dc3"):
                _click_text_sync(page, label, self._click_delay, self._jitter)

    async def _polite_wait_async(self, reason: str) -> None:
        delay = _throttle_seconds(self._delay, self._jitter)
        logger.debug("[Qimai] throttle {}: {:.2f}s", reason, delay)
        await asyncio.sleep(delay)

    def _polite_wait_sync(self, reason: str) -> None:
        delay = _throttle_seconds(self._delay, self._jitter)
        logger.debug("[Qimai] throttle {}: {:.2f}s", reason, delay)
        time.sleep(delay)

    async def _scroll_wait_async(self, page_name: str) -> None:
        delay = _throttle_seconds(self._scroll_delay, min(self._jitter, self._scroll_delay))
        logger.debug("[Qimai] throttle after scroll {}: {:.2f}s", page_name, delay)
        await asyncio.sleep(delay)

    def _scroll_wait_sync(self, page_name: str) -> None:
        delay = _throttle_seconds(self._scroll_delay, min(self._jitter, self._scroll_delay))
        logger.debug("[Qimai] throttle after scroll {}: {:.2f}s", page_name, delay)
        time.sleep(delay)

    async def _capture_public_rank_async(self, page: Any, state: "_QimaiCaptureState") -> None:
        url = f"{self.BASE_URL}/rank"
        try:
            await self._polite_wait_async("before navigating public rank")
            await page.goto(url, wait_until="domcontentloaded", timeout=self._timeout)
            await _click_text_async(page, "\u7545\u9500", self._click_delay, self._jitter)
            await _safe_scroll_async(page)
            await self._scroll_wait_async("public_rank")
            state.add_page_text("public_rank", await page.locator("body").inner_text(timeout=8000))
            state.add_page_url("public_rank", page.url)
        except Exception as exc:
            logger.debug("[Qimai] public rank fallback skipped: {}", exc)

    def _capture_public_rank_sync(self, page: Any, state: "_QimaiCaptureState") -> None:
        url = f"{self.BASE_URL}/rank"
        try:
            self._polite_wait_sync("before navigating public rank")
            page.goto(url, wait_until="domcontentloaded", timeout=self._timeout)
            _click_text_sync(page, "\u7545\u9500", self._click_delay, self._jitter)
            _safe_scroll_sync(page)
            self._scroll_wait_sync("public_rank")
            state.add_page_text("public_rank", page.locator("body").inner_text(timeout=8000))
            state.add_page_url("public_rank", page.url)
        except Exception as exc:
            logger.debug("[Qimai] public rank fallback skipped: {}", exc)

    async def _capture_pred_estimates_async(self, page: Any, state: "_QimaiCaptureState", app_id: str, country: str) -> None:
        try:
            await self._polite_wait_async("before qimai pred estimates")
            responses = await page.evaluate(_qimai_pred_estimate_script(), _qimai_pred_params(app_id, country))
            _append_pred_estimate_payloads(state, responses)
        except Exception as exc:
            state.warnings.append(f"Qimai pred estimate request failed: {exc}")
            logger.debug("[Qimai] pred estimate request failed: {}", exc)

    def _capture_pred_estimates_sync(self, page: Any, state: "_QimaiCaptureState", app_id: str, country: str) -> None:
        try:
            self._polite_wait_sync("before qimai pred estimates")
            responses = page.evaluate(_qimai_pred_estimate_script(), _qimai_pred_params(app_id, country))
            _append_pred_estimate_payloads(state, responses)
        except Exception as exc:
            state.warnings.append(f"Qimai pred estimate request failed: {exc}")
            logger.debug("[Qimai] pred estimate request failed: {}", exc)


class _QimaiCaptureState:
    def __init__(self, *, app_id: str, country: str, max_payloads: int):
        self.app_id = app_id
        self.country = country
        self.max_payloads = max_payloads
        self.page_texts: dict[str, str] = {}
        self.page_urls: dict[str, str] = {}
        self.api_payloads: list[dict[str, Any]] = []
        self.warnings: list[str] = []

    def add_page_text(self, page_name: str, text: str) -> None:
        self.page_texts[page_name] = text or ""
        if "\u5f53\u524d\u7f51\u7edc\u6216\u8d26\u53f7\u5f02\u5e38" in self.page_texts[page_name]:
            self.warnings.append("Qimai page reported network/account anomaly.")
        if "\u60a8\u8bbf\u95ee\u7684\u9875\u9762\u4e0d\u5b58\u5728" in self.page_texts[page_name]:
            self.warnings.append(f"Qimai page not found: {page_name}")

    def add_page_url(self, page_name: str, url: str) -> None:
        self.page_urls[page_name] = url

    async def capture_async(self, response: AsyncResponse) -> None:
        await self._capture_response(response, is_async=True)

    def capture_sync(self, response: SyncResponse) -> None:
        try:
            self._capture_response_sync(response)
        except Exception as exc:
            logger.debug("[Qimai] response capture skipped: {}", exc)

    async def _capture_response(self, response: Any, *, is_async: bool) -> None:
        try:
            if "api.qimai.cn" not in response.url or len(self.api_payloads) >= self.max_payloads:
                return
            content_type = response.headers.get("content-type", "")
            if "json" not in content_type and not response.url.endswith(".json"):
                return
            payload = await response.json() if is_async else response.json()
            self._append_payload(response.url, payload)
        except Exception as exc:
            logger.debug("[Qimai] async response capture skipped: {}", exc)

    def _capture_response_sync(self, response: Any) -> None:
        if "api.qimai.cn" not in response.url or len(self.api_payloads) >= self.max_payloads:
            return
        content_type = response.headers.get("content-type", "")
        if "json" not in content_type and not response.url.endswith(".json"):
            return
        self._append_payload(response.url, response.json())

    def _append_payload(self, url: str, payload: Any) -> None:
        if not isinstance(payload, (dict, list)):
            return
        if isinstance(payload, dict):
            code = payload.get("code")
            if code not in (None, 0, "0", 10000, "10000"):
                message = payload.get("msg") or payload.get("message") or "unknown error"
                self.warnings.append(f"Qimai API skipped non-success response code={code}: {message}")
                return
        self.api_payloads.append({"url": url, "payload": payload})

    def build_result(self) -> dict[str, Any]:
        result: dict[str, Any] = {
            "app_id": self.app_id,
            "country": self.country,
            "pages_crawled": self.page_urls,
            "api_response_count": len(self.api_payloads),
        }

        self._extract_visible_base_fields(result)
        self._extract_api_fields(result)
        self._finalize_derived_fields(result)

        missing = [
            key
            for key in (
                "grossing_rank_cn",
                "ios_grossing_rank_trend",
                "appstore_rating_cn",
                "appstore_review_trend",
                "dau_avg_30d",
                "dau_trend_90d",
                "downloads_avg_30d",
                "downloads_trend_90d",
                "revenue_avg_30d",
                "revenue_trend_90d",
            )
            if result.get(key) in (None, "", [])
        ]
        if missing:
            self.warnings.append(
                "Missing fields may require a logged-in Qimai session or paid permission: "
                + ", ".join(missing)
            )
        if self.warnings:
            result["extraction_warnings"] = list(dict.fromkeys(self.warnings))
        return result

    def _extract_visible_base_fields(self, result: dict[str, Any]) -> None:
        base_text = self.page_texts.get("baseinfo", "")
        all_text = "\n".join(self.page_texts.values())

        app_name = _first_nonempty_line(base_text)
        if app_name and not _looks_like_navigation_text(app_name):
            result["app_name"] = app_name

        rating, rating_count = _extract_rating(base_text or all_text)
        if rating is not None:
            result["rating"] = rating
            result["appstore_rating_cn"] = rating
        if rating_count is not None:
            result["rating_count"] = rating_count

        free_rank = _extract_rank_near_label(all_text, "\u514d\u8d39")
        grossing_rank = _extract_rank_near_label(all_text, "\u7545\u9500")
        rank_page_grossing = _extract_grossing_rank_from_public_rank(
            self.page_texts.get("public_rank", ""),
            result.get("app_name", ""),
        )
        if free_rank:
            result["free_rank"] = free_rank
        if grossing_rank or rank_page_grossing:
            result["grossing_rank"] = rank_page_grossing or grossing_rank
            result["grossing_rank_cn"] = rank_page_grossing or grossing_rank
            if rank_page_grossing:
                result["grossing_rank_source"] = "qimai_public_rank_page"

        yesterday_downloads = _extract_yesterday_downloads(all_text)
        if yesterday_downloads is not None:
            result["yesterday_downloads"] = yesterday_downloads

    def _extract_api_fields(self, result: dict[str, Any]) -> None:
        all_payloads = [item["payload"] for item in self.api_payloads]
        joined_by_url = "\n".join(item["url"].lower() for item in self.api_payloads)

        scalar_candidates = list(_walk_json(all_payloads))
        if "appstore_rating_cn" not in result:
            rating = _find_scalar(scalar_candidates, ("rating", "score"), prefer_float=True)
            if rating is not None:
                result["rating"] = rating
                result["appstore_rating_cn"] = rating
        if "rating_count" not in result:
            count = _find_scalar(scalar_candidates, ("rating_count", "comment_num", "commentnum", "comment"))
            if count is not None:
                result["rating_count"] = count

        series_candidates = _extract_series_candidates(self.api_payloads)
        result.setdefault("raw_series_candidates", _series_debug_summary(series_candidates))

        if not result.get("ios_grossing_rank_trend"):
            result["ios_grossing_rank_trend"] = _pick_series(
                series_candidates,
                include=("rank", "ranking", "grossing", "brand"),
                url_include=("rank",),
                prefer_lower_values=True,
            )
        if not result.get("appstore_review_trend"):
            result["appstore_review_trend"] = _pick_series(
                series_candidates,
                include=("comment", "review", "rating"),
                url_include=("comment",),
            )
        if not result.get("dau_trend_90d"):
            result["dau_trend_90d"] = _pick_series(
                series_candidates,
                include=("dau", "active", "act", "\u65e5\u6d3b"),
                url_include=("appstatus", "status", "active"),
            )
        if not result.get("downloads_trend_90d"):
            result["downloads_trend_90d"] = _pick_series(
                series_candidates,
                include=("download", "downloads", "estimate"),
                url_include=("download", "pred"),
            )
        if not result.get("revenue_trend_90d"):
            result["revenue_trend_90d"] = _pick_series(
                series_candidates,
                include=("revenue", "income", "sales"),
                url_include=("revenue", "income", "pred"),
            )

        if not result.get("grossing_rank_cn"):
            rank = _find_scalar(scalar_candidates, ("grossing_rank", "grossing"))
            if rank is not None:
                result["grossing_rank_cn"] = rank
                result["grossing_rank"] = rank

        _extract_pred_estimate_scalars(self.api_payloads, result)

        result["api_urls"] = sorted({item["url"].split("?")[0] for item in self.api_payloads})
        if "api.qimai.cn" not in joined_by_url and not self.api_payloads:
            self.warnings.append("No api.qimai.cn JSON responses were captured.")

    def _finalize_derived_fields(self, result: dict[str, Any]) -> None:
        for key in ("ios_grossing_rank_trend", "appstore_review_trend", "dau_trend_90d", "downloads_trend_90d", "revenue_trend_90d"):
            if result.get(key):
                result[key] = _trim_series(_sort_series(result[key]), 90)

        result["dau_avg_30d"] = result.get("dau_avg_30d") or _average_last_n(result.get("dau_trend_90d", []), 30)
        result["downloads_avg_30d"] = result.get("downloads_avg_30d") or _average_last_n(result.get("downloads_trend_90d", []), 30)
        result["revenue_avg_30d"] = result.get("revenue_avg_30d") or _average_last_n(result.get("revenue_trend_90d", []), 30)


def _normalize_app_id(value: Any) -> str:
    text = str(value or "").strip()
    digits = re.sub(r"\D+", "", text)
    return digits or text


def _desktop_user_agent() -> str:
    return (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
    )


def _stealth_script() -> str:
    return """
        Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
        window.navigator.chrome = { runtime: {} };
    """


def _qimai_pred_params(app_id: str, country: str) -> dict[str, Any]:
    end_date = date.today() - timedelta(days=1)
    start_date = end_date - timedelta(days=89)
    return {
        "appid": app_id,
        "country": country,
        "platform": "iphone",
        "sdate": start_date.isoformat(),
        "edate": end_date.isoformat(),
        "_timeout": 60000,
    }


def _qimai_pred_estimate_script() -> str:
    return """
        async (params) => {
            const app = document.querySelector('#app');
            const vm = app && app.__vue__;
            const http = vm && (vm.$http || (vm.$root && vm.$root.$http));
            if (!http) {
                return { error: 'Qimai Vue $http is unavailable' };
            }
            const call = async (name, path) => {
                try {
                    const response = await http.get(path, { params });
                    return {
                        url: 'https://api.qimai.cn' + path,
                        payload: response && response.data ? response.data : response,
                    };
                } catch (error) {
                    return {
                        url: 'https://api.qimai.cn' + path,
                        payload: error && error.response && error.response.data
                            ? error.response.data
                            : { code: 'client_error', msg: String(error) },
                    };
                }
            };
            return {
                download: await call('download', '/pred/download'),
                revenue: await call('revenue', '/pred/revenue'),
            };
        }
    """


def _append_pred_estimate_payloads(state: "_QimaiCaptureState", responses: Any) -> None:
    if not isinstance(responses, dict):
        state.warnings.append("Qimai pred estimate request returned an invalid response.")
        return
    if responses.get("error"):
        state.warnings.append(str(responses["error"]))
        return
    for key in ("download", "revenue"):
        item = responses.get(key)
        if not isinstance(item, dict):
            continue
        url = str(item.get("url") or f"https://api.qimai.cn/pred/{key}")
        state._append_payload(url, item.get("payload"))


async def _click_text_async(page: Any, text: str, delay: float = 0.7, jitter: float = 0.0) -> None:
    try:
        await page.get_by_text(text, exact=True).first.click(timeout=1500)
        wait_seconds = _throttle_seconds(delay, min(jitter, delay))
        logger.debug("[Qimai] throttle after click '{}': {:.2f}s", text, wait_seconds)
        await asyncio.sleep(wait_seconds)
    except Exception:
        return


def _click_text_sync(page: Any, text: str, delay: float = 0.7, jitter: float = 0.0) -> None:
    try:
        page.get_by_text(text, exact=True).first.click(timeout=1500)
        wait_seconds = _throttle_seconds(delay, min(jitter, delay))
        logger.debug("[Qimai] throttle after click '{}': {:.2f}s", text, wait_seconds)
        time.sleep(wait_seconds)
    except Exception:
        return


def _throttle_seconds(base_delay: float, jitter: float) -> float:
    base = max(float(base_delay or 0), 0.0)
    spread = max(float(jitter or 0), 0.0)
    return base + random.uniform(0, spread)


async def _safe_scroll_async(page: Any) -> None:
    try:
        await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
    except Exception:
        return


def _safe_scroll_sync(page: Any) -> None:
    try:
        page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
    except Exception:
        return


def _first_nonempty_line(text: str) -> str:
    ignored = {"\u4e03\u9ea6\u6570\u636e", "qimai", "\u767b\u5f55", "\u6ce8\u518c"}
    for raw in (text or "").splitlines():
        line = raw.strip()
        if not line or line.lower() in ignored:
            continue
        if len(line) > 40:
            continue
        return line
    return ""


def _looks_like_navigation_text(value: Any) -> bool:
    text = str(value or "").strip()
    return text in {
        "\u4e03\u9ea6\u6570\u636e",
        "\u699c\u5355",
        "\u5de5\u5177",
        "Apple Ads",
        "\u7814\u7a76\u9662",
        "\u4e03\u9ea6\u670d\u52a1",
        "NextWorld",
        "\u767b\u5f55",
        "\u6ce8\u518c",
    }


def _extract_rating(text: str) -> tuple[Any | None, int | None]:
    rating = None
    rating_count = None
    count_match = re.search(r"([0-9,.\u4e07]+)\s*\u4e2a\u8bc4\u5206", text)
    if count_match:
        rating_count = _to_int(count_match.group(1))
        after_count = text[count_match.end() : count_match.end() + 80]
        rating_match = re.search(r"([0-5](?:\.\d+)?)", after_count)
        if rating_match:
            rating = _to_number(rating_match.group(1))
    if rating is None:
        rating_match = re.search(r"\u8bc4\u5206\s*([0-5](?:\.\d+)?)", text)
        if rating_match:
            rating = _to_number(rating_match.group(1))
    return rating, rating_count


def _extract_rank_near_label(text: str, label: str) -> str:
    compact = re.sub(r"\s+", " ", text or "")
    patterns = [
        rf"{re.escape(label)}[^0-9\u7b2c-]{{0,20}}\u7b2c\s*([0-9,]+)\s*\u540d",
        rf"\u7b2c\s*([0-9,]+)\s*\u540d[^。；;\n]{{0,20}}{re.escape(label)}",
    ]
    for pattern in patterns:
        match = re.search(pattern, compact)
        if match:
            return f"#{match.group(1).replace(',', '')}"
    return ""


def _extract_grossing_rank_from_public_rank(text: str, app_name: str) -> str:
    if not text or not app_name:
        return ""
    lines = [line.strip() for line in text.splitlines() if line.strip()]
    if not lines:
        return ""

    section_start = 0
    for index, line in enumerate(lines):
        if "\u7545\u9500\u699c" in line:
            section_start = index + 1
            break
    section = lines[section_start:]
    for index, line in enumerate(section):
        if app_name not in line:
            continue
        for previous in reversed(section[max(0, index - 4):index]):
            if re.fullmatch(r"\d{1,3}", previous):
                return f"#{previous}"
        match = re.search(rf"(?:^|\s)(\d{{1,3}})\s+{re.escape(app_name)}", " ".join(section[max(0, index - 4):index + 1]))
        if match:
            return f"#{match.group(1)}"
    return ""


def _extract_yesterday_downloads(text: str) -> int | None:
    match = re.search(r"\u6628\u65e5\u4e0b\u8f7d\u91cf\s*([0-9,.\u4e07]+)", text or "")
    if not match:
        return None
    return _to_int(match.group(1))


def _walk_json(value: Any, path: str = "") -> Iterable[tuple[str, Any]]:
    if isinstance(value, dict):
        for key, child in value.items():
            child_path = f"{path}.{key}" if path else str(key)
            yield child_path, child
            yield from _walk_json(child, child_path)
    elif isinstance(value, list):
        for index, child in enumerate(value):
            child_path = f"{path}[{index}]"
            yield child_path, child
            yield from _walk_json(child, child_path)


def _find_scalar(candidates: Iterable[tuple[str, Any]], keys: tuple[str, ...], *, prefer_float: bool = False) -> Any | None:
    for path, value in candidates:
        lowered = path.lower()
        if not any(key in lowered for key in keys):
            continue
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            return float(value) if prefer_float else value
        if isinstance(value, str):
            converted = _to_number(value)
            if converted is not None:
                return converted
    return None


def _extract_series_candidates(api_payloads: list[dict[str, Any]]) -> list[dict[str, Any]]:
    candidates: list[dict[str, Any]] = []
    for item in api_payloads:
        url = item.get("url", "")
        api_category = _qimai_api_category(url)
        for path, value in _walk_json(item.get("payload")):
            if not isinstance(value, list) or len(value) < 2:
                continue
            series = _normalize_series(value)
            if len(series) >= 2:
                candidates.append({"url": url, "path": path, "category": api_category, "series": series})
    return candidates


def _extract_pred_estimate_scalars(api_payloads: list[dict[str, Any]], result: dict[str, Any]) -> None:
    for item in api_payloads:
        category = _qimai_api_category(str(item.get("url", "")))
        if category not in {"download", "revenue"}:
            continue
        payload = item.get("payload")
        data = payload.get("data") if isinstance(payload, dict) else None
        if not isinstance(data, dict):
            continue
        if category == "download":
            result["downloads_avg_30d"] = result.get("downloads_avg_30d") or _first_metric(
                data,
                (
                    "monthDownloadIphoneAvg",
                    "monthDownloadAvg",
                    "monthDownloadIpadAvg",
                    "month_download_iphone_avg",
                    "month_download_avg",
                ),
            )
            result["yesterday_downloads"] = result.get("yesterday_downloads") or _first_metric(
                data,
                (
                    "yesterdayDownloadIphone",
                    "yesterdayDownload",
                    "yesterdayDownloadIpad",
                    "yesterday_download_iphone",
                    "yesterday_download",
                ),
            )
        elif category == "revenue":
            result["revenue_avg_30d"] = result.get("revenue_avg_30d") or _first_metric(
                data,
                (
                    "monthRevenueIphoneAvg",
                    "monthRevenueAvg",
                    "monthRevenueIpadAvg",
                    "month_revenue_iphone_avg",
                    "month_revenue_avg",
                ),
            )


def _first_metric(data: dict[str, Any], keys: tuple[str, ...]) -> int | float | None:
    for key in keys:
        if key not in data:
            continue
        value = _to_number(data.get(key))
        if value is not None:
            return value
    return None


def _qimai_api_category(url: str) -> str:
    lowered = (url or "").lower()
    category_tokens = {
        "rank": ("rank", "ranking", "brand"),
        "comment": ("comment", "review"),
        "appstatus": ("appstatus", "active", "dau", "status"),
        "download": ("download", "downloads", "down"),
        "revenue": ("revenue", "income", "sales"),
    }
    for category, tokens in category_tokens.items():
        if any(token in lowered for token in tokens):
            return category
    return ""


def _normalize_series(items: list[Any]) -> list[dict[str, Any]]:
    series: list[dict[str, Any]] = []
    for item in items:
        point = _normalize_point(item)
        if point:
            series.append(point)
    return _dedupe_series(series)


def _normalize_point(item: Any) -> dict[str, Any] | None:
    if isinstance(item, dict):
        date_value = _first_key_value(item, ("date", "day", "time", "timestamp", "dt", "x"))
        metric_value = _first_key_value(
            item,
            ("value", "num", "count", "rank", "ranking", "income", "revenue", "download", "downloads", "dau", "y"),
        )
        if metric_value is None:
            for key, value in item.items():
                if key == date_value:
                    continue
                if _to_number(value) is not None:
                    metric_value = value
                    break
        date_text = _normalize_date(date_value)
        value = _to_number(metric_value)
        if date_text and value is not None:
            return {"date": date_text, "value": value}
    elif isinstance(item, (list, tuple)) and len(item) >= 2:
        date_text = _normalize_date(item[0])
        value = _to_number(item[1])
        if date_text and value is not None:
            return {"date": date_text, "value": value}
    return None


def _first_key_value(item: dict[str, Any], keys: tuple[str, ...]) -> Any:
    lowered = {str(key).lower(): value for key, value in item.items()}
    for key in keys:
        if key in lowered:
            return lowered[key]
    for raw_key, value in item.items():
        raw_lower = str(raw_key).lower()
        if any(key in raw_lower for key in keys):
            return value
    return None


def _normalize_date(value: Any) -> str:
    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, (int, float)):
        timestamp = float(value)
        if timestamp > 10_000_000_000:
            timestamp = timestamp / 1000
        try:
            return datetime.fromtimestamp(timestamp, tz=timezone.utc).date().isoformat()
        except Exception:
            return ""
    text = str(value or "").strip()
    if not text:
        return ""
    match = re.search(r"20\d{2}[-/.]\d{1,2}[-/.]\d{1,2}", text)
    if match:
        return match.group(0).replace("/", "-").replace(".", "-")
    match = re.search(r"(\d{1,2})[-/.](\d{1,2})", text)
    if match:
        current_year = date.today().year
        return f"{current_year}-{int(match.group(1)):02d}-{int(match.group(2)):02d}"
    return ""


def _to_number(value: Any) -> float | int | None:
    if isinstance(value, bool) or value is None:
        return None
    if isinstance(value, (int, float)):
        return value
    text = str(value).strip().replace(",", "")
    if not text or text in {"-", "--"}:
        return None
    multiplier = 1
    if "\u4e07" in text:
        multiplier = 10000
        text = text.replace("\u4e07", "")
    if "\u4ebf" in text:
        multiplier = 100000000
        text = text.replace("\u4ebf", "")
    text = re.sub(r"[^0-9.\-]", "", text)
    if not text or text in {"-", "."}:
        return None
    try:
        number = float(text) * multiplier
    except ValueError:
        return None
    return int(number) if number.is_integer() else number


def _to_int(value: Any) -> int | None:
    number = _to_number(value)
    if number is None:
        return None
    return int(number)


def _pick_series(
    candidates: list[dict[str, Any]],
    *,
    include: tuple[str, ...],
    url_include: tuple[str, ...] = (),
    prefer_lower_values: bool = False,
) -> list[dict[str, Any]]:
    scored: list[tuple[int, dict[str, Any]]] = []
    expected_categories = _expected_qimai_categories(url_include)
    for candidate in candidates:
        category = str(candidate.get("category", ""))
        if expected_categories and category not in expected_categories:
            continue

        haystack = f"{candidate.get('path', '')}".lower()
        score = 0
        score += sum(5 for token in include if token.lower() in haystack)
        if category in expected_categories:
            score += 20
        score += min(len(candidate.get("series", [])), 90) // 10
        if prefer_lower_values:
            values = [point["value"] for point in candidate.get("series", []) if isinstance(point.get("value"), (int, float))]
            if values and max(values) <= 2000:
                score += 5
        if score > 0:
            scored.append((score, candidate))
    if not scored:
        return []
    scored.sort(key=lambda item: item[0], reverse=True)
    return scored[0][1]["series"]


def _expected_qimai_categories(url_include: tuple[str, ...]) -> set[str]:
    categories: set[str] = set()
    for token in url_include:
        category = _qimai_api_category(token)
        if category:
            categories.add(category)
    return categories


def _dedupe_series(series: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by_date: dict[str, dict[str, Any]] = {}
    for point in series:
        by_date[point["date"]] = point
    return list(by_date.values())


def _sort_series(series: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return sorted(series, key=lambda point: point.get("date", ""))


def _trim_series(series: list[dict[str, Any]], days: int) -> list[dict[str, Any]]:
    cutoff = date.today() - timedelta(days=days + 7)
    trimmed: list[dict[str, Any]] = []
    for point in series:
        try:
            point_date = datetime.fromisoformat(str(point["date"])).date()
        except Exception:
            trimmed.append(point)
            continue
        if point_date >= cutoff:
            trimmed.append(point)
    return trimmed[-days:]


def _average_last_n(series: list[dict[str, Any]], n: int) -> float | None:
    values = [
        float(point["value"])
        for point in _sort_series(series)[-n:]
        if isinstance(point.get("value"), (int, float)) and not isinstance(point.get("value"), bool)
    ]
    if not values:
        return None
    return round(statistics.fmean(values), 2)


def _series_debug_summary(candidates: list[dict[str, Any]]) -> list[dict[str, Any]]:
    summary = []
    for candidate in candidates[:20]:
        summary.append(
            {
                "url": candidate.get("url", "").split("?")[0],
                "path": candidate.get("path"),
                "category": candidate.get("category", ""),
                "points": len(candidate.get("series", [])),
            }
        )
    return summary
