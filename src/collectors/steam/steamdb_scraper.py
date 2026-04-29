"""
SteamDB browser automation collector.

Uses Playwright/CDP to collect SteamDB charts, info, patch notes, sales,
and top-seller data for internal analysis. If SteamDB or Cloudflare blocks
access, this module raises SteamDBScrapeFailed so callers can fall back.
"""
from __future__ import annotations

import asyncio
import random
import re
import sys
import time
from datetime import datetime, timezone
from typing import Any

from loguru import logger

from src.collectors.steam.human_behavior import HumanBehaviorSimulator
from src.collectors.steam.rate_limiter import AdaptiveRateLimiter


class SteamDBScrapeFailed(Exception):
    """SteamDB 閲囬泦澶辫触锛堣Е鍙戝厹搴曞垏鎹級"""
    pass


_HIGHCHARTS_EXTRACT_SCRIPT = """
() => {
  const charts = (window.Highcharts && window.Highcharts.charts || []).filter(Boolean);
  return charts.map((chart) => ({
    title: chart.title && chart.title.textStr || "",
    series: (chart.series || []).map((series) => ({
      name: series.name || "",
      points: (series.points || []).map((point) => ({
        x: point.x,
        y: point.y,
        name: point.name || "",
      })),
      data: (series.options && series.options.data || []).slice(0, 5000),
    })),
  }));
}
"""


class SteamDBScraper:
    """Playwright 椹卞姩鐨?SteamDB 閲囬泦鍣"""

    BASE_URL = "https://steamdb.info"

    def __init__(
        self,
        headless: bool = True,
        timeout: int = 30000,
        request_delay: float = 3.0,
        cookie: str = "",
        extra_headers: dict[str, str] | None = None,
        cdp_enabled: bool = True,
        cdp_port: int = 9222,
        request_jitter: float = 4.0,
        page_delay: float = 5.0,
        max_games_per_session: int = 10,
    ):
        self._headless = headless
        self._timeout = timeout
        self._delay = request_delay
        self._jitter = request_jitter
        self._page_delay = page_delay
        self._cdp_enabled = cdp_enabled
        self._cdp_port = cdp_port
        self._max_games_per_session = max_games_per_session
        self._extra_headers = dict(extra_headers or {})
        if cookie:
            self._extra_headers.setdefault("Cookie", cookie)
        self._browser = None
        self._browser_is_cdp = False
        self._playwright = None
        self._rate_limiter = AdaptiveRateLimiter(
            base_delay=request_delay,
            jitter_std=request_jitter,
            max_requests_per_session=max_games_per_session * 5,
        )
        self._behavior = HumanBehaviorSimulator()

    async def setup(self) -> None:
        """鍚姩 Playwright 娴忚鍣"""
        try:
            from playwright.async_api import async_playwright
        except ImportError:
            raise SteamDBScrapeFailed(
                "playwright 鏈畨瑁呫€傝繍琛? pip install playwright && playwright install chromium"
            )

        self._playwright = await async_playwright().start()
        if self._cdp_enabled:
            try:
                self._browser = await self._playwright.chromium.connect_over_cdp(
                    f"http://127.0.0.1:{self._cdp_port}"
                )
                self._browser_is_cdp = True
                logger.info(f"[SteamDB] Connected to local browser over CDP port {self._cdp_port}")
                return
            except Exception as exc:
                logger.warning(f"[SteamDB] CDP connection failed, falling back to new browser: {exc}")
        self._browser = await self._playwright.chromium.launch(
            headless=self._headless,
            args=[
                "--disable-blink-features=AutomationControlled",
                "--no-sandbox",
            ],
        )
        self._browser_is_cdp = False
        logger.info("[SteamDB] Playwright 娴忚鍣ㄥ凡鍚姩")

    def _should_use_threaded_playwright(self) -> bool:
        """Windows SelectorEventLoop 涓嬫敼鐢ㄧ嫭绔嬬嚎绋嬩腑鐨勫悓姝?Playwright銆"""
        if sys.platform != "win32":
            return False
        loop_name = asyncio.get_running_loop().__class__.__name__
        return "Selector" in loop_name

    async def teardown(self) -> None:
        """鍏抽棴娴忚鍣"""
        if self._browser and not self._browser_is_cdp:
            try:
                await self._browser.close()
            except Exception:
                pass
        if self._playwright:
            await self._playwright.stop()
        self._browser = None
        self._browser_is_cdp = False
        self._playwright = None

    async def scrape(
        self,
        app_id: str | int,
        time_slice: str = "monthly_peak_1y",
        *,
        cookie: str = "",
        extra_headers: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        """
        閲囬泦 SteamDB 涓婃寚瀹氭父鎴忕殑鏁版嵁銆?
        Returns:
            鍖呭惈 charts 鍜?info 鏁版嵁鐨勫瓧鍏搞€?
        Raises:
            SteamDBScrapeFailed: 褰?Cloudflare 鎷︽埅鎴栭〉闈㈣В鏋愬け璐ユ椂銆?        """
        if self._should_use_threaded_playwright():
            logger.info("[SteamDB] 褰撳墠浜嬩欢寰幆涓嶅吋瀹癸紝鏀圭敤鐙珛绾跨▼鍚姩 Playwright")
            return await asyncio.to_thread(self._scrape_sync, app_id, time_slice, cookie, extra_headers)

        if not self._browser:
            await self.setup()

        result: dict[str, Any] = {
            "source": "steamdb_playwright",
            "app_id": int(app_id),
            "requested_time_slice": time_slice,
        }

        close_context = False
        if self._browser_is_cdp and self._browser.contexts:
            context = self._browser.contexts[0]
        else:
            context_kwargs: dict[str, Any] = {
                "viewport": {"width": 1920, "height": 1080},
                "user_agent": _random_user_agent(),
                "locale": "en-US",
            }
            context_headers = self._request_headers(cookie=cookie, extra_headers=extra_headers)
            if context_headers:
                context_kwargs["extra_http_headers"] = context_headers
            context = await self._browser.new_context(**context_kwargs)
            close_context = True

        page = None
        try:
            page = await context.new_page()

            # 鈹€鈹€ Charts 椤甸潰 鈹€鈹€
            charts_data = await self._scrape_charts(page, app_id, time_slice=time_slice)
            result["charts"] = charts_data

            await self._rate_limiter.wait("charts -> info")

            # 鈹€鈹€ Info 椤甸潰 鈹€鈹€
            info_data = await self._scrape_info(page, app_id)
            result["info"] = info_data

            await self._rate_limiter.wait("info -> patchnotes")

            result["patchnotes"] = await self._scrape_patchnotes(page, app_id)

            await self._rate_limiter.wait("patchnotes -> sales")

            result["sales"] = await self._scrape_sales(page, app_id)

            await self._rate_limiter.wait("sales -> top_sellers")

            # 鈹€鈹€ 褰撳墠 Steam 鍏ㄧ悆鐣呴攢姒?鈹€鈹€
            result["top_sellers"] = await self._scrape_top_sellers(page, app_id)

        except SteamDBScrapeFailed:
            raise
        except Exception as e:
            logger.error(f"[SteamDB] 閲囬泦寮傚父: {e}")
            raise SteamDBScrapeFailed(f"Playwright 閲囬泦澶辫触: {e}") from e
        finally:
            if page is not None:
                try:
                    await page.close()
                except Exception:
                    pass
            if close_context:
                await context.close()

        return result

    def _scrape_sync(
        self,
        app_id: str | int,
        time_slice: str = "monthly_peak_1y",
        cookie: str = "",
        extra_headers: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        """鍦ㄧ嫭绔嬬嚎绋嬩腑浣跨敤鍚屾 Playwright锛岀粫寮€ Windows SelectorEventLoop 闄愬埗銆"""
        try:
            from playwright.sync_api import sync_playwright
        except ImportError:
            raise SteamDBScrapeFailed(
                "playwright 鏈畨瑁呫€傝繍琛? pip install playwright && playwright install chromium"
            )

        result: dict[str, Any] = {
            "source": "steamdb_playwright",
            "app_id": int(app_id),
            "requested_time_slice": time_slice,
        }

        try:
            with sync_playwright() as playwright:
                is_cdp = False
                if self._cdp_enabled:
                    try:
                        browser = playwright.chromium.connect_over_cdp(f"http://127.0.0.1:{self._cdp_port}")
                        is_cdp = True
                        logger.info(f"[SteamDB] Connected to local browser over CDP port {self._cdp_port}")
                    except Exception as exc:
                        logger.warning(f"[SteamDB] CDP connection failed, falling back to new browser: {exc}")
                        browser = None
                else:
                    browser = None
                if browser is None:
                    browser = playwright.chromium.launch(
                        headless=self._headless,
                        args=[
                            "--disable-blink-features=AutomationControlled",
                            "--no-sandbox",
                        ],
                    )
                close_context = False
                if is_cdp and browser.contexts:
                    context = browser.contexts[0]
                else:
                    context_kwargs: dict[str, Any] = {
                        "viewport": {"width": 1920, "height": 1080},
                        "user_agent": _random_user_agent(),
                        "locale": "en-US",
                    }
                    context_headers = self._request_headers(cookie=cookie, extra_headers=extra_headers)
                    if context_headers:
                        context_kwargs["extra_http_headers"] = context_headers
                    context = browser.new_context(**context_kwargs)
                    close_context = True
                try:
                    page = context.new_page()
                    result["charts"] = self._scrape_charts_sync(page, app_id, time_slice=time_slice)
                    self._rate_limiter.wait_sync("charts -> info")
                    result["info"] = self._scrape_info_sync(page, app_id)
                    self._rate_limiter.wait_sync("info -> patchnotes")
                    result["patchnotes"] = self._scrape_patchnotes_sync(page, app_id)
                    self._rate_limiter.wait_sync("patchnotes -> sales")
                    result["sales"] = self._scrape_sales_sync(page, app_id)
                    self._rate_limiter.wait_sync("sales -> top_sellers")
                    result["top_sellers"] = self._scrape_top_sellers_sync(page, app_id)
                finally:
                    try:
                        page.close()
                    except Exception:
                        pass
                    if close_context:
                        context.close()
                    if not is_cdp:
                        browser.close()
        except SteamDBScrapeFailed:
            raise
        except Exception as e:
            logger.error(f"[SteamDB] 鍚屾绾跨▼閲囬泦寮傚父: {e}")
            raise SteamDBScrapeFailed(f"Playwright 绾跨▼閲囬泦澶辫触: {e}") from e

        return result

    # 鈹€鈹€ 鍐呴儴閲囬泦鏂规硶 鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€

    def _request_headers(
        self,
        *,
        cookie: str = "",
        extra_headers: dict[str, str] | None = None,
    ) -> dict[str, str]:
        headers = dict(self._extra_headers)
        if isinstance(extra_headers, dict):
            headers.update(
                {
                    str(key): str(value)
                    for key, value in extra_headers.items()
                    if key not in (None, "") and value not in (None, "")
                }
            )
        if cookie:
            headers["Cookie"] = cookie
        return headers

    async def _wait_out_cloudflare_async(self, page: Any, label: str) -> bool:
        logger.info(f"[SteamDB] Cloudflare/403 detected on {label}; waiting for browser session")
        for wait_ms in (5000, 10000, 15000, 30000):
            await page.wait_for_timeout(wait_ms)
            content = await page.content()
            if "challenge-platform" not in content and "Just a moment" not in content:
                try:
                    response = await page.goto(page.url, wait_until="domcontentloaded", timeout=self._timeout)
                    if not response or response.status != 403:
                        return True
                except Exception:
                    return True
        return False

    def _wait_out_cloudflare_sync(self, page: Any, label: str) -> bool:
        logger.info(f"[SteamDB] Cloudflare/403 detected on {label}; waiting for browser session")
        for wait_ms in (5000, 10000, 15000, 30000):
            page.wait_for_timeout(wait_ms)
            content = page.content()
            if "challenge-platform" not in content and "Just a moment" not in content:
                try:
                    response = page.goto(page.url, wait_until="domcontentloaded", timeout=self._timeout)
                    if not response or response.status != 403:
                        return True
                except Exception:
                    return True
        return False

    async def _scrape_charts(
        self, page: Any, app_id: str | int, time_slice: str = "monthly_peak_1y"
    ) -> dict[str, Any]:
        """閲囬泦 charts 椤甸潰: 鍦ㄧ嚎瓒嬪娍銆佸嘲鍊间汉鏁"""
        url = _build_charts_url(self.BASE_URL, app_id, time_slice)
        logger.info(f"[SteamDB] 璁块棶 charts: {url}")

        try:
            resp = await page.goto(url, wait_until="domcontentloaded", timeout=self._timeout)
        except Exception as e:
            raise SteamDBScrapeFailed(f"Charts 椤甸潰鍔犺浇澶辫触: {e}")

        if resp and resp.status == 403 and await self._wait_out_cloudflare_async(page, "charts"):
            resp = None
        if resp and resp.status == 403:
            raise SteamDBScrapeFailed("Cloudflare 403 blocked")

        # 妫€娴?Cloudflare challenge
        content = await page.content()
        if ("challenge-platform" in content or "Just a moment" in content) and await self._wait_out_cloudflare_async(page, "charts"):
            content = await page.content()
        if "challenge-platform" in content or "Just a moment" in content:
            raise SteamDBScrapeFailed("Cloudflare challenge blocked")

        await page.wait_for_timeout(2000)
        await _wait_for_highcharts_async(page)
        await self._behavior.after_navigation(page)

        charts: dict[str, Any] = {
            "requested_time_slice": time_slice,
            "chart_url": url,
        }

        # Extract key chart summary values.
        try:
            # 褰撳墠鍦ㄧ嚎鏁版嵁锛堥〉闈㈤《閮ㄧ粺璁″尯鍩燂級
            stat_elements = await page.query_selector_all(
                ".app-chart-numbers .number-group"
            )
            for el in stat_elements:
                label = await el.query_selector(".label")
                value = await el.query_selector(".value")
                if label and value:
                    label_text = (await label.inner_text()).strip().lower()
                    value_text = (await value.inner_text()).strip()
                    charts[_normalize_key(label_text)] = value_text
        except Exception as e:
            logger.debug(f"[SteamDB] charts 缁熻鍖烘彁鍙栧け璐? {e}")

        # 灏濊瘯浠庨〉闈㈡枃鏈腑鎻愬彇鍏抽敭鏁板瓧
        try:
            page_text = await page.inner_text("body")
            charts.update(_extract_numbers_from_text(page_text))
        except Exception:
            pass

        try:
            highcharts_payload = await page.evaluate(_HIGHCHARTS_EXTRACT_SCRIPT)
            _merge_highcharts_payload(charts, highcharts_payload)
        except Exception as e:
            logger.debug(f"[SteamDB] Highcharts 搴忓垪鎻愬彇澶辫触: {e}")

        if not _has_meaningful_chart_data(charts):
            # Fallback: extract a few visible tables.
            try:
                tables = await page.query_selector_all("table")
                for i, table in enumerate(tables[:3]):
                    rows = await table.query_selector_all("tr")
                    table_data = []
                    for row in rows[:20]:
                        cells = await row.query_selector_all("td, th")
                        row_text = []
                        for cell in cells:
                            row_text.append((await cell.inner_text()).strip())
                        if row_text:
                            table_data.append(row_text)
                    if table_data:
                        charts[f"table_{i}"] = table_data
            except Exception as e:
                logger.debug(f"[SteamDB] 琛ㄦ牸鎻愬彇澶辫触: {e}")

        return charts

    async def _scrape_patchnotes(self, page: Any, app_id: str | int) -> dict[str, Any]:
        url = f"{self.BASE_URL}/app/{app_id}/patchnotes/"
        logger.info(f"[SteamDB] visit patchnotes: {url}")
        try:
            resp = await page.goto(url, wait_until="domcontentloaded", timeout=self._timeout)
        except Exception as e:
            return {"source": "steamdb_patchnotes", "url": url, "error": str(e), "items": []}
        if resp and resp.status == 403 and await self._wait_out_cloudflare_async(page, "patchnotes"):
            resp = None
        if resp and resp.status == 403:
            self._rate_limiter.report_blocked()
            raise SteamDBScrapeFailed("Cloudflare 403")
        content = await page.content()
        if ("challenge-platform" in content or "Just a moment" in content) and await self._wait_out_cloudflare_async(page, "patchnotes"):
            content = await page.content()
        if "challenge-platform" in content or "Just a moment" in content:
            self._rate_limiter.report_challenge()
            raise SteamDBScrapeFailed("Cloudflare challenge")
        await page.wait_for_timeout(1500)
        await self._behavior.after_navigation(page)
        text = await page.inner_text("body")
        return _parse_patchnotes_text(text, url)

    async def _scrape_sales(self, page: Any, app_id: str | int) -> dict[str, Any]:
        url = f"{self.BASE_URL}/app/{app_id}/sales/"
        logger.info(f"[SteamDB] visit sales: {url}")
        try:
            resp = await page.goto(url, wait_until="domcontentloaded", timeout=self._timeout)
        except Exception as e:
            return {"source": "steamdb_sales", "url": url, "error": str(e)}
        if resp and resp.status == 403 and await self._wait_out_cloudflare_async(page, "sales"):
            resp = None
        if resp and resp.status == 403:
            self._rate_limiter.report_blocked()
            raise SteamDBScrapeFailed("Cloudflare 403")
        content = await page.content()
        if ("challenge-platform" in content or "Just a moment" in content) and await self._wait_out_cloudflare_async(page, "sales"):
            content = await page.content()
        if "challenge-platform" in content or "Just a moment" in content:
            self._rate_limiter.report_challenge()
            raise SteamDBScrapeFailed("Cloudflare challenge")
        await page.wait_for_timeout(1500)
        await self._behavior.after_navigation(page)
        text = await page.inner_text("body")
        return _parse_sales_text(text, url)

    async def _scrape_info(
        self, page: Any, app_id: str | int
    ) -> dict[str, Any]:
        """閲囬泦 info 椤甸潰: 鐗堟湰鏇存柊銆佸彂甯冨晢淇℃伅"""
        url = f"{self.BASE_URL}/app/{app_id}/info/"
        logger.info(f"[SteamDB] 璁块棶 info: {url}")

        try:
            resp = await page.goto(url, wait_until="domcontentloaded", timeout=self._timeout)
        except Exception as e:
            raise SteamDBScrapeFailed(f"Info 椤甸潰鍔犺浇澶辫触: {e}")

        if resp and resp.status == 403:
            if await self._wait_out_cloudflare_async(page, "info"):
                resp = None
        if resp and resp.status == 403:
            raise SteamDBScrapeFailed("Cloudflare 403 blocked")

        content = await page.content()
        if "challenge-platform" in content or "Just a moment" in content:
            if await self._wait_out_cloudflare_async(page, "info"):
                content = await page.content()
        if "challenge-platform" in content or "Just a moment" in content:
            raise SteamDBScrapeFailed("Cloudflare challenge blocked")

        await page.wait_for_timeout(2000)
        await self._behavior.after_navigation(page)

        info: dict[str, Any] = {}

        # Extract key-value info rows.
        try:
            kv_rows = await page.query_selector_all(
                "table.table-dark tr, .app-info tr"
            )
            for row in kv_rows[:50]:
                cells = await row.query_selector_all("td")
                if len(cells) >= 2:
                    key = (await cells[0].inner_text()).strip()
                    val = (await cells[1].inner_text()).strip()
                    if key and val:
                        info[_normalize_key(key)] = val
        except Exception as e:
            logger.debug(f"[SteamDB] info KV 鎻愬彇澶辫触: {e}")

        # 灏濊瘯鎻愬彇鏇存柊鍘嗗彶
        try:
            page_text = await page.inner_text("body")
            info["steamdb_signed_in"] = not _looks_signed_out(page_text)
            info["page_text_preview"] = page_text[:2000]
        except Exception:
            pass

        return info

    async def _scrape_top_sellers(self, page: Any, app_id: str | int) -> dict[str, Any]:
        """閲囬泦 SteamDB 褰撳墠鍏ㄧ悆鐣呴攢姒滄帓鍚嶃€"""
        url = f"{self.BASE_URL}/stats/globaltopsellers/"
        logger.info(f"[SteamDB] 璁块棶 global top sellers: {url}")
        try:
            resp = await page.goto(url, wait_until="domcontentloaded", timeout=self._timeout)
        except Exception as e:
            logger.debug(f"[SteamDB] global top sellers 鍔犺浇澶辫触: {e}")
            return {"source": "steamdb_globaltopsellers", "url": url, "rank": "", "error": str(e)}

        if resp and resp.status == 403:
            if await self._wait_out_cloudflare_async(page, "top_sellers"):
                resp = None
        if resp and resp.status == 403:
            raise SteamDBScrapeFailed("Cloudflare 403 blocked")

        content = await page.content()
        if "challenge-platform" in content or "Just a moment" in content:
            if await self._wait_out_cloudflare_async(page, "top_sellers"):
                content = await page.content()
        if "challenge-platform" in content or "Just a moment" in content:
            raise SteamDBScrapeFailed("Cloudflare challenge blocked")

        rows = await page.query_selector_all("table tbody tr")
        return await _parse_top_sellers_rows_async(rows, app_id, url)

    def _scrape_charts_sync(
        self, page: Any, app_id: str | int, time_slice: str = "monthly_peak_1y"
    ) -> dict[str, Any]:
        """鍚屾鐗堟湰鐨?charts 閲囬泦銆"""
        url = _build_charts_url(self.BASE_URL, app_id, time_slice)
        logger.info(f"[SteamDB] 璁块棶 charts: {url}")

        try:
            resp = page.goto(url, wait_until="domcontentloaded", timeout=self._timeout)
        except Exception as e:
            raise SteamDBScrapeFailed(f"Charts 椤甸潰鍔犺浇澶辫触: {e}")

        if resp and resp.status == 403:
            if self._wait_out_cloudflare_sync(page, "charts"):
                resp = None
        if resp and resp.status == 403:
            raise SteamDBScrapeFailed("Cloudflare 403 blocked")

        content = page.content()
        if "challenge-platform" in content or "Just a moment" in content:
            if self._wait_out_cloudflare_sync(page, "charts"):
                content = page.content()
        if "challenge-platform" in content or "Just a moment" in content:
            raise SteamDBScrapeFailed("Cloudflare challenge blocked")

        page.wait_for_timeout(2000)
        _wait_for_highcharts_sync(page)
        self._behavior.after_navigation_sync(page)

        charts: dict[str, Any] = {
            "requested_time_slice": time_slice,
            "chart_url": url,
        }

        try:
            stat_elements = page.query_selector_all(".app-chart-numbers .number-group")
            for el in stat_elements:
                label = el.query_selector(".label")
                value = el.query_selector(".value")
                if label and value:
                    label_text = label.inner_text().strip().lower()
                    value_text = value.inner_text().strip()
                    charts[_normalize_key(label_text)] = value_text
        except Exception as e:
            logger.debug(f"[SteamDB] charts 缁熻鍖烘彁鍙栧け璐? {e}")

        try:
            page_text = page.inner_text("body")
            charts.update(_extract_numbers_from_text(page_text))
        except Exception:
            pass

        try:
            highcharts_payload = page.evaluate(_HIGHCHARTS_EXTRACT_SCRIPT)
            _merge_highcharts_payload(charts, highcharts_payload)
        except Exception as e:
            logger.debug(f"[SteamDB] Highcharts 搴忓垪鎻愬彇澶辫触: {e}")

        if not _has_meaningful_chart_data(charts):
            try:
                tables = page.query_selector_all("table")
                for i, table in enumerate(tables[:3]):
                    rows = table.query_selector_all("tr")
                    table_data = []
                    for row in rows[:20]:
                        cells = row.query_selector_all("td, th")
                        row_text = [cell.inner_text().strip() for cell in cells]
                        if row_text:
                            table_data.append(row_text)
                    if table_data:
                        charts[f"table_{i}"] = table_data
            except Exception as e:
                logger.debug(f"[SteamDB] 琛ㄦ牸鎻愬彇澶辫触: {e}")

        return charts

    def _scrape_patchnotes_sync(self, page: Any, app_id: str | int) -> dict[str, Any]:
        url = f"{self.BASE_URL}/app/{app_id}/patchnotes/"
        logger.info(f"[SteamDB] visit patchnotes: {url}")
        try:
            resp = page.goto(url, wait_until="domcontentloaded", timeout=self._timeout)
        except Exception as e:
            return {"source": "steamdb_patchnotes", "url": url, "error": str(e), "items": []}
        if resp and resp.status == 403:
            raise SteamDBScrapeFailed("Cloudflare 403")
        content = page.content()
        if "challenge-platform" in content or "Just a moment" in content:
            raise SteamDBScrapeFailed("Cloudflare challenge")
        page.wait_for_timeout(1500)
        self._behavior.after_navigation_sync(page)
        return _parse_patchnotes_text(page.inner_text("body"), url)

    def _scrape_sales_sync(self, page: Any, app_id: str | int) -> dict[str, Any]:
        url = f"{self.BASE_URL}/app/{app_id}/sales/"
        logger.info(f"[SteamDB] visit sales: {url}")
        try:
            resp = page.goto(url, wait_until="domcontentloaded", timeout=self._timeout)
        except Exception as e:
            return {"source": "steamdb_sales", "url": url, "error": str(e)}
        if resp and resp.status == 403:
            raise SteamDBScrapeFailed("Cloudflare 403")
        content = page.content()
        if "challenge-platform" in content or "Just a moment" in content:
            raise SteamDBScrapeFailed("Cloudflare challenge")
        page.wait_for_timeout(1500)
        self._behavior.after_navigation_sync(page)
        return _parse_sales_text(page.inner_text("body"), url)

    def _scrape_info_sync(
        self, page: Any, app_id: str | int
    ) -> dict[str, Any]:
        """鍚屾鐗堟湰鐨?info 閲囬泦銆"""
        url = f"{self.BASE_URL}/app/{app_id}/info/"
        logger.info(f"[SteamDB] 璁块棶 info: {url}")

        try:
            resp = page.goto(url, wait_until="domcontentloaded", timeout=self._timeout)
        except Exception as e:
            raise SteamDBScrapeFailed(f"Info 椤甸潰鍔犺浇澶辫触: {e}")

        if resp and resp.status == 403:
            if self._wait_out_cloudflare_sync(page, "info"):
                resp = None
        if resp and resp.status == 403:
            raise SteamDBScrapeFailed("Cloudflare 403 blocked")

        content = page.content()
        if "challenge-platform" in content or "Just a moment" in content:
            if self._wait_out_cloudflare_sync(page, "info"):
                content = page.content()
        if "challenge-platform" in content or "Just a moment" in content:
            raise SteamDBScrapeFailed("Cloudflare challenge blocked")

        page.wait_for_timeout(2000)
        self._behavior.after_navigation_sync(page)

        info: dict[str, Any] = {}

        try:
            kv_rows = page.query_selector_all("table.table-dark tr, .app-info tr")
            for row in kv_rows[:50]:
                cells = row.query_selector_all("td")
                if len(cells) >= 2:
                    key = cells[0].inner_text().strip()
                    val = cells[1].inner_text().strip()
                    if key and val:
                        info[_normalize_key(key)] = val
        except Exception as e:
            logger.debug(f"[SteamDB] info KV 鎻愬彇澶辫触: {e}")

        try:
            page_text = page.inner_text("body")
            info["steamdb_signed_in"] = not _looks_signed_out(page_text)
            info["page_text_preview"] = page_text[:2000]
        except Exception:
            pass

        return info

    def _scrape_top_sellers_sync(self, page: Any, app_id: str | int) -> dict[str, Any]:
        """鍚屾鐗堟湰鐨?SteamDB 褰撳墠鍏ㄧ悆鐣呴攢姒滄帓鍚嶉噰闆嗐€"""
        url = f"{self.BASE_URL}/stats/globaltopsellers/"
        logger.info(f"[SteamDB] 璁块棶 global top sellers: {url}")
        try:
            resp = page.goto(url, wait_until="domcontentloaded", timeout=self._timeout)
        except Exception as e:
            logger.debug(f"[SteamDB] global top sellers 鍔犺浇澶辫触: {e}")
            return {"source": "steamdb_globaltopsellers", "url": url, "rank": "", "error": str(e)}

        if resp and resp.status == 403:
            if self._wait_out_cloudflare_sync(page, "top_sellers"):
                resp = None
        if resp and resp.status == 403:
            raise SteamDBScrapeFailed("Cloudflare 403 blocked")

        content = page.content()
        if "challenge-platform" in content or "Just a moment" in content:
            if self._wait_out_cloudflare_sync(page, "top_sellers"):
                content = page.content()
        if "challenge-platform" in content or "Just a moment" in content:
            raise SteamDBScrapeFailed("Cloudflare challenge blocked")

        rows = page.query_selector_all("table tbody tr")
        return _parse_top_sellers_rows_sync(rows, app_id, url)


# 鈹€鈹€ 宸ュ叿鍑芥暟 鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€

def _build_charts_url(base_url: str, app_id: str | int, time_slice: str = "monthly_peak_1y") -> str:
    fragment = _chart_fragment_for_time_slice(time_slice)
    return f"{base_url}/app/{app_id}/charts/{fragment}"


def _chart_fragment_for_time_slice(time_slice: str) -> str:
    if time_slice == "daily_precise_30d":
        return "#1m"
    if time_slice == "daily_precise_90d":
        return "#3m"
    return ""


async def _parse_top_sellers_rows_async(rows: list[Any], app_id: str | int, url: str) -> dict[str, Any]:
    target = f"/app/{app_id}/"
    for fallback_rank, row in enumerate(rows, start=1):
        html = await row.inner_html()
        if target not in html:
            continue
        text = (await row.inner_text()).strip()
        return _top_seller_result_from_text(text, fallback_rank, url)
    return {"source": "steamdb_globaltopsellers", "url": url, "rank": "", "matched": False}


def _parse_top_sellers_rows_sync(rows: list[Any], app_id: str | int, url: str) -> dict[str, Any]:
    target = f"/app/{app_id}/"
    for fallback_rank, row in enumerate(rows, start=1):
        html = row.inner_html()
        if target not in html:
            continue
        text = row.inner_text().strip()
        return _top_seller_result_from_text(text, fallback_rank, url)
    return {"source": "steamdb_globaltopsellers", "url": url, "rank": "", "matched": False}


def _top_seller_result_from_text(text: str, fallback_rank: int, url: str) -> dict[str, Any]:
    lines = [line.strip() for line in re.split(r"[\r\n\t]+", text) if line.strip()]
    rank = fallback_rank
    if lines:
        match = re.match(r"^(\d+)\.?", lines[0])
        if match:
            rank = int(match.group(1))
    name = next((line for line in lines if not re.fullmatch(r"\d+\.?", line)), "")
    return {
        "source": "steamdb_globaltopsellers",
        "url": url,
        "rank": rank,
        "name": name,
        "matched": True,
    }


def _parse_patchnotes_text(text: str, url: str) -> dict[str, Any]:
    lines = [line.strip() for line in str(text or "").splitlines() if line.strip()]
    items: list[dict[str, Any]] = []
    for index, line in enumerate(lines):
        if len(items) >= 20:
            break
        if not re.search(r"\b(update|patch|hotfix|build|release|version)\b", line, re.IGNORECASE):
            continue
        context = " ".join(lines[max(0, index - 2): index + 3])
        items.append(
            {
                "title": line[:240],
                "summary": context[:1000],
                "source": "steamdb_patchnotes",
            }
        )
    return {
        "source": "steamdb_patchnotes",
        "url": url,
        "items": items,
        "raw_preview": "\n".join(lines[:80])[:3000],
    }


def _parse_sales_text(text: str, url: str) -> dict[str, Any]:
    normalized = re.sub(r"\s+", " ", str(text or "")).strip()
    prices = re.findall(r"(?:\$|鈧瑋拢|楼|R\$)\s?[0-9]+(?:[.,][0-9]{2})?", normalized)
    discounts = re.findall(r"-\s?[0-9]{1,3}\s?%", normalized)
    return {
        "source": "steamdb_sales",
        "url": url,
        "prices": list(dict.fromkeys(prices))[:20],
        "discounts": list(dict.fromkeys(discounts))[:20],
        "raw_preview": normalized[:3000],
    }


def _merge_highcharts_payload(charts: dict[str, Any], payload: Any) -> None:
    if not isinstance(payload, list):
        return

    series_records = _extract_highcharts_series_records(payload)
    followers_records = _extract_named_series(payload, ("follower",))
    wishlist_records = _extract_named_series(payload, ("wishlist", "wishlists"))
    review_history_records = _extract_user_reviews_history(payload)
    
    availability = charts.get("online_history_availability")
    if not isinstance(availability, dict):
        availability = {}
        charts["online_history_availability"] = availability

    if followers_records:
        charts["followers_history"] = followers_records
    if wishlist_records:
        charts["wishlist_history"] = wishlist_records
    if review_history_records:
        charts["user_reviews_history_90d"] = review_history_records[-90:]
        charts["user_reviews_history"] = review_history_records
        charts["user_reviews_history_availability"] = {
            "positive_rate_90d": bool(charts["user_reviews_history_90d"]),
            "source": "steamdb_user_reviews_history",
        }
    else:
        charts["user_reviews_history_90d"] = []
        charts["user_reviews_history"] = []
        charts["user_reviews_history_availability"] = {
            "positive_rate_90d": False,
            "source": "steamdb_user_reviews_history",
            "reason": "SteamDB charts page did not expose User reviews history. This commonly requires a signed-in SteamDB browser session.",
        }

    if not series_records:
        availability["daily_precise_30d"] = False
        charts.setdefault("online_history_unavailable_reasons", {})["daily_precise_30d"] = (
            "SteamDB charts page did not expose a usable Highcharts player-count series."
        )
        return

    charts["online_history_daily_precise_90d"] = series_records[-90:]
    charts["online_history_daily_precise_30d"] = series_records[-31:]
    availability["daily_precise_90d"] = bool(charts["online_history_daily_precise_90d"])
    availability["daily_precise_30d"] = bool(charts["online_history_daily_precise_30d"])


def _extract_highcharts_series_records(payload: list[Any]) -> list[dict[str, Any]]:
    best: list[dict[str, Any]] = []
    for chart in payload:
        if not isinstance(chart, dict):
            continue
        for series in chart.get("series") or []:
            if not isinstance(series, dict):
                continue
            name = str(series.get("name") or "").lower()
            if name and not any(token in name for token in ("player", "online", "playing")):
                continue
            records = _series_to_daily_records(series.get("points") or series.get("data") or [])
            if len(records) > len(best):
                best = records
    return best


def _extract_user_reviews_history(payload: list[Any]) -> list[dict[str, Any]]:
    best: list[dict[str, Any]] = []
    for chart in payload:
        if not isinstance(chart, dict):
            continue
        chart_title = str(chart.get("title") or "").lower()
        series_list = [series for series in chart.get("series") or [] if isinstance(series, dict)]
        series_names = " ".join(str(series.get("name") or "").lower() for series in series_list)
        if "review" not in chart_title and not (
            "positive" in series_names and "negative" in series_names
        ):
            continue

        by_date: dict[str, dict[str, Any]] = {}
        for series in series_list:
            name = str(series.get("name") or "").lower()
            if "positive" in name:
                bucket_name = "positive"
            elif "negative" in name:
                bucket_name = "negative"
            else:
                continue
            for point in series.get("points") or series.get("data") or []:
                x_value, y_value = _extract_point_xy(point)
                if x_value is None or y_value is None:
                    continue
                dt = _timestamp_to_datetime(x_value)
                value = _safe_int(y_value)
                if dt is None or value is None:
                    continue
                date_key = dt.date().isoformat()
                entry = by_date.setdefault(
                    date_key,
                    {
                        "date": date_key,
                        "positive": 0,
                        "negative": 0,
                        "timestamp": dt.isoformat(),
                        "source": "steamdb_user_reviews_history",
                    },
                )
                entry[bucket_name] += abs(value)

        records: list[dict[str, Any]] = []
        for date_key in sorted(by_date):
            entry = by_date[date_key]
            positive = int(entry.get("positive") or 0)
            negative = int(entry.get("negative") or 0)
            total = positive + negative
            if total <= 0:
                continue
            entry["total"] = total
            entry["positive_rate"] = round((positive / total) * 100, 2)
            records.append(entry)
        if len(records) > len(best):
            best = records
    return best

def _extract_named_series(payload: list[Any], name_tokens: tuple[str, ...]) -> list[dict[str, Any]]:
    best: list[dict[str, Any]] = []
    for chart in payload:
        if not isinstance(chart, dict):
            continue
        for series in chart.get("series") or []:
            if not isinstance(series, dict):
                continue
            name = str(series.get("name") or "").lower()
            if not any(token in name for token in name_tokens):
                continue
            records = _series_to_daily_records(series.get("points") or series.get("data") or [])
            if len(records) > len(best):
                best = records
    return best


def _series_to_daily_records(points: list[Any]) -> list[dict[str, Any]]:
    records: dict[str, dict[str, Any]] = {}
    for point in points:
        x_value, y_value = _extract_point_xy(point)
        if x_value is None or y_value is None:
            continue
        dt = _timestamp_to_datetime(x_value)
        if dt is None:
            continue
        date_key = dt.date().isoformat()
        players = _safe_int(y_value)
        if players is None:
            continue
        existing = records.get(date_key)
        if existing is None or players > existing["peak_players"]:
            records[date_key] = {
                "date": date_key,
                "peak_players": players,
                "timestamp": dt.isoformat(),
            }
    return [records[key] for key in sorted(records)]


def _extract_point_xy(point: Any) -> tuple[Any | None, Any | None]:
    if isinstance(point, dict):
        return point.get("x"), point.get("y")
    if isinstance(point, (list, tuple)) and len(point) >= 2:
        return point[0], point[1]
    return None, None


def _looks_signed_out(page_text: str) -> bool:
    text = str(page_text or "")
    return "Sign in via Steam" in text or re.search(r"(^|\n)\s*Sign in\s*(\n|$)", text) is not None


def _timestamp_to_datetime(value: Any) -> datetime | None:
    try:
        timestamp = float(value)
    except (TypeError, ValueError):
        return None
    if timestamp > 10_000_000_000:
        timestamp /= 1000
    try:
        return datetime.fromtimestamp(timestamp, tz=timezone.utc)
    except (OSError, OverflowError, ValueError):
        return None


def _safe_int(value: Any) -> int | None:
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return None


def _has_meaningful_chart_data(charts: dict[str, Any]) -> bool:
    ignored = {"requested_time_slice", "chart_url", "online_history_availability", "online_history_unavailable_reasons"}
    return any(key not in ignored and value not in (None, "", [], {}) for key, value in charts.items())


async def _wait_for_highcharts_async(page: Any) -> None:
    try:
        await page.wait_for_function(
            "() => window.Highcharts && window.Highcharts.charts && window.Highcharts.charts.some(Boolean)",
            timeout=5000,
        )
    except Exception:
        pass


def _wait_for_highcharts_sync(page: Any) -> None:
    try:
        page.wait_for_function(
            "() => window.Highcharts && window.Highcharts.charts && window.Highcharts.charts.some(Boolean)",
            timeout=5000,
        )
    except Exception:
        pass


def _random_user_agent() -> str:
    """鐢熸垚闅忔満 User-Agent"""
    agents = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:128.0) Gecko/20100101 Firefox/128.0",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.5 Safari/605.1.15",
    ]
    return random.choice(agents)


def _normalize_key(text: str) -> str:
    """灏嗘爣绛炬枃鏈爣鍑嗗寲涓?snake_case key"""
    text = text.lower().strip()
    text = re.sub(r"[^a-z0-9\s]", "", text)
    text = re.sub(r"\s+", "_", text)
    return text[:50]


def _extract_numbers_from_text(text: str) -> dict[str, Any]:
    """浠庨〉闈㈡枃鏈腑鎻愬彇鍏抽敭缁熻鏁板瓧"""
    result: dict[str, Any] = {}
    patterns = {
        "all_time_peak": r"all.time peak[:\s]*([0-9,]+)",
        "24h_peak": r"24.hour peak[:\s]*([0-9,]+)",
        "current_players": r"playing now[:\s]*([0-9,]+)",
    }
    for key, pattern in patterns.items():
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            num_str = match.group(1).replace(",", "")
            try:
                result[key] = int(num_str)
            except ValueError:
                pass
    return result





