"""
SteamDB 浏览器自动化采集 (Playwright)

使用 Playwright 控制 Chromium 浏览器访问 SteamDB，
采集游戏的 charts (在线趋势/峰值) 和 info (版本/更新) 数据。

⚠️ SteamDB 明确禁止爬取。本模块仅供内部分析使用。
   采集会被 Cloudflare 拦截时自动抛出 SteamDBScrapeFailed，
   由上层切换到 Firecrawl 兜底。
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


class SteamDBScrapeFailed(Exception):
    """SteamDB 采集失败（触发兜底切换）"""
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
    """Playwright 驱动的 SteamDB 采集器"""

    BASE_URL = "https://steamdb.info"

    def __init__(
        self,
        headless: bool = True,
        timeout: int = 30000,
        request_delay: float = 3.0,
        cookie: str = "",
        extra_headers: dict[str, str] | None = None,
    ):
        self._headless = headless
        self._timeout = timeout
        self._delay = request_delay
        self._extra_headers = dict(extra_headers or {})
        if cookie:
            self._extra_headers.setdefault("Cookie", cookie)
        self._browser = None
        self._playwright = None

    async def setup(self) -> None:
        """启动 Playwright 浏览器"""
        try:
            from playwright.async_api import async_playwright
        except ImportError:
            raise SteamDBScrapeFailed(
                "playwright 未安装。运行: pip install playwright && playwright install chromium"
            )

        self._playwright = await async_playwright().start()
        self._browser = await self._playwright.chromium.launch(
            headless=self._headless,
            args=[
                "--disable-blink-features=AutomationControlled",
                "--no-sandbox",
            ],
        )
        logger.info("[SteamDB] Playwright 浏览器已启动")

    def _should_use_threaded_playwright(self) -> bool:
        """Windows SelectorEventLoop 下改用独立线程中的同步 Playwright。"""
        if sys.platform != "win32":
            return False
        loop_name = asyncio.get_running_loop().__class__.__name__
        return "Selector" in loop_name

    async def teardown(self) -> None:
        """关闭浏览器"""
        if self._browser:
            await self._browser.close()
        if self._playwright:
            await self._playwright.stop()
        self._browser = None
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
        采集 SteamDB 上指定游戏的数据。

        Returns:
            包含 charts 和 info 数据的字典。

        Raises:
            SteamDBScrapeFailed: 当 Cloudflare 拦截或页面解析失败时。
        """
        if self._should_use_threaded_playwright():
            logger.info("[SteamDB] 当前事件循环不兼容，改用独立线程启动 Playwright")
            return await asyncio.to_thread(self._scrape_sync, app_id, time_slice, cookie, extra_headers)

        if not self._browser:
            await self.setup()

        result: dict[str, Any] = {
            "source": "steamdb_playwright",
            "app_id": int(app_id),
            "requested_time_slice": time_slice,
        }

        context_kwargs: dict[str, Any] = {
            "viewport": {"width": 1920, "height": 1080},
            "user_agent": _random_user_agent(),
            "locale": "en-US",
        }
        context_headers = self._request_headers(cookie=cookie, extra_headers=extra_headers)
        if context_headers:
            context_kwargs["extra_http_headers"] = context_headers
        context = await self._browser.new_context(**context_kwargs)

        try:
            page = await context.new_page()

            # ── Charts 页面 ──
            charts_data = await self._scrape_charts(page, app_id, time_slice=time_slice)
            result["charts"] = charts_data

            await asyncio.sleep(self._delay + random.uniform(0.5, 2.0))

            # ── Info 页面 ──
            info_data = await self._scrape_info(page, app_id)
            result["info"] = info_data

            await asyncio.sleep(self._delay + random.uniform(0.5, 2.0))

            # ── 当前 Steam 全球畅销榜 ──
            result["top_sellers"] = await self._scrape_top_sellers(page, app_id)

        except SteamDBScrapeFailed:
            raise
        except Exception as e:
            logger.error(f"[SteamDB] 采集异常: {e}")
            raise SteamDBScrapeFailed(f"Playwright 采集失败: {e}") from e
        finally:
            await context.close()

        return result

    def _scrape_sync(
        self,
        app_id: str | int,
        time_slice: str = "monthly_peak_1y",
        cookie: str = "",
        extra_headers: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        """在独立线程中使用同步 Playwright，绕开 Windows SelectorEventLoop 限制。"""
        try:
            from playwright.sync_api import sync_playwright
        except ImportError:
            raise SteamDBScrapeFailed(
                "playwright 未安装。运行: pip install playwright && playwright install chromium"
            )

        result: dict[str, Any] = {
            "source": "steamdb_playwright",
            "app_id": int(app_id),
            "requested_time_slice": time_slice,
        }

        try:
            with sync_playwright() as playwright:
                browser = playwright.chromium.launch(
                    headless=self._headless,
                    args=[
                        "--disable-blink-features=AutomationControlled",
                        "--no-sandbox",
                    ],
                )
                context_kwargs: dict[str, Any] = {
                    "viewport": {"width": 1920, "height": 1080},
                    "user_agent": _random_user_agent(),
                    "locale": "en-US",
                }
                context_headers = self._request_headers(cookie=cookie, extra_headers=extra_headers)
                if context_headers:
                    context_kwargs["extra_http_headers"] = context_headers
                context = browser.new_context(**context_kwargs)
                try:
                    page = context.new_page()
                    result["charts"] = self._scrape_charts_sync(page, app_id, time_slice=time_slice)
                    time.sleep(self._delay + random.uniform(0.5, 2.0))
                    result["info"] = self._scrape_info_sync(page, app_id)
                    time.sleep(self._delay + random.uniform(0.5, 2.0))
                    result["top_sellers"] = self._scrape_top_sellers_sync(page, app_id)
                finally:
                    context.close()
                    browser.close()
        except SteamDBScrapeFailed:
            raise
        except Exception as e:
            logger.error(f"[SteamDB] 同步线程采集异常: {e}")
            raise SteamDBScrapeFailed(f"Playwright 线程采集失败: {e}") from e

        return result

    # ── 内部采集方法 ──────────────────────────────────

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

    async def _scrape_charts(
        self, page: Any, app_id: str | int, time_slice: str = "monthly_peak_1y"
    ) -> dict[str, Any]:
        """采集 charts 页面: 在线趋势、峰值人数"""
        url = _build_charts_url(self.BASE_URL, app_id, time_slice)
        logger.info(f"[SteamDB] 访问 charts: {url}")

        try:
            resp = await page.goto(url, wait_until="domcontentloaded", timeout=self._timeout)
        except Exception as e:
            raise SteamDBScrapeFailed(f"Charts 页面加载失败: {e}")

        if resp and resp.status == 403:
            raise SteamDBScrapeFailed("Cloudflare 403 拦截")

        # 检测 Cloudflare challenge
        content = await page.content()
        if "challenge-platform" in content or "Just a moment" in content:
            logger.warning("[SteamDB] 检测到 Cloudflare challenge 页面")
            raise SteamDBScrapeFailed("Cloudflare challenge 拦截")

        await page.wait_for_timeout(2000)
        await _wait_for_highcharts_async(page)

        charts: dict[str, Any] = {
            "requested_time_slice": time_slice,
            "chart_url": url,
        }

        # 尝试提取关键数据点
        try:
            # 当前在线数据（页面顶部统计区域）
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
            logger.debug(f"[SteamDB] charts 统计区提取失败: {e}")

        # 尝试从页面文本中提取关键数字
        try:
            page_text = await page.inner_text("body")
            charts.update(_extract_numbers_from_text(page_text))
        except Exception:
            pass

        try:
            highcharts_payload = await page.evaluate(_HIGHCHARTS_EXTRACT_SCRIPT)
            _merge_highcharts_payload(charts, highcharts_payload)
        except Exception as e:
            logger.debug(f"[SteamDB] Highcharts 序列提取失败: {e}")

        if not _has_meaningful_chart_data(charts):
            # 最后尝试: 提取所有表格数据
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
                logger.debug(f"[SteamDB] 表格提取失败: {e}")

        return charts

    async def _scrape_info(
        self, page: Any, app_id: str | int
    ) -> dict[str, Any]:
        """采集 info 页面: 版本更新、发布商信息"""
        url = f"{self.BASE_URL}/app/{app_id}/info/"
        logger.info(f"[SteamDB] 访问 info: {url}")

        try:
            resp = await page.goto(url, wait_until="domcontentloaded", timeout=self._timeout)
        except Exception as e:
            raise SteamDBScrapeFailed(f"Info 页面加载失败: {e}")

        if resp and resp.status == 403:
            raise SteamDBScrapeFailed("Cloudflare 403 拦截")

        content = await page.content()
        if "challenge-platform" in content or "Just a moment" in content:
            raise SteamDBScrapeFailed("Cloudflare challenge 拦截")

        await page.wait_for_timeout(2000)

        info: dict[str, Any] = {}

        # 提取 key-value 信息表
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
            logger.debug(f"[SteamDB] info KV 提取失败: {e}")

        # 尝试提取更新历史
        try:
            page_text = await page.inner_text("body")
            info["page_text_preview"] = page_text[:2000]
        except Exception:
            pass

        return info

    async def _scrape_top_sellers(self, page: Any, app_id: str | int) -> dict[str, Any]:
        """采集 SteamDB 当前全球畅销榜排名。"""
        url = f"{self.BASE_URL}/stats/globaltopsellers/"
        logger.info(f"[SteamDB] 访问 global top sellers: {url}")
        try:
            resp = await page.goto(url, wait_until="domcontentloaded", timeout=self._timeout)
        except Exception as e:
            logger.debug(f"[SteamDB] global top sellers 加载失败: {e}")
            return {"source": "steamdb_globaltopsellers", "url": url, "rank": "", "error": str(e)}

        if resp and resp.status == 403:
            raise SteamDBScrapeFailed("Cloudflare 403 拦截")

        content = await page.content()
        if "challenge-platform" in content or "Just a moment" in content:
            raise SteamDBScrapeFailed("Cloudflare challenge 拦截")

        rows = await page.query_selector_all("table tbody tr")
        return await _parse_top_sellers_rows_async(rows, app_id, url)

    def _scrape_charts_sync(
        self, page: Any, app_id: str | int, time_slice: str = "monthly_peak_1y"
    ) -> dict[str, Any]:
        """同步版本的 charts 采集。"""
        url = _build_charts_url(self.BASE_URL, app_id, time_slice)
        logger.info(f"[SteamDB] 访问 charts: {url}")

        try:
            resp = page.goto(url, wait_until="domcontentloaded", timeout=self._timeout)
        except Exception as e:
            raise SteamDBScrapeFailed(f"Charts 页面加载失败: {e}")

        if resp and resp.status == 403:
            raise SteamDBScrapeFailed("Cloudflare 403 拦截")

        content = page.content()
        if "challenge-platform" in content or "Just a moment" in content:
            logger.warning("[SteamDB] 检测到 Cloudflare challenge 页面")
            raise SteamDBScrapeFailed("Cloudflare challenge 拦截")

        page.wait_for_timeout(2000)
        _wait_for_highcharts_sync(page)

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
            logger.debug(f"[SteamDB] charts 统计区提取失败: {e}")

        try:
            page_text = page.inner_text("body")
            charts.update(_extract_numbers_from_text(page_text))
        except Exception:
            pass

        try:
            highcharts_payload = page.evaluate(_HIGHCHARTS_EXTRACT_SCRIPT)
            _merge_highcharts_payload(charts, highcharts_payload)
        except Exception as e:
            logger.debug(f"[SteamDB] Highcharts 序列提取失败: {e}")

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
                logger.debug(f"[SteamDB] 表格提取失败: {e}")

        return charts

    def _scrape_info_sync(
        self, page: Any, app_id: str | int
    ) -> dict[str, Any]:
        """同步版本的 info 采集。"""
        url = f"{self.BASE_URL}/app/{app_id}/info/"
        logger.info(f"[SteamDB] 访问 info: {url}")

        try:
            resp = page.goto(url, wait_until="domcontentloaded", timeout=self._timeout)
        except Exception as e:
            raise SteamDBScrapeFailed(f"Info 页面加载失败: {e}")

        if resp and resp.status == 403:
            raise SteamDBScrapeFailed("Cloudflare 403 拦截")

        content = page.content()
        if "challenge-platform" in content or "Just a moment" in content:
            raise SteamDBScrapeFailed("Cloudflare challenge 拦截")

        page.wait_for_timeout(2000)

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
            logger.debug(f"[SteamDB] info KV 提取失败: {e}")

        try:
            page_text = page.inner_text("body")
            info["page_text_preview"] = page_text[:2000]
        except Exception:
            pass

        return info

    def _scrape_top_sellers_sync(self, page: Any, app_id: str | int) -> dict[str, Any]:
        """同步版本的 SteamDB 当前全球畅销榜排名采集。"""
        url = f"{self.BASE_URL}/stats/globaltopsellers/"
        logger.info(f"[SteamDB] 访问 global top sellers: {url}")
        try:
            resp = page.goto(url, wait_until="domcontentloaded", timeout=self._timeout)
        except Exception as e:
            logger.debug(f"[SteamDB] global top sellers 加载失败: {e}")
            return {"source": "steamdb_globaltopsellers", "url": url, "rank": "", "error": str(e)}

        if resp and resp.status == 403:
            raise SteamDBScrapeFailed("Cloudflare 403 拦截")

        content = page.content()
        if "challenge-platform" in content or "Just a moment" in content:
            raise SteamDBScrapeFailed("Cloudflare challenge 拦截")

        rows = page.query_selector_all("table tbody tr")
        return _parse_top_sellers_rows_sync(rows, app_id, url)


# ── 工具函数 ──────────────────────────────────────────

def _build_charts_url(base_url: str, app_id: str | int, time_slice: str = "monthly_peak_1y") -> str:
    fragment = _chart_fragment_for_time_slice(time_slice)
    return f"{base_url}/app/{app_id}/charts/{fragment}"


def _chart_fragment_for_time_slice(time_slice: str) -> str:
    if time_slice in {"daily_precise_30d", "daily_precise_90d"}:
        return "#1m"
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
    """生成随机 User-Agent"""
    agents = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:128.0) Gecko/20100101 Firefox/128.0",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.5 Safari/605.1.15",
    ]
    return random.choice(agents)


def _normalize_key(text: str) -> str:
    """将标签文本标准化为 snake_case key"""
    text = text.lower().strip()
    text = re.sub(r"[^a-z0-9\s]", "", text)
    text = re.sub(r"\s+", "_", text)
    return text[:50]


def _extract_numbers_from_text(text: str) -> dict[str, Any]:
    """从页面文本中提取关键统计数字"""
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
