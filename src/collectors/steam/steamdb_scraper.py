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
from typing import Any

from loguru import logger


class SteamDBScrapeFailed(Exception):
    """SteamDB 采集失败（触发兜底切换）"""
    pass


class SteamDBScraper:
    """Playwright 驱动的 SteamDB 采集器"""

    BASE_URL = "https://steamdb.info"

    def __init__(
        self,
        headless: bool = True,
        timeout: int = 30000,
        request_delay: float = 3.0,
    ):
        self._headless = headless
        self._timeout = timeout
        self._delay = request_delay
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

    async def scrape(self, app_id: str | int) -> dict[str, Any]:
        """
        采集 SteamDB 上指定游戏的数据。

        Returns:
            包含 charts 和 info 数据的字典。

        Raises:
            SteamDBScrapeFailed: 当 Cloudflare 拦截或页面解析失败时。
        """
        if self._should_use_threaded_playwright():
            logger.info("[SteamDB] 当前事件循环不兼容，改用独立线程启动 Playwright")
            return await asyncio.to_thread(self._scrape_sync, app_id)

        if not self._browser:
            await self.setup()

        result: dict[str, Any] = {
            "source": "steamdb_playwright",
            "app_id": int(app_id),
        }

        context = await self._browser.new_context(
            viewport={"width": 1920, "height": 1080},
            user_agent=_random_user_agent(),
            locale="en-US",
        )

        try:
            page = await context.new_page()

            # ── Charts 页面 ──
            charts_data = await self._scrape_charts(page, app_id)
            result["charts"] = charts_data

            await asyncio.sleep(self._delay + random.uniform(0.5, 2.0))

            # ── Info 页面 ──
            info_data = await self._scrape_info(page, app_id)
            result["info"] = info_data

        except SteamDBScrapeFailed:
            raise
        except Exception as e:
            logger.error(f"[SteamDB] 采集异常: {e}")
            raise SteamDBScrapeFailed(f"Playwright 采集失败: {e}") from e
        finally:
            await context.close()

        return result

    def _scrape_sync(self, app_id: str | int) -> dict[str, Any]:
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
                context = browser.new_context(
                    viewport={"width": 1920, "height": 1080},
                    user_agent=_random_user_agent(),
                    locale="en-US",
                )
                try:
                    page = context.new_page()
                    result["charts"] = self._scrape_charts_sync(page, app_id)
                    time.sleep(self._delay + random.uniform(0.5, 2.0))
                    result["info"] = self._scrape_info_sync(page, app_id)
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

    async def _scrape_charts(
        self, page: Any, app_id: str | int
    ) -> dict[str, Any]:
        """采集 charts 页面: 在线趋势、峰值人数"""
        url = f"{self.BASE_URL}/app/{app_id}/charts/"
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

        charts: dict[str, Any] = {}

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

        if not charts:
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

    def _scrape_charts_sync(
        self, page: Any, app_id: str | int
    ) -> dict[str, Any]:
        """同步版本的 charts 采集。"""
        url = f"{self.BASE_URL}/app/{app_id}/charts/"
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

        charts: dict[str, Any] = {}

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

        if not charts:
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


# ── 工具函数 ──────────────────────────────────────────

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
