"""
七麦数据 (Qimai) 采集器

使用 Playwright 和持久化会话 (user-data-dir) 采集 iOS App Store 在七麦的评分和榜单。
必须提前在系统浏览器或同目录通过 user-data-dir 登录七麦，否则会被风控或只能看到受限数据。
"""

from __future__ import annotations

import asyncio
import os
import sys
import time
from typing import Any

from loguru import logger

from src.collectors.base import BaseCollector, CollectTarget, CollectResult
from src.core.registry import registry

try:
    from playwright.async_api import async_playwright
    from playwright.sync_api import sync_playwright
except ImportError:
    pass


class QimaiScrapeFailed(Exception):
    """七麦采集失败"""
    pass


@registry.register("collector", "qimai")
class QimaiCollector(BaseCollector):
    """
    七麦数据采集器。
    
    提取参数:
      - qimai_app_id: 必须提供，七麦的应用 ID 或包名，例如 "123456789" 或 "com.example.app"
    """

    BASE_URL = "https://www.qimai.cn"

    def __init__(self, config: dict[str, Any] | None = None):
        super().__init__(config)
        self._headless = self.config.get("headless", True)
        self._timeout = self.config.get("timeout", 30000)
        self._delay = self.config.get("request_delay", 3.0)
        self._user_data_dir = self.config.get(
            "user_data_dir", 
            os.path.join(os.getcwd(), "data", "qimai_profile")
        )

    async def setup(self, config: dict[str, Any] | None = None) -> None:
        await super().setup(config)
        logger.info(f"[QimaiCollector] 初始化完成, user-data-dir: {self._user_data_dir}")

    async def collect(self, target: CollectTarget) -> CollectResult:
        app_id = target.params.get("qimai_app_id", target.params.get("app_id", ""))
        if not app_id:
            return CollectResult(
                target=target,
                success=False,
                error="Qimai采集需要 'qimai_app_id' 或 'app_id' 参数"
            )

        logger.info(f"[Qimai] 开始采集: {target.name} (app_id={app_id})")

        try:
            if self._should_use_threaded_playwright():
                qimai_data = await asyncio.to_thread(self._scrape_sync, app_id)
            else:
                qimai_data = await self._scrape_async(app_id)
        except Exception as e:
            logger.error(f"[Qimai] 采集失败: {e}")
            return CollectResult(
                target=target,
                success=False,
                error=f"Qimai采集失败: {e}",
                metadata={"collector": "qimai"}
            )

        merged_data = {
            "game_name": target.name,
            "app_id": app_id,
            "qimai": qimai_data,
            "snapshot": {
                "name": qimai_data.get("app_name", target.name),
                "review_score": qimai_data.get("rating", ""),
                "total_reviews": qimai_data.get("rating_count", 0),
                "free_rank": qimai_data.get("free_rank", ""),
                "grossing_rank": qimai_data.get("grossing_rank", ""),
            }
        }

        return CollectResult(
            target=target,
            data=merged_data,
            success=True,
            metadata={"collector": "qimai"}
        )

    def _should_use_threaded_playwright(self) -> bool:
        if sys.platform != "win32":
            return False
        loop_name = asyncio.get_running_loop().__class__.__name__
        return "Selector" in loop_name

    async def _scrape_async(self, app_id: str) -> dict[str, Any]:
        result = {"app_id": app_id}
        url = f"{self.BASE_URL}/app/baseinfo/appid/{app_id}/country/cn"

        async with async_playwright() as p:
            context = await p.chromium.launch_persistent_context(
                self._user_data_dir,
                headless=self._headless,
                args=["--disable-blink-features=AutomationControlled"],
                ignore_default_args=["--enable-automation"],
                viewport={"width": 1920, "height": 1080},
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
            )
            try:
                page = await context.new_page()
                await page.add_init_script("""
                    Object.defineProperty(navigator, 'webdriver', {
                        get: () => undefined
                    });
                    window.navigator.chrome = {
                        runtime: {},
                    };
                """)
                await page.goto(url, wait_until="domcontentloaded", timeout=self._timeout)
                await asyncio.sleep(self._delay)

                # 提取评分
                rating_el = await page.query_selector(".app-info-rating .rating-num")
                if rating_el:
                    result["rating"] = await rating_el.inner_text()
                
                count_el = await page.query_selector(".app-info-rating .rating-count")
                if count_el:
                    text = await count_el.inner_text()
                    import re
                    match = re.search(r'\d+', text.replace(',', ''))
                    if match:
                        result["rating_count"] = int(match.group())

                # 提取排名
                ranks = await page.query_selector_all(".app-info-rank .rank-item")
                for rank in ranks:
                    title_el = await rank.query_selector(".rank-title")
                    val_el = await rank.query_selector(".rank-num")
                    if title_el and val_el:
                        title = await title_el.inner_text()
                        val = await val_el.inner_text()
                        if "免费" in title:
                            result["free_rank"] = val.strip()
                        elif "畅销" in title:
                            result["grossing_rank"] = val.strip()
            finally:
                await context.close()

        return result

    def _scrape_sync(self, app_id: str) -> dict[str, Any]:
        result = {"app_id": app_id}
        url = f"{self.BASE_URL}/app/baseinfo/appid/{app_id}/country/cn"

        with sync_playwright() as p:
            context = p.chromium.launch_persistent_context(
                self._user_data_dir,
                headless=self._headless,
                args=["--disable-blink-features=AutomationControlled"],
                ignore_default_args=["--enable-automation"],
                viewport={"width": 1920, "height": 1080},
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
            )
            try:
                page = context.new_page()
                page.add_init_script("""
                    Object.defineProperty(navigator, 'webdriver', {
                        get: () => undefined
                    });
                    window.navigator.chrome = {
                        runtime: {},
                    };
                """)
                page.goto(url, wait_until="domcontentloaded", timeout=self._timeout)
                time.sleep(self._delay)

                rating_el = page.query_selector(".app-info-rating .rating-num")
                if rating_el:
                    result["rating"] = rating_el.inner_text()
                
                count_el = page.query_selector(".app-info-rating .rating-count")
                if count_el:
                    text = count_el.inner_text()
                    import re
                    match = re.search(r'\d+', text.replace(',', ''))
                    if match:
                        result["rating_count"] = int(match.group())

                ranks = page.query_selector_all(".app-info-rank .rank-item")
                for rank in ranks:
                    title_el = rank.query_selector(".rank-title")
                    val_el = rank.query_selector(".rank-num")
                    if title_el and val_el:
                        title = title_el.inner_text()
                        val = val_el.inner_text()
                        if "免费" in title:
                            result["free_rank"] = val.strip()
                        elif "畅销" in title:
                            result["grossing_rank"] = val.strip()
            finally:
                context.close()

        return result
