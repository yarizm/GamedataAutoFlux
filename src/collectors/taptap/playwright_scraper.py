from __future__ import annotations

import asyncio
import random
import sys
from typing import Any

from loguru import logger


class TapTapPlaywrightFailed(Exception):
    """Raised when TapTap Playwright supplementation fails."""


class TapTapPlaywrightScraper:
    def __init__(
        self,
        *,
        headless: bool = True,
        timeout: int = 30000,
        request_delay: float = 1.5,
    ):
        self._headless = headless
        self._timeout = timeout
        self._delay = request_delay
        self._playwright = None
        self._browser = None

    async def setup(self) -> None:
        try:
            from playwright.async_api import async_playwright
        except ImportError as exc:
            raise TapTapPlaywrightFailed("playwright is not installed") from exc

        self._playwright = await async_playwright().start()
        self._browser = await self._playwright.chromium.launch(
            headless=self._headless,
            args=["--disable-blink-features=AutomationControlled", "--no-sandbox"],
        )

    async def teardown(self) -> None:
        if self._browser:
            await self._browser.close()
        if self._playwright:
            await self._playwright.stop()
        self._browser = None
        self._playwright = None

    async def fetch(
        self,
        *,
        detail_url: str,
        review_url: str,
        reviews_pages: int = 1,
    ) -> dict[str, Any]:
        if self._should_use_threaded_playwright():
            return await asyncio.to_thread(
                self._fetch_sync,
                detail_url,
                review_url,
                reviews_pages,
            )

        if not self._browser:
            await self.setup()

        context = await self._browser.new_context(
            viewport={"width": 1440, "height": 2400},
            user_agent=_random_user_agent(),
            locale="en-US",
        )
        try:
            page = await context.new_page()
            detail_html = await self._load_page_html(page, detail_url)
            review_pages_html = [await self._load_page_html(page, review_url)]

            for _ in range(max(0, reviews_pages - 1)):
                advanced = await self._try_advance_review_page(page)
                if not advanced:
                    break
                review_pages_html.append(await page.content())

            return {
                "detail_html": detail_html,
                "review_pages_html": review_pages_html,
            }
        finally:
            await context.close()

    def _fetch_sync(self, detail_url: str, review_url: str, reviews_pages: int) -> dict[str, Any]:
        try:
            from playwright.sync_api import sync_playwright
        except ImportError as exc:
            raise TapTapPlaywrightFailed("playwright is not installed") from exc

        with sync_playwright() as playwright:
            browser = playwright.chromium.launch(
                headless=self._headless,
                args=["--disable-blink-features=AutomationControlled", "--no-sandbox"],
            )
            context = browser.new_context(
                viewport={"width": 1440, "height": 2400},
                user_agent=_random_user_agent(),
                locale="en-US",
            )
            try:
                page = context.new_page()
                detail_html = self._load_page_html_sync(page, detail_url)
                review_pages_html = [self._load_page_html_sync(page, review_url)]
                for _ in range(max(0, reviews_pages - 1)):
                    locator = page.get_by_text("Next Page")
                    if locator.count() == 0:
                        break
                    before = page.url
                    locator.first.click()
                    page.wait_for_load_state("domcontentloaded", timeout=self._timeout)
                    page.wait_for_timeout(1000)
                    if page.url == before:
                        break
                    review_pages_html.append(page.content())
                return {
                    "detail_html": detail_html,
                    "review_pages_html": review_pages_html,
                }
            except Exception as exc:
                raise TapTapPlaywrightFailed(f"Playwright scrape failed: {exc}") from exc
            finally:
                context.close()
                browser.close()

    async def _load_page_html(self, page: Any, url: str) -> str:
        logger.info(f"[TapTap Playwright] Visit: {url}")
        try:
            response = await page.goto(url, wait_until="domcontentloaded", timeout=self._timeout)
        except Exception as exc:
            raise TapTapPlaywrightFailed(f"Page load failed: {exc}") from exc
        if response and response.status and response.status >= 400:
            raise TapTapPlaywrightFailed(f"HTTP {response.status}")
        await page.wait_for_timeout(int(self._delay * 1000))
        return await page.content()

    def _load_page_html_sync(self, page: Any, url: str) -> str:
        logger.info(f"[TapTap Playwright] Visit: {url}")
        response = page.goto(url, wait_until="domcontentloaded", timeout=self._timeout)
        if response and response.status and response.status >= 400:
            raise TapTapPlaywrightFailed(f"HTTP {response.status}")
        page.wait_for_timeout(int(self._delay * 1000))
        return page.content()

    async def _try_advance_review_page(self, page: Any) -> bool:
        locator = page.get_by_text("Next Page")
        if await locator.count() == 0:
            return False
        before = page.url
        await locator.first.click()
        await page.wait_for_load_state("domcontentloaded", timeout=self._timeout)
        await page.wait_for_timeout(1000)
        return page.url != before

    def _should_use_threaded_playwright(self) -> bool:
        if sys.platform != "win32":
            return False
        return "Selector" in asyncio.get_running_loop().__class__.__name__


def _random_user_agent() -> str:
    agents = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:136.0) Gecko/20100101 Firefox/136.0",
    ]
    return random.choice(agents)
