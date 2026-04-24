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
            args=[
                "--disable-blink-features=AutomationControlled",
                "--disable-dev-shm-usage",
                "--no-sandbox",
            ],
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
        all_info_url: str,
        reviews_pages: int = 1,
        region: str = "cn",
    ) -> dict[str, Any]:
        if self._should_use_threaded_playwright():
            return await asyncio.to_thread(
                self._fetch_sync,
                detail_url,
                review_url,
                all_info_url,
                reviews_pages,
                region,
            )

        if not self._browser:
            await self.setup()

        context = await self._browser.new_context(**_browser_context_options(region=region, referer=detail_url))
        await context.add_init_script(_stealth_init_script(region=region))
        try:
            page = await context.new_page()
            detail_html = await self._load_page_html(page, detail_url, kind="detail")
            detail_text = await self._extract_page_text(page, kind="detail")
            all_info_html = await self._load_page_html(page, all_info_url, kind="all_info")
            all_info_text = await self._extract_page_text(page, kind="all_info")
            review_pages_html = [await self._load_page_html(page, review_url, kind="review")]
            review_pages_text = [await self._extract_page_text(page, kind="review")]
            review_items = await self._extract_review_items(page)

            for _ in range(max(0, reviews_pages - 1)):
                advanced = await self._try_advance_review_page(page)
                if not advanced:
                    break
                await self._post_load_stabilize(page, kind="review")
                review_pages_html.append(await page.content())
                review_pages_text.append(await self._extract_page_text(page, kind="review"))
                review_items.extend(await self._extract_review_items(page))

            return {
                "detail_html": detail_html,
                "detail_text": detail_text,
                "all_info_html": all_info_html,
                "all_info_text": all_info_text,
                "review_pages_html": review_pages_html,
                "review_pages_text": review_pages_text,
                "review_items": review_items,
            }
        finally:
            await context.close()

    def _fetch_sync(self, detail_url: str, review_url: str, all_info_url: str, reviews_pages: int, region: str) -> dict[str, Any]:
        try:
            from playwright.sync_api import sync_playwright
        except ImportError as exc:
            raise TapTapPlaywrightFailed("playwright is not installed") from exc

        with sync_playwright() as playwright:
            browser = playwright.chromium.launch(
                headless=self._headless,
                args=[
                    "--disable-blink-features=AutomationControlled",
                    "--disable-dev-shm-usage",
                    "--no-sandbox",
                ],
            )
            context = browser.new_context(**_browser_context_options(region=region, referer=detail_url))
            context.add_init_script(_stealth_init_script(region=region))
            try:
                page = context.new_page()
                detail_html = self._load_page_html_sync(page, detail_url, kind="detail")
                detail_text = self._extract_page_text_sync(page, kind="detail")
                all_info_html = self._load_page_html_sync(page, all_info_url, kind="all_info")
                all_info_text = self._extract_page_text_sync(page, kind="all_info")
                review_pages_html = [self._load_page_html_sync(page, review_url, kind="review")]
                review_pages_text = [self._extract_page_text_sync(page, kind="review")]
                review_items = self._extract_review_items_sync(page)

                for _ in range(max(0, reviews_pages - 1)):
                    locator = _review_next_locator_sync(page)
                    if locator is None or locator.count() == 0:
                        break
                    before = page.url
                    before_size = len(page.content())
                    locator.first.click()
                    page.wait_for_load_state("domcontentloaded", timeout=self._timeout)
                    self._post_load_stabilize_sync(page, kind="review")
                    after_html = page.content()
                    if page.url == before and len(after_html) <= before_size:
                        break
                    review_pages_html.append(after_html)
                    review_pages_text.append(self._extract_page_text_sync(page, kind="review"))
                    review_items.extend(self._extract_review_items_sync(page))

                return {
                    "detail_html": detail_html,
                    "detail_text": detail_text,
                    "all_info_html": all_info_html,
                    "all_info_text": all_info_text,
                    "review_pages_html": review_pages_html,
                    "review_pages_text": review_pages_text,
                    "review_items": review_items,
                }
            except Exception as exc:
                raise TapTapPlaywrightFailed(f"Playwright scrape failed: {exc}") from exc
            finally:
                context.close()
                browser.close()

    async def _load_page_html(self, page: Any, url: str, *, kind: str) -> str:
        logger.info(f"[TapTap Playwright] Visit: {url}")
        try:
            response = await page.goto(url, wait_until="domcontentloaded", timeout=self._timeout)
        except Exception as exc:
            raise TapTapPlaywrightFailed(f"Page load failed: {exc}") from exc
        if response and response.status and response.status >= 400:
            raise TapTapPlaywrightFailed(f"HTTP {response.status}")
        await self._post_load_stabilize(page, kind=kind)
        return await page.content()

    def _load_page_html_sync(self, page: Any, url: str, *, kind: str) -> str:
        logger.info(f"[TapTap Playwright] Visit: {url}")
        response = page.goto(url, wait_until="domcontentloaded", timeout=self._timeout)
        if response and response.status and response.status >= 400:
            raise TapTapPlaywrightFailed(f"HTTP {response.status}")
        self._post_load_stabilize_sync(page, kind=kind)
        return page.content()

    async def _post_load_stabilize(self, page: Any, *, kind: str) -> None:
        await self._wait_for_taptap_content(page, kind=kind)
        await page.wait_for_timeout(int(self._delay * 1000))
        await page.evaluate("window.scrollTo(0, document.body.scrollHeight * 0.35)")
        await page.wait_for_timeout(400)
        await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        await page.wait_for_timeout(600)
        await page.evaluate("window.scrollTo(0, 0)")
        await page.wait_for_timeout(400)

    def _post_load_stabilize_sync(self, page: Any, *, kind: str) -> None:
        self._wait_for_taptap_content_sync(page, kind=kind)
        page.wait_for_timeout(int(self._delay * 1000))
        page.evaluate("window.scrollTo(0, document.body.scrollHeight * 0.35)")
        page.wait_for_timeout(400)
        page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        page.wait_for_timeout(600)
        page.evaluate("window.scrollTo(0, 0)")
        page.wait_for_timeout(400)

    async def _wait_for_taptap_content(self, page: Any, *, kind: str) -> None:
        selectors = _candidate_selectors(kind)
        for selector in selectors:
            try:
                await page.wait_for_selector(selector, timeout=min(5000, self._timeout))
                return
            except Exception:
                continue
        try:
            await page.wait_for_load_state("networkidle", timeout=min(8000, self._timeout))
        except Exception:
            pass

    def _wait_for_taptap_content_sync(self, page: Any, *, kind: str) -> None:
        selectors = _candidate_selectors(kind)
        for selector in selectors:
            try:
                page.wait_for_selector(selector, timeout=min(5000, self._timeout))
                return
            except Exception:
                continue
        try:
            page.wait_for_load_state("networkidle", timeout=min(8000, self._timeout))
        except Exception:
            pass

    async def _try_advance_review_page(self, page: Any) -> bool:
        locator = await _review_next_locator(page)
        if locator is None or await locator.count() == 0:
            return False
        before = page.url
        before_size = len(await page.content())
        await locator.first.click()
        await page.wait_for_load_state("domcontentloaded", timeout=self._timeout)
        await page.wait_for_timeout(800)
        after_size = len(await page.content())
        return page.url != before or after_size > before_size

    async def _extract_page_text(self, page: Any, *, kind: str) -> str:
        return await page.evaluate(_extract_text_script(kind=kind))

    def _extract_page_text_sync(self, page: Any, *, kind: str) -> str:
        return page.evaluate(_extract_text_script(kind=kind))

    async def _extract_review_items(self, page: Any) -> list[dict[str, Any]]:
        return await page.evaluate(_extract_review_items_script())

    def _extract_review_items_sync(self, page: Any) -> list[dict[str, Any]]:
        return page.evaluate(_extract_review_items_script())

    def _should_use_threaded_playwright(self) -> bool:
        if sys.platform != "win32":
            return False
        return "Selector" in asyncio.get_running_loop().__class__.__name__


def _browser_context_options(*, region: str, referer: str) -> dict[str, Any]:
    locale = "zh-CN" if region == "cn" else "en-US"
    timezone_id = "Asia/Shanghai" if region == "cn" else "Etc/UTC"
    return {
        "viewport": {"width": 1440, "height": 2400},
        "user_agent": _random_user_agent(),
        "locale": locale,
        "timezone_id": timezone_id,
        "extra_http_headers": {
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
            "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8" if region == "cn" else "en-US,en;q=0.9",
            "Cache-Control": "no-cache",
            "Pragma": "no-cache",
            "Referer": referer,
            "DNT": "1",
        },
    }


def _stealth_init_script(*, region: str) -> str:
    languages = '["zh-CN","zh","en-US","en"]' if region == "cn" else '["en-US","en"]'
    platform = "Win32" if sys.platform == "win32" else "MacIntel"
    return f"""
Object.defineProperty(navigator, 'webdriver', {{ get: () => undefined }});
Object.defineProperty(navigator, 'languages', {{ get: () => {languages} }});
Object.defineProperty(navigator, 'platform', {{ get: () => '{platform}' }});
window.chrome = window.chrome || {{ runtime: {{}} }};
"""


def _candidate_selectors(kind: str) -> list[str]:
    if kind == "review":
        return [
            "text=Ratings & Reviews",
            "text=\u8bc4\u5206\u4e0e\u8bc4\u4ef7",
            "text=\u8bc4\u5206\u53ca\u8bc4\u4ef7",
            "main",
            "h1",
        ]
    return [
        "h1",
        "text=About the Game",
        "text=\u5173\u4e8e\u8fd9\u6b3e\u6e38\u620f",
        "main",
    ]


def _extract_text_script(*, kind: str) -> str:
    primary_selector = "main, article, [role='main']"
    review_markers = ["Ratings & Reviews", "\u8bc4\u5206\u4e0e\u8bc4\u4ef7", "\u8bc4\u5206\u53ca\u8bc4\u4ef7", "\u4e0b\u4e00\u9875", "\u52a0\u8f7d\u66f4\u591a"]
    detail_markers = ["About the Game", "\u5173\u4e8e\u8fd9\u6b3e\u6e38\u620f", "Announcements", "\u516c\u544a", "Download", "\u4e0b\u8f7d"]
    markers = review_markers if kind == "review" else detail_markers
    return f"""
(() => {{
  const normalize = (value) => (value || '').replace(/\\u00a0/g, ' ').replace(/\\s+/g, ' ').trim();
  const blocks = [];
  const push = (value) => {{
    const normalized = normalize(value);
    if (normalized && !blocks.includes(normalized)) blocks.push(normalized);
  }};
  const main = document.querySelector({primary_selector!r});
  if (main) {{
    push(main.innerText);
  }}
  const markers = {markers!r};
  for (const marker of markers) {{
    const walker = document.createTreeWalker(document.body, NodeFilter.SHOW_ELEMENT);
    let node = walker.currentNode;
    while (node) {{
      const text = normalize(node.innerText || '');
      if (text && (text === marker || text.startsWith(marker + ' ') || text.includes(marker))) {{
        push(text);
        if (node.parentElement) push(node.parentElement.innerText);
        break;
      }}
      node = walker.nextNode();
    }}
  }}
  push(document.body ? document.body.innerText : '');
  return blocks.join('\\n\\n');
}})()
"""


def _extract_review_items_script() -> str:
    return r"""
(() => {
  const normalize = (value) => (value || '').replace(/\u00a0/g, ' ').replace(/\s+/g, ' ').trim();
  const toInt = (value) => {
    const text = normalize(value).replace(/,/g, '');
    if (!text) return null;
    const number = parseInt(text, 10);
    return Number.isFinite(number) ? number : null;
  };
  return Array.from(document.querySelectorAll('.review-item')).map((card) => {
    const pickText = (selector) => normalize(card.querySelector(selector)?.innerText || '');
    const author = pickText('.review-item__author-name');
    const content = pickText('.review-item__body') || pickText('.review-item__contents') || pickText('.collapse-text-emoji__content');
    const publishedAt = pickText('.review-item__updated-time') || pickText('.review-item__footer');
    const ratingText = pickText('.review-item__time-label');
    const device = pickText('.review-item__device');
    const likes = toInt(pickText('.review-vote-up__text'));
    const footerCounts = Array.from(card.querySelectorAll('.footer-operate__text'))
      .map((el) => toInt(el.innerText))
      .filter((value) => value !== null);
    const replyCount = footerCounts.length > 1 ? footerCounts[0] : null;
    return {
      author: author || null,
      published_at: publishedAt || null,
      rating_text_or_score: ratingText || null,
      content: content || null,
      likes,
      reply_count: replyCount,
      device: device || null,
    };
  }).filter((item) => item.author && item.content);
})()
"""


async def _review_next_locator(page: Any) -> Any | None:
    for label in ("Next Page", "\u4e0b\u4e00\u9875", "\u52a0\u8f7d\u66f4\u591a"):
        locator = page.get_by_text(label)
        if await locator.count() > 0:
            return locator
    return None


def _review_next_locator_sync(page: Any) -> Any | None:
    for label in ("Next Page", "\u4e0b\u4e00\u9875", "\u52a0\u8f7d\u66f4\u591a"):
        locator = page.get_by_text(label)
        if locator.count() > 0:
            return locator
    return None


def _random_user_agent() -> str:
    agents = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:136.0) Gecko/20100101 Firefox/136.0",
    ]
    return random.choice(agents)
