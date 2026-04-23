from __future__ import annotations

import asyncio
from typing import Any

from loguru import logger

from src.collectors.taptap.parser import merge_taptap_payloads, parse_taptap_page


class TapTapFirecrawlFallback:
    """Fallback collector for TapTap public pages backed by Firecrawl."""

    def __init__(self, api_key: str = "", timeout: int = 30):
        self._api_key = api_key
        self._timeout = timeout
        self._app = None

    async def setup(self) -> None:
        if not self._api_key:
            logger.warning("[TapTap Firecrawl] api_key is not configured")
            return

        try:
            from firecrawl import FirecrawlApp
        except ImportError:
            logger.error("[TapTap Firecrawl] firecrawl-py is not installed")
            return

        self._app = FirecrawlApp(api_key=self._api_key)
        logger.info("[TapTap Firecrawl] Client initialized")

    async def teardown(self) -> None:
        self._app = None

    async def scrape(
        self,
        *,
        detail_url: str,
        review_url: str,
        review_limit: int,
    ) -> dict[str, Any]:
        if not self._app:
            await self.setup()
            if not self._app:
                return {"error": "Firecrawl is unavailable"}

        detail_markdown = await self._scrape_url(detail_url)
        review_markdown = await self._scrape_url(review_url)
        detail_payload = (
            parse_taptap_page(
                detail_markdown,
                page_url=detail_url,
                source_format="markdown",
                review_limit=review_limit,
            )
            if detail_markdown
            else {}
        )
        review_payload = (
            parse_taptap_page(
                review_markdown,
                page_url=review_url,
                source_format="markdown",
                review_limit=review_limit,
            )
            if review_markdown
            else {}
        )
        merged = merge_taptap_payloads(detail_payload, review_payload)
        merged["raw_snapshots"]["firecrawl"] = {
            "detail_markdown_preview": (detail_markdown or "")[:1500],
            "review_markdown_preview": (review_markdown or "")[:1500],
        }
        return merged

    async def _scrape_url(self, url: str) -> str | None:
        logger.info(f"[TapTap Firecrawl] Scrape: {url}")
        try:
            scrape_fn = getattr(self._app, "scrape", None)
            if callable(scrape_fn):
                result = await asyncio.to_thread(scrape_fn, url, formats=["markdown"])
            else:
                legacy_scrape_fn = getattr(self._app, "scrape_url", None)
                if not callable(legacy_scrape_fn):
                    raise AttributeError("Firecrawl client does not expose scrape() or scrape_url()")
                result = await asyncio.to_thread(
                    legacy_scrape_fn,
                    url,
                    params={"formats": ["markdown"]},
                )
        except Exception as exc:
            logger.error(f"[TapTap Firecrawl] Scrape failed: {exc}")
            return None
        return _extract_markdown(result)


def _extract_markdown(result: Any) -> str | None:
    if result is None:
        return None
    if isinstance(result, dict):
        markdown = result.get("markdown")
        if isinstance(markdown, str):
            return markdown
        data = result.get("data")
        if isinstance(data, dict) and isinstance(data.get("markdown"), str):
            return data["markdown"]
        return None

    markdown = getattr(result, "markdown", None)
    if isinstance(markdown, str):
        return markdown

    data = getattr(result, "data", None)
    if isinstance(data, dict) and isinstance(data.get("markdown"), str):
        return data["markdown"]

    return None
