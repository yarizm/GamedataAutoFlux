from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import Any

import httpx
from loguru import logger

from src.collectors.base import BaseCollector, CollectTarget, CollectResult
from src.collectors.taptap.firecrawl_fallback import TapTapFirecrawlFallback
from src.collectors.taptap.parser import merge_taptap_payloads, parse_taptap_page
from src.collectors.taptap.playwright_scraper import (
    TapTapPlaywrightFailed,
    TapTapPlaywrightScraper,
)
from src.core.registry import registry


@registry.register("collector", "taptap")
class TapTapCollector(BaseCollector):
    BASE_URL = "https://www.taptap.cn"
    INTL_BASE_URL = "https://www.taptap.io"

    def __init__(self, config: dict[str, Any] | None = None):
        super().__init__(config)
        self._client: httpx.AsyncClient | None = None
        self._playwright: TapTapPlaywrightScraper | None = None
        self._firecrawl: TapTapFirecrawlFallback | None = None

    async def setup(self, config: dict[str, Any] | None = None) -> None:
        await super().setup(config)

        try:
            from src.core.config import get_settings

            settings = get_settings()
            collector_cfg = settings.get("collector", {})
            taptap_cfg = settings.get("taptap", {})
            firecrawl_cfg = settings.get("firecrawl", {})
        except Exception:
            collector_cfg = {}
            taptap_cfg = {}
            firecrawl_cfg = {}

        timeout = float(self.config.get("timeout", taptap_cfg.get("timeout", 20)))
        user_agent = self.config.get("user_agent", taptap_cfg.get("user_agent", collector_cfg.get("user_agent", "")))
        self.config.setdefault("request_retries", int(taptap_cfg.get("request_retries", 2)))
        self.config.setdefault("request_delay", float(taptap_cfg.get("request_delay", 1.5)))
        self._client = httpx.AsyncClient(
            timeout=timeout,
            follow_redirects=True,
            headers={
                "User-Agent": user_agent or _default_user_agent(),
                "Accept-Language": "en-US,en;q=0.9",
            },
        )

        playwright_enabled = self.config.get("playwright_enabled", taptap_cfg.get("playwright_enabled", True))
        if playwright_enabled:
            self._playwright = TapTapPlaywrightScraper(
                headless=bool(taptap_cfg.get("headless", True)),
                timeout=int(taptap_cfg.get("playwright_timeout", 30000)),
                request_delay=float(taptap_cfg.get("request_delay", 1.5)),
            )

        firecrawl_key = (
            self.config.get("firecrawl_api_key")
            or firecrawl_cfg.get("api_key", "")
        )
        if firecrawl_key and not str(firecrawl_key).startswith("${") and bool(taptap_cfg.get("firecrawl_enabled", True)):
            self._firecrawl = TapTapFirecrawlFallback(
                api_key=str(firecrawl_key),
                timeout=int(firecrawl_cfg.get("timeout", 30)),
            )

    async def teardown(self) -> None:
        if self._client is not None:
            await self._client.aclose()
            self._client = None
        if self._playwright is not None:
            await self._playwright.teardown()
        if self._firecrawl is not None:
            await self._firecrawl.teardown()
        await super().teardown()

    async def collect(self, target: CollectTarget) -> CollectResult:
        page_url, review_url = self._resolve_urls(target)
        metrics = list(target.params.get("metrics", ["details", "reviews", "updates"]))
        reviews_limit = int(target.params.get("reviews_limit", self.config.get("reviews_limit_default", 20)))
        reviews_pages = max(1, int(target.params.get("reviews_pages", self.config.get("reviews_pages_default", 1))))
        use_playwright = str(target.params.get("use_playwright", "auto")).lower()
        save_raw_snapshots = bool(self.config.get("save_raw_snapshots", True))

        logger.info(f"[TapTap] Start collect: {target.name} -> {page_url}")
        warnings: list[str] = []
        layer_used = "html"
        layers = {"details": "html", "reviews": "html", "updates": "html"}

        try:
            detail_html, review_html = await self._fetch_http_pages(detail_url=page_url, review_url=review_url)
            detail_payload = (
                parse_taptap_page(detail_html, page_url=page_url, source_format="html", review_limit=reviews_limit)
                if "details" in metrics or "updates" in metrics
                else {}
            )
            review_payload = (
                parse_taptap_page(review_html, page_url=review_url, source_format="html", review_limit=reviews_limit)
                if "reviews" in metrics
                else {}
            )
            merged = merge_taptap_payloads(detail_payload, review_payload)
        except Exception as exc:
            merged = {}
            warnings.append(f"HTTP HTML collection failed: {exc!r}")
            logger.warning(f"[TapTap] HTTP collection failed: {exc!r}")

        needs_playwright = use_playwright == "always"
        if use_playwright == "auto":
            needs_playwright = _needs_playwright(merged, metrics=metrics, reviews_pages=reviews_pages)

        if needs_playwright and self._playwright:
            logger.info("[TapTap] Supplement with Playwright")
            try:
                supplement = await self._playwright.fetch(
                    detail_url=page_url,
                    review_url=review_url,
                    reviews_pages=reviews_pages,
                )
                detail_payload = (
                    parse_taptap_page(
                        supplement["detail_html"],
                        page_url=page_url,
                        source_format="html",
                        review_limit=reviews_limit,
                    )
                    if "details" in metrics or "updates" in metrics
                    else {}
                )
                review_payloads = []
                for page_html in supplement["review_pages_html"]:
                    review_payloads.append(
                        parse_taptap_page(
                            page_html,
                            page_url=review_url,
                            source_format="html",
                            review_limit=reviews_limit,
                        )
                    )
                merged = merge_taptap_payloads(merged, detail_payload, *review_payloads)
                layer_used = "playwright"
                if detail_payload.get("game"):
                    layers["details"] = "playwright"
                if review_payloads:
                    layers["reviews"] = "playwright"
                if detail_payload.get("updates", {}).get("items"):
                    layers["updates"] = "playwright"
            except TapTapPlaywrightFailed as exc:
                warnings.append(f"Playwright supplement failed: {exc}")
                logger.warning(f"[TapTap] Playwright failed: {exc}")

        if _needs_firecrawl(merged, metrics=metrics) and self._firecrawl:
            logger.info("[TapTap] Fallback to Firecrawl")
            fallback = await self._firecrawl.scrape(
                detail_url=page_url,
                review_url=review_url,
                review_limit=reviews_limit,
            )
            if fallback and not fallback.get("error"):
                merged = merge_taptap_payloads(merged, fallback)
                layer_used = "firecrawl"
                for metric_name in ("details", "reviews", "updates"):
                    if fallback.get("game") and metric_name == "details":
                        layers["details"] = "firecrawl"
                    if fallback.get("reviews", {}).get("items") and metric_name == "reviews":
                        layers["reviews"] = "firecrawl"
                    if fallback.get("updates", {}).get("items") and metric_name == "updates":
                        layers["updates"] = "firecrawl"
            elif fallback and fallback.get("error"):
                warnings.append(f"Firecrawl failed: {fallback['error']}")

        if not merged.get("game"):
            return CollectResult(
                target=target,
                success=False,
                error="TapTap collector did not extract any game details",
                metadata={
                    "collector": "taptap",
                    "warnings": warnings,
                },
                raw_data=merged,
            )

        data = {
            "collector": "taptap",
            "game_name": merged["game"].get("title", target.name),
            "source_meta": {
                "collector": "taptap",
                "region": str(target.params.get("region", "cn")).lower(),
                "page_url": page_url,
                "review_url": review_url,
                "collected_at": datetime.now(timezone.utc).isoformat(),
                "layer": layer_used,
                "layers": layers,
            },
            "game": merged.get("game", {}),
            "reviews_summary": merged.get("reviews_summary", {}),
            "reviews": merged.get("reviews", {}),
            "updates": merged.get("updates", {}),
            "availability": _build_availability(merged, warnings=warnings),
            "raw_snapshots": merged.get("raw_snapshots", {}) if save_raw_snapshots else {},
            "snapshot": {
                "title": merged.get("game", {}).get("title", target.name),
                "provider": merged.get("game", {}).get("provider"),
                "score": merged.get("reviews_summary", {}).get("score"),
                "ratings_count": merged.get("reviews_summary", {}).get("ratings_count"),
                "last_updated_at": merged.get("game", {}).get("last_updated_at"),
                "updates_count": len(merged.get("updates", {}).get("items", [])),
                "availability_summary": _build_availability_summary(merged, warnings=warnings),
            },
        }

        metadata: dict[str, Any] = {
            "collector": "taptap",
            "data_sources": [layer_used],
        }
        if warnings:
            metadata["warnings"] = warnings

        return CollectResult(target=target, success=True, data=data, metadata=metadata)

    async def _fetch_http_pages(self, *, detail_url: str, review_url: str) -> tuple[str, str]:
        if self._client is None:
            raise RuntimeError("TapTap collector client is not initialized")
        detail_response = await self._fetch_with_retry(detail_url)
        review_response = await self._fetch_with_retry(review_url)
        return detail_response.text, review_response.text

    async def _fetch_with_retry(self, url: str) -> httpx.Response:
        retries = max(1, int(self.config.get("request_retries", 2)))
        last_error: Exception | None = None
        for attempt in range(1, retries + 1):
            try:
                response = await self._client.get(url)
                response.raise_for_status()
                return response
            except httpx.HTTPError as exc:
                last_error = exc
                if attempt >= retries:
                    break
                logger.warning(f"[TapTap] HTTP fetch retry {attempt}/{retries} for {url}: {exc!r}")
                await asyncio.sleep(float(self.config.get("request_delay", 1.5)))
        raise last_error or RuntimeError(f"TapTap fetch failed: {url}")

    def _resolve_urls(self, target: CollectTarget) -> tuple[str, str]:
        region = str(target.params.get("region", "cn")).lower()
        if region not in {"cn", "intl"}:
            raise ValueError("TapTap target region must be 'cn' or 'intl'")

        page_url = str(target.params.get("page_url", "")).strip()
        app_id = str(target.params.get("app_id", "")).strip()

        if not page_url and not app_id:
            raise ValueError("TapTap target requires page_url or app_id")
        if app_id and not page_url:
            base_url = self.BASE_URL if region == "cn" else self.INTL_BASE_URL
            page_url = f"{base_url}/app/{app_id}"
        if "/review" in page_url:
            detail_url = page_url.replace("/review", "")
        else:
            detail_url = page_url.rstrip("/")
        review_url = detail_url.rstrip("/") + "/review"
        return detail_url, review_url

    def validate_config(self, config: dict[str, Any] | None = None) -> bool:
        return True


def _needs_playwright(merged: dict[str, Any], *, metrics: list[str], reviews_pages: int) -> bool:
    if not merged:
        return True
    if "details" in metrics and not merged.get("game"):
        return True
    if "updates" in metrics and not merged.get("updates", {}).get("items"):
        return True
    if "reviews" in metrics:
        if not merged.get("reviews", {}).get("items"):
            return True
        if reviews_pages > 1:
            return True
    return False


def _needs_firecrawl(merged: dict[str, Any], *, metrics: list[str]) -> bool:
    if not merged:
        return True
    if "details" in metrics and not merged.get("game"):
        return True
    if "reviews" in metrics and not merged.get("reviews", {}).get("items"):
        return True
    if "updates" in metrics and not merged.get("updates", {}).get("items"):
        return True
    return False


def _default_user_agent() -> str:
    return (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36"
    )


def _build_availability(merged: dict[str, Any], *, warnings: list[str]) -> dict[str, Any]:
    game = merged.get("game", {}) or {}
    review_items = (merged.get("reviews", {}) or {}).get("items", []) or []
    updates_items = (merged.get("updates", {}) or {}).get("items", []) or []
    reviews_summary = merged.get("reviews_summary", {}) or {}

    http_failed = any("HTTP HTML collection failed" in warning for warning in warnings)
    firecrawl_failed = any("Firecrawl failed" in warning for warning in warnings)
    playwright_failed = any("Playwright supplement failed" in warning for warning in warnings)

    def reason_for_collection_gap(metric: str) -> str | None:
        if metric == "details" and game:
            return None
        if metric == "reviews" and (review_items or reviews_summary.get("ratings_count") is not None or reviews_summary.get("score") is not None):
            return None
        if metric == "updates" and updates_items:
            return None
        if firecrawl_failed:
            return "firecrawl_failed"
        if http_failed and not (playwright_failed or firecrawl_failed):
            return "public_page_request_failed"
        if playwright_failed:
            return "playwright_supplement_failed"
        return "not_exposed_on_public_page_or_not_detected"

    provider_available = bool(game.get("provider"))

    return {
        "details": {
            "available": bool(game),
            "reason": reason_for_collection_gap("details"),
        },
        "provider": {
            "available": provider_available,
            "reason": None if provider_available else "publisher_or_provider_not_reliably_exposed_on_public_page",
        },
        "reviews": {
            "available": bool(review_items) or reviews_summary.get("ratings_count") is not None or reviews_summary.get("score") is not None,
            "reason": reason_for_collection_gap("reviews"),
            "has_summary": reviews_summary.get("ratings_count") is not None or reviews_summary.get("score") is not None,
            "has_items": bool(review_items),
        },
        "updates": {
            "available": bool(updates_items),
            "reason": reason_for_collection_gap("updates"),
        },
    }


def _build_availability_summary(merged: dict[str, Any], *, warnings: list[str]) -> dict[str, bool]:
    availability = _build_availability(merged, warnings=warnings)
    return {
        "details": availability["details"]["available"],
        "provider": availability["provider"]["available"],
        "reviews": availability["reviews"]["available"],
        "updates": availability["updates"]["available"],
    }
