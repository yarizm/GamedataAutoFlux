"""Generic official game website collector."""

from __future__ import annotations

import asyncio
import hashlib
import json
import re
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from html.parser import HTMLParser
from typing import Any
from urllib.parse import urljoin, urlparse, urlunparse
from xml.etree import ElementTree

import httpx
from loguru import logger

from src.collectors.base import BaseCollector, CollectResult, CollectTarget
from src.core.config import get as get_config
from src.core.registry import registry


DEFAULT_INCLUDE_PATTERNS = (
    "news",
    "update",
    "patch",
    "event",
    "notice",
    "announcement",
    "新闻",
    "资讯",
    "公告",
    "活动",
    "更新",
    "赛事",
)
DEFAULT_EXCLUDE_PATTERNS = ("privacy", "terms", "support", "account", "login", "shop", "cart")
COMMON_PATHS = (
    "/news",
    "/updates",
    "/update",
    "/patch",
    "/patch-notes",
    "/events",
    "/notice",
    "/announcements",
)


@dataclass
class FetchResult:
    url: str
    status_code: int
    html: str
    strategy: str = "httpx"
    error: str = ""


@dataclass
class PageCandidate:
    url: str
    score: int = 0
    depth: int = 0
    anchor_text: str = ""


@dataclass
class ParsedPage:
    url: str
    title: str = ""
    description: str = ""
    published_at: str = ""
    content: str = ""
    links: list[tuple[str, str]] = field(default_factory=list)
    metadata: dict[str, str] = field(default_factory=dict)
    json_ld: list[Any] = field(default_factory=list)


@registry.register("collector", "official_site")
class OfficialSiteCollector(BaseCollector):
    """Collect news, patch notes, events and links from a generic official website."""

    def __init__(self, config: dict[str, Any] | None = None):
        super().__init__(config)
        self.timeout = float(self.config.get("timeout", get_config("official_site.timeout", 20)))
        self.request_delay = float(self.config.get("request_delay", get_config("official_site.request_delay", 0.5)))
        self.max_pages = int(self.config.get("max_pages", get_config("official_site.max_pages", 30)))
        self.max_depth = int(self.config.get("max_depth", get_config("official_site.max_depth", 2)))
        self.user_agent = str(
            self.config.get(
                "user_agent",
                get_config("official_site.user_agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"),
            )
        )
        self.playwright_enabled = bool(self.config.get("playwright_enabled", get_config("official_site.playwright_enabled", True)))
        self.headless = bool(self.config.get("headless", get_config("official_site.headless", True)))
        self.playwright_timeout = int(
            self.config.get("playwright_timeout", get_config("official_site.playwright_timeout", 30000))
        )

    async def collect(self, target: CollectTarget) -> CollectResult:
        raw_params = _compact_params(target.params or {})
        recipe_name, recipe_params = _resolve_recipe(target.name, raw_params)
        params = {**recipe_params, **raw_params}
        entry_url = _canonical_url(params.get("official_url") or params.get("url") or getattr(target, "url", ""))
        if not entry_url:
            return CollectResult(target=target, success=False, error="official_site requires official_url or url")

        include_patterns = _patterns(params.get("include_patterns"), get_config("official_site.include_patterns", DEFAULT_INCLUDE_PATTERNS))
        exclude_patterns = _patterns(params.get("exclude_patterns"), get_config("official_site.exclude_patterns", DEFAULT_EXCLUDE_PATTERNS))
        max_pages = int(params.get("max_pages") or self.max_pages)
        max_depth = int(params.get("max_depth") or self.max_depth)
        since_days = int(params.get("since_days") or 180)
        only_same_domain = bool(params.get("only_same_domain", True))
        use_playwright = str(params.get("use_playwright") or "auto").lower()
        request_delay = float(params.get("request_delay") or self.request_delay)
        listing_only = bool(params.get("listing_only", False))
        warnings: list[str] = []

        home = await self._fetch_page(entry_url, use_playwright=use_playwright)
        if home.status_code < 200 or home.status_code >= 300 or not home.html:
            return CollectResult(
                target=target,
                success=False,
                error=home.error or f"Failed to fetch official site homepage: HTTP {home.status_code}",
                metadata={"collector": "official_site"},
            )

        home_parsed = _parse_html(home.url, home.html)
        embedded_items: list[dict[str, Any]] = (
            _extract_embedded_news_items(home.html, home.url)
            + _extract_listing_items(home_parsed, home.html, include_patterns=include_patterns)
        )
        candidates: list[PageCandidate] = []
        if not (listing_only and embedded_items):
            candidates = await self._discover_pages(
                entry_url=home.url,
                home=home_parsed,
                max_pages=max_pages,
                max_depth=max_depth,
                include_patterns=include_patterns,
                exclude_patterns=exclude_patterns,
                only_same_domain=only_same_domain,
                use_playwright=use_playwright,
            )
        if not candidates and not embedded_items:
            warnings.append("No official news/update/event pages were discovered.")

        pages: list[ParsedPage] = []
        seen_pages = {home.url}
        for candidate in candidates[:max_pages]:
            if candidate.url in seen_pages:
                continue
            seen_pages.add(candidate.url)
            if request_delay > 0:
                await asyncio.sleep(request_delay)
            fetched = await self._fetch_page(candidate.url, use_playwright=use_playwright)
            if fetched.status_code < 200 or fetched.status_code >= 300 or not fetched.html:
                warnings.append(f"Failed to fetch candidate page: {candidate.url}")
                continue
            embedded_items.extend(_extract_embedded_news_items(fetched.html, fetched.url))
            parsed_page = _parse_html(fetched.url, fetched.html)
            embedded_items.extend(_extract_listing_items(parsed_page, fetched.html, include_patterns=include_patterns))
            pages.append(parsed_page)

        items = _dedupe_items(
            embedded_items
            + [
                _page_to_item(page, include_patterns=include_patterns)
                for page in pages
                if _is_content_page(page, include_patterns)
            ]
        )
        cutoff = datetime.now(timezone.utc).date() - timedelta(days=since_days)
        items = [item for item in items if not item.get("date") or _date_from_text(item["date"]) is None or _date_from_text(item["date"]) >= cutoff]

        news_items = [item for item in items if item.get("category") not in {"patch", "event"}]
        patch_items = [item for item in items if item.get("category") == "patch"]
        event_items = [_item_to_event(item) for item in items if item.get("category") == "event"]

        latest = _latest_item(items)
        game_name = target.name or _game_name_from_page(home_parsed) or _domain_name(entry_url)
        now = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

        data = {
            "collector": "official_site",
            "game_name": game_name,
            "official_url": entry_url,
            "source_meta": {
                "collector": "official_site",
                "collected_at": now,
                "entry_url": entry_url,
                "recipe": recipe_name,
                "validation_status": params.get("validation_status", ""),
                "validation_notes": params.get("validation_notes", ""),
                "pages_discovered": len(candidates),
                "pages_crawled": len(pages),
                "fetch_strategy": home.strategy,
                "warnings": list(dict.fromkeys(warnings)),
            },
            "news": {"items": news_items},
            "patch_notes": {"items": patch_items},
            "events": {"items": event_items},
            "snapshot": {
                "name": game_name,
                "latest_news_title": latest.get("title", "") if latest else "",
                "latest_news_date": latest.get("date", "") if latest else "",
                "news_count": len(news_items),
                "patch_notes_count": len(patch_items),
                "events_count": len(event_items),
            },
        }

        return CollectResult(target=target, data=data, success=True, metadata={"collector": "official_site"})

    async def _discover_pages(
        self,
        *,
        entry_url: str,
        home: ParsedPage,
        max_pages: int,
        max_depth: int,
        include_patterns: tuple[str, ...],
        exclude_patterns: tuple[str, ...],
        only_same_domain: bool,
        use_playwright: str,
    ) -> list[PageCandidate]:
        candidates: dict[str, PageCandidate] = {}

        def add(url: str, anchor_text: str = "", depth: int = 1, score_bonus: int = 0) -> None:
            canonical = _canonical_url(urljoin(entry_url, url))
            if not canonical or _is_excluded_url(canonical, exclude_patterns):
                return
            if only_same_domain and not _same_domain(entry_url, canonical):
                return
            score = _score_link(canonical, anchor_text, include_patterns) + score_bonus
            if score <= 0 and depth > 1:
                return
            current = candidates.get(canonical)
            if current is None or score > current.score:
                candidates[canonical] = PageCandidate(canonical, score=score, depth=depth, anchor_text=anchor_text)

        for href, text in home.links:
            add(href, text, 1)
        for path in COMMON_PATHS:
            add(path, path.strip("/"), 1)

        for sitemap_url in (urljoin(entry_url, "/sitemap.xml"),):
            sitemap = await self._fetch_page(sitemap_url, use_playwright="never")
            if sitemap.status_code == 200 and sitemap.html:
                for url in _parse_sitemap_urls(sitemap.html):
                    add(url, "", 1)

        if max_depth > 1:
            shallow = sorted(candidates.values(), key=lambda item: item.score, reverse=True)[: min(max_pages, 12)]
            for candidate in shallow:
                fetched = await self._fetch_page(candidate.url, use_playwright=use_playwright)
                if fetched.status_code != 200 or not fetched.html:
                    continue
                parsed = _parse_html(fetched.url, fetched.html)
                for href, text in parsed.links:
                    add(href, text, candidate.depth + 1)
                    nested_url = _canonical_url(urljoin(candidate.url, href))
                    if _looks_like_listing_url(nested_url, text):
                        dynamic_links = await self._discover_dynamic_listing_links(
                            nested_url,
                            max_links=max_pages,
                            use_playwright=use_playwright,
                        )
                        for rank, (dynamic_href, dynamic_text) in enumerate(dynamic_links):
                            add(dynamic_href, dynamic_text, candidate.depth + 2, score_bonus=max(1, 500 - rank))
                if _looks_like_listing_page(candidate.url, parsed):
                    dynamic_links = await self._discover_dynamic_listing_links(
                        candidate.url,
                        max_links=max_pages,
                        use_playwright=use_playwright,
                    )
                    for rank, (href, text) in enumerate(dynamic_links):
                        add(href, text, candidate.depth + 1, score_bonus=max(1, 500 - rank))

        return sorted(candidates.values(), key=lambda item: (item.score, -item.depth, item.url), reverse=True)[:max_pages]

    async def _discover_dynamic_listing_links(
        self,
        url: str,
        *,
        max_links: int,
        use_playwright: str,
    ) -> list[tuple[str, str]]:
        if use_playwright == "never" or not self.playwright_enabled:
            return []

        try:
            from playwright.async_api import async_playwright
        except ImportError:
            return []

        links: list[tuple[str, str]] = []
        seen: set[str] = set()

        def remember(items: list[dict[str, Any]]) -> int:
            added = 0
            for item in items:
                href = _canonical_url(item.get("href", ""))
                if not href or href in seen:
                    continue
                text = _clean_text(item.get("text", ""))
                seen.add(href)
                links.append((href, text))
                added += 1
                if len(links) >= max_links:
                    break
            return added

        try:
            async with async_playwright() as p:
                browser = await p.chromium.launch(headless=self.headless)
                page = await browser.new_page(user_agent=self.user_agent)
                await page.goto(url, wait_until="domcontentloaded", timeout=self.playwright_timeout)
                await page.wait_for_timeout(1200)
                remember(await _page_anchor_links(page))

                has_milo_get_news = await page.evaluate(
                    "() => typeof getNews === 'function' && typeof NewsArr === 'object'"
                )
                if has_milo_get_news and len(links) < max_links:
                    channels = await page.evaluate(
                        "() => Object.values(NewsArr).map(String).filter(Boolean)"
                    )
                    channels = list(dict.fromkeys(channels))
                    if channels:
                        # The first channel is normally the combined/latest feed. Walk it
                        # first so the collector covers a date window before category splits.
                        ordered_channels = channels[:1] + channels[1:]
                        limit = 9
                        for channel in ordered_channels:
                            empty_rounds = 0
                            max_offsets = max(4, min(20, (max_links // limit) + 4))
                            for offset_index in range(max_offsets):
                                if len(links) >= max_links:
                                    break
                                start = offset_index * limit
                                await page.evaluate(
                                    "([channel, limit, start]) => getNews(3, channel, 1, limit, start)",
                                    [channel, limit, start],
                                )
                                await page.wait_for_timeout(650)
                                added = remember(await _page_anchor_links(page))
                                empty_rounds = empty_rounds + 1 if added == 0 else 0
                                if empty_rounds >= 2:
                                    break
                            if len(links) >= max_links:
                                break

                await browser.close()
        except Exception as exc:
            logger.debug("[OfficialSite] dynamic listing discovery failed {}: {}", url, exc)
            return links

        return links

    async def _fetch_page(self, url: str, *, use_playwright: str = "auto") -> FetchResult:
        if use_playwright == "always":
            result = await self._fetch_with_playwright(url)
            if result.status_code == 404:
                retry_url = _slash_retry_url(url)
                if retry_url:
                    retry = await self._fetch_with_playwright(retry_url)
                    if retry.status_code < 400 and retry.html:
                        return retry
            return result

        result = await self._fetch_with_httpx(url)
        if result.status_code == 404:
            retry_url = _slash_retry_url(url)
            if retry_url:
                retry = await self._fetch_with_httpx(retry_url)
                if retry.status_code < 400 and retry.html:
                    result = retry
        if use_playwright == "never" or not self.playwright_enabled:
            return result
        if _needs_playwright_fallback(result):
            fallback = await self._fetch_with_playwright(url)
            if fallback.status_code == 404:
                retry_url = _slash_retry_url(url)
                if retry_url:
                    retry = await self._fetch_with_playwright(retry_url)
                    if retry.status_code < 400 and retry.html:
                        fallback = retry
            if fallback.html:
                return fallback
        return result

    async def _fetch_with_httpx(self, url: str) -> FetchResult:
        try:
            async with httpx.AsyncClient(
                timeout=self.timeout,
                follow_redirects=True,
                headers={"User-Agent": self.user_agent, "Accept": "text/html,application/xhtml+xml"},
            ) as client:
                response = await client.get(url)
                return FetchResult(str(response.url), response.status_code, response.text or "", "httpx")
        except Exception as exc:
            logger.debug("[OfficialSite] httpx fetch failed {}: {}", url, exc)
            return FetchResult(url, 0, "", "httpx", str(exc))

    async def _fetch_with_playwright(self, url: str) -> FetchResult:
        try:
            from playwright.async_api import async_playwright
        except ImportError:
            return FetchResult(url, 0, "", "playwright", "playwright is not installed")

        try:
            async with async_playwright() as p:
                browser = await p.chromium.launch(headless=self.headless)
                page = await browser.new_page(user_agent=self.user_agent)
                response = await page.goto(url, wait_until="domcontentloaded", timeout=self.playwright_timeout)
                if "bungie.net" in urlparse(url).netloc.lower():
                    try:
                        await page.wait_for_load_state("networkidle", timeout=min(self.playwright_timeout, 15000))
                    except Exception:
                        pass
                await page.wait_for_timeout(800)
                html = await page.content()
                final_url = page.url
                status = response.status if response else 200
                await browser.close()
                return FetchResult(final_url, status, html, "playwright")
        except Exception as exc:
            logger.debug("[OfficialSite] playwright fetch failed {}: {}", url, exc)
            return FetchResult(url, 0, "", "playwright", str(exc))


class _SimpleHTMLExtractor(HTMLParser):
    def __init__(self, base_url: str):
        super().__init__(convert_charrefs=True)
        self.base_url = base_url
        self.title_parts: list[str] = []
        self.text_parts: list[str] = []
        self.links: list[tuple[str, str]] = []
        self.metadata: dict[str, str] = {}
        self.json_ld_texts: list[str] = []
        self._tag_stack: list[str] = []
        self._skip_depth = 0
        self._current_link: dict[str, Any] | None = None
        self._in_title = False
        self._in_json_ld = False
        self._json_buffer: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        tag = tag.lower()
        attrs_dict = {key.lower(): value or "" for key, value in attrs}
        self._tag_stack.append(tag)
        if tag in {"script", "style", "nav", "footer", "header", "noscript", "svg"}:
            self._skip_depth += 1
        if tag == "title":
            self._in_title = True
        if tag == "script" and "ld+json" in attrs_dict.get("type", "").lower():
            self._in_json_ld = True
            self._json_buffer = []
            self._skip_depth = max(self._skip_depth - 1, 0)
        if tag == "meta":
            key = attrs_dict.get("property") or attrs_dict.get("name")
            content = attrs_dict.get("content", "")
            if key and content:
                self.metadata[key.lower()] = content.strip()
        if tag == "a" and attrs_dict.get("href"):
            self._current_link = {"href": attrs_dict["href"], "text": []}

    def handle_endtag(self, tag: str) -> None:
        tag = tag.lower()
        if tag in {"script", "style", "nav", "footer", "header", "noscript", "svg"} and not self._in_json_ld:
            self._skip_depth = max(self._skip_depth - 1, 0)
        if tag == "title":
            self._in_title = False
        if tag == "script" and self._in_json_ld:
            self._in_json_ld = False
            self.json_ld_texts.append("".join(self._json_buffer))
        if tag == "a" and self._current_link:
            href = self._current_link.get("href", "")
            text = " ".join(self._current_link.get("text", [])).strip()
            self.links.append((href, text))
            self._current_link = None
        if self._tag_stack:
            self._tag_stack.pop()

    def handle_data(self, data: str) -> None:
        text = re.sub(r"\s+", " ", data or "").strip()
        if not text:
            return
        if self._in_json_ld:
            self._json_buffer.append(data)
            return
        if self._in_title:
            self.title_parts.append(text)
        if self._current_link is not None:
            self._current_link["text"].append(text)
        if self._skip_depth == 0:
            self.text_parts.append(text)


def _parse_html(url: str, html: str) -> ParsedPage:
    parser = _SimpleHTMLExtractor(url)
    parser.feed(html or "")
    json_ld = _parse_json_ld(parser.json_ld_texts)
    metadata = parser.metadata
    title = _first_nonempty(
        _json_ld_value(json_ld, "headline"),
        _json_ld_value(json_ld, "name"),
        metadata.get("og:title"),
        " ".join(parser.title_parts).strip(),
    )
    description = _first_nonempty(
        metadata.get("og:description"),
        metadata.get("description"),
        _json_ld_value(json_ld, "description"),
    )
    published = _first_nonempty(
        metadata.get("article:published_time"),
        metadata.get("date"),
        _json_ld_value(json_ld, "datePublished"),
        _find_date(" ".join(parser.text_parts[:80])),
    )
    content = _clean_text(" ".join(parser.text_parts))
    return ParsedPage(
        url=_canonical_url(url),
        title=_clean_text(title),
        description=_clean_text(description),
        published_at=_normalize_date_text(published),
        content=content,
        links=parser.links,
        metadata=metadata,
        json_ld=json_ld,
    )


def _page_to_item(page: ParsedPage, *, include_patterns: tuple[str, ...]) -> dict[str, Any]:
    category = _classify_category(page.url, page.title, page.content)
    summary = page.description or _truncate(page.content, 280)
    return {
        "title": page.title or _title_from_url(page.url),
        "url": page.url,
        "date": page.published_at or _normalize_date_text(_find_date(page.url + " " + page.content[:500])),
        "category": category if category in {"patch", "event", "announcement"} else "news",
        "summary": summary,
        "content": _truncate(page.content, 4000),
    }


def _item_to_event(item: dict[str, Any]) -> dict[str, Any]:
    return {
        "date": item.get("date", ""),
        "type": "official_site",
        "title": item.get("title", ""),
        "summary": item.get("summary", ""),
        "source": "official_site",
        "url": item.get("url", ""),
        "category": item.get("category", ""),
    }


def _extract_embedded_news_items(html: str, page_url: str) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    if not html or ("news-item" not in html and "nitem" not in html):
        return items

    sequence_pattern = re.compile(
        r'class="[^"]*currentCategory[^"]*"[^>]*>(?P<category>.*?)</div>.*?'
        r'class="[^"]*news-title[^"]*"[^>]*>(?P<title>.*?)</div>.*?'
        r'class="[^"]*time[^"]*"[^>]*>(?P<date>.*?)</div>',
        re.IGNORECASE | re.DOTALL,
    )
    for match in sequence_pattern.finditer(html):
        category = _html_fragment_text(match.group("category"))
        title = _html_fragment_text(match.group("title"))
        date = _normalize_date_text(_html_fragment_text(match.group("date")))
        if title:
            items.append(_embedded_item(page_url, title, category, date))

    for match in re.finditer(r'<div[^>]+class="[^"]*news-item[^"]*"[^>]*>(.*?)</div>\s*</div>', html, re.IGNORECASE | re.DOTALL):
        block = match.group(1)
        category = _html_fragment_text(_first_regex(block, r'class="[^"]*(?:currentCategory|newsType|category)[^"]*"[^>]*>(.*?)</div>'))
        title = _html_fragment_text(
            _first_regex(block, r'class="[^"]*(?:news-title|newsTitle|title)[^"]*"[^>]*>(.*?)</div>')
            or _first_regex(block, r"<h[1-3][^>]*>(.*?)</h[1-3]>")
        )
        date = _normalize_date_text(
            _html_fragment_text(_first_regex(block, r'class="[^"]*(?:time|date|newsTime)[^"]*"[^>]*>(.*?)</div>'))
        )
        if not title:
            continue
        items.append(_embedded_item(page_url, title, category, date))

    for match in re.finditer(r'<div[^>]+class="[^"]*\bnitem\b[^"]*"[^>]*>(.*?)</div>\s*</div>\s*</div>', html, re.IGNORECASE | re.DOTALL):
        block = match.group(1)
        title = _html_fragment_text(_first_regex(block, r'class="[^"]*\btitle\b[^"]*"[^>]*>(.*?)</div>'))
        date = _normalize_date_text(_html_fragment_text(_first_regex(block, r'class="[^"]*\btime\b[^"]*"[^>]*>(.*?)</div>')))
        if title:
            items.append(_embedded_item(page_url, title, "", date))

    if "nitem" in html:
        card_pattern = re.compile(
            r'class="[^"]*\btitle\b[^"]*"[^>]*>(?P<title>.*?)</div>.*?'
            r'class="[^"]*\btime\b[^"]*"[^>]*>(?P<date>.*?)</div>',
            re.IGNORECASE | re.DOTALL,
        )
        for match in card_pattern.finditer(html):
            title = _html_fragment_text(match.group("title"))
            date = _normalize_date_text(_html_fragment_text(match.group("date")))
            if title:
                items.append(_embedded_item(page_url, title, "", date))
    return items


def _extract_listing_items(page: ParsedPage, html: str, *, include_patterns: tuple[str, ...]) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    for href, text in page.links:
        title_text = _clean_text(text)
        date = _normalize_date_text(_find_date(title_text) or _find_date(href))
        if not date:
            continue
        title = _listing_title_from_text(title_text, date) or _title_from_url(href)
        url = _canonical_url(urljoin(page.url, href))
        haystack = f"{url} {title}".lower()
        if not any(pattern.lower() in haystack for pattern in include_patterns) and not re.search(
            r"ver\.|version|アップ|更新|配信|お知らせ|公告|活动|联动|发布日期",
            title,
            re.IGNORECASE,
        ):
            continue
        category = _classify_category(url, title, title_text)
        items.append(
            {
                "title": title,
                "url": url,
                "date": date,
                "category": category if category in {"patch", "event", "announcement"} else "news",
                "summary": title_text,
                "content": title_text,
            }
        )
    return items


def _listing_title_from_text(text: str, date: str) -> str:
    title = _clean_text(text)
    if not title:
        return ""
    title = re.sub(r"20\d{2}[-/.]\d{1,2}[-/.]\d{1,2}", " ", title)
    title = re.sub(r"\b\d{1,2}/\d{1,2}/20\d{2}\b", " ", title)
    title = re.sub(r"20\d{2}\s*年\s*\d{1,2}\s*月\s*\d{1,2}\s*日(?:（[^）]*）)?", " ", title)
    title = re.sub(r"发布日期", " ", title)
    title = re.sub(r"\bCST_\d{1,2}:\d{2}（UTC_\d{1,2}:\d{2}）", " ", title)
    title = re.sub(r"\s+", " ", title).strip(" ・|-/")
    return title


def _embedded_item(page_url: str, title: str, category: str, date: str) -> dict[str, Any]:
    item_category = _classify_category(page_url, f"{category} {title}", "")
    if category and "公告" in category:
        item_category = "announcement"
    elif category and "活动" in category:
        item_category = "event"
    elif category and "更新" in category:
        item_category = "patch"
    return {
        "title": title,
        "url": page_url,
        "date": date,
        "category": item_category if item_category in {"patch", "event", "announcement"} else "news",
        "summary": title,
        "content": title,
    }


def _first_regex(text: str, pattern: str) -> str:
    match = re.search(pattern, text or "", re.IGNORECASE | re.DOTALL)
    return match.group(1) if match else ""


def _html_fragment_text(fragment: str) -> str:
    text = re.sub(r"<[^>]+>", " ", fragment or "")
    text = (
        text.replace("&nbsp;", " ")
        .replace("&amp;", "&")
        .replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&quot;", '"')
        .replace("&#39;", "'")
    )
    return _clean_text(text)


def _is_content_page(page: ParsedPage, include_patterns: tuple[str, ...]) -> bool:
    path_name = (urlparse(page.url).path.rstrip("/").split("/")[-1] or "").lower()
    if _is_error_like_page(page):
        return False
    list_like_names = {
        "",
        "index.html",
        "index.shtml",
        "main.html",
        "main.shtml",
        "news",
        "updates",
        "update",
        "patch",
        "patch-notes",
        "events",
        "notice",
        "announcements",
        "official",
        "tag",
        "tags",
        "newslist.html",
        "newslist.shtml",
    }
    if path_name in list_like_names and not re.search(r"20\d{2}[-/]?\d{1,2}[-/]?\d{0,2}", page.url):
        return False
    haystack = f"{page.url} {page.title} {page.content[:800]}".lower()
    return len(page.content) >= 80 and any(pattern.lower() in haystack for pattern in include_patterns)


def _is_error_like_page(page: ParsedPage) -> bool:
    title = _clean_text(page.title).lower()
    content = _clean_text(page.content).lower()
    if title in {"error", "not found", "404", "403 forbidden"}:
        return True
    if len(content) < 500 and re.search(r"\b(error|not found|404|403|page not found)\b", f"{title} {content}"):
        return True
    return False


def _looks_like_listing_page(url: str, page: ParsedPage) -> bool:
    haystack = f"{url} {page.title} {page.content[:1000]}".lower()
    path_name = (urlparse(url).path.rstrip("/").split("/")[-1] or "").lower()
    return (
        "list" in path_name
        or "news" in path_name
        or "notice" in path_name
        or "announcement" in path_name
        or "event" in path_name
        or "新闻" in haystack
        or "公告" in haystack
        or "资讯" in haystack
        or "milo.emit" in haystack
        or "fillnewsgicp" in haystack
    )


def _looks_like_listing_url(url: str, anchor_text: str = "") -> bool:
    if not url:
        return False
    parsed = urlparse(url)
    path = parsed.path.lower()
    text = (anchor_text or "").lower()
    if "detail" in path or "video" in path or "media" in path or parsed.query:
        return False
    return (
        "list" in path
        or "news" in path
        or "notice" in path
        or "announcement" in path
        or "event" in path
        or "新闻" in text
        or "资讯" in text
        or "公告" in text
        or "活动" in text
    )


async def _page_anchor_links(page: Any) -> list[dict[str, str]]:
    return await page.evaluate(
        """() => Array.from(document.querySelectorAll('a[href]'))
            .map((a) => ({ href: a.href || a.getAttribute('href') || '', text: a.innerText || a.textContent || '' }))
            .filter((item) => {
                const href = String(item.href || '').toLowerCase();
                const text = String(item.text || '');
                if (!href || href.startsWith('javascript:') || href.startsWith('mailto:') || href.startsWith('tel:')) return false;
                return /newsdetail|detail|article|notice|announcement|event/.test(href)
                    || /20\\d{2}[-/.]\\d{1,2}[-/.]\\d{1,2}|\\d{1,2}[-/.]\\d{1,2}/.test(text);
            })"""
    )


def _score_link(url: str, anchor_text: str, include_patterns: tuple[str, ...]) -> int:
    haystack_url = url.lower()
    haystack_text = (anchor_text or "").lower()
    score = 0
    score += sum(10 for pattern in include_patterns if pattern.lower() in haystack_url)
    score += sum(6 for pattern in include_patterns if pattern.lower() in haystack_text)
    if re.search(r"/20\d{2}[/-]\d{1,2}", haystack_url):
        score += 3
    return score


def _classify_category(url: str, title: str, content: str) -> str:
    primary = f"{url} {title}".lower()
    fallback = f"{primary} {content[:1000]}".lower()
    if re.search(r"patch|update|version|changelog|版本|更新|新赛季", primary):
        return "patch"
    if re.search(r"notice|announcement|公告|通知|说明|处罚|提醒|指引", primary):
        return "announcement"
    if re.search(r"event|campaign|活动|赛事|直播|福利|联动|报名|大赛|杯|联赛|邀请赛", primary):
        return "event"
    if re.search(r"patch|update|version|changelog", fallback):
        return "patch"
    if re.search(r"event|campaign", fallback):
        return "event"
    if re.search(r"notice|announcement", fallback):
        return "announcement"
    return "news"


def _dedupe_items(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by_key: dict[str, dict[str, Any]] = {}
    for item in items:
        key = _dedupe_key(item)
        current = by_key.get(key)
        if (
            current is None
            or (item.get("date") and not current.get("date"))
            or len(item.get("content", "")) > len(current.get("content", ""))
        ):
            by_key[key] = item
    return sorted(by_key.values(), key=lambda item: item.get("date", ""), reverse=True)


def _dedupe_key(item: dict[str, Any]) -> str:
    url = str(item.get("url") or "")
    title = str(item.get("title") or "")
    date = str(item.get("date") or "")
    parsed = urlparse(url)
    path_name = (parsed.path.rstrip("/").split("/")[-1] or "").lower()
    if url and title and (parsed.fragment or path_name in {"", "news", "news.html", "newslist", "newslist.html"}):
        return hashlib.sha1(f"{url}|{title}".encode("utf-8")).hexdigest()
    return url or hashlib.sha1(f"{title}|{date}|{item.get('content','')[:200]}".encode("utf-8")).hexdigest()


def _parse_sitemap_urls(xml_text: str) -> list[str]:
    urls: list[str] = []
    try:
        root = ElementTree.fromstring(xml_text.encode("utf-8"))
    except Exception:
        return urls
    for elem in root.iter():
        if elem.tag.lower().endswith("loc") and elem.text:
            urls.append(elem.text.strip())
    return urls


def _parse_json_ld(texts: list[str]) -> list[Any]:
    parsed: list[Any] = []
    for text in texts:
        try:
            value = json.loads(text)
        except Exception:
            continue
        if isinstance(value, list):
            parsed.extend(value)
        else:
            parsed.append(value)
    return parsed


def _json_ld_value(items: list[Any], key: str) -> str:
    for item in items:
        if isinstance(item, dict):
            value = item.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()
            graph = item.get("@graph")
            if isinstance(graph, list):
                nested = _json_ld_value(graph, key)
                if nested:
                    return nested
    return ""


def _needs_playwright_fallback(result: FetchResult) -> bool:
    if result.status_code < 200 or result.status_code >= 300:
        return True
    html = result.html or ""
    parsed = _parse_html(result.url, html)
    lowered = html.lower()
    js_shell = any(token in lowered for token in ("__next_data__", "id=\"app\"", "id=\"root\"", "nuxt", "vite"))
    dynamic_article = any(
        token in lowered
        for token in (
            "milo.emit",
            "getinfo(params",
            "id=\"newstitle\"",
            "id='newstitle'",
            "id=\"newstime\"",
            "id='newstime'",
            "id=\"scontent\"",
            "id='scontent'",
            "fillnewsgicp",
        )
    )
    if dynamic_article:
        return True
    return len(html) < 800 or len(parsed.content) < 300 or (js_shell and len(parsed.content) < 700) or (not parsed.title and len(parsed.content) < 300)


def _compact_params(params: dict[str, Any]) -> dict[str, Any]:
    compacted: dict[str, Any] = {}
    for key, value in params.items():
        if value is None:
            continue
        if isinstance(value, str) and not value.strip():
            continue
        compacted[key] = value
    return compacted


def _resolve_recipe(target_name: str, params: dict[str, Any]) -> tuple[str, dict[str, Any]]:
    recipes = get_config("official_site.recipes", {}) or {}
    if not isinstance(recipes, dict) or not recipes:
        return "", {}

    aliases = get_config("official_site.recipe_aliases", {}) or {}
    if not isinstance(aliases, dict):
        aliases = {}

    requested_names = [
        str(params.get("recipe") or "").strip(),
        str(params.get("game_id") or "").strip(),
        str(target_name or "").strip(),
    ]
    recipe_index = {_recipe_key(name): (str(name), value) for name, value in recipes.items()}

    for requested in requested_names:
        if not requested:
            continue
        alias_target = aliases.get(requested) or aliases.get(_recipe_key(requested))
        for candidate in (requested, alias_target):
            if not candidate:
                continue
            match = recipe_index.get(_recipe_key(candidate))
            if match and isinstance(match[1], dict):
                return match[0], _compact_params(match[1])
    return "", {}


def _recipe_key(value: Any) -> str:
    lowered = str(value or "").casefold()
    return re.sub(r"[\s_\-:：/|]+", "", lowered)


def _canonical_url(url: Any) -> str:
    text = str(url or "").strip()
    if not text or text.lower().startswith(("mailto:", "javascript:", "tel:")):
        return ""
    parsed = urlparse(text)
    if not parsed.scheme:
        return text.rstrip("/")
    path = re.sub(r"/+", "/", parsed.path or "/")
    path = "" if path == "/" else path.rstrip("/")
    fragment = parsed.fragment if parsed.fragment.startswith(("/", "!/")) else ""
    return urlunparse((parsed.scheme.lower(), parsed.netloc.lower(), path, "", parsed.query, fragment))


def _slash_retry_url(url: str) -> str:
    parsed = urlparse(url)
    if not parsed.scheme or not parsed.netloc or parsed.query or parsed.path.endswith("/"):
        return ""
    if not parsed.path or "." in parsed.path.rsplit("/", 1)[-1]:
        return ""
    return urlunparse((parsed.scheme, parsed.netloc, parsed.path + "/", "", "", ""))


def _same_domain(a: str, b: str) -> bool:
    return urlparse(a).netloc.lower().lstrip("www.") == urlparse(b).netloc.lower().lstrip("www.")


def _is_excluded_url(url: str, exclude_patterns: tuple[str, ...]) -> bool:
    lowered = url.lower()
    return any(pattern.lower() in lowered for pattern in exclude_patterns)


def _patterns(value: Any, default: Any) -> tuple[str, ...]:
    raw = value if isinstance(value, (list, tuple)) else default
    return tuple(str(item).lower() for item in raw if str(item).strip())


def _clean_text(text: Any) -> str:
    return re.sub(r"\s+", " ", str(text or "")).strip()


def _truncate(text: Any, limit: int) -> str:
    cleaned = _clean_text(text)
    return cleaned if len(cleaned) <= limit else cleaned[:limit].rstrip() + "..."


def _first_nonempty(*values: Any) -> str:
    for value in values:
        text = _clean_text(value)
        if text:
            return text
    return ""


def _find_date(text: str) -> str:
    patterns = [
        r"20\d{2}[-/.]\d{1,2}[-/.]\d{1,2}",
        r"\d{1,2}/\d{1,2}/20\d{2}",
        r"20\d{2}\s*年\s*\d{1,2}\s*月\s*\d{1,2}\s*日",
        r"(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Sept|Oct|Nov|Dec)[a-z]*\s+\d{1,2},\s+20\d{2}",
        r"\d{1,2}\s+(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Sept|Oct|Nov|Dec)[a-z]*\s+20\d{2}",
    ]
    for pattern in patterns:
        match = re.search(pattern, text or "", re.IGNORECASE)
        if match:
            return match.group(0)
    return ""


def _normalize_date_text(text: Any) -> str:
    raw = _clean_text(text)
    if not raw:
        return ""
    iso = re.search(r"20\d{2}[-/.]\d{1,2}[-/.]\d{1,2}", raw)
    if iso:
        parts = re.split(r"[-/.]", iso.group(0))
        return f"{int(parts[0]):04d}-{int(parts[1]):02d}-{int(parts[2]):02d}"
    us_numeric = re.search(r"\b(\d{1,2})/(\d{1,2})/(20\d{2})\b", raw)
    if us_numeric:
        return f"{int(us_numeric.group(3)):04d}-{int(us_numeric.group(1)):02d}-{int(us_numeric.group(2)):02d}"
    zh = re.search(r"(20\d{2})\s*年\s*(\d{1,2})\s*月\s*(\d{1,2})\s*日", raw)
    if zh:
        return f"{int(zh.group(1)):04d}-{int(zh.group(2)):02d}-{int(zh.group(3)):02d}"
    for fmt in ("%b %d, %Y", "%B %d, %Y", "%d %b %Y", "%d %B %Y"):
        try:
            return datetime.strptime(raw[:32], fmt).date().isoformat()
        except ValueError:
            continue
    return raw[:32]


def _date_from_text(text: str):
    normalized = _normalize_date_text(text)
    try:
        return datetime.fromisoformat(normalized).date()
    except ValueError:
        return None


def _latest_item(items: list[dict[str, Any]]) -> dict[str, Any]:
    dated = [item for item in items if item.get("date")]
    return (dated or items or [{}])[0]


def _game_name_from_page(page: ParsedPage) -> str:
    title = page.metadata.get("og:site_name") or page.title
    return re.split(r"\s+[|-]\s+", title or "")[0].strip()


def _domain_name(url: str) -> str:
    return urlparse(url).netloc.replace("www.", "")


def _title_from_url(url: str) -> str:
    segment = (urlparse(url).path.rstrip("/").split("/")[-1] or "official update").replace("-", " ").replace("_", " ")
    return segment.title()
