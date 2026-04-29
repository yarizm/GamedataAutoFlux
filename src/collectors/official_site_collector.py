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


DEFAULT_INCLUDE_PATTERNS = ("news", "update", "patch", "event", "notice", "announcement")
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
        params = target.params or {}
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
        if not candidates:
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
            pages.append(_parse_html(fetched.url, fetched.html))

        items = _dedupe_items(
            [
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

        def add(url: str, anchor_text: str = "", depth: int = 1) -> None:
            canonical = _canonical_url(urljoin(entry_url, url))
            if not canonical or _is_excluded_url(canonical, exclude_patterns):
                return
            if only_same_domain and not _same_domain(entry_url, canonical):
                return
            score = _score_link(canonical, anchor_text, include_patterns)
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

        return sorted(candidates.values(), key=lambda item: (item.score, -item.depth, item.url), reverse=True)[:max_pages]

    async def _fetch_page(self, url: str, *, use_playwright: str = "auto") -> FetchResult:
        if use_playwright == "always":
            return await self._fetch_with_playwright(url)

        result = await self._fetch_with_httpx(url)
        if use_playwright == "never" or not self.playwright_enabled:
            return result
        if _needs_playwright_fallback(result):
            fallback = await self._fetch_with_playwright(url)
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


def _is_content_page(page: ParsedPage, include_patterns: tuple[str, ...]) -> bool:
    haystack = f"{page.url} {page.title} {page.content[:800]}".lower()
    return len(page.content) >= 80 and any(pattern.lower() in haystack for pattern in include_patterns)


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
    text = f"{url} {title} {content[:1000]}".lower()
    if re.search(r"patch|update|version|changelog|版本|更新", text):
        return "patch"
    if re.search(r"event|campaign|活动|赛事", text):
        return "event"
    if re.search(r"notice|announcement|公告|通知", text):
        return "announcement"
    return "news"


def _dedupe_items(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by_key: dict[str, dict[str, Any]] = {}
    for item in items:
        key = item.get("url") or hashlib.sha1(
            f"{item.get('title','')}|{item.get('date','')}|{item.get('content','')[:200]}".encode("utf-8")
        ).hexdigest()
        current = by_key.get(key)
        if current is None or len(item.get("content", "")) > len(current.get("content", "")):
            by_key[key] = item
    return sorted(by_key.values(), key=lambda item: item.get("date", ""), reverse=True)


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
    return len(html) < 800 or len(parsed.content) < 300 or (js_shell and len(parsed.content) < 700) or (not parsed.title and len(parsed.content) < 300)


def _canonical_url(url: Any) -> str:
    text = str(url or "").strip()
    if not text or text.lower().startswith(("mailto:", "javascript:", "tel:")):
        return ""
    parsed = urlparse(text)
    if not parsed.scheme:
        return text.rstrip("/")
    path = re.sub(r"/+", "/", parsed.path or "/")
    path = "" if path == "/" else path.rstrip("/")
    return urlunparse((parsed.scheme.lower(), parsed.netloc.lower(), path, "", parsed.query, ""))


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
