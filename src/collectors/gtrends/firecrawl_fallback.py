"""
Firecrawl fallback collector for Google Trends pages.

When pytrends is blocked (429), Firecrawl renders the Google Trends explore
page and extracts related queries, related topics, and interest-by-region data
from the rendered Markdown/HTML.
"""

from __future__ import annotations

import asyncio
import re
from typing import Any
from urllib.parse import quote

from loguru import logger


class GtrendsFirecrawlFallback:
    """Fallback Google Trends scraper backed by Firecrawl."""

    TRENDS_BASE = "https://trends.google.com/trends/explore"

    def __init__(
        self,
        api_key: str = "",
        timeout: int = 30,
        headers: dict[str, str] | None = None,
    ):
        self._api_key = api_key
        self._timeout = timeout
        self._headers = dict(headers or {})
        self._app = None

    async def setup(self) -> None:
        if not self._api_key:
            logger.warning("[Gtrends:Firecrawl] api_key not configured; fallback unavailable")
            return
        try:
            from firecrawl import FirecrawlApp

            self._app = FirecrawlApp(api_key=self._api_key)
            logger.info("[Gtrends:Firecrawl] Client initialized")
        except ImportError:
            logger.error("[Gtrends:Firecrawl] firecrawl-py not installed")
            self._app = None

    async def teardown(self) -> None:
        self._app = None

    async def scrape(
        self,
        keyword: str,
        *,
        hl: str = "zh-CN",
        geo: str = "",
        timeframe: str = "today 12-m",
    ) -> dict[str, Any]:
        """Scrape Google Trends explore page and extract related queries/topics."""
        if not self._app:
            await self.setup()
            if not self._app:
                return {"source": "firecrawl", "error": "Firecrawl unavailable"}

        url = self._build_trends_url(keyword, hl=hl, geo=geo, timeframe=timeframe)
        logger.info(f"[Gtrends:Firecrawl] Scrape: {url}")

        result: dict[str, Any] = {
            "source": "firecrawl",
            "keyword": keyword,
            "url": url,
        }

        try:
            markdown = await self._scrape_url(url)
            if not markdown:
                return {**result, "error": "Firecrawl returned empty response"}

            result["related_queries"] = _extract_related_queries(markdown)
            result["related_topics"] = _extract_related_topics(markdown)
            result["interest_by_region"] = _extract_interest_by_region(markdown)
            result["markdown_preview"] = markdown[:2000]

            logger.info(
                f"[Gtrends:Firecrawl] Extracted: "
                f"related_queries={len(result.get('related_queries', {}).get('top', []))} top / "
                f"{len(result.get('related_queries', {}).get('rising', []))} rising, "
                f"topics={len(result.get('related_topics', {}).get('top', []))} top / "
                f"{len(result.get('related_topics', {}).get('rising', []))} rising"
            )
        except Exception as exc:
            logger.error(f"[Gtrends:Firecrawl] Scrape failed: {exc}")
            result["error"] = str(exc)

        return result

    def _build_trends_url(
        self,
        keyword: str,
        *,
        hl: str = "zh-CN",
        geo: str = "",
        timeframe: str = "today 12-m",
    ) -> str:
        params_parts = [f"q={quote(keyword)}"]
        if hl:
            params_parts.append(f"hl={hl}")
        if geo:
            params_parts.append(f"geo={geo}")
        if timeframe:
            params_parts.append(f"date={quote(timeframe)}")
        return f"{self.TRENDS_BASE}?{'&'.join(params_parts)}"

    async def _scrape_url(self, url: str) -> str | None:
        try:
            scrape_fn = getattr(self._app, "scrape", None)
            if callable(scrape_fn):
                kwargs: dict[str, Any] = {"formats": ["markdown"]}
                if self._headers:
                    kwargs["headers"] = self._headers
                result = await asyncio.to_thread(scrape_fn, url, **kwargs)
            else:
                legacy_fn = getattr(self._app, "scrape_url", None)
                if not callable(legacy_fn):
                    raise AttributeError("Firecrawl client missing scrape() / scrape_url()")
                params: dict[str, Any] = {"formats": ["markdown"]}
                if self._headers:
                    params["headers"] = self._headers
                result = await asyncio.to_thread(legacy_fn, url, params=params)

            markdown = _extract_markdown(result)
            if markdown:
                logger.debug(f"[Gtrends:Firecrawl] Retrieved {len(markdown)} chars")
                return markdown

            logger.warning(f"[Gtrends:Firecrawl] Empty response: {url}")
            return None
        except Exception as exc:
            logger.error(f"[Gtrends:Firecrawl] Request failed: {exc}")
            return None


# ---------------------------------------------------------------------------
# Markdown extraction helpers
# ---------------------------------------------------------------------------

_RELATED_QUERIES_HEADER = re.compile(
    r"(?:related\s+queries|相关查询|関連クエリ|관련\s+검색어|consultas\s+relacionadas)",
    re.IGNORECASE,
)
_RELATED_TOPICS_HEADER = re.compile(
    r"(?:related\s+topics|相关主题|関連トピック|관련\s+주제|temas\s+relacionados)",
    re.IGNORECASE,
)
_RISING_HEADER = re.compile(
    r"(?:rising|飙升|急上昇|급상승|en\s+aumento|aumento)",
    re.IGNORECASE,
)
_TOP_HEADER = re.compile(r"^\s*(?:top|热门|人気|인기|principales)\s*$", re.IGNORECASE)
_REGION_HEADER = re.compile(
    r"(?:interest\s+by\s+(?:sub-?)?region|按区域划分的(?:子区域)?兴趣|地域別|지역별|interés\s+por\s+(?:sub)?región)",
    re.IGNORECASE,
)

# Table row with query name and value
_TABLE_QUERY_ROW = re.compile(
    r"\|\s*(?P<rank>\d+)\s*\|\s*(?P<keyword>[^|]+?)\s*\|\s*(?P<value>\d+|Breakout|爆発|飙升|急上昇)\s*\|",
    re.IGNORECASE,
)
# Simpler list-based extraction fallback
_LIST_ITEM = re.compile(r"^\s*[-*]\s+(.+)$", re.MULTILINE)


def _extract_related_queries(markdown: str) -> dict[str, Any]:
    result: dict[str, Any] = {"top": [], "rising": []}
    section = _find_section(markdown, _RELATED_QUERIES_HEADER)
    if not section:
        return result

    result["top"] = _extract_query_rows(section, "top")
    result["rising"] = _extract_query_rows(section, "rising")
    return result


def _extract_related_topics(markdown: str) -> dict[str, Any]:
    result: dict[str, Any] = {"top": [], "rising": []}
    section = _find_section(markdown, _RELATED_TOPICS_HEADER)
    if not section:
        return result

    result["top"] = _extract_query_rows(section, "top")
    result["rising"] = _extract_query_rows(section, "rising")
    return result


def _extract_interest_by_region(markdown: str) -> list[dict[str, Any]]:
    section = _find_section(markdown, _REGION_HEADER)
    if not section:
        return []

    regions: list[dict[str, Any]] = []
    for match in _TABLE_QUERY_ROW.finditer(section):
        regions.append(
            {
                "region": match.group("keyword").strip(),
                "value": _parse_trend_value(match.group("value").strip()),
            }
        )
    return regions


def _find_section(markdown: str, header_pattern: re.Pattern) -> str | None:
    """Find a section of markdown starting with the given header."""
    lines = markdown.splitlines()
    for idx, line in enumerate(lines):
        if header_pattern.search(line):
            # Collect lines until next heading or empty section
            section_lines = []
            for next_line in lines[idx + 1 :]:
                if re.match(r"^#{1,4}\s", next_line) and not header_pattern.search(next_line):
                    break
                section_lines.append(next_line)
            return "\n".join(section_lines)
    return None


def _extract_query_rows(section: str, category: str) -> list[dict[str, Any]]:
    """Extract query rows from a section, grouped by top/rising."""
    # Find the category sub-header
    pattern = _RISING_HEADER if category == "rising" else _TOP_HEADER
    sub_idx = None
    lines = section.splitlines()
    for idx, line in enumerate(lines):
        if pattern.search(line):
            sub_idx = idx
            break

    if sub_idx is None and category == "top":
        sub_idx = 0  # top is usually first
    if sub_idx is None:
        return []

    # Collect lines for this category until next category header
    category_lines: list[str] = []
    other_pattern = _RISING_HEADER if category == "top" else _TOP_HEADER
    for line in lines[sub_idx + 1 :]:
        if other_pattern.search(line):
            break
        category_lines.append(line)

    category_text = "\n".join(category_lines)
    rows: list[dict[str, Any]] = []
    for match in _TABLE_QUERY_ROW.finditer(category_text):
        rows.append(
            {
                "query": match.group("keyword").strip(),
                "value": _parse_trend_value(match.group("value").strip()),
                "rank": int(match.group("rank")),
            }
        )

    # Fallback: list items
    if not rows:
        for match in _LIST_ITEM.finditer(category_text):
            parts = re.split(r"\s{2,}|\t|\s*[-–]\s*", match.group(1), maxsplit=1)
            keyword = parts[0].strip()
            value = _parse_trend_value(parts[1].strip()) if len(parts) > 1 else 0
            if keyword:
                rows.append({"query": keyword, "value": value})

    return rows


def _parse_trend_value(raw: str) -> int | str:
    """Parse a trend value — numeric relative popularity or 'Breakout'."""
    trimmed = raw.strip()
    if trimmed.lower() in ("breakout", "飙升", "爆発", "急上昇", "급상승", "aumento"):
        return "breakout"
    try:
        return int(re.sub(r"[^\d]", "", trimmed) or "0")
    except ValueError:
        return 0


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
