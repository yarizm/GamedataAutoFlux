from __future__ import annotations

import asyncio
import html
import re
from datetime import datetime, time, timezone
from html.parser import HTMLParser
from typing import Any
from urllib.parse import parse_qsl, urlencode, urljoin, urlparse, urlunparse

import httpx
from loguru import logger

from src.collectors.base import BaseCollector, CollectResult, CollectTarget
from src.core.config import get as get_config
from src.core.registry import registry


STEAM_COMMUNITY_BASE = "https://steamcommunity.com"
TOPIC_URL_RE = re.compile(
    r"https?://steamcommunity\.com/app/(?P<app_id>\d+)/discussions/(?P<forum_id>\d+)/(?P<topic_id>\d+)/?",
    re.IGNORECASE,
)
VOID_TAGS = {"area", "base", "br", "col", "embed", "hr", "img", "input", "link", "meta", "source", "track", "wbr"}


@registry.register("collector", "steam_discussions")
class SteamDiscussionsCollector(BaseCollector):
    """Collect public Steam Community discussions for a game forum."""

    def __init__(self, config: dict[str, Any] | None = None):
        super().__init__(config)
        self._client: httpx.AsyncClient | None = None

    async def setup(self, config: dict[str, Any] | None = None) -> None:
        await super().setup(config)
        timeout = float(self.config.get("timeout", get_config("steam_discussions.timeout", 30)))
        user_agent = self.config.get("user_agent") or get_config(
            "collector.user_agent",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        )
        self.config.setdefault("request_delay", float(get_config("steam_discussions.request_delay", 2)))
        self._client = httpx.AsyncClient(
            timeout=timeout,
            follow_redirects=True,
            headers={
                "User-Agent": user_agent,
                "Accept-Language": "en-US,en;q=0.9",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            },
        )

    async def teardown(self) -> None:
        if self._client is not None:
            await self._client.aclose()
            self._client = None
        await super().teardown()

    async def collect(self, target: CollectTarget) -> CollectResult:
        if self._client is None:
            raise RuntimeError("Steam discussions collector client is not initialized")

        app_id = _optional_int(target.params.get("app_id"))
        forum_url = _build_forum_url(target.params, app_id=app_id)
        if app_id is None and not forum_url:
            raise ValueError("steam_discussions target requires app_id or forum_url")

        start_at = _parse_datetime_param(
            target.params.get("start_time") or target.params.get("start_at"),
            end_of_day=False,
        )
        end_at = _parse_datetime_param(
            target.params.get("end_time") or target.params.get("end_at"),
            end_of_day=True,
        )
        max_pages = max(
            1,
            int(target.params.get("max_pages", self.config.get("max_pages", get_config("steam_discussions.max_pages", 50)))),
        )
        max_topics = max(
            1,
            int(target.params.get("max_topics", self.config.get("max_topics", get_config("steam_discussions.max_topics", 1000)))),
        )
        include_replies = bool(
            target.params.get(
                "include_replies",
                self.config.get("include_replies", get_config("steam_discussions.include_replies", True)),
            )
        )
        max_reply_pages = max(
            1,
            int(
                target.params.get(
                    "max_reply_pages",
                    self.config.get("max_reply_pages", get_config("steam_discussions.max_reply_pages", 5)),
                )
            ),
        )

        logger.info(
            f"[SteamDiscussions] Start collect: {target.name} app_id={app_id} "
            f"range={start_at}..{end_at} max_pages={max_pages}"
        )

        topics: list[dict[str, Any]] = []
        seen_urls: set[str] = set()
        warnings: list[str] = []

        for forum_page in range(1, max_pages + 1):
            listing_url = _with_query_param(forum_url, "fp", forum_page)
            listing_html = await self._fetch_text(listing_url)
            candidates = _parse_topic_links(listing_html, base_url=listing_url)
            if not candidates:
                break

            page_times: list[datetime] = []
            for candidate in candidates:
                topic_url = candidate["url"]
                if topic_url in seen_urls:
                    continue
                seen_urls.add(topic_url)

                try:
                    detail = await self._collect_topic_detail(
                        topic_url=topic_url,
                        summary=candidate,
                        include_replies=include_replies,
                        max_reply_pages=max_reply_pages,
                    )
                except Exception as exc:  # noqa: BLE001
                    warnings.append(f"{topic_url}: {exc}")
                    logger.warning(f"[SteamDiscussions] Topic fetch failed: {topic_url} - {exc}")
                    continue

                topic_time = _topic_filter_time(detail)
                if topic_time is not None:
                    page_times.append(topic_time)
                if _in_range(topic_time, start_at=start_at, end_at=end_at):
                    topics.append(detail)
                    if len(topics) >= max_topics:
                        break

                await asyncio.sleep(float(self.config.get("request_delay", 2)))

            if len(topics) >= max_topics:
                break

            if start_at and page_times and max(page_times) < start_at:
                break

            await asyncio.sleep(float(self.config.get("request_delay", 2)))

        if not topics:
            return CollectResult(
                target=target,
                success=False,
                error="Steam discussions collector did not find any topics in the requested range",
                metadata={"collector": "steam_discussions", "warnings": warnings},
                raw_data={"forum_url": forum_url},
            )

        data = {
            "collector": "steam_discussions",
            "game_name": target.name,
            "app_id": app_id,
            "source_meta": {
                "collector": "steam_discussions",
                "forum_url": forum_url,
                "collected_at": datetime.now(timezone.utc).isoformat(),
                "start_time": start_at.isoformat() if start_at else None,
                "end_time": end_at.isoformat() if end_at else None,
                "include_replies": include_replies,
            },
            "discussions": {
                "topics": topics,
                "topic_count": len(topics),
                "post_count": sum(len(topic.get("posts", [])) for topic in topics),
            },
            "snapshot": _build_snapshot(target.name, app_id, topics),
        }

        metadata: dict[str, Any] = {
            "collector": "steam_discussions",
            "data_sources": ["steamcommunity_discussions"],
        }
        if warnings:
            metadata["warnings"] = warnings

        return CollectResult(target=target, success=True, data=data, metadata=metadata)

    async def _collect_topic_detail(
        self,
        *,
        topic_url: str,
        summary: dict[str, Any],
        include_replies: bool,
        max_reply_pages: int,
    ) -> dict[str, Any]:
        topic_html = await self._fetch_text(topic_url)
        detail = _parse_topic_detail(topic_html, topic_url=topic_url, summary=summary)
        if not include_replies:
            detail["posts"] = detail.get("posts", [])[:1]
            return detail

        seen_posts = {
            (post.get("author", ""), post.get("published_at", ""), post.get("content", ""))
            for post in detail.get("posts", [])
        }
        for reply_page in range(2, max_reply_pages + 1):
            page_url = _with_query_param(topic_url, "ctp", reply_page)
            page_html = await self._fetch_text(page_url)
            page_detail = _parse_topic_detail(page_html, topic_url=page_url, summary=summary)
            new_posts = []
            for post in page_detail.get("posts", []):
                identity = (post.get("author", ""), post.get("published_at", ""), post.get("content", ""))
                if identity in seen_posts:
                    continue
                seen_posts.add(identity)
                new_posts.append(post)
            if not new_posts:
                break
            detail.setdefault("posts", []).extend(new_posts)
            await asyncio.sleep(float(self.config.get("request_delay", 2)))
        detail["post_count"] = len(detail.get("posts", []))
        return detail

    async def _fetch_text(self, url: str) -> str:
        response = await self._client.get(url)
        response.raise_for_status()
        return response.text

    def validate_config(self, config: dict[str, Any] | None = None) -> bool:
        return True


class _DiscussionCaptureParser(HTMLParser):
    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.captures: list[dict[str, Any]] = []
        self._capture: dict[str, Any] | None = None
        self._depth = 0

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        attr_map = {key: value or "" for key, value in attrs}
        class_name = attr_map.get("class", "")
        kind = _capture_kind(class_name)
        if self._capture is not None:
            if tag.lower() == "br":
                self._capture["text"].append("\n")
            elif tag.lower() not in VOID_TAGS:
                self._depth += 1
            return
        if kind:
            self._capture = {
                "kind": kind,
                "text": [],
                "timestamp": attr_map.get("data-timestamp"),
            }
            self._depth = 1

    def handle_startendtag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if self._capture is not None and tag.lower() == "br":
            self._capture["text"].append("\n")

    def handle_endtag(self, tag: str) -> None:
        if self._capture is None:
            return
        self._depth -= 1
        if self._depth <= 0:
            text = _normalize_text(" ".join(self._capture["text"]))
            if text or self._capture.get("timestamp"):
                self.captures.append(
                    {
                        "kind": self._capture["kind"],
                        "text": text,
                        "timestamp": self._capture.get("timestamp"),
                    }
                )
            self._capture = None
            self._depth = 0

    def handle_data(self, data: str) -> None:
        if self._capture is not None:
            self._capture["text"].append(data)


def _capture_kind(class_name: str) -> str | None:
    classes = set(class_name.split())
    if "actual_persona_name" in classes or "commentthread_author_link" in classes:
        return "author"
    if "commentthread_comment_timestamp" in classes:
        return "timestamp"
    if "commentthread_comment_text" in classes or "forum_op" in classes:
        return "text"
    return None


def _build_forum_url(params: dict[str, Any], *, app_id: int | None) -> str:
    forum_url = str(params.get("forum_url") or "").strip()
    if forum_url:
        return forum_url
    if app_id is None:
        return ""
    forum_id = str(params.get("forum_id") or "").strip()
    if forum_id:
        return f"{STEAM_COMMUNITY_BASE}/app/{app_id}/discussions/{forum_id}/"
    return f"{STEAM_COMMUNITY_BASE}/app/{app_id}/discussions/"


def _parse_topic_links(html_text: str, *, base_url: str) -> list[dict[str, Any]]:
    candidates: list[dict[str, Any]] = []
    seen: set[str] = set()
    anchor_pattern = re.compile(
        r"<a\b(?P<attrs>[^>]*href=[\"'][^\"']+[\"'][^>]*)>(?P<label>.*?)</a>",
        re.IGNORECASE | re.DOTALL,
    )
    for match in anchor_pattern.finditer(html_text):
        attrs = match.group("attrs")
        href_match = re.search(r"href=[\"'](?P<href>[^\"']+)[\"']", attrs, re.IGNORECASE)
        if not href_match:
            continue
        url = urljoin(base_url, html.unescape(href_match.group("href")).strip())
        topic_match = TOPIC_URL_RE.match(url.rstrip("/") + "/")
        if not topic_match:
            continue
        canonical_url = urlunparse(urlparse(url)._replace(query="", fragment="")).rstrip("/") + "/"
        if canonical_url in seen:
            continue
        seen.add(canonical_url)
        candidates.append(
            {
                "url": canonical_url,
                "title": _normalize_text(_strip_tags(match.group("label"))),
                "app_id": int(topic_match.group("app_id")),
                "forum_id": topic_match.group("forum_id"),
                "topic_id": topic_match.group("topic_id"),
            }
        )
    return candidates


def _parse_topic_detail(
    html_text: str,
    *,
    topic_url: str,
    summary: dict[str, Any] | None = None,
) -> dict[str, Any]:
    summary = summary or {}
    parser = _DiscussionCaptureParser()
    parser.feed(html_text)

    posts = _captures_to_posts(parser.captures)
    title = (
        _extract_first_text(html_text, r"<div[^>]+class=[\"'][^\"']*topic[^\"']*[\"'][^>]*>(?P<value>.*?)</div>")
        or summary.get("title")
        or _extract_title(html_text)
        or topic_url.rstrip("/").split("/")[-1]
    )
    title = _normalize_text(title)

    first_post_time = _first_post_time(posts)
    latest_post_time = _latest_post_time(posts) or first_post_time
    app_id = summary.get("app_id")
    topic_match = TOPIC_URL_RE.match(topic_url.rstrip("/") + "/")
    if topic_match:
        app_id = int(topic_match.group("app_id"))

    return {
        "title": title,
        "url": topic_url,
        "app_id": app_id,
        "forum_id": summary.get("forum_id") or (topic_match.group("forum_id") if topic_match else None),
        "topic_id": summary.get("topic_id") or (topic_match.group("topic_id") if topic_match else None),
        "created_at": first_post_time.isoformat() if first_post_time else None,
        "updated_at": latest_post_time.isoformat() if latest_post_time else None,
        "post_count": len(posts),
        "posts": posts,
    }


def _captures_to_posts(captures: list[dict[str, Any]]) -> list[dict[str, Any]]:
    posts: list[dict[str, Any]] = []
    pending: dict[str, Any] = {}
    for capture in captures:
        kind = capture["kind"]
        if kind == "author":
            pending["author"] = capture.get("text")
        elif kind == "timestamp":
            parsed = _parse_steam_timestamp(capture.get("timestamp") or capture.get("text"))
            pending["published_at"] = parsed.isoformat() if parsed else capture.get("text")
            pending["published_at_raw"] = capture.get("text")
        elif kind == "text":
            content = _normalize_text(capture.get("text", ""))
            if not content:
                continue
            published_at = pending.get("published_at")
            posts.append(
                {
                    "author": pending.get("author"),
                    "published_at": published_at,
                    "published_at_raw": pending.get("published_at_raw"),
                    "content": content,
                }
            )
            pending = {}
    return posts


def _parse_steam_timestamp(value: Any) -> datetime | None:
    if value in (None, ""):
        return None
    text = str(value).strip()
    if text.isdigit():
        return datetime.fromtimestamp(int(text), tz=timezone.utc)

    cleaned = re.sub(r"\s+", " ", text.replace(",", " ")).strip()
    formats = [
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%b %d %Y @ %I:%M%p",
        "%d %b %Y @ %I:%M%p",
    ]
    for fmt in formats:
        try:
            parsed = datetime.strptime(cleaned, fmt)
        except ValueError:
            continue
        return parsed.replace(tzinfo=timezone.utc)

    current_year = datetime.now().year
    for fmt in ("%b %d @ %I:%M%p", "%d %b @ %I:%M%p"):
        try:
            parsed = datetime.strptime(f"{cleaned} {current_year}", f"{fmt} %Y")
        except ValueError:
            continue
        return parsed.replace(tzinfo=timezone.utc)

    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def _parse_datetime_param(value: Any, *, end_of_day: bool = False) -> datetime | None:
    if value in (None, ""):
        return None
    text = str(value).strip()
    try:
        if re.fullmatch(r"\d{4}-\d{2}-\d{2}", text):
            boundary = time.max if end_of_day else time.min
            return datetime.combine(datetime.fromisoformat(text).date(), boundary, tzinfo=timezone.utc)
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(f"Invalid datetime value: {value}") from exc
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def _in_range(value: datetime | None, *, start_at: datetime | None, end_at: datetime | None) -> bool:
    if value is None:
        return True
    if start_at and value < start_at:
        return False
    if end_at and value > end_at:
        return False
    return True


def _topic_filter_time(topic: dict[str, Any]) -> datetime | None:
    return _parse_steam_timestamp(topic.get("updated_at") or topic.get("created_at"))


def _first_post_time(posts: list[dict[str, Any]]) -> datetime | None:
    for post in posts:
        parsed = _parse_steam_timestamp(post.get("published_at"))
        if parsed:
            return parsed
    return None


def _latest_post_time(posts: list[dict[str, Any]]) -> datetime | None:
    values = [
        parsed
        for post in posts
        if (parsed := _parse_steam_timestamp(post.get("published_at"))) is not None
    ]
    return max(values) if values else None


def _build_snapshot(target_name: str, app_id: int | None, topics: list[dict[str, Any]]) -> dict[str, Any]:
    times = [
        parsed
        for topic in topics
        if (parsed := _parse_steam_timestamp(topic.get("updated_at"))) is not None
    ]
    latest = max(times) if times else None
    return {
        "name": target_name,
        "app_id": app_id,
        "topic_count": len(topics),
        "post_count": sum(len(topic.get("posts", [])) for topic in topics),
        "latest_topic_at": latest.isoformat() if latest else None,
    }


def _with_query_param(url: str, key: str, value: int | str) -> str:
    parsed = urlparse(url)
    pairs = [(k, v) for k, v in parse_qsl(parsed.query, keep_blank_values=True) if k != key]
    pairs.append((key, str(value)))
    return urlunparse(parsed._replace(query=urlencode(pairs)))


def _extract_title(html_text: str) -> str | None:
    title = _extract_first_text(html_text, r"<title[^>]*>(?P<value>.*?)</title>")
    if not title:
        return None
    return title.split("::")[0].strip()


def _extract_first_text(html_text: str, pattern: str) -> str | None:
    match = re.search(pattern, html_text, re.IGNORECASE | re.DOTALL)
    if not match:
        return None
    return _normalize_text(_strip_tags(match.group("value")))


def _strip_tags(value: str) -> str:
    cleaned = re.sub(r"(?is)<(script|style).*?>.*?</\1>", " ", value or "")
    cleaned = re.sub(r"(?i)<br\s*/?>", "\n", cleaned)
    cleaned = re.sub(r"(?is)<[^>]+>", " ", cleaned)
    return html.unescape(cleaned)


def _normalize_text(value: str) -> str:
    return re.sub(r"\s+", " ", html.unescape(value or "")).strip()


def _optional_int(value: Any) -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(str(value).strip())
    except ValueError:
        return None
