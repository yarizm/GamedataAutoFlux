"""YouTube Data API v3 共享函数 — REST 封装 + 工具函数。"""

from __future__ import annotations

import asyncio
import re
from typing import Any
from urllib.parse import parse_qs, urlparse

import httpx
from loguru import logger


# ── REST 端点 ──────────────────────────────────────────


async def videos_list(
    pool,
    ids: list[str],
    parts: str = "snippet,statistics,contentDetails",
) -> dict:
    """GET /youtube/v3/videos — 批量获取视频详情，单批最多 50 个。"""
    return await pool.request(
        "GET",
        "/videos",
        part=parts,
        id=",".join(ids),
        maxResults=min(50, len(ids)),
    )


async def channels_list(pool, **kwargs) -> dict:
    """GET /youtube/v3/channels — 获取频道信息。

    kwargs 传入 id / forHandle / forUsername 等 YouTube API 参数。
    """
    return await pool.request(
        "GET",
        "/channels",
        part="snippet,statistics",
        **kwargs,
    )


async def comment_threads_list(
    pool,
    video_id: str,
    page_token: str | None = None,
    max_results: int = 100,
) -> dict:
    """GET /youtube/v3/commentThreads — 评论分页列表。"""
    params: dict = {
        "part": "snippet",
        "videoId": video_id,
        "maxResults": min(max_results, 100),
        "order": "relevance",
        "textFormat": "plainText",
    }
    if page_token:
        params["pageToken"] = page_token
    return await pool.request("GET", "/commentThreads", **params)


async def fetch_paginated_comments(
    pool,
    video_id: str,
    max_scan: int = 500,
    top_limit: int = 100,
    *,
    start_page_token: str | None = None,
    seed_comments: list[dict] | None = None,
    already_scanned: int = 0,
    on_page: Any | None = None,
) -> list[dict]:
    """分页拉取主楼评论，按点赞降序取前 top_limit 条。

    Resume kwargs:
      - start_page_token: continue from this nextPageToken
      - seed_comments: pre-fetched comments (seed path); never pass [] for count-only
      - already_scanned: prior scan count when not seeding (count-only)
      - on_page(page_token=next, comments=comments): after each page
    """
    comments: list[dict] = list(seed_comments or [])
    page_token: str | None = start_page_token

    # With seed, max_scan is absolute (including seed). Without seed,
    # already_scanned counts toward the total so we only fetch remaining.
    if seed_comments is not None:
        stop_at = max_scan
    else:
        stop_at = max(0, max_scan - int(already_scanned or 0))

    if stop_at <= 0 or len(comments) >= stop_at:
        comments.sort(key=lambda c: c.get("like_count", 0), reverse=True)
        return comments[:top_limit]

    while len(comments) < stop_at:
        data = await comment_threads_list(pool, video_id, page_token=page_token, max_results=100)

        for item in data.get("items", []):
            top_comment = item.get("snippet", {}).get("topLevelComment", {})
            snippet = top_comment.get("snippet", {})
            text = (
                (snippet.get("textDisplay") or snippet.get("textOriginal") or "")
                .replace("\r", "")
                .replace("\n", " | ")
                .strip()
            )
            if not text:
                text = "[非文本]"

            published_at = format_datetime(snippet.get("publishedAt", ""))

            comments.append(
                {
                    "like_count": int(snippet.get("likeCount", 0) or 0),
                    "text": text,
                    "published_at": published_at,
                }
            )

            if len(comments) >= stop_at:
                break

        page_token = data.get("nextPageToken")
        if on_page is not None:
            await on_page(page_token=page_token, comments=comments)

        if not page_token:
            break

    comments.sort(key=lambda c: c.get("like_count", 0), reverse=True)
    return comments[:top_limit]


# ── 视频类型检测 ───────────────────────────────────────

SHORTS = "Shorts"
NORMAL_VIDEO = "普通视频"
UNKNOWN = "未知"
REDIRECT_STATUSES = (301, 302, 303, 307, 308)


async def _head_no_redirect(
    client: httpx.AsyncClient, url: str, headers: dict | None = None
) -> httpx.Response:
    """发送 HEAD 请求，禁用自动跟随重定向。"""
    return await client.head(url, follow_redirects=False, headers=headers)


async def check_video_type(video_id: str) -> str:
    """HEAD https://youtube.com/shorts/{id} → 200=Shorts, 30x=普通视频。"""
    if not video_id:
        return UNKNOWN

    url = f"https://www.youtube.com/shorts/{video_id}"
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
        )
    }
    async with httpx.AsyncClient() as client:
        for attempt in range(3):
            try:
                resp = await _head_no_redirect(client, url, headers)
                if resp.status_code == 200:
                    return SHORTS
                if resp.status_code in REDIRECT_STATUSES:
                    return NORMAL_VIDEO
                logger.debug(f"[check_video_type] {video_id}: status={resp.status_code}")
            except httpx.HTTPError:
                if attempt < 2:
                    await asyncio.sleep(0.5 * (2**attempt))
                    continue

    return UNKNOWN


async def check_video_type_bulk(
    video_ids: list[str],
    max_workers: int = 2,
) -> dict[str, str]:
    """并发检测视频类型。"""

    unique_ids = list(dict.fromkeys(v.strip() for v in video_ids if v.strip()))
    if not unique_ids:
        return {}

    semaphore = asyncio.Semaphore(max_workers)

    async def _detect_one(vid: str) -> tuple[str, str]:
        async with semaphore:
            return vid, await check_video_type(vid)

    tasks = [_detect_one(vid) for vid in unique_ids]
    results = await asyncio.gather(*tasks)
    return dict(results)


# ── 工具函数 ───────────────────────────────────────────


def format_duration(iso: str) -> str:
    """PT1H23M45S → 01:23:45。"""
    match = re.fullmatch(
        r"P(?:(?P<days>\d+)D)?"
        r"(?:T(?:(?P<hours>\d+)H)?(?:(?P<minutes>\d+)M)?(?:(?P<seconds>\d+)S)?)?",
        iso or "",
    )
    if not match:
        return ""
    days = int(match.group("days") or 0)
    hours = int(match.group("hours") or 0) + days * 24
    minutes = int(match.group("minutes") or 0)
    seconds = int(match.group("seconds") or 0)
    return f"{hours:02d}:{minutes:02d}:{seconds:02d}"


def chunked(values: list, size: int) -> list[list]:
    """将列表按 size 分块。"""
    return [values[i : i + size] for i in range(0, len(values), size)]


def extract_video_id(url: str) -> str:
    """从各种 YouTube URL 格式提取 11 位 video ID。"""
    value = (url or "").strip()
    if not value:
        return ""

    # youtu.be / shorts / embed / live
    parsed = urlparse(value if "://" in value else f"https://{value}")
    path_parts = [p for p in (parsed.path or "").split("/") if p]

    if "youtu.be" in (parsed.netloc or "").lower() and path_parts:
        return path_parts[0]

    query_id = parse_qs(parsed.query or "").get("v", [""])[0]
    if query_id:
        return query_id

    if len(path_parts) >= 2 and path_parts[0] in {"shorts", "embed", "live"}:
        return path_parts[1]

    # 正则兜底
    match = re.search(r"(?:v=|youtu\.be/|/shorts/|/embed/)([A-Za-z0-9_-]{11})", value)
    return match.group(1) if match else ""


def build_video_url(video_id: str, video_type: str = NORMAL_VIDEO) -> str:
    """videoId → 完整播放 URL。"""
    if not video_id:
        return ""
    if video_type == SHORTS:
        return f"https://www.youtube.com/shorts/{video_id}"
    return f"https://www.youtube.com/watch?v={video_id}"


def build_channel_url(channel_id: str) -> str:
    """channelId → 频道主页 URL。"""
    return f"https://www.youtube.com/channel/{channel_id}" if channel_id else ""


def parse_channel_hint(url: str) -> tuple[str, str]:
    """解析频道 URL 类型。返回 (hint_type, hint_value)。"""
    normalized = (url or "").strip()
    if not normalized:
        return "", ""

    parsed = urlparse(normalized if "://" in normalized else f"https://{normalized}")
    parts = [p for p in (parsed.path or "").split("/") if p]

    if not parts:
        return "", ""

    first = parts[0]
    if first == "channel" and len(parts) >= 2:
        return "id", parts[1]
    if first.startswith("@"):
        return "handle", first
    if first == "user" and len(parts) >= 2:
        return "username", parts[1]
    return "", ""


def format_datetime(date_str: str) -> str:
    """YouTube API datetime → YYYY-MM-DD HH:MM:SS。"""
    if not date_str:
        return ""
    cleaned = str(date_str).strip().replace("T", " ").replace("Z", "")
    if "." in cleaned:
        cleaned = cleaned.split(".")[0]
    return cleaned
