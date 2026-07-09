"""YouTube 视频数据与评论采集器。"""

from __future__ import annotations

import json
import re
from typing import Any

import httpx

from src.collectors.base import CollectResult, CollectTarget
from src.collectors.youtube import api
from src.collectors.youtube.base import BaseYouTubeCollector
from src.collectors.youtube.client_pool import YouTubeQuotaExhausted
from src.core.config import get_settings
from src.core.errors import ErrorCode
from src.core.registry import registry


@registry.register("collector", "youtube_comments")
class YouTubeCommentCollector(BaseYouTubeCollector):
    """采集 YouTube 视频详情指标及热门评论。"""

    async def collect(self, target: CollectTarget) -> CollectResult:
        video_url = str(target.params.get("video_url", "")).strip()
        video_id = api.extract_video_id(video_url)
        if not video_id:
            return CollectResult(
                target=target,
                success=False,
                error=f"无法从 URL 解析 video ID: {video_url}",
                error_code=ErrorCode.invalid_params.value,
            )

        settings = get_settings()
        yt_cfg = settings.get("youtube", {})
        top_comment_limit = int(target.params.get(
            "comment_top_limit",
            yt_cfg.get("top_comment_limit", 100),
        ))
        scan_limit = int(target.params.get(
            "max_scan_comments",
            yt_cfg.get("scan_comment_limit", 500),
        ))
        get_comments_flag = str(target.params.get("get_comments", "否")).strip() == "是"
        check_type_flag = str(target.params.get("check_video_type", "否")).strip() == "是"
        live_policy = str(target.params.get("live_stream_policy", "不处理")).strip()
        fetch_shorts_flag = str(target.params.get("fetch_shorts_related", "否")).strip() == "是"

        # ── 1. 获取视频详情 ──
        parts = "snippet,statistics,contentDetails"
        if live_policy != "不处理":
            parts += ",liveStreamingDetails"

        try:
            video_data = await api.videos_list(self._pool, [video_id], parts=parts)
        except YouTubeQuotaExhausted:
            raise

        items = video_data.get("items", [])
        if not items:
            return CollectResult(
                target=target,
                success=False,
                error=f"视频未找到或不可用: {video_id}",
                error_code=ErrorCode.empty_data.value,
            )

        item = items[0]
        snippet = item.get("snippet", {})
        stats = item.get("statistics", {})
        content = item.get("contentDetails", {})

        title = snippet.get("title", "")
        channel_id = snippet.get("channelId", "")
        channel_name = snippet.get("channelTitle", "")
        published_at = api.format_datetime(snippet.get("publishedAt", ""))
        duration = api.format_duration(content.get("duration", ""))
        description = (snippet.get("description") or "").replace("\n", " | ").replace("\r", "")
        if len(description) > 300:
            description = description[:300] + "..."

        view_count = str(stats.get("viewCount", ""))
        like_count = str(stats.get("likeCount", ""))
        comment_count = str(stats.get("commentCount", ""))

        # ── 2. 直播状态检测 ──
        live_status = ""
        if live_policy != "不处理":
            broadcast = snippet.get("liveBroadcastContent", "none").lower()
            has_live_details = "liveStreamingDetails" in item
            if broadcast == "live":
                live_status = "正在直播"
            elif broadcast == "upcoming":
                live_status = "预告直播"
            elif has_live_details:
                live_status = "直播回放"
            else:
                live_status = "非直播"

            if live_policy == "直接排除" and live_status != "非直播":
                return CollectResult(
                    target=target,
                    success=True,
                    data={
                        "collector": "youtube_comments",
                        "source": "youtube_api",
                        "video_id": video_id,
                        "video_url": api.build_video_url(video_id),
                        "title": title,
                        "live_status": live_status,
                        "skipped": True,
                        "skip_reason": "直播内容已排除",
                    },
                )

        # ── 3. 获取频道粉丝数 ──
        subscriber_count = ""
        if channel_id:
            try:
                ch_data = await api.channels_list(self._pool, id=channel_id)
                ch_items = ch_data.get("items", [])
                if ch_items:
                    subscriber_count = str(
                        ch_items[0].get("statistics", {}).get("subscriberCount", ""))
            except YouTubeQuotaExhausted:
                raise
            except Exception:
                pass

        # ── 4. 视频类型检测 ──
        video_type = "未知"
        if check_type_flag:
            video_type = await api.check_video_type(video_id)

        # ── 5. 评论采集 ──
        comments: list[dict] = []
        if get_comments_flag:
            try:
                if comment_count not in ("0", ""):
                    comments = await api.fetch_paginated_comments(
                        self._pool,
                        video_id,
                        max_scan=scan_limit,
                        top_limit=top_comment_limit,
                    )
            except YouTubeQuotaExhausted:
                raise
            except Exception:
                pass  # 评论获取失败不影响视频信息输出

        # ── 6. Shorts 关联视频 ──
        related_title = ""
        related_url = ""
        if fetch_shorts_flag and video_type == api.SHORTS:
            try:
                short_url = f"https://www.youtube.com/shorts/{video_id}"
                async with httpx.AsyncClient() as client:
                    resp = await client.get(short_url, headers={
                        "User-Agent": (
                            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                            "AppleWebKit/537.36 (KHTML, like Gecko) "
                            "Chrome/126.0.0.0 Safari/537.36"
                        ),
                        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
                    }, timeout=10.0)

                    match = re.search(
                        r"ytInitialData\s*=\s*(\{.*?\});\s*</script>",
                        resp.text,
                    )
                    if match:
                        data = json.loads(match.group(1))
                        related_title, related_url = _extract_related(data)
            except Exception:
                pass

        # ── 7. 构建结果 ──
        result_data: dict[str, Any] = {
            "collector": "youtube_comments",
            "source": "youtube_api",
            "video_id": video_id,
            "video_url": api.build_video_url(video_id, video_type),
            "title": title,
            "channel_name": channel_name,
            "channel_id": channel_id,
            "channel_url": api.build_channel_url(channel_id),
            "subscriber_count": subscriber_count,
            "published_at": published_at,
            "video_type": video_type,
            "live_status": live_status,
            "duration": duration,
            "description": description,
            "view_count": view_count,
            "like_count": like_count,
            "comment_count": comment_count,
        }
        if fetch_shorts_flag and related_title:
            result_data["related_video_title"] = related_title
            result_data["related_video_url"] = related_url
        if get_comments_flag:
            result_data["comments"] = comments

        return CollectResult(
            target=target,
            success=True,
            data=result_data,
            metadata={"collector": "youtube_comments"},
        )


def _extract_related(data: dict) -> tuple[str, str]:
    """从 ytInitialData 中提取 Shorts 关联长视频。"""
    _title: str = ""
    _url: str = ""

    def _find(d: Any) -> None:
        nonlocal _title, _url
        if _title and _url:
            return
        if isinstance(d, dict):
            if "reelMultiFormatLinkViewModel" in d:
                vm = d["reelMultiFormatLinkViewModel"]
                try:
                    t = vm.get("title", {}).get("content", "")
                    cmd = vm.get("command", {}).get("innertubeCommand", {})
                    ep = cmd.get("watchEndpoint", {})
                    vid = ep.get("videoId", "")
                    if t and vid:
                        _title, _url = t, f"https://www.youtube.com/watch?v={vid}"
                except Exception:
                    pass
            for v in d.values():
                _find(v)
        elif isinstance(d, list):
            for item in d:
                _find(item)

    _find(data)
    return _title, _url
