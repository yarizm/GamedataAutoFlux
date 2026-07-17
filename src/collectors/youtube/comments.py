"""YouTube 视频数据与评论采集器。"""

from __future__ import annotations

import json
import re
from typing import Any

import httpx
from loguru import logger

from src.collectors.base import CollectResult, CollectTarget
from src.collectors.youtube import api
from src.collectors.youtube.base import BaseYouTubeCollector
from src.collectors.youtube.client_pool import YouTubeQuotaExhausted
from src.core.collector_resume import (
    build_collector_cursor,
    cap_partial_list,
    parse_recovery_cursor,
)
from src.core.config import get_settings
from src.core.errors import ErrorCode, classify_exception
from src.core.registry import registry

# Stage machine (S1 deep resume)
_STAGE_VIDEO_META = "video_meta"
_STAGE_CHANNEL_META = "channel_meta"
_STAGE_COMMENTS_SCAN = "comments_scan"
_STAGE_DONE = "done"


def _is_comments_disabled(exc: BaseException) -> bool:
    """True when YouTube reports commentsDisabled (soft-skip OK)."""
    if isinstance(exc, httpx.HTTPStatusError) and exc.response is not None:
        try:
            body = exc.response.json()
            for err in body.get("error", {}).get("errors", []) or []:
                if str(err.get("reason") or "") == "commentsDisabled":
                    return True
        except Exception:
            pass
    msg = str(exc).lower()
    return "commentsdisabled" in msg or "comments disabled" in msg


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
        top_comment_limit = int(
            target.params.get(
                "comment_top_limit",
                yt_cfg.get("top_comment_limit", 100),
            )
        )
        scan_limit = int(
            target.params.get(
                "max_scan_comments",
                yt_cfg.get("scan_comment_limit", 500),
            )
        )
        get_comments_flag = str(target.params.get("get_comments", "否")).strip() == "是"
        check_type_flag = str(target.params.get("check_video_type", "否")).strip() == "是"
        live_policy = str(target.params.get("live_stream_policy", "不处理")).strip()
        fetch_shorts_flag = str(target.params.get("fetch_shorts_related", "否")).strip() == "是"

        target_key = f"video:{video_id}"
        recovery_cursor = parse_recovery_cursor(
            self.config.get("recovery_checkpoint") if isinstance(self.config, dict) else None,
            collector_id="youtube_comments",
            target_key=target_key,
        )
        resume_payload: dict[str, Any] = {}
        if isinstance(recovery_cursor, dict):
            raw_payload = recovery_cursor.get("payload")
            if isinstance(raw_payload, dict):
                resume_payload = dict(raw_payload)

        completed_stages = [
            str(s).strip()
            for s in (resume_payload.get("completed_stages") or [])
            if str(s or "").strip()
        ]
        try:
            resume_scanned = int(resume_payload.get("scanned_count") or 0)
        except (TypeError, ValueError):
            resume_scanned = 0
        resume_scanned = max(0, resume_scanned)
        resume_page_token = str(resume_payload.get("page_token") or "").strip()
        raw_partial = resume_payload.get("partial_comments")
        partial_source = list(raw_partial) if isinstance(raw_partial, list) else []
        partial_comments, partial_was_truncated = cap_partial_list(partial_source)
        comments_done = _STAGE_COMMENTS_SCAN in completed_stages

        last_cursor: dict[str, Any] | None = None

        def _base_payload(
            *,
            stage_list: list[str],
            page_token: str = "",
            scanned_count: int = 0,
            partial: list[Any] | None = None,
            extra: dict[str, Any] | None = None,
        ) -> dict[str, Any]:
            capped, truncated = cap_partial_list(list(partial or []))
            body: dict[str, Any] = {
                "video_id": str(video_id),
                "completed_stages": list(stage_list),
                "page_token": str(page_token or ""),
                "scanned_count": int(scanned_count),
                "max_scan": int(scan_limit),
                "partial_comments": capped,
            }
            if truncated:
                body["partial_comments_truncated"] = True
            if extra:
                body.update(extra)
            return body

        async def _emit(
            stage: str,
            payload: dict[str, Any],
            *,
            state: dict[str, Any] | None = None,
            stats: dict[str, Any] | None = None,
        ) -> dict[str, Any]:
            nonlocal last_cursor
            cursor = build_collector_cursor(
                collector_id="youtube_comments",
                target_key=target_key,
                stage=stage,
                payload=payload,
            )
            last_cursor = cursor
            emit_fn = self.config.get("_emit_checkpoint") if isinstance(self.config, dict) else None
            if callable(emit_fn):
                try:
                    await emit_fn(cursor, state=state, stats=stats)
                except Exception as emit_err:
                    logger.warning(
                        "[YouTubeComments] checkpoint emit failed: {}",
                        emit_err,
                    )
            return cursor

        async def _emit_failure_cursor() -> None:
            if last_cursor is not None:
                await _emit(
                    str(last_cursor.get("stage") or _STAGE_COMMENTS_SCAN),
                    dict(last_cursor.get("payload") or {}),
                )
                return
            await _emit(
                _STAGE_COMMENTS_SCAN if get_comments_flag else _STAGE_VIDEO_META,
                _base_payload(
                    stage_list=completed_stages,
                    page_token=resume_page_token,
                    scanned_count=resume_scanned,
                    partial=partial_comments,
                ),
            )

        if recovery_cursor:
            logger.info(
                "[YouTubeComments] resume cursor stage={} completed={} page_token={} scanned={}",
                recovery_cursor.get("stage"),
                completed_stages,
                resume_page_token or "(start)",
                resume_scanned,
            )

        # ── 1. 获取视频详情 ──
        parts = "snippet,statistics,contentDetails"
        if live_policy != "不处理":
            parts += ",liveStreamingDetails"

        try:
            video_data = await api.videos_list(self._pool, [video_id], parts=parts)
        except YouTubeQuotaExhausted:
            await _emit_failure_cursor()
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

        if _STAGE_VIDEO_META not in completed_stages:
            completed_stages.append(_STAGE_VIDEO_META)
        await _emit(
            _STAGE_VIDEO_META,
            _base_payload(
                stage_list=completed_stages,
                page_token=resume_page_token,
                scanned_count=resume_scanned,
                partial=partial_comments,
            ),
        )

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
                if _STAGE_DONE not in completed_stages:
                    completed_stages.append(_STAGE_DONE)
                await _emit(
                    _STAGE_DONE,
                    _base_payload(stage_list=completed_stages),
                )
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
                    metadata={
                        "collector": "youtube_comments",
                        "target_key": target_key,
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
                        ch_items[0].get("statistics", {}).get("subscriberCount", "")
                    )
                if _STAGE_CHANNEL_META not in completed_stages:
                    completed_stages.append(_STAGE_CHANNEL_META)
            except YouTubeQuotaExhausted:
                await _emit_failure_cursor()
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
            if comments_done:
                # Stage finished earlier — reuse partial (may be empty on count-only).
                seed_reuse = [c for c in partial_comments if isinstance(c, dict)]
                seed_reuse.sort(key=lambda c: int(c.get("like_count", 0) or 0), reverse=True)
                comments = seed_reuse[:top_comment_limit]
            elif comment_count in ("0", ""):
                comments = []
                if _STAGE_COMMENTS_SCAN not in completed_stages:
                    completed_stages.append(_STAGE_COMMENTS_SCAN)
                await _emit(
                    _STAGE_COMMENTS_SCAN,
                    _base_payload(
                        stage_list=completed_stages,
                        page_token="",
                        scanned_count=0,
                        partial=[],
                    ),
                    stats={"scanned_count": 0},
                )
            else:
                # Resume kwargs: never seed_comments=[] — only non-empty seed or already_scanned.
                start_token: str | None = resume_page_token or None
                seed_comments: list[dict] | None = None
                already_scanned = 0
                seed_candidate = [c for c in partial_comments if isinstance(c, dict)]
                partial_complete = (
                    bool(seed_candidate)
                    and resume_scanned <= len(seed_candidate)
                    and not (partial_was_truncated and resume_scanned > len(seed_candidate))
                )
                if partial_complete:
                    seed_comments = seed_candidate
                    already_scanned = 0
                else:
                    seed_comments = None
                    already_scanned = resume_scanned

                using_seed = seed_comments is not None
                base_already = already_scanned

                async def on_page(
                    *,
                    page_token: str | None,
                    comments: list[Any],
                ) -> None:
                    if using_seed:
                        count = len(comments)
                        partial_for_emit: list[Any] = [c for c in comments if isinstance(c, dict)]
                    else:
                        count = base_already + len(comments)
                        # Count-only: empty partial so resume never mis-seeds.
                        partial_for_emit = []
                    stages = list(completed_stages)
                    await _emit(
                        _STAGE_COMMENTS_SCAN,
                        _base_payload(
                            stage_list=stages,
                            page_token=str(page_token or ""),
                            scanned_count=count,
                            partial=partial_for_emit,
                        ),
                        stats={"scanned_count": count},
                    )

                try:
                    comments = await api.fetch_paginated_comments(
                        self._pool,
                        video_id,
                        max_scan=scan_limit,
                        top_limit=top_comment_limit,
                        start_page_token=start_token,
                        seed_comments=seed_comments,
                        already_scanned=already_scanned,
                        on_page=on_page,
                    )
                    if _STAGE_COMMENTS_SCAN not in completed_stages:
                        completed_stages.append(_STAGE_COMMENTS_SCAN)
                    # Seed path keeps full-ish list; count-only stores empty partial.
                    if using_seed:
                        final_partial: list[Any] = [c for c in comments if isinstance(c, dict)]
                        final_scanned = len(seed_comments or []) + max(
                            0, len(comments) - len(seed_comments or [])
                        )
                        # comments is already top-sorted/capped; prefer on_page last count.
                        last_payload = (
                            last_cursor.get("payload") if isinstance(last_cursor, dict) else None
                        )
                        if isinstance(last_payload, dict):
                            try:
                                final_scanned = int(
                                    last_payload.get("scanned_count") or final_scanned
                                )
                            except (TypeError, ValueError):
                                pass
                            if isinstance(last_payload.get("partial_comments"), list):
                                final_partial = list(last_payload.get("partial_comments") or [])
                        else:
                            final_scanned = max(resume_scanned, len(final_partial))
                    else:
                        final_partial = []
                        last_payload = (
                            last_cursor.get("payload") if isinstance(last_cursor, dict) else None
                        )
                        if isinstance(last_payload, dict):
                            try:
                                final_scanned = int(
                                    last_payload.get("scanned_count")
                                    or (base_already + len(comments))
                                )
                            except (TypeError, ValueError):
                                final_scanned = base_already + len(comments)
                        else:
                            final_scanned = base_already + len(comments)
                    await _emit(
                        _STAGE_COMMENTS_SCAN,
                        _base_payload(
                            stage_list=completed_stages,
                            page_token="",
                            scanned_count=final_scanned,
                            partial=final_partial,
                        ),
                        stats={"scanned_count": final_scanned},
                    )
                except YouTubeQuotaExhausted:
                    await _emit_failure_cursor()
                    return CollectResult(
                        target=target,
                        success=False,
                        error="YouTube API 配额耗尽",
                        error_code=ErrorCode.rate_limited.value,
                        metadata={
                            "collector": "youtube_comments",
                            "target_key": target_key,
                        },
                    )
                except Exception as exc:
                    if _is_comments_disabled(exc):
                        logger.info(
                            "[YouTubeComments] comments disabled for video={}",
                            video_id,
                        )
                        comments = []
                        if _STAGE_COMMENTS_SCAN not in completed_stages:
                            completed_stages.append(_STAGE_COMMENTS_SCAN)
                        await _emit(
                            _STAGE_COMMENTS_SCAN,
                            _base_payload(
                                stage_list=completed_stages,
                                page_token="",
                                scanned_count=0,
                                partial=[],
                                extra={"comments_disabled": True},
                            ),
                        )
                    else:
                        # Retryable / other errors: fail target (no bare pass).
                        await _emit_failure_cursor()
                        code = classify_exception(exc)
                        return CollectResult(
                            target=target,
                            success=False,
                            error=f"评论采集失败: {exc}",
                            error_code=code.value,
                            metadata={
                                "collector": "youtube_comments",
                                "target_key": target_key,
                            },
                        )

        # ── 6. Shorts 关联视频 ──
        related_title = ""
        related_url = ""
        if fetch_shorts_flag and video_type == api.SHORTS:
            try:
                short_url = f"https://www.youtube.com/shorts/{video_id}"
                async with httpx.AsyncClient() as client:
                    resp = await client.get(
                        short_url,
                        headers={
                            "User-Agent": (
                                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                                "AppleWebKit/537.36 (KHTML, like Gecko) "
                                "Chrome/126.0.0.0 Safari/537.36"
                            ),
                            "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
                        },
                        timeout=10.0,
                    )

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
        if _STAGE_DONE not in completed_stages:
            completed_stages.append(_STAGE_DONE)
        await _emit(
            _STAGE_DONE,
            _base_payload(
                stage_list=completed_stages,
                page_token="",
                scanned_count=resume_scanned
                if not get_comments_flag
                else (
                    int(
                        (last_cursor or {}).get("payload", {}).get("scanned_count")
                        or resume_scanned
                    )
                    if isinstance(last_cursor, dict)
                    else resume_scanned
                ),
                partial=[],
            ),
        )

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

        resume_meta: dict[str, Any] = {}
        if recovery_cursor:
            resume_meta["resume"] = {
                "resumed": True,
                "target_key": target_key,
                "stage": recovery_cursor.get("stage"),
            }

        return CollectResult(
            target=target,
            success=True,
            data=result_data,
            metadata={
                "collector": "youtube_comments",
                "target_key": target_key,
                **resume_meta,
            },
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
