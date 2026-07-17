"""YouTube comments pageToken deep-cursor resume tests."""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock

import httpx
import pytest

from src.collectors.base import CollectTarget
from src.collectors.youtube.api import fetch_paginated_comments
from src.collectors.youtube.client_pool import YouTubeQuotaExhausted
from src.collectors.youtube.comments import YouTubeCommentCollector
from src.core.collector_metadata import get_collector_metadata
from src.core.collector_resume import build_collector_cursor
from src.core.errors import ErrorCode


class FakePool:
    def __init__(self, pages: dict[str, dict]):
        self.pages = pages  # token -> response ("" = first page)
        self.calls: list[str | None] = []

    async def request(self, method, path, **params):
        token = params.get("pageToken")
        self.calls.append(token)
        key = token or ""
        return self.pages.get(key, {"items": [], "nextPageToken": None})


def _comment_item(text: str, likes: int = 0) -> dict:
    return {
        "snippet": {
            "topLevelComment": {
                "snippet": {
                    "textDisplay": text,
                    "likeCount": likes,
                    "publishedAt": "2024-01-01T00:00:00Z",
                }
            }
        }
    }


def _video_payload(
    video_id: str = "vid123",
    *,
    comment_count: str = "50",
    channel_id: str = "UC123",
) -> dict:
    return {
        "items": [
            {
                "id": video_id,
                "snippet": {
                    "title": "Test Video",
                    "channelId": channel_id,
                    "channelTitle": "Test Channel",
                    "publishedAt": "2024-01-01T00:00:00Z",
                    "description": "desc",
                    "liveBroadcastContent": "none",
                },
                "statistics": {
                    "viewCount": "1000",
                    "likeCount": "10",
                    "commentCount": comment_count,
                },
                "contentDetails": {"duration": "PT5M"},
            }
        ]
    }


def _channel_payload(channel_id: str = "UC123") -> dict:
    return {"items": [{"id": channel_id, "statistics": {"subscriberCount": "999"}}]}


@pytest.mark.asyncio
async def test_fetch_paginated_resumes_from_page_token():
    pool = FakePool(
        {
            "": {"items": [_comment_item("a", 1)], "nextPageToken": "T2"},
            "T2": {"items": [_comment_item("b", 5)], "nextPageToken": "T3"},
            "T3": {"items": [_comment_item("c", 3)], "nextPageToken": None},
        }
    )
    result = await fetch_paginated_comments(
        pool, "vid", max_scan=500, top_limit=100, start_page_token="T2"
    )
    assert pool.calls[0] == "T2"
    texts = {c["text"] for c in result}
    assert "b" in texts
    assert "a" not in texts
    assert result[0]["text"] == "b"  # highest likes first


@pytest.mark.asyncio
async def test_fetch_paginated_seed_and_on_page():
    pool = FakePool(
        {
            "T2": {
                "items": [_comment_item("new", 9)],
                "nextPageToken": "T3",
            },
            "T3": {"items": [], "nextPageToken": None},
        }
    )
    seed = [{"like_count": 1, "text": "seed", "published_at": "2024-01-01 00:00:00"}]
    hooks: list[tuple[str | None, int]] = []

    async def on_page(*, page_token, comments):
        hooks.append((page_token, len(comments)))

    result = await fetch_paginated_comments(
        pool,
        "vid",
        max_scan=10,
        top_limit=10,
        start_page_token="T2",
        seed_comments=seed,
        on_page=on_page,
    )
    assert any(c["text"] == "seed" for c in result)
    assert any(c["text"] == "new" for c in result)
    assert hooks
    assert hooks[0][0] == "T3"
    assert hooks[0][1] == 2


@pytest.mark.asyncio
async def test_fetch_paginated_already_scanned_stop():
    pool = FakePool(
        {
            "T2": {
                "items": [_comment_item(f"c{i}", i) for i in range(3)],
                "nextPageToken": None,
            }
        }
    )
    result = await fetch_paginated_comments(
        pool,
        "vid",
        max_scan=5,
        top_limit=10,
        start_page_token="T2",
        already_scanned=4,
        seed_comments=None,
    )
    # stop_at = 5 - 4 = 1 → only one new comment kept before sort/top
    assert len(result) == 1
    assert pool.calls[0] == "T2"


def test_youtube_comments_metadata_l1():
    meta = get_collector_metadata("youtube_comments")
    assert meta is not None
    assert meta.supports_checkpoint is True
    assert meta.recovery_level == "L1"


@pytest.mark.asyncio
async def test_collector_resumes_comments_with_token_and_emits(monkeypatch):
    collector = YouTubeCommentCollector(config={})
    emitted: list[dict] = []
    fetch_kwargs: dict = {}

    async def fake_emit(cursor, state=None, stats=None):
        emitted.append({"cursor": cursor, "stats": stats or {}})

    async def fake_fetch(pool, video_id, max_scan=500, top_limit=100, **kwargs):
        fetch_kwargs.update(kwargs)
        on_page = kwargs.get("on_page")
        if on_page is not None:
            await on_page(
                page_token="T3",
                comments=[{"like_count": 2, "text": "n", "published_at": ""}],
            )
        return [{"like_count": 2, "text": "n", "published_at": ""}]

    async def fake_videos(pool, ids, parts=""):
        return _video_payload(ids[0], comment_count="20")

    async def fake_channels(pool, **kwargs):
        return _channel_payload()

    monkeypatch.setattr(
        "src.collectors.youtube.comments.api.fetch_paginated_comments",
        fake_fetch,
    )
    monkeypatch.setattr("src.collectors.youtube.comments.api.videos_list", fake_videos)
    monkeypatch.setattr("src.collectors.youtube.comments.api.channels_list", fake_channels)

    collector._pool = AsyncMock()
    collector.config["_emit_checkpoint"] = fake_emit
    collector.config["recovery_checkpoint"] = {
        "cursor": build_collector_cursor(
            collector_id="youtube_comments",
            target_key="video:vid123",
            stage="comments_scan",
            payload={
                "video_id": "vid123",
                "completed_stages": ["video_meta", "channel_meta"],
                "page_token": "T2",
                "scanned_count": 5,
                "max_scan": 20,
                "partial_comments": [],
            },
        )
    }

    target = CollectTarget(
        name="Test",
        params={
            "video_url": "https://www.youtube.com/watch?v=vid123",
            "get_comments": "是",
            "max_scan_comments": 20,
        },
    )
    result = await collector.collect(target)

    assert result.success is True
    assert fetch_kwargs.get("start_page_token") == "T2"
    # Empty partial → seed_comments must be None, already_scanned used.
    assert fetch_kwargs.get("seed_comments") is None
    assert fetch_kwargs.get("already_scanned") == 5
    assert fetch_kwargs.get("on_page") is not None
    assert emitted
    progress = [
        e
        for e in emitted
        if e["cursor"].get("stage") == "comments_scan"
        and e["cursor"].get("payload", {}).get("page_token") == "T3"
    ]
    assert progress, f"missing comments_scan progress emit, got={emitted!r}"
    assert progress[0]["cursor"]["payload"]["scanned_count"] == 6  # 5 + 1
    assert progress[0]["cursor"]["payload"]["partial_comments"] == []
    assert result.metadata.get("target_key") == "video:vid123"
    assert result.metadata.get("resume", {}).get("resumed") is True


@pytest.mark.asyncio
async def test_collector_uses_nonempty_seed(monkeypatch):
    collector = YouTubeCommentCollector(config={})
    fetch_kwargs: dict = {}

    async def fake_fetch(pool, video_id, max_scan=500, top_limit=100, **kwargs):
        fetch_kwargs.update(kwargs)
        return list(kwargs.get("seed_comments") or [])

    async def fake_videos(pool, ids, parts=""):
        return _video_payload(ids[0])

    async def fake_channels(pool, **kwargs):
        return _channel_payload()

    monkeypatch.setattr(
        "src.collectors.youtube.comments.api.fetch_paginated_comments",
        fake_fetch,
    )
    monkeypatch.setattr("src.collectors.youtube.comments.api.videos_list", fake_videos)
    monkeypatch.setattr("src.collectors.youtube.comments.api.channels_list", fake_channels)

    seed = [{"like_count": 3, "text": "seed", "published_at": ""}]
    collector._pool = AsyncMock()
    collector.config["recovery_checkpoint"] = {
        "cursor": build_collector_cursor(
            collector_id="youtube_comments",
            target_key="video:vid123",
            stage="comments_scan",
            payload={
                "completed_stages": ["video_meta"],
                "page_token": "TOK",
                "scanned_count": 1,
                "partial_comments": seed,
            },
        )
    }
    target = CollectTarget(
        name="Test",
        params={
            "video_url": "https://www.youtube.com/watch?v=vid123",
            "get_comments": "是",
        },
    )
    result = await collector.collect(target)
    assert result.success is True
    assert fetch_kwargs.get("start_page_token") == "TOK"
    assert fetch_kwargs.get("seed_comments") == seed
    assert fetch_kwargs.get("already_scanned") == 0


@pytest.mark.asyncio
async def test_incomplete_partial_does_not_enter_seed_mode(monkeypatch):
    collector = YouTubeCommentCollector(config={})
    fetch_kwargs: dict = {}

    async def fake_fetch(pool, video_id, max_scan=500, top_limit=100, **kwargs):
        fetch_kwargs.update(kwargs)
        return []

    async def fake_videos(pool, ids, parts=""):
        return _video_payload(ids[0])

    async def fake_channels(pool, **kwargs):
        return _channel_payload()

    monkeypatch.setattr(
        "src.collectors.youtube.comments.api.fetch_paginated_comments",
        fake_fetch,
    )
    monkeypatch.setattr("src.collectors.youtube.comments.api.videos_list", fake_videos)
    monkeypatch.setattr("src.collectors.youtube.comments.api.channels_list", fake_channels)

    short_partial = [{"like_count": 1, "text": "only-one", "published_at": ""}]
    collector._pool = AsyncMock()
    collector.config["recovery_checkpoint"] = {
        "cursor": build_collector_cursor(
            collector_id="youtube_comments",
            target_key="video:vid123",
            stage="comments_scan",
            payload={
                "completed_stages": ["video_meta"],
                "page_token": "T5",
                "scanned_count": 50,
                "partial_comments": short_partial,
            },
        )
    }
    target = CollectTarget(
        name="Test",
        params={
            "video_url": "https://www.youtube.com/watch?v=vid123",
            "get_comments": "是",
            "max_scan_comments": 100,
        },
    )
    result = await collector.collect(target)
    assert result.success is True
    assert fetch_kwargs.get("start_page_token") == "T5"
    assert fetch_kwargs.get("seed_comments") is None
    assert fetch_kwargs.get("already_scanned") == 50


@pytest.mark.asyncio
async def test_count_only_on_page_emits_empty_partial(monkeypatch):
    collector = YouTubeCommentCollector(config={})
    emitted: list[dict] = []
    fetch_kwargs: dict = {}

    async def fake_emit(cursor, state=None, stats=None):
        emitted.append({"cursor": cursor, "stats": stats or {}})

    async def fake_fetch(pool, video_id, max_scan=500, top_limit=100, **kwargs):
        fetch_kwargs.update(kwargs)
        on_page = kwargs.get("on_page")
        if on_page is not None:
            await on_page(
                page_token="T6",
                comments=[
                    {"like_count": 1, "text": "n1", "published_at": ""},
                    {"like_count": 2, "text": "n2", "published_at": ""},
                ],
            )
        return [
            {"like_count": 2, "text": "n2", "published_at": ""},
            {"like_count": 1, "text": "n1", "published_at": ""},
        ]

    async def fake_videos(pool, ids, parts=""):
        return _video_payload(ids[0])

    async def fake_channels(pool, **kwargs):
        return _channel_payload()

    monkeypatch.setattr(
        "src.collectors.youtube.comments.api.fetch_paginated_comments",
        fake_fetch,
    )
    monkeypatch.setattr("src.collectors.youtube.comments.api.videos_list", fake_videos)
    monkeypatch.setattr("src.collectors.youtube.comments.api.channels_list", fake_channels)

    collector._pool = AsyncMock()
    collector.config["_emit_checkpoint"] = fake_emit
    collector.config["recovery_checkpoint"] = {
        "cursor": build_collector_cursor(
            collector_id="youtube_comments",
            target_key="video:vid123",
            stage="comments_scan",
            payload={
                "completed_stages": ["video_meta"],
                "page_token": "T5",
                "scanned_count": 40,
                "partial_comments": [],
            },
        )
    }
    target = CollectTarget(
        name="Test",
        params={
            "video_url": "https://www.youtube.com/watch?v=vid123",
            "get_comments": "是",
            "max_scan_comments": 100,
        },
    )
    result = await collector.collect(target)
    assert result.success is True
    assert fetch_kwargs.get("seed_comments") is None
    assert fetch_kwargs.get("already_scanned") == 40

    progress = [
        e
        for e in emitted
        if e["cursor"].get("stage") == "comments_scan"
        and e["cursor"].get("payload", {}).get("page_token") == "T6"
    ]
    assert progress, f"missing count-only progress emit, got={emitted!r}"
    payload = progress[0]["cursor"]["payload"]
    assert payload.get("partial_comments") == []
    assert payload.get("scanned_count") == 42  # 40 + 2


@pytest.mark.asyncio
async def test_quota_exhausted_emits_and_fails(monkeypatch):
    collector = YouTubeCommentCollector(config={})
    emitted: list[dict] = []

    async def fake_emit(cursor, state=None, stats=None):
        emitted.append(cursor)

    async def fake_fetch(pool, video_id, max_scan=500, top_limit=100, **kwargs):
        raise YouTubeQuotaExhausted("all keys exhausted")

    async def fake_videos(pool, ids, parts=""):
        return _video_payload(ids[0])

    async def fake_channels(pool, **kwargs):
        return _channel_payload()

    monkeypatch.setattr(
        "src.collectors.youtube.comments.api.fetch_paginated_comments",
        fake_fetch,
    )
    monkeypatch.setattr("src.collectors.youtube.comments.api.videos_list", fake_videos)
    monkeypatch.setattr("src.collectors.youtube.comments.api.channels_list", fake_channels)

    collector._pool = AsyncMock()
    collector.config["_emit_checkpoint"] = fake_emit
    collector.config["recovery_checkpoint"] = {
        "cursor": build_collector_cursor(
            collector_id="youtube_comments",
            target_key="video:vid123",
            stage="comments_scan",
            payload={
                "completed_stages": ["video_meta"],
                "page_token": "T2",
                "scanned_count": 3,
                "partial_comments": [],
            },
        )
    }
    target = CollectTarget(
        name="Test",
        params={
            "video_url": "https://www.youtube.com/watch?v=vid123",
            "get_comments": "是",
        },
    )
    result = await collector.collect(target)
    assert result.success is False
    assert result.error_code == ErrorCode.rate_limited.value
    assert emitted, "quota path should emit cursor"
    last = emitted[-1]
    assert last.get("collector_id") == "youtube_comments"
    assert last.get("target_key") == "video:vid123"
    assert last.get("payload", {}).get("page_token") == "T2"
    assert last.get("payload", {}).get("scanned_count") == 3


@pytest.mark.asyncio
async def test_network_error_fails_target_not_soft_pass(monkeypatch):
    collector = YouTubeCommentCollector(config={})
    emitted: list[dict] = []

    async def fake_emit(cursor, state=None, stats=None):
        emitted.append(cursor)

    async def fake_fetch(pool, video_id, max_scan=500, top_limit=100, **kwargs):
        raise ConnectionError("network down")

    async def fake_videos(pool, ids, parts=""):
        return _video_payload(ids[0])

    async def fake_channels(pool, **kwargs):
        return _channel_payload()

    monkeypatch.setattr(
        "src.collectors.youtube.comments.api.fetch_paginated_comments",
        fake_fetch,
    )
    monkeypatch.setattr("src.collectors.youtube.comments.api.videos_list", fake_videos)
    monkeypatch.setattr("src.collectors.youtube.comments.api.channels_list", fake_channels)

    collector._pool = AsyncMock()
    collector.config["_emit_checkpoint"] = fake_emit
    target = CollectTarget(
        name="Test",
        params={
            "video_url": "https://www.youtube.com/watch?v=vid123",
            "get_comments": "是",
        },
    )
    result = await collector.collect(target)
    assert result.success is False
    assert result.error_code == ErrorCode.network_unreachable.value
    assert emitted, "failure should emit cursor"


@pytest.mark.asyncio
async def test_comments_disabled_soft_ok(monkeypatch):
    collector = YouTubeCommentCollector(config={})

    async def fake_fetch(pool, video_id, max_scan=500, top_limit=100, **kwargs):
        request = httpx.Request("GET", "https://youtube.googleapis.com/youtube/v3/commentThreads")
        response = httpx.Response(
            403,
            json={
                "error": {
                    "errors": [{"reason": "commentsDisabled", "message": "disabled"}],
                    "code": 403,
                    "message": "The video identified by the `id` parameter has disabled comments.",
                }
            },
            request=request,
        )
        raise httpx.HTTPStatusError("Forbidden", request=request, response=response)

    async def fake_videos(pool, ids, parts=""):
        return _video_payload(ids[0])

    async def fake_channels(pool, **kwargs):
        return _channel_payload()

    monkeypatch.setattr(
        "src.collectors.youtube.comments.api.fetch_paginated_comments",
        fake_fetch,
    )
    monkeypatch.setattr("src.collectors.youtube.comments.api.videos_list", fake_videos)
    monkeypatch.setattr("src.collectors.youtube.comments.api.channels_list", fake_channels)

    collector._pool = AsyncMock()
    target = CollectTarget(
        name="Test",
        params={
            "video_url": "https://www.youtube.com/watch?v=vid123",
            "get_comments": "是",
        },
    )
    result = await collector.collect(target)
    assert result.success is True
    assert result.data.get("comments") == []
