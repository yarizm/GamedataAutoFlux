"""Tests for YouTubeCommentCollector."""

import pytest
from unittest.mock import AsyncMock, patch


class TestYouTubeCommentCollector:
    @pytest.mark.asyncio
    async def test_collect_video_basic(self):
        from src.collectors.base import CollectTarget
        from src.collectors.youtube.comments import YouTubeCommentCollector

        collector = YouTubeCommentCollector()
        collector._pool = AsyncMock()

        # videos.list response
        collector._pool.request.side_effect = [
            {
                "items": [{
                    "id": "vid123",
                    "snippet": {
                        "title": "Test Video",
                        "channelId": "UC123",
                        "channelTitle": "Test Channel",
                        "publishedAt": "2024-01-01T00:00:00Z",
                        "description": "A test video description",
                        "liveBroadcastContent": "none",
                    },
                    "statistics": {
                        "viewCount": "10000",
                        "likeCount": "500",
                        "commentCount": "20",
                    },
                    "contentDetails": {"duration": "PT10M30S"},
                }]
            },
            # channels.list response
            {
                "items": [{
                    "id": "UC123",
                    "statistics": {"subscriberCount": "1000"},
                }]
            },
        ]

        target = CollectTarget(
            name="Test Video",
            target_type="youtube_video",
            params={"video_url": "https://www.youtube.com/watch?v=vid123"},
        )

        result = await collector.collect(target)
        assert result.success is True
        assert result.data["title"] == "Test Video"
        assert result.data["view_count"] == "10000"
        assert result.data["like_count"] == "500"
        assert result.data["comment_count"] == "20"
        assert result.data["duration"] == "00:10:30"
        assert result.data["live_status"] == ""
        assert "comments" not in result.data  # get_comments not enabled

    @pytest.mark.asyncio
    async def test_collect_with_comments(self):
        from src.collectors.base import CollectTarget
        from src.collectors.youtube.comments import YouTubeCommentCollector

        collector = YouTubeCommentCollector()
        collector._pool = AsyncMock()

        collector._pool.request.side_effect = [
            {
                "items": [{
                    "id": "vid456",
                    "snippet": {
                        "title": "Video With Comments",
                        "channelId": "UC456",
                        "channelTitle": "Comment Channel",
                        "publishedAt": "2024-02-02T00:00:00Z",
                        "description": "",
                        "liveBroadcastContent": "none",
                    },
                    "statistics": {
                        "viewCount": "5000",
                        "likeCount": "200",
                        "commentCount": "5",
                    },
                    "contentDetails": {"duration": "PT5M"},
                }]
            },
            {"items": [{"id": "UC456", "statistics": {"subscriberCount": "300"}}]},
            # commentThreads.list response
            {
                "items": [{
                    "snippet": {
                        "topLevelComment": {
                            "snippet": {
                                "textDisplay": "Nice video!",
                                "likeCount": 10,
                                "publishedAt": "2024-03-01T00:00:00Z",
                            }
                        }
                    }
                }],
                # no nextPageToken
            },
        ]

        target = CollectTarget(
            name="Video With Comments",
            target_type="youtube_video",
            params={
                "video_url": "https://www.youtube.com/watch?v=vid456",
                "get_comments": "是",
                "max_scan_comments": 500,
            },
        )

        result = await collector.collect(target)
        assert result.success is True
        assert "comments" in result.data
        assert len(result.data["comments"]) == 1
        assert result.data["comments"][0]["text"] == "Nice video!"
        assert result.data["comments"][0]["like_count"] == 10

    @pytest.mark.asyncio
    async def test_collect_zero_comments(self):
        from src.collectors.base import CollectTarget
        from src.collectors.youtube.comments import YouTubeCommentCollector

        collector = YouTubeCommentCollector()
        collector._pool = AsyncMock()

        collector._pool.request.side_effect = [
            {
                "items": [{
                    "id": "vid789",
                    "snippet": {
                        "title": "No Comments",
                        "channelId": "UC789",
                        "channelTitle": "Empty",
                        "publishedAt": "2024-01-01T00:00:00Z",
                        "description": "",
                        "liveBroadcastContent": "none",
                    },
                    "statistics": {
                        "viewCount": "100",
                        "likeCount": "5",
                        "commentCount": "0",
                    },
                    "contentDetails": {"duration": "PT1M"},
                }]
            },
            {"items": [{"id": "UC789", "statistics": {"subscriberCount": "50"}}]},
        ]

        target = CollectTarget(
            name="No Comments",
            target_type="youtube_video",
            params={
                "video_url": "https://www.youtube.com/watch?v=vid789",
                "get_comments": "是",
            },
        )

        result = await collector.collect(target)
        assert result.success is True
        assert result.data.get("comments", []) == []

    @pytest.mark.asyncio
    async def test_collect_invalid_url(self):
        from src.collectors.base import CollectTarget
        from src.collectors.youtube.comments import YouTubeCommentCollector
        from src.core.errors import ErrorCode

        collector = YouTubeCommentCollector()
        collector._pool = AsyncMock()

        target = CollectTarget(
            name="Invalid",
            target_type="youtube_video",
            params={"video_url": "https://example.com/not-a-video"},
        )

        result = await collector.collect(target)
        assert result.success is False
        assert result.error_code == ErrorCode.invalid_params.value

    @pytest.mark.asyncio
    async def test_check_video_type(self):
        from src.collectors.base import CollectTarget
        from src.collectors.youtube.comments import YouTubeCommentCollector

        collector = YouTubeCommentCollector()
        collector._pool = AsyncMock()

        collector._pool.request.side_effect = [
            {
                "items": [{
                    "id": "vid_shorts",
                    "snippet": {
                        "title": "A Short",
                        "channelId": "UC_shorts",
                        "channelTitle": "Shorts Channel",
                        "publishedAt": "2024-06-01T00:00:00Z",
                        "description": "",
                        "liveBroadcastContent": "none",
                    },
                    "statistics": {
                        "viewCount": "500",
                        "likeCount": "30",
                        "commentCount": "2",
                    },
                    "contentDetails": {"duration": "PT30S"},
                }]
            },
            {"items": [{"id": "UC_shorts", "statistics": {"subscriberCount": "100"}}]},
        ]

        target = CollectTarget(
            name="A Short",
            target_type="youtube_video",
            params={
                "video_url": "https://www.youtube.com/watch?v=vid_shorts",
                "check_video_type": "是",
            },
        )

        with patch(
            "src.collectors.youtube.comments.api.check_video_type",
            new_callable=AsyncMock,
        ) as mock_check:
            mock_check.return_value = "Shorts"
            result = await collector.collect(target)

        assert result.success is True
        assert result.data["video_type"] == "Shorts"
        mock_check.assert_awaited_once_with("vid_shorts")

    @pytest.mark.asyncio
    async def test_live_stream_excluded(self):
        from src.collectors.base import CollectTarget
        from src.collectors.youtube.comments import YouTubeCommentCollector

        collector = YouTubeCommentCollector()
        collector._pool = AsyncMock()

        collector._pool.request.side_effect = [
            {
                "items": [{
                    "id": "vid_live",
                    "snippet": {
                        "title": "Live Stream",
                        "channelId": "UC_live",
                        "channelTitle": "Live Channel",
                        "publishedAt": "2024-07-01T00:00:00Z",
                        "description": "",
                        "liveBroadcastContent": "live",
                    },
                    "statistics": {
                        "viewCount": "1000",
                        "likeCount": "100",
                        "commentCount": "50",
                    },
                    "contentDetails": {"duration": "PT0S"},
                }]
            },
        ]

        target = CollectTarget(
            name="Live Stream",
            target_type="youtube_video",
            params={
                "video_url": "https://www.youtube.com/watch?v=vid_live",
                "live_stream_policy": "直接排除",
            },
        )

        result = await collector.collect(target)
        assert result.success is True
        assert result.data["skipped"] is True
        assert result.data["skip_reason"] == "直播内容已排除"
        assert result.data["live_status"] == "正在直播"

    @pytest.mark.asyncio
    async def test_live_stream_marked(self):
        from src.collectors.base import CollectTarget
        from src.collectors.youtube.comments import YouTubeCommentCollector

        collector = YouTubeCommentCollector()
        collector._pool = AsyncMock()

        collector._pool.request.side_effect = [
            {
                "items": [{
                    "id": "vid_upcoming",
                    "snippet": {
                        "title": "Upcoming Stream",
                        "channelId": "UC_upcoming",
                        "channelTitle": "Upcoming Channel",
                        "publishedAt": "2024-08-01T00:00:00Z",
                        "description": "",
                        "liveBroadcastContent": "upcoming",
                    },
                    "statistics": {
                        "viewCount": "0",
                        "likeCount": "0",
                        "commentCount": "0",
                    },
                    "contentDetails": {"duration": "PT0S"},
                }]
            },
            {"items": [{"id": "UC_upcoming", "statistics": {"subscriberCount": "200"}}]},
        ]

        target = CollectTarget(
            name="Upcoming Stream",
            target_type="youtube_video",
            params={
                "video_url": "https://www.youtube.com/watch?v=vid_upcoming",
                "live_stream_policy": "仅标记",
            },
        )

        result = await collector.collect(target)
        assert result.success is True
        assert "skipped" not in result.data
        assert result.data["live_status"] == "预告直播"

    @pytest.mark.asyncio
    async def test_fetch_shorts_related(self):
        from src.collectors.base import CollectTarget
        from src.collectors.youtube.comments import YouTubeCommentCollector

        collector = YouTubeCommentCollector()
        collector._pool = AsyncMock()

        collector._pool.request.side_effect = [
            {
                "items": [{
                    "id": "shorts_vid",
                    "snippet": {
                        "title": "My Short",
                        "channelId": "UC_shorts2",
                        "channelTitle": "Shorts Maker",
                        "publishedAt": "2024-09-01T00:00:00Z",
                        "description": "",
                        "liveBroadcastContent": "none",
                    },
                    "statistics": {
                        "viewCount": "2000",
                        "likeCount": "150",
                        "commentCount": "10",
                    },
                    "contentDetails": {"duration": "PT45S"},
                }]
            },
            {"items": [{"id": "UC_shorts2", "statistics": {"subscriberCount": "500"}}]},
        ]

        mock_html = (
            '<script>var ytInitialData = '
            '{"reelMultiFormatLinkViewModel":'
            '{"title":{"content":"Related Long Video"},'
            '"command":{"innertubeCommand":'
            '{"watchEndpoint":{"videoId":"related_vid123"}}}}};'
            '</script>'
        )

        class FakeResponse:
            text = mock_html

        class FakeClient:
            async def __aenter__(self):
                return self

            async def __aexit__(self, *args):
                pass

            async def get(self, *args, **kwargs):
                return FakeResponse()

        target = CollectTarget(
            name="My Short",
            target_type="youtube_video",
            params={
                "video_url": "https://www.youtube.com/watch?v=shorts_vid",
                "check_video_type": "是",
                "fetch_shorts_related": "是",
            },
        )

        with patch(
            "src.collectors.youtube.comments.api.check_video_type",
            new_callable=AsyncMock,
        ) as mock_check:
            mock_check.return_value = "Shorts"
            with patch(
                "src.collectors.youtube.comments.httpx.AsyncClient",
                new=FakeClient,
            ):
                result = await collector.collect(target)

        assert result.success is True
        assert result.data["related_video_title"] == "Related Long Video"
        assert result.data["related_video_url"] == "https://www.youtube.com/watch?v=related_vid123"
        mock_check.assert_awaited_once_with("shorts_vid")
