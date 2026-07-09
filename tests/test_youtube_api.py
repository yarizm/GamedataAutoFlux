"""Tests for YouTube shared API functions."""

import pytest
from unittest.mock import AsyncMock


class TestFormatDuration:
    def test_standard(self):
        from src.collectors.youtube.api import format_duration
        assert format_duration("PT1H23M45S") == "01:23:45"

    def test_minutes_only(self):
        from src.collectors.youtube.api import format_duration
        assert format_duration("PT5M30S") == "00:05:30"

    def test_empty_input(self):
        from src.collectors.youtube.api import format_duration
        assert format_duration("") == ""

    def test_with_days(self):
        from src.collectors.youtube.api import format_duration
        assert format_duration("P1DT2H3M4S") == "26:03:04"


class TestChunked:
    def test_even_split(self):
        from src.collectors.youtube.api import chunked
        assert chunked([1, 2, 3, 4], 2) == [[1, 2], [3, 4]]

    def test_uneven_split(self):
        from src.collectors.youtube.api import chunked
        assert chunked([1, 2, 3, 4, 5], 2) == [[1, 2], [3, 4], [5]]


class TestExtractVideoId:
    def test_standard_watch_url(self):
        from src.collectors.youtube.api import extract_video_id
        assert extract_video_id("https://www.youtube.com/watch?v=dQw4w9WgXcQ") == "dQw4w9WgXcQ"

    def test_shorts_url(self):
        from src.collectors.youtube.api import extract_video_id
        assert extract_video_id("https://www.youtube.com/shorts/abc123DEF45") == "abc123DEF45"

    def test_youtu_be(self):
        from src.collectors.youtube.api import extract_video_id
        assert extract_video_id("https://youtu.be/dQw4w9WgXcQ") == "dQw4w9WgXcQ"

    def test_invalid_url(self):
        from src.collectors.youtube.api import extract_video_id
        assert extract_video_id("https://example.com") == ""


class TestBuildVideoUrl:
    def test_normal_video(self):
        from src.collectors.youtube.api import build_video_url
        assert build_video_url("abc", "普通视频") == "https://www.youtube.com/watch?v=abc"

    def test_shorts(self):
        from src.collectors.youtube.api import build_video_url
        assert build_video_url("abc", "Shorts") == "https://www.youtube.com/shorts/abc"

    def test_empty_id(self):
        from src.collectors.youtube.api import build_video_url
        assert build_video_url("", "普通视频") == ""


class TestParseChannelHint:
    def test_channel_id(self):
        from src.collectors.youtube.api import parse_channel_hint
        assert parse_channel_hint("https://www.youtube.com/channel/UC123") == ("id", "UC123")

    def test_handle(self):
        from src.collectors.youtube.api import parse_channel_hint
        assert parse_channel_hint("https://www.youtube.com/@MrBeast") == ("handle", "@MrBeast")

    def test_username(self):
        from src.collectors.youtube.api import parse_channel_hint
        assert parse_channel_hint("https://www.youtube.com/user/username") == ("username", "username")

    def test_invalid(self):
        from src.collectors.youtube.api import parse_channel_hint
        assert parse_channel_hint("https://example.com") == ("", "")


class TestVideosList:
    @pytest.mark.asyncio
    async def test_calls_pool_request(self):
        from src.collectors.youtube.api import videos_list
        mock_pool = AsyncMock()
        mock_pool.request.return_value = {"items": [{"id": "abc"}]}
        result = await videos_list(mock_pool, ["abc"], "snippet,statistics")
        mock_pool.request.assert_called_once()
        assert result == {"items": [{"id": "abc"}]}


class TestChannelsList:
    @pytest.mark.asyncio
    async def test_by_id(self):
        from src.collectors.youtube.api import channels_list
        mock_pool = AsyncMock()
        mock_pool.request.return_value = {"items": [{"id": "UC123"}]}
        result = await channels_list(mock_pool, id="UC123")
        assert result["items"][0]["id"] == "UC123"

    @pytest.mark.asyncio
    async def test_by_handle(self):
        from src.collectors.youtube.api import channels_list
        mock_pool = AsyncMock()
        mock_pool.request.return_value = {"items": [{"id": "UC456"}]}
        result = await channels_list(mock_pool, forHandle="@MrBeast")
        assert result["items"][0]["id"] == "UC456"


class TestFetchPaginatedComments:
    @pytest.mark.asyncio
    async def test_single_page(self):
        from src.collectors.youtube.api import fetch_paginated_comments

        mock_pool = AsyncMock()
        comment = {
            "snippet": {
                "topLevelComment": {
                    "snippet": {
                        "textDisplay": "Great video",
                        "likeCount": 10,
                        "publishedAt": "2024-01-01T00:00:00Z",
                    }
                }
            }
        }
        mock_pool.request.return_value = {"items": [comment]}  # no nextPageToken

        result = await fetch_paginated_comments(mock_pool, "vid123", max_scan=500, top_limit=100)
        assert len(result) == 1
        assert result[0]["text"] == "Great video"
        assert result[0]["like_count"] == 10
