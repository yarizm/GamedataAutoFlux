"""Tests for YouTubeProfileCollector."""

import pytest
from unittest.mock import AsyncMock


class TestYouTubeProfileCollector:
    @pytest.mark.asyncio
    async def test_collect_by_channel_id(self):
        from src.collectors.base import CollectTarget
        from src.collectors.youtube.profiles import YouTubeProfileCollector

        collector = YouTubeProfileCollector()
        collector._pool = AsyncMock()
        collector._pool.request.return_value = {
            "items": [{
                "id": "UC123",
                "snippet": {
                    "title": "Test Channel",
                    "description": "A test channel",
                },
                "statistics": {"subscriberCount": "1000"},
            }]
        }

        target = CollectTarget(
            name="Test Channel",
            target_type="youtube_channel",
            params={"channel_url": "https://www.youtube.com/channel/UC123"},
        )

        result = await collector.collect(target)
        assert result.success is True
        assert result.data["author_name"] == "Test Channel"
        assert result.data["channel_id"] == "UC123"
        assert result.data["subscriber_count"] == "1000"
        assert result.data["resolution_method"] == "id"
        assert result.data["resolution_status"] == "success"

    @pytest.mark.asyncio
    async def test_collect_by_handle(self):
        from src.collectors.base import CollectTarget
        from src.collectors.youtube.profiles import YouTubeProfileCollector

        collector = YouTubeProfileCollector()
        collector._pool = AsyncMock()
        collector._pool.request.return_value = {
            "items": [{
                "id": "UC456",
                "snippet": {"title": "Handle Channel", "description": ""},
                "statistics": {"subscriberCount": "500"},
            }]
        }

        target = CollectTarget(
            name="@testhandle",
            target_type="youtube_channel",
            params={"handle": "@testhandle"},
        )

        result = await collector.collect(target)
        assert result.success is True
        assert result.data["resolution_method"] == "handle"
        assert result.data["channel_id"] == "UC456"

    @pytest.mark.asyncio
    async def test_collect_by_direct_handle_in_params(self):
        from src.collectors.base import CollectTarget
        from src.collectors.youtube.profiles import YouTubeProfileCollector

        collector = YouTubeProfileCollector()
        collector._pool = AsyncMock()
        collector._pool.request.return_value = {
            "items": [{
                "id": "UC789",
                "snippet": {"title": "Direct Handle", "description": ""},
                "statistics": {"subscriberCount": "200"},
            }]
        }

        target = CollectTarget(
            name="@direct",
            target_type="youtube_channel",
            params={"channel_url": "https://www.youtube.com/@direct"},
        )

        result = await collector.collect(target)
        assert result.success is True
        assert result.data["resolution_method"] == "handle"

    @pytest.mark.asyncio
    async def test_collect_not_found(self):
        from src.collectors.base import CollectTarget
        from src.collectors.youtube.profiles import YouTubeProfileCollector

        collector = YouTubeProfileCollector()
        collector._pool = AsyncMock()
        collector._pool.request.return_value = {"items": []}

        target = CollectTarget(
            name="nonexistent",
            target_type="youtube_channel",
            params={"channel_url": "https://www.youtube.com/@nonexistent"},
        )

        result = await collector.collect(target)
        assert result.success is False
        assert result.data["resolution_status"] == "not_found"
        assert result.data["error_code"] == "not_found"
