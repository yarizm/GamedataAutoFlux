"""Tests for BaseYouTubeCollector."""

import pytest
from unittest.mock import patch


class _ConcreteYouTubeCollector:
    """Minimal concrete subclass for testing the abstract base."""

    @classmethod
    def _create(cls, config=None):
        from src.collectors.youtube.base import BaseYouTubeCollector

        class _Concrete(BaseYouTubeCollector):
            async def collect(self, target):
                pass

        return _Concrete(config=config)


class TestBaseYouTubeCollector:
    @pytest.mark.asyncio
    async def test_setup_reads_api_keys_from_settings(self):
        mock_settings = {
            "youtube": {
                "api_keys": ["key_from_settings"],
                "request_delay": 0.5,
                "request_timeout": 45,
                "api_base_url": "https://example.test/youtube/v3/",
            }
        }

        with patch("src.collectors.youtube.base.get_settings", return_value=mock_settings):
            collector = _ConcreteYouTubeCollector._create()
            await collector.setup()

        assert collector._pool is not None
        assert collector._pool._keys == ["key_from_settings"]
        assert collector._pool._delay == 0.5
        assert collector._pool._base_url == "https://example.test/youtube/v3"
        await collector.teardown()

    @pytest.mark.asyncio
    async def test_setup_filters_unresolved_keys(self):
        mock_settings = {
            "youtube": {
                "api_keys": ["${UNRESOLVED}", "real_key"],
            }
        }

        with patch("src.collectors.youtube.base.get_settings", return_value=mock_settings):
            collector = _ConcreteYouTubeCollector._create()
            await collector.setup()

        assert collector._pool._keys == ["real_key"]
        await collector.teardown()

    @pytest.mark.asyncio
    async def test_setup_raises_on_no_valid_keys(self):
        mock_settings = {"youtube": {"api_keys": ["${UNRESOLVED}"]}}

        with patch("src.collectors.youtube.base.get_settings", return_value=mock_settings):
            collector = _ConcreteYouTubeCollector._create()
            with pytest.raises(ValueError, match="API Key"):
                await collector.setup()

    @pytest.mark.asyncio
    async def test_config_api_key_overrides_settings(self):
        mock_settings = {"youtube": {"api_keys": ["settings_key"]}}

        with patch("src.collectors.youtube.base.get_settings", return_value=mock_settings):
            collector = _ConcreteYouTubeCollector._create(config={"api_key": "config_key"})
            await collector.setup()

        assert collector._pool._keys == ["config_key", "settings_key"]
        await collector.teardown()

    @pytest.mark.asyncio
    async def test_teardown_closes_pool(self):
        mock_settings = {"youtube": {"api_keys": ["key1"]}}

        with patch("src.collectors.youtube.base.get_settings", return_value=mock_settings):
            collector = _ConcreteYouTubeCollector._create()
            await collector.setup()
            assert collector._pool._client is not None
            await collector.teardown()
            assert collector._pool._client is None
