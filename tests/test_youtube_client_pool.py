"""Tests for YouTubeClientPool."""

import pytest
import httpx
from unittest.mock import AsyncMock, patch, MagicMock


class TestYouTubeClientPool:
    """Tests for YouTubeClientPool initialization and key management."""

    def test_init_filters_empty_keys(self):
        from src.collectors.youtube.client_pool import YouTubeClientPool
        pool = YouTubeClientPool(api_keys=["  ", "valid_key", "${UNRESOLVED}"])
        assert pool._keys == ["valid_key"]

    def test_init_raises_on_all_invalid_keys(self):
        from src.collectors.youtube.client_pool import YouTubeClientPool
        with pytest.raises(ValueError, match="API Key"):
            YouTubeClientPool(api_keys=["${UNRESOLVED}"])

    def test_current_key_returns_first_valid(self):
        from src.collectors.youtube.client_pool import YouTubeClientPool
        pool = YouTubeClientPool(api_keys=["key1", "key2"])
        assert pool.current_key == "key1"

    def test_init_uses_configurable_base_url(self):
        from src.collectors.youtube.client_pool import YouTubeClientPool

        pool = YouTubeClientPool(
            api_keys=["key1"],
            api_base_url="https://example.test/youtube/v3/",
        )

        assert pool._base_url == "https://example.test/youtube/v3"

    @pytest.mark.asyncio
    async def test_setup_creates_client(self):
        from src.collectors.youtube.client_pool import YouTubeClientPool
        pool = YouTubeClientPool(api_keys=["key1"])
        await pool.setup()
        assert pool._client is not None
        await pool.close()

    @pytest.mark.asyncio
    async def test_close_cleans_up(self):
        from src.collectors.youtube.client_pool import YouTubeClientPool
        pool = YouTubeClientPool(api_keys=["key1"])
        await pool.setup()
        await pool.close()
        assert pool._client is None

    @pytest.mark.asyncio
    async def test_next_key_cycles(self):
        from src.collectors.youtube.client_pool import YouTubeClientPool
        pool = YouTubeClientPool(api_keys=["key1", "key2"])
        assert pool.current_key == "key1"
        result = await pool._next_key()
        assert result is True
        assert pool.current_key == "key2"
        result = await pool._next_key()
        assert result is False  # no more keys
        assert pool.current_key == "key2"


class TestQuotaDetection:
    """Tests for _is_quota_exhausted."""

    def test_quota_exceeded_reason_flag(self):
        from src.collectors.youtube.client_pool import _is_quota_exhausted
        resp = MagicMock(spec=httpx.Response)
        resp.json.return_value = {
            "error": {"errors": [{"reason": "quotaExceeded"}]}
        }
        assert _is_quota_exhausted(resp) is True

    def test_daily_limit_exceeded_flag(self):
        from src.collectors.youtube.client_pool import _is_quota_exhausted
        resp = MagicMock(spec=httpx.Response)
        resp.json.return_value = {
            "error": {"errors": [{"reason": "dailyLimitExceeded"}]}
        }
        assert _is_quota_exhausted(resp) is True

    def test_not_quota_error(self):
        from src.collectors.youtube.client_pool import _is_quota_exhausted
        resp = MagicMock(spec=httpx.Response)
        resp.json.return_value = {
            "error": {"errors": [{"reason": "forbidden"}]}
        }
        assert _is_quota_exhausted(resp) is False

    def test_malformed_body_returns_false(self):
        from src.collectors.youtube.client_pool import _is_quota_exhausted
        resp = MagicMock(spec=httpx.Response)
        resp.json.side_effect = ValueError
        assert _is_quota_exhausted(resp) is False


class TestRequestMethod:
    """Tests for the request method."""

    @pytest.mark.asyncio
    async def test_successful_request(self):
        from src.collectors.youtube.client_pool import YouTubeClientPool
        pool = YouTubeClientPool(api_keys=["key1"])
        await pool.setup()

        mock_resp = MagicMock(spec=httpx.Response)
        mock_resp.status_code = 200
        mock_resp.json.return_value = {"items": []}
        mock_resp.raise_for_status.return_value = None

        with patch.object(pool._client, "request", new_callable=AsyncMock) as mock_request:
            mock_request.return_value = mock_resp
            result = await pool.request("GET", "/channels", part="snippet", id="UC123")

        assert result == {"items": []}
        await pool.close()

    @pytest.mark.asyncio
    async def test_429_rate_limit_retries(self):
        from src.collectors.youtube.client_pool import YouTubeClientPool
        pool = YouTubeClientPool(api_keys=["key1"])
        await pool.setup()

        rate_limit_resp = MagicMock(spec=httpx.Response)
        rate_limit_resp.status_code = 429
        rate_limit_resp.json.return_value = {}

        success_resp = MagicMock(spec=httpx.Response)
        success_resp.status_code = 200
        success_resp.json.return_value = {"items": [{"id": "1"}]}

        with patch.object(pool._client, "request", new_callable=AsyncMock) as mock_request:
            mock_request.side_effect = [rate_limit_resp, success_resp]
            result = await pool.request("GET", "/videos", part="snippet", id="abc")

        assert result == {"items": [{"id": "1"}]}
        assert mock_request.call_count == 2
        await pool.close()

    @pytest.mark.asyncio
    async def test_429_quota_switches_key(self):
        from src.collectors.youtube.client_pool import YouTubeClientPool
        pool = YouTubeClientPool(api_keys=["key1", "key2"])
        await pool.setup()

        quota_resp = MagicMock(spec=httpx.Response)
        quota_resp.status_code = 429
        quota_resp.json.return_value = {"error": {"errors": [{"reason": "quotaExceeded"}]}}

        success_resp = MagicMock(spec=httpx.Response)
        success_resp.status_code = 200
        success_resp.json.return_value = {"items": []}

        with patch.object(pool._client, "request", new_callable=AsyncMock) as mock_request:
            mock_request.side_effect = [quota_resp, success_resp]
            result = await pool.request("GET", "/search", part="id", q="test")

        assert result == {"items": []}
        assert pool.current_key == "key2"
        call_kwargs = mock_request.call_args_list[1].kwargs
        assert call_kwargs["params"]["key"] == "key2"
        await pool.close()

    @pytest.mark.asyncio
    async def test_quota_rotation_can_reach_fourth_key(self):
        from src.collectors.youtube.client_pool import YouTubeClientPool

        pool = YouTubeClientPool(api_keys=["key1", "key2", "key3", "key4"])
        await pool.setup()

        quota_resp = MagicMock(spec=httpx.Response)
        quota_resp.status_code = 429
        quota_resp.json.return_value = {"error": {"errors": [{"reason": "quotaExceeded"}]}}

        success_resp = MagicMock(spec=httpx.Response)
        success_resp.status_code = 200
        success_resp.json.return_value = {"items": [{"id": "ok"}]}
        success_resp.raise_for_status.return_value = None

        with patch.object(pool._client, "request", new_callable=AsyncMock) as mock_request, \
             patch("asyncio.sleep", new_callable=AsyncMock):
            mock_request.side_effect = [
                quota_resp,
                quota_resp,
                quota_resp,
                success_resp,
            ]
            result = await pool.request("GET", "/search", part="id", q="test")

        assert result == {"items": [{"id": "ok"}]}
        assert pool.current_key == "key4"
        assert mock_request.call_count == 4
        assert mock_request.call_args_list[3].kwargs["params"]["key"] == "key4"
        await pool.close()

    @pytest.mark.asyncio
    async def test_500_retries_with_backoff(self):
        from src.collectors.youtube.client_pool import YouTubeClientPool
        pool = YouTubeClientPool(api_keys=["key1"])
        await pool.setup()

        error_resp = MagicMock(spec=httpx.Response)
        error_resp.status_code = 503
        error_resp.json.return_value = {}

        success_resp = MagicMock(spec=httpx.Response)
        success_resp.status_code = 200
        success_resp.json.return_value = {"items": [{"id": "1"}]}

        with patch.object(pool._client, "request", new_callable=AsyncMock) as mock_request, \
             patch("asyncio.sleep", new_callable=AsyncMock):
            mock_request.side_effect = [error_resp, success_resp]
            result = await pool.request("GET", "/videos", part="snippet", id="abc")

        assert result == {"items": [{"id": "1"}]}
        assert mock_request.call_count == 2
        await pool.close()

    @pytest.mark.asyncio
    async def test_connect_error_retries(self):
        from src.collectors.youtube.client_pool import YouTubeClientPool
        pool = YouTubeClientPool(api_keys=["key1"])
        await pool.setup()

        success_resp = MagicMock(spec=httpx.Response)
        success_resp.status_code = 200
        success_resp.json.return_value = {"items": [{"id": "1"}]}

        with patch.object(pool._client, "request", new_callable=AsyncMock) as mock_request, \
             patch.object(pool, "_refresh_client", new_callable=AsyncMock) as mock_refresh, \
             patch("asyncio.sleep", new_callable=AsyncMock):
            mock_request.side_effect = [httpx.ConnectError("connection failed"), success_resp]
            result = await pool.request("GET", "/videos", part="snippet", id="abc")

        assert result == {"items": [{"id": "1"}]}
        assert mock_request.call_count == 2
        assert mock_refresh.called
        await pool.close()

    @pytest.mark.asyncio
    async def test_request_raises_if_not_setup(self):
        from src.collectors.youtube.client_pool import YouTubeClientPool
        pool = YouTubeClientPool(api_keys=["key1"])
        with pytest.raises(RuntimeError, match="setup"):
            await pool.request("GET", "/channels", part="snippet", id="UC123")

    @pytest.mark.asyncio
    async def test_all_keys_exhausted_raises(self):
        from src.collectors.youtube.client_pool import (
            YouTubeClientPool,
            YouTubeQuotaExhausted,
        )
        pool = YouTubeClientPool(api_keys=["key1"])
        await pool.setup()

        quota_resp = MagicMock(spec=httpx.Response)
        quota_resp.status_code = 403
        quota_resp.json.return_value = {"error": {"errors": [{"reason": "quotaExceeded"}]}}

        with patch.object(pool._client, "request", new_callable=AsyncMock) as mock_request:
            mock_request.return_value = quota_resp
            with pytest.raises(YouTubeQuotaExhausted):
                await pool.request("GET", "/search", part="id", q="test")

        await pool.close()
