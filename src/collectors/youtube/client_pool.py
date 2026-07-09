"""YouTube API 客户端连接池 — 多 Key 轮换 + 异步 httpx 请求。"""

from __future__ import annotations

import asyncio
from typing import Any

import httpx
from loguru import logger


class YouTubeQuotaExhausted(Exception):
    """所有 API Key 配额均已耗尽。"""


def _is_quota_exhausted(resp: httpx.Response) -> bool:
    """区分 403/429 中的配额耗尽 vs 其他错误。"""
    try:
        body = resp.json()
        errors = body.get("error", {}).get("errors", [])
        for e in errors:
            if e.get("reason") in (
                "quotaExceeded",
                "dailyLimitExceeded",
                "rateLimitExceeded",
                "userRateLimitExceeded",
            ):
                return True
    except Exception:
        pass
    return False


class YouTubeClientPool:
    """YouTube Data API v3 多 Key 异步客户端池。"""

    def __init__(
        self,
        api_keys: list[str],
        request_delay: float = 0.1,
        request_timeout: float = 30.0,
        api_base_url: str = "https://youtube.googleapis.com/youtube/v3",
    ):
        self._keys = [k.strip() for k in api_keys if k.strip() and not k.startswith("${")]
        if not self._keys:
            raise ValueError("至少需要一个有效的 YouTube API Key")
        self._idx = 0
        self._client: httpx.AsyncClient | None = None
        self._delay = request_delay
        self._timeout = request_timeout
        self._base_url = str(api_base_url or "https://youtube.googleapis.com/youtube/v3").rstrip(
            "/"
        )

    async def setup(self) -> None:
        self._client = httpx.AsyncClient(timeout=httpx.Timeout(self._timeout))

    async def close(self) -> None:
        if self._client is not None:
            await self._client.aclose()
            self._client = None

    @property
    def current_key(self) -> str:
        return self._keys[self._idx]

    async def _next_key(self) -> bool:
        """切换到下一个 Key，返回 False 表示已耗尽。"""
        if self._idx + 1 >= len(self._keys):
            return False
        self._idx += 1
        logger.info(
            f"[YouTubeClientPool] 切换 Key ({self._idx + 1}/{len(self._keys)})")
        return True

    async def _refresh_client(self) -> None:
        """重建 httpx 客户端，丢弃可能失效的连接。"""
        await self.close()
        await self.setup()
        logger.debug("[YouTubeClientPool] 已刷新 HTTP 客户端")

    async def request(
        self, method: str, path: str, **params: Any
    ) -> dict[str, Any]:
        """
        统一请求入口。每请求间自动 delay。

        错误处理:
        - 403/429 + quotaExceeded → 切换 Key 后重试
        - 429 无 quota 标记 → 退避重试（速率限制）
        - 500/503 → 退避重试
        - 连接断开 → 重建客户端 + 重试
        """
        if self._client is None:
            raise RuntimeError("YouTubeClientPool 未初始化，请先调用 setup()")

        max_retries = max(3, len(self._keys) + 2)
        for attempt in range(max_retries):
            await asyncio.sleep(self._delay)

            try:
                resp = await self._client.request(
                    method,
                    f"{self._base_url}{path}",
                    params={**params, "key": self.current_key},
                )

                # 配额耗尽 → 换 Key 重试
                if resp.status_code in (403, 429) and _is_quota_exhausted(resp):
                    if await self._next_key():
                        continue
                    raise YouTubeQuotaExhausted(
                        f"所有 API Key 配额均已耗尽 (status={resp.status_code})")

                # 速率限制 → 退避重试
                if resp.status_code == 429:
                    wait = 2 ** attempt
                    logger.debug(f"[YouTubeClientPool] 429 速率限制，{wait}s 后退避重试")
                    await asyncio.sleep(wait)
                    continue

                # 服务端临时故障 → 退避重试
                if resp.status_code in (500, 503):
                    wait = 2 ** attempt
                    logger.debug(f"[YouTubeClientPool] HTTP {resp.status_code}，{wait}s 后退避重试")
                    await asyncio.sleep(wait)
                    continue

                resp.raise_for_status()
                return resp.json()

            except httpx.ConnectError:
                if attempt < max_retries - 1:
                    await self._refresh_client()
                    wait = 2 ** attempt
                    logger.debug(f"[YouTubeClientPool] 连接断开，{wait}s 后重试")
                    await asyncio.sleep(wait)
                    continue
                raise

            except httpx.HTTPStatusError:
                raise  # 非重试的错误直接抛出

        raise RuntimeError("请求重试次数耗尽")
