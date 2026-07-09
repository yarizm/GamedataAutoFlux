"""YouTube 采集器基类。"""

from __future__ import annotations

from typing import Any

from src.collectors.base import BaseCollector
from src.collectors.youtube.client_pool import YouTubeClientPool
from src.core.config import get_settings


class BaseYouTubeCollector(BaseCollector):
    """YouTube 采集器基类。子类只需实现 collect(target)。"""

    async def setup(self, config: dict[str, Any] | None = None) -> None:
        await super().setup(config)
        settings = get_settings()
        yt_cfg = settings.get("youtube", {})

        api_keys: list[str] = list(yt_cfg.get("api_keys", []))
        api_keys = [k for k in api_keys if k and not k.startswith("${")]

        if not api_keys:
            raise ValueError(
                "至少需要一个有效的 YouTube API Key，"
                "请在 .env 中设置 YOUTUBE_API_KEY_1 等并"
                "在 settings.yaml 中引用"
            )

        # 向下兼容：构造时传入的单 Key
        single_key = str(self.config.get("api_key", "")).strip()
        if single_key and not single_key.startswith("${"):
            api_keys.insert(0, single_key)

        self._pool = YouTubeClientPool(
            api_keys=api_keys,
            request_delay=float(yt_cfg.get("request_delay", 0.1)),
            request_timeout=int(yt_cfg.get("request_timeout", 30)),
            api_base_url=str(
                yt_cfg.get("api_base_url", "https://youtube.googleapis.com/youtube/v3")
            ),
        )
        await self._pool.setup()

    async def teardown(self) -> None:
        if self._pool is not None:
            await self._pool.close()
        await super().teardown()
