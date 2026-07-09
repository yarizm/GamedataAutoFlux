"""YouTube 博主信息采集器。"""

from __future__ import annotations

from typing import Any

from src.collectors.base import CollectResult, CollectTarget
from src.collectors.youtube import api
from src.collectors.youtube.base import BaseYouTubeCollector
from src.core.errors import ErrorCode
from src.core.registry import registry


@registry.register("collector", "youtube_profiles")
class YouTubeProfileCollector(BaseYouTubeCollector):
    """采集 YouTube 博主/频道元数据（名称、ID、粉丝数、简介）。"""

    async def collect(self, target: CollectTarget) -> CollectResult:
        input_url = str(target.params.get("channel_url", target.name))
        channel_id = str(target.params.get("channel_id", ""))
        handle = str(target.params.get("handle", ""))

        # 解析 URL 获取 hint
        hint_type, hint_value = "", ""
        if channel_id:
            hint_type = "id"
        elif handle:
            hint_type = "handle"
        else:
            hint_type, hint_value = api.parse_channel_hint(input_url)
            if hint_type == "id":
                channel_id = hint_value
            elif hint_type == "handle":
                handle = hint_value

        # 构建 API 参数
        kwargs: dict[str, Any] = {}
        if channel_id:
            kwargs["id"] = channel_id
        elif handle:
            kwargs["forHandle"] = handle
        else:
            return CollectResult(
                target=target,
                success=False,
                error=f"无法解析 YouTube 频道 URL: {input_url}",
                error_code="invalid_params",
                data={
                    "collector": "youtube_profiles",
                    "source": "youtube_api",
                    "input_url": input_url,
                    "resolution_status": "failed",
                    "error_code": "invalid_url",
                },
            )

        data = await api.channels_list(self._pool, **kwargs)
        items = data.get("items", [])

        if not items:
            return CollectResult(
                target=target,
                success=False,
                error=f"频道未找到: {input_url}",
                error_code=ErrorCode.empty_data.value,
                data={
                    "collector": "youtube_profiles",
                    "source": "youtube_api",
                    "input_url": input_url,
                    "channel_url": input_url,
                    "author_name": "未找到",
                    "channel_id": channel_id or handle or "",
                    "subscriber_count": "",
                    "description": "",
                    "resolution_method": hint_type or "unknown",
                    "resolution_status": "not_found",
                    "error_code": "not_found",
                },
            )

        snippet = items[0].get("snippet", {})
        stats = items[0].get("statistics", {})
        resolved_id = items[0].get("id", "")
        description = (
            (snippet.get("description") or "")
            .replace("\n", " | ")
            .replace("\r", "")
            .strip()
        )

        return CollectResult(
            target=target,
            success=True,
            data={
                "collector": "youtube_profiles",
                "source": "youtube_api",
                "input_url": input_url,
                "author_name": snippet.get("title", ""),
                "channel_id": resolved_id,
                "channel_url": api.build_channel_url(resolved_id),
                "subscriber_count": stats.get("subscriberCount", "已隐藏"),
                "description": description,
                "resolution_method": hint_type or "channel_id",
                "resolution_status": "success",
                "error_code": "",
            },
            metadata={"collector": "youtube_profiles"},
        )
