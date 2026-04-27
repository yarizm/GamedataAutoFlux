"""
Steam 官方 API 客户端

封装 Steam Web API 和 Store API 的 6 个数据端点:
  1. appdetails       — 游戏详情/价格/标签 (Store API, 无需 key)
  2. GetNumberOfCurrentPlayers — 当前在线人数 (无需 key)
  3. appreviews       — 评论分页采集 (Store API, 无需 key)
  4. GetGlobalAchievementPercentagesForApp — 成就完成率 (无需 key)
  5. GetNewsForApp    — 游戏新闻/更新公告 (无需 key)
  6. GetAppList       — 全量 Steam 游戏列表 (无需 key)

速率控制:
  - 所有请求间添加可配置延迟 (默认 1.5s)
  - 自动重试 (默认 3 次, 指数退避)
  - 当收到 429 时自动加大延迟
"""

from __future__ import annotations

import asyncio
import re
from typing import Any

import httpx
from loguru import logger


class SteamAPIClient:
    """Steam 官方 API 异步客户端"""

    STORE_BASE = "https://store.steampowered.com"
    API_BASE = "https://api.steampowered.com"

    def __init__(
        self,
        api_key: str = "",
        request_delay: float = 1.5,
        max_retries: int = 3,
        timeout: float = 30.0,
        proxy: str | None = None,
    ):
        self._api_key = api_key
        self._delay = request_delay
        self._max_retries = max_retries
        self._timeout = timeout
        self._proxy = proxy
        self._client: httpx.AsyncClient | None = None

    # ── 生命周期 ──────────────────────────────────────

    async def setup(self) -> None:
        self._client = httpx.AsyncClient(
            timeout=httpx.Timeout(self._timeout),
            proxy=self._proxy,
            headers={
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/126.0.0.0 Safari/537.36"
                ),
                "Accept-Language": "en-US,en;q=0.9,zh-CN;q=0.8",
            },
            follow_redirects=True,
        )

    async def teardown(self) -> None:
        if self._client:
            await self._client.aclose()
            self._client = None

    # ── 公共 API 方法 ────────────────────────────────

    async def get_app_details(self, app_id: str | int) -> dict[str, Any]:
        """
        获取游戏详情 (名称、描述、价格、标签、开发商、发行商、截图等)。

        Endpoint: store.steampowered.com/api/appdetails?appids={app_id}
        """
        url = f"{self.STORE_BASE}/api/appdetails"
        data = await self._request(url, params={"appids": str(app_id)})

        app_data = data.get(str(app_id), {})
        if not app_data.get("success"):
            logger.warning(f"[SteamAPI] appdetails 失败: app_id={app_id}")
            return {}

        raw = app_data.get("data", {})

        # 提取并标准化关键字段
        price_info = raw.get("price_overview", {})
        return {
            "app_id": int(app_id),
            "name": raw.get("name", ""),
            "type": raw.get("type", ""),
            "is_free": raw.get("is_free", False),
            "short_description": raw.get("short_description", ""),
            "developers": raw.get("developers", []),
            "publishers": raw.get("publishers", []),
            "price": {
                "currency": price_info.get("currency", ""),
                "initial": price_info.get("initial", 0),
                "final": price_info.get("final", 0),
                "discount_percent": price_info.get("discount_percent", 0),
                "final_formatted": price_info.get("final_formatted", ""),
            } if price_info else None,
            "platforms": raw.get("platforms", {}),
            "categories": [c.get("description", "") for c in raw.get("categories", [])],
            "genres": [g.get("description", "") for g in raw.get("genres", [])],
            "release_date": raw.get("release_date", {}),
            "header_image": raw.get("header_image", ""),
            "total_recommendations": raw.get("recommendations", {}).get("total", 0),
            "total_achievements": raw.get("achievements", {}).get("total", 0),
            "supported_languages": _strip_html(raw.get("supported_languages", "")),
            "website": raw.get("website", ""),
        }

    async def get_current_players(self, app_id: str | int) -> int:
        """
        获取当前在线人数。

        Endpoint: ISteamUserStats/GetNumberOfCurrentPlayers/v1/
        """
        url = f"{self.API_BASE}/ISteamUserStats/GetNumberOfCurrentPlayers/v1/"
        data = await self._request(url, params={"appid": str(app_id)})
        return data.get("response", {}).get("player_count", 0)

    async def get_reviews(
        self,
        app_id: str | int,
        max_reviews: int = 200,
        language: str = "all",
    ) -> dict[str, Any]:
        """
        分页采集评论 (支持 cursor 翻页)。

        Endpoint: store.steampowered.com/appreviews/{app_id}?json=1
        """
        url = f"{self.STORE_BASE}/appreviews/{app_id}"
        all_reviews: list[dict] = []
        cursor = "*"
        pages = 0
        max_pages = max(1, max_reviews // 100 + 1)

        while len(all_reviews) < max_reviews and pages < max_pages:
            params = {
                "json": "1",
                "filter": "recent",
                "language": language,
                "review_type": "all",
                "purchase_type": "all",
                "num_per_page": "100",
                "cursor": cursor,
            }
            data = await self._request(url, params=params)

            reviews = data.get("reviews", [])
            if not reviews:
                break

            for r in reviews:
                all_reviews.append({
                    "recommendationid": r.get("recommendationid", ""),
                    "author_steamid": r.get("author", {}).get("steamid", ""),
                    "author_playtime": r.get("author", {}).get("playtime_forever", 0),
                    "voted_up": r.get("voted_up", False),
                    "review_text": r.get("review", "")[:500],  # 截断
                    "votes_up": r.get("votes_up", 0),
                    "votes_funny": r.get("votes_funny", 0),
                    "timestamp_created": r.get("timestamp_created", 0),
                    "language": r.get("language", ""),
                })

            cursor = data.get("cursor", "")
            if not cursor:
                break
            pages += 1

        # 汇总统计
        query_summary = data.get("query_summary", {}) if 'data' in dir() else {}
        total_reviews_val = query_summary.get("total_reviews", len(all_reviews))
        total_positive_val = query_summary.get("total_positive", 0)
        review_score_percent = 0
        if total_reviews_val > 0:
            review_score_percent = round((total_positive_val / total_reviews_val) * 100)
            
        return {
            "total_reviews": total_reviews_val,
            "total_positive": total_positive_val,
            "total_negative": query_summary.get("total_negative", 0),
            "review_score_desc": query_summary.get("review_score_desc", ""),
            "review_score_percent": review_score_percent,
            "review_count_fetched": len(all_reviews),
            "reviews": all_reviews[:max_reviews],
        }

    async def get_achievements(self, app_id: str | int) -> list[dict[str, Any]]:
        """
        获取全局成就完成率。

        Endpoint: ISteamUserStats/GetGlobalAchievementPercentagesForApp/v2/
        """
        url = (
            f"{self.API_BASE}/ISteamUserStats"
            f"/GetGlobalAchievementPercentagesForApp/v2/"
        )
        data = await self._request(url, params={"gameid": str(app_id)})
        achievements = (
            data.get("achievementpercentages", {}).get("achievements", [])
        )
        return [
            {"name": a.get("name", ""), "percent": round(float(a.get("percent", 0)), 2)}
            for a in achievements[:50]  # 取前50
        ]

    async def get_news(
        self, app_id: str | int, count: int = 10
    ) -> list[dict[str, Any]]:
        """
        获取游戏新闻/更新公告。

        Endpoint: ISteamNews/GetNewsForApp/v2/
        """
        url = f"{self.API_BASE}/ISteamNews/GetNewsForApp/v2/"
        params: dict[str, Any] = {
            "appid": str(app_id),
            "count": str(count),
            "maxlength": "500",
        }
        if self._api_key:
            params["key"] = self._api_key
        data = await self._request(url, params=params)

        news_items = data.get("appnews", {}).get("newsitems", [])
        return [
            {
                "gid": n.get("gid", ""),
                "title": n.get("title", ""),
                "url": n.get("url", ""),
                "author": n.get("author", ""),
                "contents": n.get("contents", "")[:500],
                "date": n.get("date", 0),
                "feed_name": n.get("feedname", ""),
            }
            for n in news_items
        ]

    async def get_app_list(self) -> list[dict[str, Any]]:
        """
        获取全量 Steam 游戏列表 (用于批量任务的 app_id 检索)。

        Endpoint: ISteamApps/GetAppList/v2/
        """
        url = f"{self.API_BASE}/ISteamApps/GetAppList/v2/"
        data = await self._request(url, params={})
        apps = data.get("applist", {}).get("apps", [])
        return apps  # [{appid: int, name: str}, ...]

    async def resolve_app_id(self, game_name: str) -> str | None:
        """按游戏名模糊匹配 app_id。"""
        apps = await self.get_app_list()
        name_lower = game_name.lower().strip()
        # 精确匹配优先
        for app in apps:
            if app.get("name", "").lower().strip() == name_lower:
                return str(app["appid"])
        # 包含匹配
        for app in apps:
            if name_lower in app.get("name", "").lower():
                return str(app["appid"])
        return None

    async def collect_all(self, app_id: str | int) -> dict[str, Any]:
        """
        一次性采集所有官方 API 数据。
        每个端点独立 try/except，部分失败不影响整体。
        """
        result: dict[str, Any] = {"source": "steam_api", "app_id": int(app_id)}

        # 并发采集所有端点
        tasks = {
            "details": self.get_app_details(app_id),
            "current_players": self.get_current_players(app_id),
            "reviews": self.get_reviews(app_id),
            "achievements": self.get_achievements(app_id),
            "news": self.get_news(app_id, count=5),
        }

        for key, coro in tasks.items():
            try:
                result[key] = await coro
                logger.debug(f"[SteamAPI] ✓ {key} (app_id={app_id})")
            except Exception as e:
                logger.warning(f"[SteamAPI] ✗ {key} 失败: {e}")
                result[key] = None
            # 请求间延迟
            await asyncio.sleep(self._delay)

        return result

    # ── 内部工具 ──────────────────────────────────────

    async def _request(
        self, url: str, params: dict[str, Any]
    ) -> dict[str, Any]:
        """带重试和限速的 HTTP GET。"""
        if not self._client:
            await self.setup()

        for attempt in range(1, self._max_retries + 1):
            try:
                resp = await self._client.get(url, params=params)

                if resp.status_code == 429:
                    wait = min(2 ** attempt * 2, 30)
                    logger.warning(
                        f"[SteamAPI] 429 限速, 等待 {wait}s (attempt {attempt})"
                    )
                    await asyncio.sleep(wait)
                    continue

                resp.raise_for_status()
                return resp.json()

            except httpx.HTTPStatusError as e:
                logger.warning(
                    f"[SteamAPI] HTTP {e.response.status_code} @ {url} "
                    f"(attempt {attempt}/{self._max_retries})"
                )
                if attempt == self._max_retries:
                    raise
                await asyncio.sleep(2 ** attempt)

            except (httpx.ConnectError, httpx.ReadTimeout) as e:
                logger.warning(
                    f"[SteamAPI] 网络错误: {e} (attempt {attempt}/{self._max_retries})"
                )
                if attempt == self._max_retries:
                    raise
                await asyncio.sleep(2 ** attempt)

        return {}


def _strip_html(text: str) -> str:
    """去除 HTML 标签"""
    return re.sub(r"<[^>]+>", "", text).strip() if text else ""
