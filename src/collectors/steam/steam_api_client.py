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
from datetime import date, datetime, timedelta, timezone
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
        review_trend_days: int = 90,
        max_review_trend_reviews: int = 10000,
        review_trend_mode: str = "summary",
        review_summary_concurrency: int = 4,
    ) -> dict[str, Any]:
        """
        分页采集评论 (支持 cursor 翻页)。

        Endpoint: store.steampowered.com/appreviews/{app_id}?json=1
        """
        logger.info(
            "[SteamAPI] reviews start app_id={} mode={} trend_days={}",
            app_id,
            review_trend_mode,
            review_trend_days,
        )
        all_reviews, query_summary, _ = await self._fetch_review_pages(
            app_id,
            max_reviews=max_reviews,
            language=language,
            filter_name="recent",
        )
        logger.debug("[SteamAPI] reviews sample fetched app_id={} count={}", app_id, len(all_reviews))

        # 汇总统计
        try:
            overall_summary = await self.get_review_summary(app_id, filter_name="all", language=language)
        except Exception as exc:
            logger.warning(f"[SteamAPI] overall review summary failed: {exc}")
            overall_summary = {}
        try:
            recent_30d_summary = await self.get_review_summary(
                app_id,
                filter_name="recent",
                language=language,
                day_range=30,
            )
        except Exception as exc:
            logger.warning(f"[SteamAPI] recent 30d review summary failed: {exc}")
            recent_30d_summary = {}
        review_trend_mode = str(review_trend_mode or "summary").lower()
        if review_trend_mode == "summary":
            try:
                trend_rows, trend_summary = await self.get_review_trend_from_histogram(
                    app_id,
                    days=review_trend_days,
                    language=language,
                )
            except Exception as exc:
                logger.warning(f"[SteamAPI] review histogram trend failed: {exc}")
                trend_rows, trend_summary = await self.get_review_trend_from_summaries(
                    app_id,
                    days=review_trend_days,
                    language=language,
                    concurrency=review_summary_concurrency,
                )
        elif review_trend_mode == "off":
            trend_rows, trend_summary = [], {
                "days": review_trend_days,
                "total_reviews": 0,
                "reviews_fetched": 0,
                "complete": True,
                "source": "disabled",
            }
        else:
            try:
                trend_summary = await self.get_review_summary(
                    app_id,
                    filter_name="all",
                    language=language,
                    day_range=review_trend_days,
                )
            except Exception as exc:
                logger.warning(f"[SteamAPI] {review_trend_days}d review summary failed: {exc}")
                trend_summary = {}

            trend_total = int(trend_summary.get("total_reviews") or 0)
            trend_fetch_limit = max(max_review_trend_reviews, 0)
            if trend_total and trend_total < trend_fetch_limit:
                trend_fetch_limit = trend_total
            trend_reviews, _, trend_exhausted = await self._fetch_review_pages(
                app_id,
                max_reviews=trend_fetch_limit,
                language=language,
                filter_name="recent",
                day_range=review_trend_days,
            ) if trend_fetch_limit else ([], {}, True)
            trend_complete = bool(trend_total == 0 or len(trend_reviews) >= trend_total or trend_exhausted)
            trend_rows = _build_review_trend(trend_reviews, days=review_trend_days, fill_missing=True)
            trend_summary = {
                "days": review_trend_days,
                "total_reviews": trend_total,
                "reviews_fetched": len(trend_reviews),
                "complete": trend_complete,
                "source": f"appreviews?filter=recent&day_range={review_trend_days}",
            }

        total_reviews_val = query_summary.get("total_reviews", len(all_reviews))
        total_positive_val = query_summary.get("total_positive", 0)
        review_score_percent = 0
        if total_reviews_val > 0:
            review_score_percent = round((total_positive_val / total_reviews_val) * 100)
        logger.info(
            "[SteamAPI] reviews done app_id={} overall={} recent30={} trend_points={}",
            app_id,
            overall_summary.get("review_score_percent"),
            recent_30d_summary.get("review_score_percent"),
            len(trend_rows),
        )
            
        return {
            "total_reviews": total_reviews_val,
            "total_positive": total_positive_val,
            "total_negative": query_summary.get("total_negative", 0),
            "review_score_desc": query_summary.get("review_score_desc", ""),
            "review_score_percent": review_score_percent,
            "review_count_fetched": len(all_reviews),
            "reviews": all_reviews[:max_reviews],
            "overall_summary": overall_summary,
            "recent_30d_summary": recent_30d_summary,
            "review_trend_90d": trend_rows,
            "review_trend_90d_summary": trend_summary,
        }

    async def _fetch_review_pages(
        self,
        app_id: str | int,
        *,
        max_reviews: int,
        language: str,
        filter_name: str = "recent",
        day_range: int | None = None,
    ) -> tuple[list[dict[str, Any]], dict[str, Any], bool]:
        url = f"{self.STORE_BASE}/appreviews/{app_id}"
        all_reviews: list[dict[str, Any]] = []
        query_summary: dict[str, Any] = {}
        cursor = "*"
        seen_cursors: set[str] = set()
        max_pages = max(1, (max_reviews + 99) // 100)

        for _ in range(max_pages):
            params: dict[str, Any] = {
                "json": "1",
                "filter": filter_name,
                "language": language,
                "review_type": "all",
                "purchase_type": "all",
                "num_per_page": "100",
                "cursor": cursor,
            }
            if day_range is not None:
                params["day_range"] = str(day_range)

            data = await self._request(url, params=params)
            if not query_summary:
                query_summary = data.get("query_summary", {}) if isinstance(data, dict) else {}

            reviews = data.get("reviews", []) if isinstance(data, dict) else []
            if not reviews:
                return all_reviews, query_summary, True

            for r in reviews:
                all_reviews.append({
                    "recommendationid": r.get("recommendationid", ""),
                    "author_steamid": r.get("author", {}).get("steamid", ""),
                    "author_playtime": r.get("author", {}).get("playtime_forever", 0),
                    "voted_up": r.get("voted_up", False),
                    "review_text": r.get("review", "")[:500],
                    "votes_up": r.get("votes_up", 0),
                    "votes_funny": r.get("votes_funny", 0),
                    "timestamp_created": r.get("timestamp_created", 0),
                    "language": r.get("language", ""),
                })
                if len(all_reviews) >= max_reviews:
                    return all_reviews, query_summary, False

            next_cursor = data.get("cursor", "") if isinstance(data, dict) else ""
            if not next_cursor or next_cursor in seen_cursors:
                return all_reviews, query_summary, True
            seen_cursors.add(cursor)
            cursor = next_cursor
            await asyncio.sleep(self._delay)

        return all_reviews, query_summary, False

    async def get_review_summary(
        self,
        app_id: str | int,
        *,
        filter_name: str,
        language: str = "all",
        day_range: int | None = None,
    ) -> dict[str, Any]:
        """Fetch only Steam review summary statistics for a given filter."""
        url = f"{self.STORE_BASE}/appreviews/{app_id}"
        params: dict[str, Any] = {
            "json": "1",
            "filter": filter_name,
            "language": language,
            "review_type": "all",
            "purchase_type": "all",
            "num_per_page": "0",
        }
        if day_range is not None:
            params["day_range"] = str(day_range)
        data = await self._request(url, params=params)
        summary = data.get("query_summary", {}) if isinstance(data, dict) else {}
        total_reviews = int(summary.get("total_reviews") or 0)
        total_positive = int(summary.get("total_positive") or 0)
        total_negative = int(summary.get("total_negative") or 0)
        percent = round((total_positive / total_reviews) * 100, 2) if total_reviews else None
        return {
            "filter": filter_name,
            "day_range": day_range,
            "total_reviews": total_reviews,
            "total_positive": total_positive,
            "total_negative": total_negative,
            "review_score_desc": summary.get("review_score_desc", ""),
            "review_score_percent": percent,
        }

    async def get_review_trend_from_histogram(
        self,
        app_id: str | int,
        *,
        days: int = 90,
        language: str = "english",
    ) -> tuple[list[dict[str, Any]], dict[str, Any]]:
        """Build review positive-rate trend from Steam's review histogram endpoint."""
        url = f"{self.STORE_BASE}/appreviewhistogram/{app_id}"
        data = await self._request(
            url,
            params={
                "l": _steam_histogram_language(language),
                "review_score_preference": "0",
            },
        )
        rows = _build_review_trend_from_histogram(data, days=days)
        logger.info("[SteamAPI] review histogram trend done app_id={} points={}", app_id, len(rows))
        return rows, {
            "days": days,
            "total_reviews": sum(int(row.get("total") or 0) for row in rows),
            "reviews_fetched": 0,
            "complete": len(rows) >= days,
            "source": "appreviewhistogram",
            "mode": "histogram",
            "estimated_daily": True,
        }

    async def get_review_trend_from_summaries(
        self,
        app_id: str | int,
        *,
        days: int = 90,
        language: str = "all",
        concurrency: int = 4,
    ) -> tuple[list[dict[str, Any]], dict[str, Any]]:
        """Build daily review positive-rate trend from cumulative Steam summaries."""
        concurrency = max(1, min(int(concurrency or 1), days))
        logger.info(
            "[SteamAPI] review summary trend start app_id={} days={} concurrency={}",
            app_id,
            days,
            concurrency,
        )
        semaphore = asyncio.Semaphore(concurrency)
        completed = 0

        async def fetch_day(day_range: int) -> dict[str, Any]:
            nonlocal completed
            async with semaphore:
                if day_range > 1:
                    await asyncio.sleep(self._delay / concurrency)
                summary = await self.get_review_summary(
                    app_id,
                    filter_name="recent",
                    language=language,
                    day_range=day_range,
                )
                completed += 1
                if completed == 1 or completed % 10 == 0 or completed == days:
                    logger.info(
                        "[SteamAPI] review summary trend progress app_id={} {}/{}",
                        app_id,
                        completed,
                        days,
                    )
                return summary

        cumulative = []
        for summary in await asyncio.gather(*(fetch_day(day_range) for day_range in range(1, days + 1))):
            cumulative.append(summary)

        today = datetime.now(timezone.utc).date()
        rows: list[dict[str, Any]] = []
        by_range = {
            int(summary.get("day_range") or index): summary
            for index, summary in enumerate(cumulative, start=1)
            if isinstance(summary, dict)
        }
        zero_summary = {"total_positive": 0, "total_negative": 0, "total_reviews": 0}
        for age in range(days, 0, -1):
            summary = by_range.get(age, zero_summary)
            previous = by_range.get(age - 1, zero_summary)
            total_positive = int(summary.get("total_positive") or 0)
            total_negative = int(summary.get("total_negative") or 0)
            total_reviews = int(summary.get("total_reviews") or 0)
            previous_positive = int(previous.get("total_positive") or 0)
            previous_negative = int(previous.get("total_negative") or 0)
            previous_total = int(previous.get("total_reviews") or 0)
            positive = max(total_positive - previous_positive, 0)
            negative = max(total_negative - previous_negative, 0)
            total = max(total_reviews - previous_total, positive + negative)
            day = today - timedelta(days=age - 1)
            rows.append(
                {
                    "date": day.isoformat(),
                    "positive": positive,
                    "negative": negative,
                    "total": total,
                    "positive_rate": round((positive / total) * 100, 2) if total else None,
                }
            )
            previous_positive = total_positive
            previous_negative = total_negative
            previous_total = total_reviews

        latest = cumulative[-1] if cumulative else {}
        logger.info("[SteamAPI] review summary trend done app_id={} points={}", app_id, len(rows))
        return rows, {
            "days": days,
            "total_reviews": latest.get("total_reviews", 0),
            "reviews_fetched": 0,
            "complete": True,
            "source": f"appreviews?filter=recent&day_range=1..{days}&num_per_page=0",
            "mode": "summary",
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

    async def collect_all(
        self,
        app_id: str | int,
        *,
        max_reviews: int = 200,
        review_language: str = "all",
        review_trend_days: int = 90,
        max_review_trend_reviews: int = 10000,
        review_trend_mode: str = "summary",
        review_summary_concurrency: int = 4,
    ) -> dict[str, Any]:
        """
        一次性采集所有官方 API 数据。
        每个端点独立 try/except，部分失败不影响整体。
        """
        result: dict[str, Any] = {"source": "steam_api", "app_id": int(app_id)}

        # 并发采集所有端点
        tasks = {
            "details": self.get_app_details(app_id),
            "current_players": self.get_current_players(app_id),
            "reviews": self.get_reviews(
                app_id,
                max_reviews=max_reviews,
                language=review_language,
                review_trend_days=review_trend_days,
                max_review_trend_reviews=max_review_trend_reviews,
                review_trend_mode=review_trend_mode,
                review_summary_concurrency=review_summary_concurrency,
            ),
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


def _build_review_trend(
    reviews: list[dict[str, Any]],
    days: int = 90,
    *,
    fill_missing: bool = False,
) -> list[dict[str, Any]]:
    """Build a daily positive-rate series from fetched recent reviews."""
    today = datetime.now(timezone.utc).date()
    cutoff_date = today - timedelta(days=days - 1)
    cutoff = datetime.combine(cutoff_date, datetime.min.time(), tzinfo=timezone.utc)
    buckets: dict[str, dict[str, int]] = {}
    for review in reviews:
        timestamp = review.get("timestamp_created")
        if not isinstance(timestamp, (int, float)):
            continue
        created_at = datetime.fromtimestamp(timestamp, tz=timezone.utc)
        if created_at < cutoff:
            continue
        day = created_at.date().isoformat()
        bucket = buckets.setdefault(day, {"positive": 0, "negative": 0, "total": 0})
        bucket["total"] += 1
        if review.get("voted_up"):
            bucket["positive"] += 1
        else:
            bucket["negative"] += 1

    rows: list[dict[str, Any]] = []
    day_keys = (
        [(cutoff_date + timedelta(days=index)).isoformat() for index in range(days)]
        if fill_missing
        else sorted(buckets)
    )
    for day in day_keys:
        bucket = buckets.get(day, {"positive": 0, "negative": 0, "total": 0})
        total = bucket["total"]
        rows.append(
            {
                "date": day,
                "positive": bucket["positive"],
                "negative": bucket["negative"],
                "total": total,
                "positive_rate": round((bucket["positive"] / total) * 100, 2) if total else None,
            }
        )
    return rows


def _build_review_trend_from_histogram(payload: dict[str, Any], *, days: int = 90) -> list[dict[str, Any]]:
    results = payload.get("results", {}) if isinstance(payload, dict) else {}
    if not isinstance(results, dict):
        return []

    recent = results.get("recent") if isinstance(results.get("recent"), list) else []
    rollups = results.get("rollups") if isinstance(results.get("rollups"), list) else []
    daily: dict[date, dict[str, int]] = {}
    recent_dates: set[date] = set()

    for item in recent:
        point = _histogram_point(item)
        if point is None:
            continue
        day, up, down = point
        daily[day] = {"positive": up, "negative": down}
        recent_dates.add(day)

    rollup_points = [point for point in (_histogram_point(item) for item in rollups) if point is not None]
    rollup_points.sort(key=lambda item: item[0])

    end_date = max(daily.keys(), default=None)
    if end_date is None:
        end_ts = results.get("end_date")
        end_date = _timestamp_to_date(end_ts) if end_ts else datetime.now(timezone.utc).date()
    start_date = end_date - timedelta(days=max(days, 1) - 1)
    target_dates = {start_date + timedelta(days=index) for index in range(max(days, 1))}

    for index, (bucket_start, up, down) in enumerate(rollup_points):
        next_start = rollup_points[index + 1][0] if index + 1 < len(rollup_points) else bucket_start + timedelta(days=7)
        bucket_end = min(next_start - timedelta(days=1), bucket_start + timedelta(days=6))
        bucket_dates = [
            bucket_start + timedelta(days=offset)
            for offset in range((bucket_end - bucket_start).days + 1)
            if bucket_start + timedelta(days=offset) in target_dates
        ]
        if not bucket_dates:
            continue

        known_up = sum(daily.get(day, {}).get("positive", 0) for day in bucket_dates if day in recent_dates)
        known_down = sum(daily.get(day, {}).get("negative", 0) for day in bucket_dates if day in recent_dates)
        missing_dates = [day for day in bucket_dates if day not in daily]
        if not missing_dates:
            continue

        remaining_up = max(up - known_up, 0)
        remaining_down = max(down - known_down, 0)
        for day, day_up, day_down in zip(
            missing_dates,
            _distribute_count(remaining_up, len(missing_dates)),
            _distribute_count(remaining_down, len(missing_dates)),
        ):
            daily[day] = {"positive": day_up, "negative": day_down}

    rows: list[dict[str, Any]] = []
    for index in range(max(days, 1)):
        day = start_date + timedelta(days=index)
        counts = daily.get(day, {"positive": 0, "negative": 0})
        positive = int(counts.get("positive") or 0)
        negative = int(counts.get("negative") or 0)
        total = positive + negative
        rows.append(
            {
                "date": day.isoformat(),
                "positive": positive,
                "negative": negative,
                "total": total,
                "positive_rate": round((positive / total) * 100, 2) if total else None,
            }
        )
    return rows


def _histogram_point(item: Any) -> tuple[date, int, int] | None:
    if not isinstance(item, dict):
        return None
    day = _timestamp_to_date(item.get("date"))
    if day is None:
        return None
    return (
        day,
        int(item.get("recommendations_up") or 0),
        int(item.get("recommendations_down") or 0),
    )


def _timestamp_to_date(value: Any) -> date | None:
    try:
        return datetime.fromtimestamp(int(value), tz=timezone.utc).date()
    except Exception:
        return None


def _distribute_count(total: int, slots: int) -> list[int]:
    if slots <= 0:
        return []
    base, remainder = divmod(max(int(total or 0), 0), slots)
    return [base + (1 if index < remainder else 0) for index in range(slots)]


def _steam_histogram_language(language: str) -> str:
    if not language or language == "all":
        return "english"
    return language
