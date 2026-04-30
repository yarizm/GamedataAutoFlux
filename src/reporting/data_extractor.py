"""
数据提取器。

从不同采集器的嵌套 JSON 中提取干净的结构化表格数据，
供 Excel 导出器使用。

每个采集器有专门的提取函数，输出统一的 dict 列表（每个 dict = 一行）。
"""

from __future__ import annotations

import copy
import re
from datetime import date, datetime, timedelta, timezone
from typing import Any

from loguru import logger

from src.reporting.report_templates import normalize_collector


class ExtractedData:
    """提取后的结构化数据容器。"""

    def __init__(self):
        self.overview: list[dict[str, Any]] = []    # 游戏概览行
        self.reviews: list[dict[str, Any]] = []     # 评论明细行
        self.trends: list[dict[str, Any]] = []      # 趋势数据行
        self.related_queries: list[dict[str, Any]] = []  # 相关搜索词
        self.steam_player_peaks: list[dict[str, Any]] = []  # Steam 在线峰值
        self.steam_monthly_peaks: list[dict[str, Any]] = []  # SteamDB 月峰值
        self.google_trends: list[dict[str, Any]] = []       # Google Trends 时序
        self.monitor_metrics: list[dict[str, Any]] = []     # Monitor 指标明细
        self.events: list[dict[str, Any]] = []              # 游戏新闻/版本/活动事件
        self.community_discussions: list[dict[str, Any]] = []  # 社区讨论
        self.raw_sources: list[dict[str, Any]] = []         # 原始 JSON 附录
        self.source_coverage: dict[str, int] = {}           # collector -> record count


def extract_from_records(
    records: list[dict[str, Any]],
    record_keys: list[str] | None = None,
    metadata_list: list[dict[str, Any]] | None = None,
) -> ExtractedData:
    """
    从存储记录列表中提取结构化数据。

    Args:
        records: StorageRecord.data 列表（已从 JSON 加载的 dict）

    Returns:
        ExtractedData 分组数据
    """
    result = ExtractedData()
    canonical_names = _build_canonical_game_names(records, metadata_list)

    for index, record_data in enumerate(records):
        if not isinstance(record_data, dict):
            continue

        record_data = _apply_canonical_game_name(
            record_data,
            canonical_names,
            metadata_list[index] if metadata_list and index < len(metadata_list) else {},
        )
        collector = normalize_collector(_detect_collector(record_data))
        result.source_coverage[collector] = result.source_coverage.get(collector, 0) + 1
        result.raw_sources.append(
            {
                "key": record_keys[index] if record_keys and index < len(record_keys) else f"record_{index + 1}",
                "collector": collector,
                "game_name": _extract_game_name(record_data),
                "metadata": metadata_list[index] if metadata_list and index < len(metadata_list) else {},
                "data": record_data,
            }
        )
        event_count_before = len(result.events)
        try:
            if collector == "steam":
                _extract_steam(record_data, result)
            elif collector == "steam_discussions":
                _extract_steam_discussions(record_data, result)
            elif collector == "taptap":
                _extract_taptap(record_data, result)
            elif collector == "gtrends":
                _extract_gtrends(record_data, result)
            elif collector == "monitor":
                _extract_monitor(record_data, result)
            elif collector == "events":
                _extract_events(record_data, result)
            elif collector == "qimai":
                _extract_qimai(record_data, result)
            elif collector == "official_site":
                _extract_official_site(record_data, result)
            else:
                _extract_generic(record_data, result)
        except Exception as exc:
            logger.warning(f"[DataExtractor] 提取失败 (collector={collector}): {exc}")
        if len(result.events) > event_count_before:
            result.source_coverage["events"] = result.source_coverage.get("events", 0) + 1

    return result


def _detect_collector(data: dict[str, Any]) -> str:
    """识别数据来源的采集器。"""
    # 直接标记
    collector = data.get("collector", "")
    if collector:
        return collector

    # 通过 content 嵌套查找
    content = data.get("content", {})
    if isinstance(content, dict):
        collector = content.get("collector", "")
        if collector:
            return collector

    # 特征检测
    if "steamdb" in data or "steam_api" in data:
        return "steam"
    if "snapshot" in data and "current_players" in data.get("snapshot", {}):
        return "steam"
    if "qimai" in data:
        return "qimai"
    if data.get("collector") == "official_site":
        return "official_site"
    if "official_url" in data and "news" in data:
        return "official_site"
    if "discussions" in data:
        return "steam_discussions"
    if "reviews_summary" in data or "availability" in data:
        return "taptap"
    if "trend_history" in data:
        return "gtrends"
    if "events" in data or "event_history" in data:
        return "events"
    if "monitor_metrics" in data or "metrics" in data:
        return "monitor"

    return "unknown"


# ==================== Qimai 提取 ====================

def _extract_official_site(data: dict[str, Any], result: ExtractedData) -> None:
    """从游戏官网采集结果中提取官网动态、版本更新和活动。"""
    snapshot = data.get("snapshot", {}) if isinstance(data.get("snapshot"), dict) else {}
    game_name = data.get("game_name") or snapshot.get("name", "")
    official_url = data.get("official_url", "")
    news_items = _list_items((data.get("news") or {}).get("items", []))
    patch_items = _list_items((data.get("patch_notes") or {}).get("items", []))
    event_items = _list_items((data.get("events") or {}).get("items", []))

    result.overview.append(
        {
            "游戏名": game_name or "未知",
            "数据来源": "官方网站",
            "官网URL": official_url,
            "官网新闻数": len(news_items),
            "官网版本更新数": len(patch_items),
            "官网活动数": len(event_items),
            "最新官网动态": snapshot.get("latest_news_title", ""),
            "最新动态时间": snapshot.get("latest_news_date", ""),
            "采集时间": _extract_time(data),
        }
    )

    def append_event(item: dict[str, Any], fallback_type: str) -> None:
        result.events.append(
            {
                "游戏名": game_name,
                "App ID": data.get("app_id", ""),
                "日期": item.get("date", ""),
                "事件类型": item.get("category") or item.get("type") or fallback_type,
                "标题": item.get("title", ""),
                "摘要": _truncate(item.get("summary") or item.get("content", ""), 800),
                "来源": "官方网站",
                "作者/来源名": "official_site",
                "URL": item.get("url", ""),
                "原始ID": item.get("id", ""),
            }
        )

    for item in news_items[:200]:
        append_event(item, "官网动态")
        if item.get("date"):
            result.trends.append(
                {
                    "游戏名": game_name,
                    "数据源": "官方网站",
                    "指标": "官网动态",
                    "日期": item.get("date", ""),
                    "值": 1,
                    "标题": item.get("title", ""),
                    "URL": item.get("url", ""),
                }
            )
    for item in patch_items[:200]:
        append_event(item, "版本更新")
    for item in event_items[:200]:
        append_event(item, "官网活动")


def _list_items(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    return [item for item in value if isinstance(item, dict)]


def _extract_qimai(data: dict[str, Any], result: ExtractedData) -> None:
    """从七麦数据中提取结构化字段。"""
    game_name = data.get("game_name", "")
    snapshot = data.get("snapshot", {})
    qimai_data = data.get("qimai", {})
    api_urls = qimai_data.get("api_urls", []) if isinstance(qimai_data, dict) else []
    
    overview_row = {
        "游戏名": game_name or snapshot.get("name", "未知"),
        "数据来源": "Qimai(AppStore)",
        "评论总量": _safe_int(snapshot.get("total_reviews")),
        "评分": snapshot.get("review_score", ""),
        "AppStore免费榜": snapshot.get("free_rank", ""),
        "AppStore畅销榜": snapshot.get("grossing_rank", ""),
        "采集时间": _extract_time(data),
    }
    overview_row.update(
        {
            "Qimai grossing rank CN": _qimai_grossing_rank_cn(qimai_data, snapshot, api_urls),
            "Qimai AppStore rating CN": qimai_data.get("appstore_rating_cn", snapshot.get("appstore_rating_cn", "")),
            "Qimai DAU avg 30d": _qimai_average_value(qimai_data, snapshot, "dau_avg_30d", "dau_trend_90d", api_urls, "appstatus"),
            "Qimai downloads avg 30d": _qimai_average_value(qimai_data, snapshot, "downloads_avg_30d", "downloads_trend_90d", api_urls, "download"),
            "Qimai revenue avg 30d": _qimai_average_value(qimai_data, snapshot, "revenue_avg_30d", "revenue_trend_90d", api_urls, "revenue"),
        }
    )
    
    result.overview.append(overview_row)

    _append_qimai_series(result, game_name, "iOS grossing rank", qimai_data.get("ios_grossing_rank_trend", []), api_urls=api_urls, required_api="rank")
    _append_qimai_series(result, game_name, "AppStore reviews", qimai_data.get("appstore_review_trend", []), api_urls=api_urls, required_api="comment")
    _append_qimai_series(result, game_name, "DAU", qimai_data.get("dau_trend_90d", []), api_urls=api_urls, required_api="appstatus")
    _append_qimai_series(result, game_name, "Downloads", qimai_data.get("downloads_trend_90d", []), api_urls=api_urls, required_api="download")
    _append_qimai_series(result, game_name, "Revenue", qimai_data.get("revenue_trend_90d", []), api_urls=api_urls, required_api="revenue")


def _append_qimai_series(
    result: ExtractedData,
    game_name: str,
    metric: str,
    series: list[dict[str, Any]],
    *,
    api_urls: list[str],
    required_api: str,
) -> None:
    if not isinstance(series, list):
        return
    if not _qimai_series_has_required_source(api_urls, required_api):
        return
    series = _sanitize_qimai_report_series(metric, series)
    if _looks_like_qimai_activity_series(series):
        return
    for point in series:
        if not isinstance(point, dict):
            continue
        result.trends.append(
            {
                "游戏名": game_name,
                "数据源": "Qimai(AppStore)",
                "指标": metric,
                "日期": point.get("date", ""),
                "值": point.get("value", ""),
            }
        )


def _sanitize_qimai_report_series(metric: str, series: list[dict[str, Any]]) -> list[dict[str, Any]]:
    sanitized: list[dict[str, Any]] = []
    for point in series:
        if not isinstance(point, dict):
            continue
        value = _safe_float(point.get("value"))
        if value is None or _looks_like_timestamp_number(value):
            continue
        if metric == "iOS grossing rank" and not (0 < value <= 2000):
            continue
        sanitized.append({**point, "value": int(value) if value.is_integer() else value})
    return sanitized


def _qimai_grossing_rank_cn(qimai_data: dict[str, Any], snapshot: dict[str, Any], api_urls: list[str]) -> Any:
    value = qimai_data.get("grossing_rank_cn", snapshot.get("grossing_rank_cn", ""))
    if value in (None, ""):
        latest_rank = _latest_qimai_series_value(qimai_data.get("ios_grossing_rank_trend", []))
        if latest_rank is not None:
            value = f"#{int(latest_rank)}"
    if value in (None, ""):
        return ""
    free_rank = qimai_data.get("free_rank", snapshot.get("free_rank", ""))
    if not _qimai_series_has_required_source(api_urls, "rank") and _normalize_rank_value(value) == _normalize_rank_value(free_rank):
        return ""
    return value


def _qimai_average_value(
    qimai_data: dict[str, Any],
    snapshot: dict[str, Any],
    average_key: str,
    series_key: str,
    api_urls: list[str],
    required_api: str,
) -> Any:
    series = qimai_data.get(series_key, [])
    if not _qimai_series_has_required_source(api_urls, required_api):
        return ""
    if isinstance(series, list) and _looks_like_qimai_activity_series(series):
        return ""
    return qimai_data.get(average_key, snapshot.get(average_key, ""))


def _normalize_rank_value(value: Any) -> str:
    text = str(value or "").strip()
    match = re.search(r"\d+", text)
    return match.group(0) if match else text


def _latest_qimai_series_value(series: Any) -> float | None:
    if not isinstance(series, list):
        return None
    for point in reversed(series):
        if not isinstance(point, dict):
            continue
        value = _safe_float(point.get("value"))
        if value is not None and not _looks_like_timestamp_number(value):
            return value
    return None


def _qimai_series_has_required_source(api_urls: list[str], required_api: str) -> bool:
    if not isinstance(api_urls, list):
        return False
    tokens = {
        "rank": ("rank", "ranking"),
        "comment": ("comment", "review"),
        "appstatus": ("appstatus", "active", "dau", "status"),
        "download": ("download", "downloads"),
        "revenue": ("revenue", "income", "sales"),
    }.get(required_api, (required_api,))
    joined = "\n".join(str(url).lower() for url in api_urls)
    return any(token in joined for token in tokens)


def _looks_like_qimai_activity_series(series: list[dict[str, Any]]) -> bool:
    values = [point.get("value") for point in series if isinstance(point, dict)]
    dates = [str(point.get("date", "")) for point in series if isinstance(point, dict)]
    return values == [2, 72600, -2493] and dates == ["2026-01-29", "2026-02-10", "2026-04-10"]


def _looks_like_timestamp_number(value: Any) -> bool:
    if isinstance(value, bool):
        return False
    try:
        number = abs(float(value))
    except (TypeError, ValueError):
        return False
    return (1_000_000_000 <= number <= 4_102_444_800) or (1_000_000_000_000 <= number <= 4_102_444_800_000)


# ==================== Steam Community Discussions 提取 ====================

def _extract_steam_discussions(data: dict[str, Any], result: ExtractedData) -> None:
    """从 Steam Community 讨论采集结果中提取可导出的表格行。"""
    game_name = data.get("game_name", "")
    snapshot = data.get("snapshot", {}) if isinstance(data.get("snapshot"), dict) else {}
    discussions = data.get("discussions", {}) if isinstance(data.get("discussions"), dict) else {}
    topics = discussions.get("topics", []) if isinstance(discussions.get("topics"), list) else []

    result.overview.append(
        {
            "游戏名": game_name or snapshot.get("name", "未知"),
            "数据来源": "Steam Community",
            "App ID": snapshot.get("app_id", data.get("app_id", "")),
            "讨论主题数": _safe_int(discussions.get("topic_count", len(topics))),
            "帖子总数": _safe_int(discussions.get("post_count", snapshot.get("post_count"))),
            "最新讨论时间": snapshot.get("latest_topic_at", ""),
            "采集时间": _extract_time(data),
        }
    )

    for topic in topics[:200]:
        if not isinstance(topic, dict):
            continue
        posts = topic.get("posts", []) if isinstance(topic.get("posts"), list) else []
        first_post = posts[0] if posts and isinstance(posts[0], dict) else {}
        result.reviews.append(
            {
                "游戏名": game_name or snapshot.get("name", ""),
                "数据来源": "Steam Community",
                "作者": first_post.get("author", ""),
                "评分": "讨论",
                "评论内容": _truncate(first_post.get("content", ""), 500),
                "点赞数": "",
                "日期": topic.get("created_at") or first_post.get("published_at", ""),
                "主题标题": topic.get("title", ""),
                "主题URL": topic.get("url", ""),
                "回复数": max(len(posts) - 1, 0),
            }
        )
        result.community_discussions.append(
            {
                "游戏名": game_name or snapshot.get("name", ""),
                "App ID": snapshot.get("app_id", data.get("app_id", "")),
                "主题标题": topic.get("title", ""),
                "主题URL": topic.get("url", ""),
                "发帖时间": topic.get("created_at") or first_post.get("published_at", ""),
                "作者": first_post.get("author", ""),
                "首帖内容": _truncate(first_post.get("content", ""), 800),
                "回复数": max(len(posts) - 1, 0),
                "帖子数": len(posts),
            }
        )


# ==================== Steam 提取 ====================

def _extract_steam(data: dict[str, Any], result: ExtractedData) -> None:
    """从 Steam 采集数据中提取结构化字段。"""
    game_name = data.get("game_name", "")
    snapshot = data.get("snapshot", {})
    steam_api = data.get("steam_api", {}) if isinstance(data.get("steam_api"), dict) else {}
    if not snapshot and "content" in data:
        content = data["content"]
        if isinstance(content, dict):
            snapshot = content.get("snapshot", {})
            game_name = game_name or content.get("game_name", "")
            steam_api = content.get("steam_api", steam_api) if isinstance(content.get("steam_api"), dict) else steam_api

    # 概览行
    reviews_data = data.get("reviews", {}) or steam_api.get("reviews", {})
    if not isinstance(reviews_data, dict):
        reviews_data = {}
    overall_summary = reviews_data.get("overall_summary", {}) if isinstance(reviews_data.get("overall_summary"), dict) else {}
    recent_30d_summary = reviews_data.get("recent_30d_summary", {}) if isinstance(reviews_data.get("recent_30d_summary"), dict) else {}
    review_trend_90d = reviews_data.get("review_trend_90d", []) if isinstance(reviews_data.get("review_trend_90d"), list) else []
    review_trend_summary = (
        reviews_data.get("review_trend_90d_summary", {})
        if isinstance(reviews_data.get("review_trend_90d_summary"), dict)
        else {}
    )
    steamdb = data.get("steamdb", {})
    steamdb_present = isinstance(steamdb, dict) and not steamdb.get("error")
    steamdb_review_score_text = ""
    steamdb_overall_positive_rate = None
    if isinstance(steamdb, dict):
        steamdb_overall_positive_rate = _steamdb_overall_positive_rate(steamdb)
        steamdb_review_score_text = _steamdb_review_score_text(steamdb)
        charts_for_review = steamdb.get("charts", {})
        if isinstance(charts_for_review, dict):
            steamdb_review_trend = _steamdb_user_review_trend(charts_for_review)
            if steamdb_review_trend:
                review_trend_90d = steamdb_review_trend
                review_trend_summary = {
                    "days": 90,
                    "complete": len(steamdb_review_trend) >= 90,
                    "reviews_fetched": int(sum(_safe_float(row.get("total")) or 0 for row in steamdb_review_trend)),
                    "source": "steamdb_user_reviews_history",
                }
            elif steamdb_present:
                review_trend_90d = []
                review_trend_summary = {
                    "days": 90,
                    "complete": False,
                    "reviews_fetched": 0,
                    "source": "steamdb_user_reviews_history",
                }
    if steamdb_present:
        overall_positive_rate = steamdb_overall_positive_rate
    else:
        overall_positive_rate = _first_present(
            overall_summary.get("review_score_percent"),
            reviews_data.get("review_score_percent"),
        )
    recent_positive_rate = _review_rate_from_trend(review_trend_90d, days=30)
    if recent_positive_rate in (None, "") and not steamdb_present:
        recent_positive_rate = recent_30d_summary.get("review_score_percent")

    overview_row = {
        "游戏名": game_name or snapshot.get("name", "未知"),
        "数据来源": "Steam",
        "当前在线": _safe_int(snapshot.get("current_players")),
        "评论总量": _safe_int(snapshot.get("total_reviews")),
        "好评率": steamdb_review_score_text or ("" if steamdb_present else snapshot.get("review_score", "")),
        "整体好评率": _format_percent(overall_positive_rate),
        "近期好评率(30 Days)": _format_percent(recent_positive_rate),
        "3个月好评率趋势图": _review_trend_summary_text(review_trend_90d, review_trend_summary),
        "价格": snapshot.get("price", ""),
        "标签": ", ".join(snapshot.get("tags", [])) if isinstance(snapshot.get("tags"), list) else "",
        "开发商": snapshot.get("developer", ""),
        "发行商": snapshot.get("publisher", ""),
        "采集时间": _extract_time(data),
    }

    # SteamDB 数据（如果有）
    if isinstance(steamdb, dict):
        overview_row["SteamDB月峰值"] = _safe_int(steamdb.get("monthly_peak"))
        overview_row["SteamDB日均在线"] = _safe_int(steamdb.get("daily_avg"))
        
        # 计算畅销榜 (Steam关注增量 7日)
        charts = steamdb.get("charts", {})
        if isinstance(charts, dict):
            ccu_excluded_date = _steam_ccu_excluded_date(data, steamdb)
            followers = charts.get("followers_history", [])
            if isinstance(followers, list) and len(followers) > 0:
                follower_gain = _series_gain_last_days(followers, 7)
                overview_row["Steam关注增量(7日)"] = follower_gain if follower_gain is not None else ""
            wishlist = charts.get("wishlist_history", [])
            wishlist_gain = _series_gain_last_days(wishlist, 7) if isinstance(wishlist, list) else None
            follower_gain = _series_gain_last_days(followers, 7) if isinstance(followers, list) else None
            overview_row["WishList Activity(7d Gain)/Follower(7d Gain)"] = _format_wishlist_follower_gain(
                wishlist_gain,
                follower_gain,
            )
            overview_row["7日ccu peak"] = _max_peak_last_days(charts, 7, excluded_date=ccu_excluded_date)
            overview_row["30日CCU peak"] = _max_peak_last_days(charts, 30, excluded_date=ccu_excluded_date)
            overview_row["3个月ccu趋势"] = _ccu_trend_summary(charts, excluded_date=ccu_excluded_date)
            overview_row["steam畅销榜"] = _extract_steam_top_sellers_rank(steamdb, charts)

        result.steam_player_peaks.extend(_extract_steam_peak_rows(data, steamdb, game_name, snapshot))
        result.steam_monthly_peaks.extend(_extract_steam_monthly_rows(data, steamdb, game_name, snapshot))
        result.events.extend(_extract_steamdb_event_rows(data, steamdb, game_name, snapshot))

    result.overview.append(overview_row)

    _append_steam_review_trend(result, game_name or snapshot.get("name", ""), review_trend_90d)

    # 评论提取
    if isinstance(reviews_data, dict):
        items = reviews_data.get("items", []) or reviews_data.get("reviews", [])
        if isinstance(items, list):
            for review in items[:100]:  # 限制条数
                if not isinstance(review, dict):
                    continue
                result.reviews.append({
                    "游戏名": game_name or snapshot.get("name", ""),
                    "数据来源": "Steam",
                    "作者": review.get("author", {}).get("steamid", "") if isinstance(review.get("author"), dict) else "",
                    "评分": "好评" if review.get("voted_up") else "差评",
                    "评论内容": _truncate(review.get("review", review.get("review_text", "")), 500),
                    "游戏时长(h)": round(review.get("author", {}).get("playtime_forever", 0) / 60, 1) if isinstance(review.get("author"), dict) else "",
                    "点赞数": _safe_int(review.get("votes_up")),
                    "日期": review.get("timestamp_created", ""),
                })

    # 新闻/事件提取
    news = data.get("news", {}) or steam_api.get("news", {})
    news_items = news.get("items", []) if isinstance(news, dict) else news
    if isinstance(news_items, list):
        for article in news_items[:50]:
            if not isinstance(article, dict):
                continue
            event_row = _build_steam_news_event(data, article, game_name, snapshot)
            result.events.append(event_row)
            result.trends.append({
                "关键词": game_name,
                "类型": "Steam新闻",
                "日期": event_row.get("日期", ""),
                "标题": event_row.get("标题", ""),
                "热度值": "",
            })


# ==================== TapTap 提取 ====================

def _extract_taptap(data: dict[str, Any], result: ExtractedData) -> None:
    """从 TapTap 采集数据中提取结构化字段。"""
    game_name = data.get("game_name", "")
    game_info = data.get("game", {})
    reviews_info = data.get("reviews", {})
    snapshot = data.get("snapshot", {})

    if isinstance(game_info, dict):
        game_name = game_name or game_info.get("title", "")

    # 概览行
    score = None
    ratings_count = None
    if isinstance(reviews_info, dict):
        score = reviews_info.get("score")
        ratings_count = reviews_info.get("ratings_count")
    if score is None and isinstance(snapshot, dict):
        score = snapshot.get("score")
        ratings_count = ratings_count or snapshot.get("ratings_count")

    platforms = ""
    if isinstance(game_info, dict) and isinstance(game_info.get("platforms"), list):
        platforms = ", ".join(game_info["platforms"])

    overview_row = {
        "游戏名": game_name or "未知",
        "数据来源": "TapTap",
        "当前在线": "",
        "评论总量": _safe_int(ratings_count),
        "好评率": "",
        "价格": "",
        "评分": score if score is not None else "",
        "平台": platforms,
        "开发商": "",
        "采集时间": _extract_time(data),
    }

    # 尝试从 source_meta 获取开发商
    source_meta = data.get("source_meta", {})
    if isinstance(source_meta, dict):
        overview_row["数据层级"] = source_meta.get("layer", "")

    result.overview.append(overview_row)

    # 评论提取
    if isinstance(reviews_info, dict):
        items = reviews_info.get("items", [])
        if isinstance(items, list):
            for review in items[:100]:
                if not isinstance(review, dict):
                    continue
                result.reviews.append({
                    "游戏名": game_name,
                    "数据来源": "TapTap",
                    "作者": review.get("author", ""),
                    "评分": review.get("score", ""),
                    "评论内容": _truncate(review.get("content", ""), 500),
                    "游戏时长(h)": "",
                    "点赞数": _safe_int(review.get("likes")),
                    "日期": review.get("date", review.get("created_at", "")),
                })


# ==================== Google Trends 提取 ====================

def _extract_gtrends(data: dict[str, Any], result: ExtractedData) -> None:
    """从 Google Trends 采集数据中提取结构化字段。"""
    keyword = data.get("keyword", data.get("game_name", ""))
    geo = data.get("geo", "全球")
    timeframe = data.get("timeframe", "")

    # 概览行
    snapshot = data.get("snapshot", {})
    trend_history = data.get("trend_history", [])
    trend_count = len(trend_history) if isinstance(trend_history, list) else 0
    result.overview.append({
        "游戏名": data.get("game_name", keyword),
        "数据来源": f"Google Trends ({geo})",
        "当前在线": "",
        "评论总量": "",
        "好评率": "",
        "价格": "",
        "最新热度": _safe_int(snapshot.get("latest_trend_value")) if isinstance(snapshot, dict) else "",
        "Google Trends（3个月趋势图）": f"{trend_count} points" if trend_count else "",
        "热门相关词数": _safe_int(snapshot.get("top_related_count")) if isinstance(snapshot, dict) else "",
        "上升相关词数": _safe_int(snapshot.get("rising_related_count")) if isinstance(snapshot, dict) else "",
        "采集时间": _extract_time(data),
    })

    # 时序热度
    if isinstance(trend_history, list):
        for point in trend_history:
            if not isinstance(point, dict):
                continue
            result.trends.append({
                "关键词": keyword,
                "类型": "搜索热度",
                "日期": point.get("date", ""),
                "热度值": _safe_int(point.get("value")),
                "标题": "",
            })
            result.google_trends.append({
                "游戏名": data.get("game_name", keyword),
                "关键词": keyword,
                "地区": geo or "全球",
                "时间范围": timeframe,
                "日期": point.get("date", ""),
                "热度值": _safe_int(point.get("value")),
            })

    # 相关查询
    related = data.get("related_queries", {})
    if isinstance(related, dict):
        top_queries = related.get("top", [])
        if isinstance(top_queries, list):
            for item in top_queries:
                if isinstance(item, dict):
                    result.related_queries.append({
                        "关键词": keyword,
                        "类型": "热门",
                        "查询词": item.get("query", ""),
                        "热度值": item.get("value", ""),
                    })

        rising_queries = related.get("rising", [])
        if isinstance(rising_queries, list):
            for item in rising_queries:
                if isinstance(item, dict):
                    result.related_queries.append({
                        "关键词": keyword,
                        "类型": "上升",
                        "查询词": item.get("query", ""),
                        "热度值": item.get("value", ""),
                    })


# ==================== Monitor 提取 ====================

def _extract_monitor(data: dict[str, Any], result: ExtractedData) -> None:
    """从 Monitor 采集数据中提取外围指标。"""
    game_name = data.get("game_name", "")
    app_id = data.get("app_id", "")
    snapshot = data.get("snapshot", {}) if isinstance(data.get("snapshot"), dict) else {}
    metrics = data.get("monitor_metrics", data.get("metrics", {}))
    if not isinstance(metrics, dict):
        metrics = {}

    overview_row = {
        "游戏名": game_name or snapshot.get("name", "未知"),
        "数据来源": "Monitor",
        "App ID": app_id or snapshot.get("app_id", ""),
        "最新Twitch均值": snapshot.get("latest_twitch_average_viewers", ""),
        "twitch tracker(7天平均观看人数)": _twitch_average_last_days(metrics, 7),
        "twitch tracker(90天趋势)": _twitch_trend_summary(metrics),
        "采集时间": _extract_time(data),
    }
    result.overview.append(overview_row)

    result.monitor_metrics.extend(
        _pivot_monitor_daily_rows(
            game_name=game_name or snapshot.get("name", ""),
            app_id=app_id or snapshot.get("app_id", ""),
            metrics=metrics,
        )
    )


# ==================== Event 提取 ====================

def _extract_events(data: dict[str, Any], result: ExtractedData) -> None:
    """提取独立上传或外部导入的事件数据。"""
    game_name = data.get("game_name", "")
    app_id = data.get("app_id", "")
    events = data.get("events", data.get("event_history", []))
    if isinstance(events, dict):
        events = events.get("items", [])
    if not isinstance(events, list):
        return
    for item in events:
        if not isinstance(item, dict):
            continue
        result.events.append(
            {
                "游戏名": game_name or item.get("game_name", ""),
                "App ID": app_id or item.get("app_id", ""),
                "日期": item.get("date") or item.get("日期") or item.get("updated_at", ""),
                "事件类型": item.get("type") or item.get("事件类型") or "事件",
                "标题": item.get("title") or item.get("标题", ""),
                "摘要": _truncate(item.get("summary") or item.get("摘要") or item.get("content", ""), 800),
                "来源": item.get("source") or item.get("来源", ""),
                "作者/来源名": item.get("author") or item.get("feed_name") or "",
                "URL": item.get("url") or item.get("URL", ""),
                "原始ID": item.get("id") or item.get("gid") or item.get("patch_id", ""),
            }
        )


# ==================== 通用提取 ====================

def _extract_generic(data: dict[str, Any], result: ExtractedData) -> None:
    """对未知来源的数据做最小化提取。"""
    snapshot = data.get("snapshot", {})
    content = data.get("content", {})

    name = ""
    if isinstance(snapshot, dict):
        name = snapshot.get("name", "")
    if not name and isinstance(content, dict):
        name = content.get("game_name", "")
    if not name:
        name = data.get("game_name", "未知")

    row = {
        "游戏名": name,
        "数据来源": _detect_collector(data),
        "采集时间": _extract_time(data),
    }

    # 尝试提取常见字段
    if isinstance(snapshot, dict):
        for key in ["current_players", "total_reviews", "review_score", "price", "score"]:
            if snapshot.get(key) is not None:
                row[key] = snapshot[key]

    result.overview.append(row)


# ==================== 工具函数 ====================

def _build_canonical_game_names(
    records: list[dict[str, Any]],
    metadata_list: list[dict[str, Any]] | None,
) -> dict[str, str]:
    candidates_by_app: dict[str, list[str]] = {}
    for index, data in enumerate(records):
        if not isinstance(data, dict):
            continue
        app_id = _extract_app_id(data)
        if not app_id:
            continue
        metadata = metadata_list[index] if metadata_list and index < len(metadata_list) else {}
        candidates = [
            _extract_game_name(data),
            str(metadata.get("target", "")) if isinstance(metadata, dict) else "",
        ]
        for candidate in candidates:
            if _is_usable_game_name(candidate):
                candidates_by_app.setdefault(app_id, []).append(candidate)

    return {
        app_id: _choose_best_game_name(candidates)
        for app_id, candidates in candidates_by_app.items()
        if candidates
    }


def _apply_canonical_game_name(
    data: dict[str, Any],
    canonical_names: dict[str, str],
    metadata: dict[str, Any],
) -> dict[str, Any]:
    app_id = _extract_app_id(data)
    canonical_name = canonical_names.get(app_id, "")
    if not _is_usable_game_name(canonical_name):
        return data

    current_name = _extract_game_name(data)
    metadata_name = str(metadata.get("target", "")) if isinstance(metadata, dict) else ""
    if _is_usable_game_name(current_name) and _is_usable_game_name(metadata_name):
        return data
    if _is_usable_game_name(current_name) and current_name == canonical_name:
        return data

    cloned = copy.deepcopy(data)
    cloned["game_name"] = canonical_name
    snapshot = cloned.get("snapshot")
    if isinstance(snapshot, dict) and not _is_usable_game_name(str(snapshot.get("name", ""))):
        snapshot["name"] = canonical_name
    source_meta = cloned.get("source_meta")
    if isinstance(source_meta, dict) and not _is_usable_game_name(str(source_meta.get("target", ""))):
        source_meta["target"] = canonical_name
    return cloned


def _choose_best_game_name(candidates: list[str]) -> str:
    unique = list(dict.fromkeys(candidate for candidate in candidates if _is_usable_game_name(candidate)))
    if not unique:
        return ""

    def score(value: str) -> tuple[int, int, int]:
        has_cjk = any("\u4e00" <= ch <= "\u9fff" for ch in value)
        ascii_only = all(ord(ch) < 128 for ch in value)
        return (1 if has_cjk else 0, 0 if ascii_only else 1, len(value))

    return max(unique, key=score)


def _is_usable_game_name(value: Any) -> bool:
    if not isinstance(value, str):
        return False
    cleaned = value.strip()
    if not cleaned:
        return False
    if set(cleaned) <= {"?", "？"}:
        return False
    question_count = cleaned.count("?") + cleaned.count("？")
    return question_count / max(len(cleaned), 1) < 0.4


def _extract_app_id(data: dict[str, Any]) -> str:
    snapshot = data.get("snapshot", {}) if isinstance(data.get("snapshot"), dict) else {}
    source_meta = data.get("source_meta", {}) if isinstance(data.get("source_meta"), dict) else {}
    content = data.get("content", {}) if isinstance(data.get("content"), dict) else {}
    game = data.get("game", {}) if isinstance(data.get("game"), dict) else {}
    for value in (
        data.get("app_id"),
        snapshot.get("app_id"),
        source_meta.get("app_id"),
        content.get("app_id"),
        game.get("app_id"),
        game.get("id"),
    ):
        if value not in (None, ""):
            return str(value)
    return ""


def _pivot_monitor_daily_rows(
    *,
    game_name: str,
    app_id: str | int,
    metrics: dict[str, Any],
) -> list[dict[str, Any]]:
    by_date: dict[str, dict[str, Any]] = {}

    def row_for(date_value: Any) -> dict[str, Any]:
        date_text = str(date_value or "")
        row = by_date.setdefault(
            date_text,
            {
                "游戏名": game_name,
                "App ID": app_id,
                "日期": date_text,
            },
        )
        return row

    twitch_payload = metrics.get("twitch_viewer_trend")
    if isinstance(twitch_payload, dict):
        for item in twitch_payload.get("daily_rows", []) or []:
            if isinstance(item, dict):
                row = row_for(item.get("date"))
                row["Twitch平均观看"] = item.get("average_viewers")
                row["Twitch峰值观看"] = item.get("peak_viewers")

    ordered = [row for date_text, row in sorted(by_date.items()) if date_text]
    return ordered


def _extract_steam_peak_rows(
    data: dict[str, Any],
    steamdb: dict[str, Any],
    game_name: str,
    snapshot: dict[str, Any],
) -> list[dict[str, Any]]:
    charts = steamdb.get("charts", steamdb)
    if not isinstance(charts, dict):
        return []

    online_history = charts.get("online_history", {})
    records: list[Any] = []
    if isinstance(online_history, dict) and isinstance(online_history.get("records"), list) and len(online_history.get("records") or []) >= 90:
        records = online_history["records"]
    if not records:
        records = charts.get("online_history_daily_precise_90d") or []
    if not records and isinstance(online_history, dict) and isinstance(online_history.get("records"), list):
        records = online_history["records"]
    if not records:
        records = charts.get("online_history_daily_precise_30d") or []
    if not records:
        records = charts.get("online_history_monthly_peak_1y") or charts.get("online_history_1y") or []
    if not isinstance(records, list):
        return []

    rows: list[dict[str, Any]] = []
    app_id = data.get("app_id") or snapshot.get("app_id", "")
    excluded_date = _steam_ccu_excluded_date(data, steamdb)
    for record in records:
        if not isinstance(record, dict):
            continue
        date_value = record.get("date") or record.get("month") or record.get("label")
        peak_value = _first_number(record, "peak_players", "peak", "players", "max_players", "daily_peak_players")
        if date_value in (None, "") or peak_value is None:
            continue
        if _is_excluded_steam_ccu_day(record, excluded_date):
            continue
        rows.append(
            {
                "游戏名": game_name or snapshot.get("name", ""),
                "App ID": app_id,
                "日期": str(date_value),
                "在线峰值": peak_value,
                "时间戳(UTC)": record.get("timestamp", ""),
                "数据源": steamdb.get("source", "steamdb"),
                "时间粒度": charts.get("requested_time_slice")
                or (online_history.get("requested_slice") if isinstance(online_history, dict) else ""),
            }
        )
    return rows


def _extract_steam_monthly_rows(
    data: dict[str, Any],
    steamdb: dict[str, Any],
    game_name: str,
    snapshot: dict[str, Any],
) -> list[dict[str, Any]]:
    charts = steamdb.get("charts", steamdb)
    if not isinstance(charts, dict):
        return []
    monthly = charts.get("online_history_monthly_peak_1y") or charts.get("online_history_1y") or []
    if not isinstance(monthly, list):
        return []
    rows: list[dict[str, Any]] = []
    app_id = data.get("app_id") or snapshot.get("app_id", "")
    for record in monthly:
        if not isinstance(record, dict):
            continue
        month = record.get("month") or record.get("date") or record.get("label")
        peak_value = _first_number(record, "peak_value", "peak_players", "peak", "players")
        if month in (None, "") or peak_value is None:
            continue
        rows.append(
            {
                "游戏名": game_name or snapshot.get("name", ""),
                "App ID": app_id,
                "月份": str(month),
                "Peak在线人数": peak_value,
                "Peak原始值": record.get("peak", ""),
                "平均在线人数": record.get("average", ""),
                "增幅": record.get("gain", ""),
                "数据源": steamdb.get("source", "steamdb"),
            }
        )
    return rows


def _append_steam_review_trend(
    result: ExtractedData,
    game_name: str,
    trend_rows: list[dict[str, Any]],
) -> None:
    if not isinstance(trend_rows, list):
        return
    for point in trend_rows:
        if not isinstance(point, dict):
            continue
        result.trends.append(
            {
                "关键词": game_name,
                "类型": "Steam好评率(90天)",
                "日期": point.get("date", ""),
                "热度值": point.get("positive_rate", ""),
                "标题": f"{point.get('positive', 0)}/{point.get('total', 0)} positive",
            }
        )


def _steamdb_user_review_trend(charts: dict[str, Any]) -> list[dict[str, Any]]:
    rows = charts.get("user_reviews_history_90d") or charts.get("user_reviews_history") or []
    if not isinstance(rows, list):
        return []
    normalized: list[dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        date_value = row.get("date") or row.get("timestamp")
        positive = _safe_float(row.get("positive"))
        negative = _safe_float(row.get("negative"))
        total = _safe_float(row.get("total"))
        if positive is None:
            continue
        if negative is None:
            negative = 0
        negative = abs(negative)
        if total is None or total <= 0:
            total = positive + negative
        if total <= 0:
            continue
        normalized.append(
            {
                "date": str(date_value or ""),
                "positive": int(positive),
                "negative": int(negative),
                "total": int(total),
                "positive_rate": round((positive / total) * 100, 2),
                "metric": row.get("metric", "bucket"),
                "source": row.get("source", "steamdb_user_reviews_history"),
            }
        )
    normalized.sort(key=lambda item: item.get("date", ""))
    return normalized[-90:]


def _steamdb_overall_positive_rate(steamdb: dict[str, Any]) -> float | None:
    charts = steamdb.get("charts", {}) if isinstance(steamdb.get("charts"), dict) else {}
    for container in (charts, steamdb.get("info", {}), steamdb):
        if not isinstance(container, dict):
            continue
        direct = _first_present(
            container.get("steamdb_rating_percent"),
            container.get("review_score_percent"),
            container.get("positive_reviews_percent"),
        )
        parsed = _safe_float(direct)
        if parsed is not None:
            return parsed

    text_blobs = []
    for container in (steamdb.get("info", {}), charts, steamdb.get("sales", {})):
        if not isinstance(container, dict):
            continue
        for key in ("page_text_preview", "raw_preview", "text_preview"):
            value = container.get(key)
            if isinstance(value, str) and value:
                text_blobs.append(value)
    for text in text_blobs:
        parsed = _parse_steamdb_rating_from_text(text)
        if parsed is not None:
            return parsed
    trend = _steamdb_user_review_trend(charts)
    if trend:
        return _review_rate_from_trend(trend, days=len(trend))
    return None


def _steamdb_review_score_text(steamdb: dict[str, Any]) -> str:
    rate = _steamdb_overall_positive_rate(steamdb)
    if rate is None:
        return ""
    return f"{rate:.2f}% (SteamDB)"


def _parse_steamdb_rating_from_text(text: str) -> float | None:
    normalized = re.sub(r"\s+", " ", str(text or "")).strip()
    patterns = (
        r"([0-9]{1,3}(?:\.[0-9]+)?)\s*%\s+SteamDB Rating",
        r"([0-9]{1,3}(?:\.[0-9]+)?)\s*%\s+[0-9][0-9,.\s]*[KMBkmb]?\s+reviews\b",
    )
    for pattern in patterns:
        match = re.search(pattern, normalized, re.IGNORECASE)
        if not match:
            continue
        parsed = _safe_float(match.group(1))
        if parsed is not None:
            return parsed
    return None


def _review_trend_summary_text(rows: list[dict[str, Any]], summary: dict[str, Any]) -> str:
    if not rows:
        return ""
    days = int(summary.get("days") or len(rows) or 90)
    total_reviews = summary.get("total_reviews")
    reviews_fetched = summary.get("reviews_fetched")
    complete = summary.get("complete")
    if complete is False and total_reviews not in (None, "") and reviews_fetched not in (None, ""):
        return f"{len(rows)}/{days} days (incomplete {reviews_fetched}/{total_reviews} reviews)"
    return f"{len(rows)}/{days} days"


def _review_rate_from_trend(rows: list[dict[str, Any]], *, days: int) -> float | None:
    if not isinstance(rows, list) or not rows:
        return None
    sorted_rows = sorted(
        (row for row in rows if isinstance(row, dict)),
        key=lambda row: str(row.get("date", "")),
    )
    if _is_cumulative_review_series(sorted_rows):
        window = sorted_rows[-days:]
        if len(window) >= 2:
            first = window[0]
            last = window[-1]
            positive_delta = (_safe_float(last.get("positive")) or 0) - (
                _safe_float(first.get("positive")) or 0
            )
            total_delta = (_safe_float(last.get("total")) or 0) - (
                _safe_float(first.get("total")) or 0
            )
            if positive_delta >= 0 and total_delta > 0:
                return round((positive_delta / total_delta) * 100, 2)
        latest_rate = _safe_float(sorted_rows[-1].get("positive_rate"))
        return round(latest_rate, 2) if latest_rate is not None else None

    positive_total = 0.0
    review_total = 0.0
    for row in sorted_rows[-days:]:
        total = _safe_float(row.get("total"))
        positive = _safe_float(row.get("positive"))
        if total is None or total <= 0 or positive is None:
            continue
        positive_total += positive
        review_total += total
    if review_total <= 0:
        return None
    return round((positive_total / review_total) * 100, 2)


def _is_cumulative_review_series(rows: list[dict[str, Any]]) -> bool:
    if any(row.get("metric") == "cumulative" for row in rows):
        return True
    comparable = [
        (_safe_float(row.get("positive")), _safe_float(row.get("total")))
        for row in rows
    ]
    comparable = [pair for pair in comparable if pair[0] is not None and pair[1] is not None]
    if len(comparable) < 3:
        return False
    positives = [pair[0] for pair in comparable]
    totals = [pair[1] for pair in comparable]
    return positives == sorted(positives) and totals == sorted(totals)


def _max_peak_last_days(
    charts: dict[str, Any],
    days: int,
    *,
    excluded_date: date | None = None,
) -> int | float | str:
    online_history = charts.get("online_history", {})
    daily = charts.get("online_history_daily_precise_90d") or charts.get("online_history_daily_precise_30d") or []
    if not daily and isinstance(online_history, dict):
        daily = online_history.get("records") or []
    if not isinstance(daily, list):
        return ""
    values: list[int | float] = []
    anchor_date = excluded_date or datetime.now(timezone.utc).date()
    cutoff = anchor_date - timedelta(days=days)
    for record in daily:
        if not isinstance(record, dict):
            continue
        day = _parse_date_only(record.get("date") or record.get("timestamp"))
        peak = _first_number(record, "peak_players", "peak", "players", "max_players")
        if day is None or peak is None:
            continue
        if excluded_date is not None and day == excluded_date:
            continue
        if day >= cutoff:
            values.append(peak)
    return max(values) if values else ""


def _ccu_trend_summary(charts: dict[str, Any], *, excluded_date: date | None = None) -> str:
    daily = charts.get("online_history_daily_precise_90d") or charts.get("online_history_daily_precise_30d") or []
    if isinstance(daily, list) and daily:
        complete_daily = [
            row
            for row in daily
            if not (isinstance(row, dict) and _is_excluded_steam_ccu_day(row, excluded_date))
        ]
        return f"{min(len(complete_daily), 90)} daily points"
    monthly = charts.get("online_history_monthly_peak_1y") or charts.get("online_history_1y") or []
    if isinstance(monthly, list) and monthly:
        return f"{min(len(monthly), 3)} monthly points"
    return ""


def _extract_steam_top_sellers_rank(steamdb: dict[str, Any], charts: dict[str, Any]) -> str:
    top_sellers = steamdb.get("top_sellers")
    if isinstance(top_sellers, dict):
        rank = top_sellers.get("rank")
        if rank not in (None, ""):
            return f"#{rank}"
        if top_sellers.get("matched") is False:
            return "未进入SteamDB当前全球畅销榜Top 100"
        if top_sellers.get("error"):
            return "Steam畅销榜采集失败"

    fallback = _first_present(
        charts.get("steam_top_sellers_rank"),
        charts.get("top_sellers_rank"),
        charts.get("sales_rank"),
        charts.get("global_top_sellers"),
        charts.get("rank"),
    )
    if fallback not in (None, ""):
        return str(fallback)
    return "未采集"


def _steam_ccu_excluded_date(data: dict[str, Any], steamdb: dict[str, Any]) -> date:
    """Return the collection day whose Steam CCU data should be excluded from reports."""
    candidates: list[Any] = [
        _extract_time(data),
        data.get("collected_at"),
        steamdb.get("collected_at") if isinstance(steamdb, dict) else None,
    ]
    source_meta = data.get("source_meta", {}) if isinstance(data.get("source_meta"), dict) else {}
    if isinstance(source_meta, dict):
        candidates.append(source_meta.get("collected_at"))
    steamdb_meta = steamdb.get("source_meta", {}) if isinstance(steamdb.get("source_meta"), dict) else {}
    if isinstance(steamdb_meta, dict):
        candidates.append(steamdb_meta.get("collected_at"))

    for value in candidates:
        parsed = _parse_datetime_value(value)
        if parsed is not None:
            return parsed.date()
    return datetime.now(timezone.utc).date()


def _is_excluded_steam_ccu_day(record: dict[str, Any], excluded_date: date | None) -> bool:
    if excluded_date is None:
        return False
    record_date = _parse_date_only(record.get("date") or record.get("timestamp"))
    return record_date == excluded_date


def _series_gain_last_days(series: list[Any], days: int) -> int | float | None:
    points: list[tuple[datetime, int | float]] = []
    for item in series:
        if not isinstance(item, dict):
            continue
        dt = _parse_datetime_value(item.get("timestamp") or item.get("date") or item.get("month"))
        value = _first_number(item, "peak_players", "value", "followers", "wishlist", "players", "peak")
        if dt is not None and value is not None:
            points.append((dt, value))
    if len(points) < 2:
        return None
    points.sort(key=lambda pair: pair[0])
    latest_dt, latest_value = points[-1]
    threshold = latest_dt - timedelta(days=days)
    baseline_value = points[0][1]
    for dt, value in points:
        if dt <= threshold:
            baseline_value = value
        else:
            break
    return latest_value - baseline_value


def _format_wishlist_follower_gain(
    wishlist_gain: int | float | None,
    follower_gain: int | float | None,
) -> str:
    wishlist_text = "" if wishlist_gain is None else str(wishlist_gain)
    follower_text = "" if follower_gain is None else str(follower_gain)
    if not wishlist_text and not follower_text:
        return ""
    return f"Wishlist {wishlist_text or 'N/A'} / Follower {follower_text or 'N/A'}"


def _twitch_average_last_days(metrics: dict[str, Any], days: int) -> int | str:
    twitch = metrics.get("twitch_viewer_trend")
    if not isinstance(twitch, dict):
        return ""
    rows = twitch.get("daily_rows", [])
    if not isinstance(rows, list):
        return ""
    values = [
        row.get("average_viewers")
        for row in rows[-days:]
        if isinstance(row, dict) and isinstance(row.get("average_viewers"), (int, float))
    ]
    return round(sum(values) / len(values)) if values else ""


def _twitch_trend_summary(metrics: dict[str, Any]) -> str:
    twitch = metrics.get("twitch_viewer_trend")
    if not isinstance(twitch, dict):
        return ""
    rows = twitch.get("daily_rows", [])
    if not isinstance(rows, list) or not rows:
        return ""
    return f"{min(len(rows), 90)} daily points"


def _build_steam_news_event(
    data: dict[str, Any],
    article: dict[str, Any],
    game_name: str,
    snapshot: dict[str, Any],
) -> dict[str, Any]:
    title = str(article.get("title", "") or "")
    return {
        "游戏名": game_name or snapshot.get("name", ""),
        "App ID": data.get("app_id") or snapshot.get("app_id", ""),
        "日期": _format_event_time(article.get("date")),
        "事件类型": _classify_event_title(title),
        "标题": title,
        "摘要": _truncate(article.get("contents", ""), 800),
        "来源": "Steam官方新闻",
        "作者/来源名": article.get("author") or article.get("feed_name", ""),
        "URL": article.get("url", ""),
        "原始ID": article.get("gid", ""),
    }


def _extract_steamdb_event_rows(
    data: dict[str, Any],
    steamdb: dict[str, Any],
    game_name: str,
    snapshot: dict[str, Any],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    seen: set[str] = set()
    app_id = data.get("app_id") or snapshot.get("app_id", "")
    containers = [steamdb]
    for key in ("charts", "info"):
        value = steamdb.get(key)
        if isinstance(value, dict):
            containers.append(value)

    for container in containers:
        updates = container.get("update_history")
        if not isinstance(updates, list):
            continue
        for update in updates[:100]:
            if not isinstance(update, dict):
                continue
            patch_id = str(update.get("patch_id", "") or "")
            url = str(update.get("patchnote_url", "") or "")
            dedupe_key = patch_id or url
            if dedupe_key and dedupe_key in seen:
                continue
            if dedupe_key:
                seen.add(dedupe_key)
            updated_at = update.get("updated_at") or _format_event_time(update.get("timestamp_unix"))
            rows.append(
                {
                    "游戏名": game_name or snapshot.get("name", ""),
                    "App ID": app_id,
                    "日期": updated_at,
                    "事件类型": "SteamDB版本更新",
                    "标题": f"SteamDB Patch {patch_id}" if patch_id else "SteamDB Patch",
                    "摘要": update.get("timestamp_raw", ""),
                    "来源": "SteamDB",
                    "作者/来源名": "",
                    "URL": url,
                    "原始ID": patch_id,
                    "相对时间": update.get("updated_at_relative", ""),
                }
            )
    return rows


def _format_event_time(value: Any) -> str:
    if value in (None, ""):
        return ""
    if isinstance(value, (int, float)):
        try:
            return datetime.fromtimestamp(float(value), tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
        except (OSError, OverflowError, ValueError):
            return str(value)
    return str(value)


def _classify_event_title(title: str) -> str:
    lowered = title.lower()
    if any(keyword in lowered for keyword in ("update", "patch", "bug", "fix", "optimization", "hotfix")):
        return "版本更新"
    if any(keyword in lowered for keyword in ("event", "season", "activity", "festival")):
        return "活动"
    return "公告/新闻"


def _extract_game_name(data: dict[str, Any]) -> str:
    snapshot = data.get("snapshot", {}) if isinstance(data.get("snapshot"), dict) else {}
    content = data.get("content", {}) if isinstance(data.get("content"), dict) else {}
    content_snapshot = (
        content.get("snapshot", {}) if isinstance(content.get("snapshot"), dict) else {}
    )
    game = data.get("game", {}) if isinstance(data.get("game"), dict) else {}
    return str(
        data.get("game_name")
        or snapshot.get("name")
        or content.get("game_name")
        or content_snapshot.get("name")
        or game.get("title")
        or data.get("keyword")
        or ""
    )


def _first_number(data: dict[str, Any], *keys: str) -> int | float | None:
    for key in keys:
        value = data.get(key)
        if isinstance(value, bool) or value in (None, ""):
            continue
        if isinstance(value, (int, float)):
            return value
        try:
            text = str(value).replace(",", "").strip()
            if "." in text:
                return float(text)
            return int(text)
        except (TypeError, ValueError):
            continue
    return None


def _first_present(*values: Any) -> Any:
    for value in values:
        if value not in (None, ""):
            return value
    return None


def _format_percent(value: Any) -> str:
    if value in (None, ""):
        return ""
    if isinstance(value, str) and value.endswith("%"):
        return value
    return f"{value}%"


def _safe_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _parse_date_only(value: Any):
    dt = _parse_datetime_value(value)
    return dt.date() if dt is not None else None


def _parse_datetime_value(value: Any) -> datetime | None:
    if value in (None, ""):
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if isinstance(value, (int, float)):
        try:
            timestamp = float(value)
            if timestamp > 10_000_000_000:
                timestamp = timestamp / 1000
            return datetime.fromtimestamp(timestamp, tz=timezone.utc)
        except Exception:
            return None
    text = str(value).strip()
    if not text:
        return None
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        pass
    for fmt in ("%Y-%m-%d", "%B %Y", "%b %Y"):
        try:
            parsed = datetime.strptime(text, fmt)
            return parsed.replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    return None


def _safe_int(value: Any) -> int | str:
    """安全转 int，失败返回空字符串。"""
    if value is None:
        return ""
    try:
        return int(value)
    except (ValueError, TypeError):
        return str(value)


def _truncate(text: str, max_len: int = 500) -> str:
    """截断过长文本。"""
    if not isinstance(text, str):
        return ""
    text = text.strip()
    if len(text) <= max_len:
        return text
    return text[:max_len] + "..."


def _extract_time(data: dict[str, Any]) -> str:
    """从多种可能位置提取采集时间。"""
    # source_meta.collected_at
    meta = data.get("source_meta", {})
    if isinstance(meta, dict) and meta.get("collected_at"):
        return str(meta["collected_at"])

    # metadata.collected_at
    metadata = data.get("metadata", {})
    if isinstance(metadata, dict) and metadata.get("collected_at"):
        return str(metadata["collected_at"])

    return ""
