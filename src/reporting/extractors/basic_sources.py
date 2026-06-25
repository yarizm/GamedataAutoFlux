"""Lower-coupling reporting extractors for non-Steam source payloads."""

from __future__ import annotations

from typing import Any

from src.reporting.extractors.common import (
    extract_time,
    list_items,
    pivot_monitor_daily_rows,
    safe_int,
    truncate,
    twitch_average_last_days,
    twitch_trend_summary,
)


def extract_official_site(data: dict[str, Any], result: Any) -> None:
    snapshot = data.get("snapshot", {}) if isinstance(data.get("snapshot"), dict) else {}
    game_name = data.get("game_name") or snapshot.get("name", "")
    official_url = data.get("official_url", "")
    news_items = list_items((data.get("news") or {}).get("items", []))
    patch_items = list_items((data.get("patch_notes") or {}).get("items", []))
    event_items = list_items((data.get("events") or {}).get("items", []))

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
            "采集时间": extract_time(data),
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
                "摘要": truncate(item.get("summary") or item.get("content", ""), 800),
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
                    "关键词": game_name,
                    "类型": "官网动态",
                    "日期": item.get("date", ""),
                    "热度值": 1,
                    "标题": item.get("title", ""),
                    "URL": item.get("url", ""),
                }
            )
    for item in patch_items[:200]:
        append_event(item, "版本更新")
    for item in event_items[:200]:
        append_event(item, "官网活动")


def extract_gtrends(data: dict[str, Any], result: Any) -> None:
    keyword = data.get("keyword", data.get("game_name", ""))
    geo = data.get("geo", "全球")
    timeframe = data.get("timeframe", "")

    snapshot = data.get("snapshot", {})
    trend_history = data.get("trend_history", [])
    trend_count = len(trend_history) if isinstance(trend_history, list) else 0
    result.overview.append(
        {
            "游戏名": data.get("game_name", keyword),
            "数据来源": f"Google Trends ({geo})",
            "当前在线": "",
            "评论总量": "",
            "好评率": "",
            "价格": "",
            "最新热度": safe_int(snapshot.get("latest_trend_value"))
            if isinstance(snapshot, dict)
            else "",
            "Google Trends（一个月趋势图）": f"{trend_count} points" if trend_count else "",
            "热门相关词数": safe_int(snapshot.get("top_related_count"))
            if isinstance(snapshot, dict)
            else "",
            "上升相关词数": safe_int(snapshot.get("rising_related_count"))
            if isinstance(snapshot, dict)
            else "",
            "采集时间": extract_time(data),
        }
    )

    if isinstance(trend_history, list):
        for point in trend_history:
            if not isinstance(point, dict):
                continue
            result.trends.append(
                {
                    "关键词": keyword,
                    "类型": "搜索热度",
                    "日期": point.get("date", ""),
                    "热度值": safe_int(point.get("value")),
                    "标题": "",
                }
            )
            result.google_trends.append(
                {
                    "游戏名": data.get("game_name", keyword),
                    "关键词": keyword,
                    "地区": geo or "全球",
                    "时间范围": timeframe,
                    "日期": point.get("date", ""),
                    "热度值": safe_int(point.get("value")),
                }
            )

    related = data.get("related_queries", {})
    if isinstance(related, dict):
        for item in related.get("top", []):
            if isinstance(item, dict):
                result.related_queries.append(
                    {
                        "关键词": keyword,
                        "类型": "热门",
                        "查询词": item.get("query", ""),
                        "热度值": item.get("value", ""),
                    }
                )
        for item in related.get("rising", []):
            if isinstance(item, dict):
                result.related_queries.append(
                    {
                        "关键词": keyword,
                        "类型": "上升",
                        "查询词": item.get("query", ""),
                        "热度值": item.get("value", ""),
                    }
                )


def extract_monitor(data: dict[str, Any], result: Any) -> None:
    game_name = data.get("game_name", "")
    app_id = data.get("app_id", "")
    snapshot = data.get("snapshot", {}) if isinstance(data.get("snapshot"), dict) else {}
    metrics = data.get("monitor_metrics", data.get("metrics", {}))
    if not isinstance(metrics, dict):
        metrics = {}

    result.overview.append(
        {
            "游戏名": game_name or snapshot.get("name", "未知"),
            "数据来源": "Monitor",
            "App ID": app_id or snapshot.get("app_id", ""),
            "最新Twitch均值": snapshot.get("latest_twitch_average_viewers", ""),
            "twitch tracker(7天平均观看人数)": twitch_average_last_days(metrics, 7),
            "twitch tracker(90天趋势)": twitch_trend_summary(metrics),
            "采集时间": extract_time(data),
        }
    )

    result.monitor_metrics.extend(
        pivot_monitor_daily_rows(
            game_name=game_name or snapshot.get("name", ""),
            app_id=app_id or snapshot.get("app_id", ""),
            metrics=metrics,
        )
    )


def extract_events(data: dict[str, Any], result: Any) -> None:
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
                "摘要": truncate(
                    item.get("summary") or item.get("摘要") or item.get("content", ""),
                    800,
                ),
                "来源": item.get("source") or item.get("来源", ""),
                "作者/来源名": item.get("author") or item.get("feed_name") or "",
                "URL": item.get("url") or item.get("URL", ""),
                "原始ID": item.get("id") or item.get("gid") or item.get("patch_id", ""),
            }
        )


def extract_generic(data: dict[str, Any], result: Any, *, collector_name: str) -> None:
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
        "数据来源": collector_name,
        "采集时间": extract_time(data),
    }

    if isinstance(snapshot, dict):
        for key in ["current_players", "total_reviews", "review_score", "price", "score"]:
            if snapshot.get(key) is not None:
                row[key] = snapshot[key]

    result.overview.append(row)
