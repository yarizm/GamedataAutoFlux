"""
数据提取器。

从不同采集器的嵌套 JSON 中提取干净的结构化表格数据，
供 Excel 导出器使用。

每个采集器有专门的提取函数，输出统一的 dict 列表（每个 dict = 一行）。
"""

from __future__ import annotations

import copy
from datetime import datetime, timezone
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
    if "snapshot" in data and "current_players" in data.get("snapshot", {}):
        return "steam"
    if "qimai" in data:
        return "qimai"
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

def _extract_qimai(data: dict[str, Any], result: ExtractedData) -> None:
    """从七麦数据中提取结构化字段。"""
    game_name = data.get("game_name", "")
    snapshot = data.get("snapshot", {})
    qimai_data = data.get("qimai", {})
    
    overview_row = {
        "游戏名": game_name or snapshot.get("name", "未知"),
        "数据来源": "Qimai(AppStore)",
        "评论总量": _safe_int(snapshot.get("total_reviews")),
        "评分": snapshot.get("review_score", ""),
        "AppStore免费榜": snapshot.get("free_rank", ""),
        "AppStore畅销榜": snapshot.get("grossing_rank", ""),
        "采集时间": _extract_time(data),
    }
    
    result.overview.append(overview_row)


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
    overview_row = {
        "游戏名": game_name or snapshot.get("name", "未知"),
        "数据来源": "Steam",
        "当前在线": _safe_int(snapshot.get("current_players")),
        "评论总量": _safe_int(snapshot.get("total_reviews")),
        "好评率": snapshot.get("review_score", ""),
        "价格": snapshot.get("price", ""),
        "标签": ", ".join(snapshot.get("tags", [])) if isinstance(snapshot.get("tags"), list) else "",
        "开发商": snapshot.get("developer", ""),
        "发行商": snapshot.get("publisher", ""),
        "采集时间": _extract_time(data),
    }

    # SteamDB 数据（如果有）
    steamdb = data.get("steamdb", {})
    if isinstance(steamdb, dict):
        overview_row["SteamDB月峰值"] = _safe_int(steamdb.get("monthly_peak"))
        overview_row["SteamDB日均在线"] = _safe_int(steamdb.get("daily_avg"))
        
        # 计算畅销榜 (Steam关注增量 7日)
        charts = steamdb.get("charts", {})
        if isinstance(charts, dict):
            followers = charts.get("followers_history", [])
            if isinstance(followers, list) and len(followers) > 0:
                latest = followers[-1].get("peak_players", 0)
                # 寻找至少 7 天前的数据
                seven_days_ago = latest
                if len(followers) >= 8:
                    seven_days_ago = followers[-8].get("peak_players", latest)
                overview_row["Steam关注增量(7日)"] = latest - seven_days_ago

        result.steam_player_peaks.extend(_extract_steam_peak_rows(data, steamdb, game_name, snapshot))
        result.steam_monthly_peaks.extend(_extract_steam_monthly_rows(data, steamdb, game_name, snapshot))
        result.events.extend(_extract_steamdb_event_rows(data, steamdb, game_name, snapshot))

    result.overview.append(overview_row)

    # 评论提取
    reviews_data = data.get("reviews", {}) or steam_api.get("reviews", {})
    if isinstance(reviews_data, dict):
        items = reviews_data.get("items", [])
        if isinstance(items, list):
            for review in items[:100]:  # 限制条数
                if not isinstance(review, dict):
                    continue
                result.reviews.append({
                    "游戏名": game_name or snapshot.get("name", ""),
                    "数据来源": "Steam",
                    "作者": review.get("author", {}).get("steamid", "") if isinstance(review.get("author"), dict) else "",
                    "评分": "好评" if review.get("voted_up") else "差评",
                    "评论内容": _truncate(review.get("review", ""), 500),
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
    result.overview.append({
        "游戏名": data.get("game_name", keyword),
        "数据来源": f"Google Trends ({geo})",
        "当前在线": "",
        "评论总量": "",
        "好评率": "",
        "价格": "",
        "最新热度": _safe_int(snapshot.get("latest_trend_value")) if isinstance(snapshot, dict) else "",
        "热门相关词数": _safe_int(snapshot.get("top_related_count")) if isinstance(snapshot, dict) else "",
        "上升相关词数": _safe_int(snapshot.get("rising_related_count")) if isinstance(snapshot, dict) else "",
        "采集时间": _extract_time(data),
    })

    # 时序热度
    trend_history = data.get("trend_history", [])
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
    if isinstance(online_history, dict) and isinstance(online_history.get("records"), list):
        records = online_history["records"]
    if not records:
        records = charts.get("online_history_daily_precise_30d") or []
    if not records:
        records = charts.get("online_history_monthly_peak_1y") or charts.get("online_history_1y") or []
    if not isinstance(records, list):
        return []

    rows: list[dict[str, Any]] = []
    app_id = data.get("app_id") or snapshot.get("app_id", "")
    for record in records:
        if not isinstance(record, dict):
            continue
        date_value = record.get("date") or record.get("month") or record.get("label")
        peak_value = _first_number(record, "peak_players", "peak", "players", "max_players", "daily_peak_players")
        if date_value in (None, "") or peak_value is None:
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
