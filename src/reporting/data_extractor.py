"""
数据提取器。

从不同采集器的嵌套 JSON 中提取干净的结构化表格数据，
供 Excel 导出器使用。

每个采集器有专门的提取函数，输出统一的 dict 列表（每个 dict = 一行）。
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

from loguru import logger


class ExtractedData:
    """提取后的结构化数据容器。"""

    def __init__(self):
        self.overview: list[dict[str, Any]] = []    # 游戏概览行
        self.reviews: list[dict[str, Any]] = []     # 评论明细行
        self.trends: list[dict[str, Any]] = []      # 趋势数据行
        self.related_queries: list[dict[str, Any]] = []  # 相关搜索词


def extract_from_records(records: list[dict[str, Any]]) -> ExtractedData:
    """
    从存储记录列表中提取结构化数据。

    Args:
        records: StorageRecord.data 列表（已从 JSON 加载的 dict）

    Returns:
        ExtractedData 分组数据
    """
    result = ExtractedData()

    for record_data in records:
        if not isinstance(record_data, dict):
            continue

        collector = _detect_collector(record_data)
        try:
            if collector == "steam":
                _extract_steam(record_data, result)
            elif collector == "taptap":
                _extract_taptap(record_data, result)
            elif collector == "gtrends":
                _extract_gtrends(record_data, result)
            else:
                _extract_generic(record_data, result)
        except Exception as exc:
            logger.warning(f"[DataExtractor] 提取失败 (collector={collector}): {exc}")

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
    if "reviews_summary" in data or "availability" in data:
        return "taptap"
    if "trend_history" in data:
        return "gtrends"

    return "unknown"


# ==================== Steam 提取 ====================

def _extract_steam(data: dict[str, Any], result: ExtractedData) -> None:
    """从 Steam 采集数据中提取结构化字段。"""
    game_name = data.get("game_name", "")
    snapshot = data.get("snapshot", {})
    if not snapshot and "content" in data:
        content = data["content"]
        if isinstance(content, dict):
            snapshot = content.get("snapshot", {})
            game_name = game_name or content.get("game_name", "")

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

    result.overview.append(overview_row)

    # 评论提取
    reviews_data = data.get("reviews", {})
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

    # 新闻提取（放入趋势 sheet）
    news = data.get("news", {})
    if isinstance(news, dict):
        items = news.get("items", [])
        if isinstance(items, list):
            for article in items[:20]:
                if not isinstance(article, dict):
                    continue
                result.trends.append({
                    "关键词": game_name,
                    "类型": "Steam新闻",
                    "日期": article.get("date", ""),
                    "标题": article.get("title", ""),
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
