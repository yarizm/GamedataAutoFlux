"""
数据提取器。

从不同采集器的嵌套 JSON 中提取干净的结构化表格数据，
供 Excel 导出器使用。

每个采集器有专门的提取函数，输出统一的 dict 列表（每个 dict = 一行）。
"""

from __future__ import annotations

import copy
import re
from typing import Any

from loguru import logger

from src.core.sensitive import redact_sensitive, redact_sensitive_text
from src.reporting.extractors.basic_sources import (
    extract_events as _extract_events,
    extract_generic as _extract_generic_basic,
    extract_gtrends as _extract_gtrends,
    extract_monitor as _extract_monitor,
    extract_official_site as _extract_official_site,
)
from src.reporting.extractors.common import (
    extract_time as _extract_time,
    pivot_monitor_daily_rows as _pivot_monitor_daily_rows,
    safe_float as _safe_float,
    safe_int as _safe_int,
    truncate as _truncate,
    twitch_average_last_days as _twitch_average_last_days,
    twitch_trend_summary as _twitch_trend_summary,
)
from src.reporting.extractors.qimai import extract_qimai as _extract_qimai
from src.reporting.extractors.steam import extract_steam as _extract_steam
from src.reporting.extractors.steam_discussions import (
    extract_steam_discussions as _extract_steam_discussions,
)
from src.reporting.report_templates import normalize_collector


class ExtractedData:
    """提取后的结构化数据容器。"""

    def __init__(self):
        self.overview: list[dict[str, Any]] = []  # 游戏概览行
        self.reviews: list[dict[str, Any]] = []  # 评论明细行
        self.trends: list[dict[str, Any]] = []  # 趋势数据行
        self.related_queries: list[dict[str, Any]] = []  # 相关搜索词
        self.steam_player_peaks: list[dict[str, Any]] = []  # Steam 在线峰值
        self.steam_monthly_peaks: list[dict[str, Any]] = []  # SteamDB 月峰值
        self.google_trends: list[dict[str, Any]] = []  # Google Trends 时序
        self.monitor_metrics: list[dict[str, Any]] = []  # Monitor 指标明细
        self.events: list[dict[str, Any]] = []  # 游戏新闻/版本/活动事件
        self.community_discussions: list[dict[str, Any]] = []  # 社区讨论
        self.raw_sources: list[dict[str, Any]] = []  # 原始 JSON 附录
        self.source_coverage: dict[str, int] = {}  # collector -> record count

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
        prepared_record = _prepare_record_for_extraction(
            index=index,
            record_data=record_data,
            canonical_names=canonical_names,
            record_keys=record_keys,
            metadata_list=metadata_list,
        )
        if prepared_record is None:
            continue

        record_data = prepared_record["record_data"]
        collector = prepared_record["collector"]
        result.source_coverage[collector] = result.source_coverage.get(collector, 0) + 1
        result.raw_sources.append(_build_raw_source_entry(prepared_record))

        event_count_before = len(result.events)
        try:
            _extract_record(record_data, result, collector=collector)
        except Exception as exc:
            logger.warning(
                f"[DataExtractor] 提取失败 (collector={collector}): "
                f"{redact_sensitive_text(str(exc))}"
            )
        _update_event_source_coverage(result, event_count_before)

    return result


def _detect_collector(data: dict[str, Any]) -> str:
    """识别数据来源的采集器。"""
    collector = _detected_collector_from_explicit_fields(data)
    if collector:
        return collector
    return _detect_collector_from_features(data)


def _detected_collector_from_explicit_fields(data: dict[str, Any]) -> str:
    collector = data.get("collector", "")
    if collector:
        return collector

    content = data.get("content", {})
    if isinstance(content, dict):
        collector = content.get("collector", "")
        if collector:
            return collector
    return ""


def _detect_collector_from_features(data: dict[str, Any]) -> str:
    for detector in (
        _detect_steam_collector,
        _detect_qimai_collector,
        _detect_official_site_collector,
        _detect_discussions_collector,
        _detect_taptap_collector,
        _detect_gtrends_collector,
        _detect_events_collector,
        _detect_monitor_collector,
    ):
        collector = detector(data)
        if collector:
            return collector
    return "unknown"


def _detect_steam_collector(data: dict[str, Any]) -> str:
    if "steamdb" in data or "steam_api" in data:
        return "steam"
    snapshot = data.get("snapshot", {})
    if isinstance(snapshot, dict) and "current_players" in snapshot:
        return "steam"
    return ""


def _detect_qimai_collector(data: dict[str, Any]) -> str:
    return "qimai" if "qimai" in data else ""


def _detect_official_site_collector(data: dict[str, Any]) -> str:
    if data.get("collector") == "official_site":
        return "official_site"
    if "official_url" in data and "news" in data:
        return "official_site"
    return ""


def _detect_discussions_collector(data: dict[str, Any]) -> str:
    return "steam_discussions" if "discussions" in data else ""


def _detect_taptap_collector(data: dict[str, Any]) -> str:
    return "taptap" if "reviews_summary" in data or "availability" in data else ""


def _detect_gtrends_collector(data: dict[str, Any]) -> str:
    return "gtrends" if "trend_history" in data else ""


def _detect_events_collector(data: dict[str, Any]) -> str:
    return "events" if "events" in data or "event_history" in data else ""


def _detect_monitor_collector(data: dict[str, Any]) -> str:
    return "monitor" if "monitor_metrics" in data or "metrics" in data else ""


def _prepare_record_for_extraction(
    *,
    index: int,
    record_data: Any,
    canonical_names: dict[str, str],
    record_keys: list[str] | None,
    metadata_list: list[dict[str, Any]] | None,
) -> dict[str, Any] | None:
    if not isinstance(record_data, dict):
        return None

    record_metadata = _record_metadata_at(metadata_list, index)
    normalized_record = _apply_canonical_game_name(record_data, canonical_names, record_metadata)
    collector_name = _detect_collector(normalized_record)
    collector = normalize_collector(collector_name)
    record_key = _record_key_at(record_keys, index)
    return {
        "record_data": normalized_record,
        "record_key": record_key,
        "record_metadata": record_metadata,
        "collector": collector,
        "collector_name": collector_name,
    }


def _record_key_at(record_keys: list[str] | None, index: int) -> str:
    if record_keys and index < len(record_keys):
        return record_keys[index]
    return f"record_{index + 1}"


def _record_metadata_at(metadata_list: list[dict[str, Any]] | None, index: int) -> dict[str, Any]:
    if metadata_list and index < len(metadata_list):
        return metadata_list[index]
    return {}


def _build_raw_source_entry(prepared_record: dict[str, Any]) -> dict[str, Any]:
    record_data = prepared_record["record_data"]
    return {
        "key": redact_sensitive_text(prepared_record["record_key"]),
        "collector": redact_sensitive_text(prepared_record["collector"]),
        "game_name": redact_sensitive_text(_extract_game_name(record_data)),
        "metadata": redact_sensitive(prepared_record["record_metadata"]),
        "data": redact_sensitive(record_data),
    }


def _extract_record(
    record_data: dict[str, Any],
    result: ExtractedData,
    *,
    collector: str,
) -> None:
    extractor = _collector_extractors().get(collector)
    if extractor is not None:
        extractor(record_data, result)
        return
    _extract_generic_basic(
        record_data,
        result,
        collector_name=_detect_collector(record_data),
    )


def _update_event_source_coverage(result: ExtractedData, event_count_before: int) -> None:
    if len(result.events) > event_count_before:
        result.source_coverage["events"] = result.source_coverage.get("events", 0) + 1


def _collector_extractors() -> dict[str, Any]:
    return {
        "steam": _extract_steam,
        "steam_discussions": _extract_steam_discussions,
        "taptap": _extract_taptap,
        "gtrends": _extract_gtrends,
        "monitor": _extract_monitor,
        "events": _extract_events,
        "qimai": _extract_qimai,
        "official_site": _extract_official_site,
    }


# ==================== Qimai 提取 ====================


def _legacy_extract_official_site(data: dict[str, Any], result: ExtractedData) -> None:
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


def _legacy_extract_qimai(data: dict[str, Any], result: ExtractedData) -> None:
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
            "Qimai AppStore rating CN": qimai_data.get(
                "appstore_rating_cn", snapshot.get("appstore_rating_cn", "")
            ),
            "Qimai DAU avg 30d": _qimai_average_value(
                qimai_data, snapshot, "dau_avg_30d", "dau_trend_90d", api_urls, "appstatus"
            ),
            "Qimai downloads avg 30d": _qimai_average_value(
                qimai_data,
                snapshot,
                "downloads_avg_30d",
                "downloads_trend_90d",
                api_urls,
                "download",
            ),
            "Qimai revenue avg 30d": _qimai_average_value(
                qimai_data, snapshot, "revenue_avg_30d", "revenue_trend_90d", api_urls, "revenue"
            ),
        }
    )

    result.overview.append(overview_row)

    _append_qimai_series(
        result,
        game_name,
        "iOS grossing rank",
        qimai_data.get("ios_grossing_rank_trend", []),
        api_urls=api_urls,
        required_api="rank",
    )
    _append_qimai_series(
        result,
        game_name,
        "AppStore reviews",
        qimai_data.get("appstore_review_trend", []),
        api_urls=api_urls,
        required_api="comment",
    )
    _append_qimai_series(
        result,
        game_name,
        "DAU",
        qimai_data.get("dau_trend_90d", []),
        api_urls=api_urls,
        required_api="appstatus",
    )
    _append_qimai_series(
        result,
        game_name,
        "Downloads",
        qimai_data.get("downloads_trend_90d", []),
        api_urls=api_urls,
        required_api="download",
    )
    _append_qimai_series(
        result,
        game_name,
        "Revenue",
        qimai_data.get("revenue_trend_90d", []),
        api_urls=api_urls,
        required_api="revenue",
    )


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


def _sanitize_qimai_report_series(
    metric: str, series: list[dict[str, Any]]
) -> list[dict[str, Any]]:
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


def _qimai_grossing_rank_cn(
    qimai_data: dict[str, Any], snapshot: dict[str, Any], api_urls: list[str]
) -> Any:
    value = qimai_data.get("grossing_rank_cn", snapshot.get("grossing_rank_cn", ""))
    if value in (None, ""):
        latest_rank = _latest_qimai_series_value(qimai_data.get("ios_grossing_rank_trend", []))
        if latest_rank is not None:
            value = f"#{int(latest_rank)}"
    if value in (None, ""):
        return ""
    free_rank = qimai_data.get("free_rank", snapshot.get("free_rank", ""))
    if not _qimai_series_has_required_source(api_urls, "rank") and _normalize_rank_value(
        value
    ) == _normalize_rank_value(free_rank):
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
    return (1_000_000_000 <= number <= 4_102_444_800) or (
        1_000_000_000_000 <= number <= 4_102_444_800_000
    )


# ==================== Steam Community Discussions 提取 ====================


def _legacy_extract_steam_discussions(data: dict[str, Any], result: ExtractedData) -> None:
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
                result.reviews.append(
                    {
                        "游戏名": game_name,
                        "数据来源": "TapTap",
                        "作者": review.get("author", ""),
                        "评分": review.get("score", ""),
                        "评论内容": _truncate(review.get("content", ""), 500),
                        "游戏时长(h)": "",
                        "点赞数": _safe_int(review.get("likes")),
                        "日期": review.get("date", review.get("created_at", "")),
                    }
                )


# ==================== Google Trends 提取 ====================


def _legacy_extract_gtrends(data: dict[str, Any], result: ExtractedData) -> None:
    """从 Google Trends 采集数据中提取结构化字段。"""
    keyword = data.get("keyword", data.get("game_name", ""))
    geo = data.get("geo", "全球")
    timeframe = data.get("timeframe", "")

    snapshot = data.get("snapshot", {})
    trend_history = data.get("trend_history", [])
    result.overview.append(
        _build_gtrends_overview_row(
            data=data,
            keyword=keyword,
            geo=geo,
            snapshot=snapshot,
            trend_history=trend_history,
        )
    )
    _append_gtrends_trend_rows(
        result,
        data=data,
        keyword=keyword,
        geo=geo,
        timeframe=timeframe,
        trend_history=trend_history,
    )
    _append_gtrends_related_queries(result, keyword=keyword, related=data.get("related_queries", {}))


def _build_gtrends_overview_row(
    *,
    data: dict[str, Any],
    keyword: str,
    geo: str,
    snapshot: Any,
    trend_history: Any,
) -> dict[str, Any]:
    trend_count = len(trend_history) if isinstance(trend_history, list) else 0
    snapshot_data = snapshot if isinstance(snapshot, dict) else {}
    return {
        "游戏名": data.get("game_name", keyword),
        "数据来源": f"Google Trends ({geo})",
        "当前在线": "",
        "评论总量": "",
        "好评率": "",
        "价格": "",
        "最新热度": _safe_int(snapshot_data.get("latest_trend_value")) if snapshot_data else "",
        "Google Trends（3个月趋势图）": f"{trend_count} points" if trend_count else "",
        "热门相关词数": _safe_int(snapshot_data.get("top_related_count")) if snapshot_data else "",
        "上升相关词数": _safe_int(snapshot_data.get("rising_related_count")) if snapshot_data else "",
        "采集时间": _extract_time(data),
    }


def _append_gtrends_trend_rows(
    result: ExtractedData,
    *,
    data: dict[str, Any],
    keyword: str,
    geo: str,
    timeframe: str,
    trend_history: Any,
) -> None:
    if not isinstance(trend_history, list):
        return
    for point in trend_history:
        if not isinstance(point, dict):
            continue
        result.trends.append(
            {
                "关键词": keyword,
                "类型": "搜索热度",
                "日期": point.get("date", ""),
                "热度值": _safe_int(point.get("value")),
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
                "热度值": _safe_int(point.get("value")),
            }
        )


def _append_gtrends_related_queries(
    result: ExtractedData,
    *,
    keyword: str,
    related: Any,
) -> None:
    if not isinstance(related, dict):
        return
    _append_gtrends_query_rows(result, keyword=keyword, query_type="热门", items=related.get("top", []))
    _append_gtrends_query_rows(
        result,
        keyword=keyword,
        query_type="上升",
        items=related.get("rising", []),
    )


def _append_gtrends_query_rows(
    result: ExtractedData,
    *,
    keyword: str,
    query_type: str,
    items: Any,
) -> None:
    if not isinstance(items, list):
        return
    for item in items:
        if not isinstance(item, dict):
            continue
        result.related_queries.append(
            {
                "关键词": keyword,
                "类型": query_type,
                "查询词": item.get("query", ""),
                "热度值": item.get("value", ""),
            }
        )


# ==================== Monitor 提取 ====================


def _legacy_extract_monitor(data: dict[str, Any], result: ExtractedData) -> None:
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


def _legacy_extract_events(data: dict[str, Any], result: ExtractedData) -> None:
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
                "摘要": _truncate(
                    item.get("summary") or item.get("摘要") or item.get("content", ""), 800
                ),
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
    if isinstance(source_meta, dict) and not _is_usable_game_name(
        str(source_meta.get("target", ""))
    ):
        source_meta["target"] = canonical_name
    return cloned


def _choose_best_game_name(candidates: list[str]) -> str:
    unique = list(
        dict.fromkeys(candidate for candidate in candidates if _is_usable_game_name(candidate))
    )
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


