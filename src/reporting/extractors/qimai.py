"""Qimai/App Store reporting extractor."""

from __future__ import annotations

import re
from typing import Any

from src.reporting.extractors.common import extract_time, safe_float, safe_int


def extract_qimai(data: dict[str, Any], result: Any) -> None:
    game_name = data.get("game_name", "")
    snapshot = data.get("snapshot", {})
    qimai_data = data.get("qimai", {})
    api_urls = qimai_data.get("api_urls", []) if isinstance(qimai_data, dict) else []

    overview_row = {
        "游戏名": game_name or snapshot.get("name", "未知"),
        "数据来源": "Qimai(AppStore)",
        "评论总量": safe_int(snapshot.get("total_reviews")),
        "评分": snapshot.get("review_score", ""),
        "AppStore免费榜": snapshot.get("free_rank", ""),
        "AppStore畅销榜": snapshot.get("grossing_rank", ""),
        "采集时间": extract_time(data),
    }
    overview_row.update(
        {
            "Qimai grossing rank CN": qimai_grossing_rank_cn(qimai_data, snapshot, api_urls),
            "Qimai AppStore rating CN": qimai_data.get(
                "appstore_rating_cn", snapshot.get("appstore_rating_cn", "")
            ),
            "Qimai DAU avg 30d": qimai_average_value(
                qimai_data, snapshot, "dau_avg_30d", "dau_trend_90d", api_urls, "appstatus"
            ),
            "Qimai downloads avg 30d": qimai_average_value(
                qimai_data,
                snapshot,
                "downloads_avg_30d",
                "downloads_trend_90d",
                api_urls,
                "download",
            ),
            "Qimai revenue avg 30d": qimai_average_value(
                qimai_data, snapshot, "revenue_avg_30d", "revenue_trend_90d", api_urls, "revenue"
            ),
        }
    )

    result.overview.append(overview_row)

    append_qimai_series(
        result,
        game_name,
        "iOS grossing rank",
        qimai_data.get("ios_grossing_rank_trend", []),
        api_urls=api_urls,
        required_api="rank",
    )
    append_qimai_series(
        result,
        game_name,
        "AppStore reviews",
        qimai_data.get("appstore_review_trend", []),
        api_urls=api_urls,
        required_api="comment",
    )
    append_qimai_series(
        result,
        game_name,
        "DAU",
        qimai_data.get("dau_trend_90d", []),
        api_urls=api_urls,
        required_api="appstatus",
    )
    append_qimai_series(
        result,
        game_name,
        "Downloads",
        qimai_data.get("downloads_trend_90d", []),
        api_urls=api_urls,
        required_api="download",
    )
    append_qimai_series(
        result,
        game_name,
        "Revenue",
        qimai_data.get("revenue_trend_90d", []),
        api_urls=api_urls,
        required_api="revenue",
    )


def append_qimai_series(
    result: Any,
    game_name: str,
    metric: str,
    series: list[dict[str, Any]],
    *,
    api_urls: list[str],
    required_api: str,
) -> None:
    if not isinstance(series, list):
        return
    if not qimai_series_has_required_source(api_urls, required_api):
        return
    series = sanitize_qimai_report_series(metric, series)
    if looks_like_qimai_activity_series(series):
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


def sanitize_qimai_report_series(metric: str, series: list[dict[str, Any]]) -> list[dict[str, Any]]:
    sanitized: list[dict[str, Any]] = []
    for point in series:
        if not isinstance(point, dict):
            continue
        value = safe_float(point.get("value"))
        if value is None or looks_like_timestamp_number(value):
            continue
        if metric == "iOS grossing rank" and not (0 < value <= 2000):
            continue
        sanitized.append({**point, "value": int(value) if value.is_integer() else value})
    return sanitized


def qimai_grossing_rank_cn(
    qimai_data: dict[str, Any], snapshot: dict[str, Any], api_urls: list[str]
) -> Any:
    value = qimai_data.get("grossing_rank_cn", snapshot.get("grossing_rank_cn", ""))
    if value in (None, ""):
        latest_rank = latest_qimai_series_value(qimai_data.get("ios_grossing_rank_trend", []))
        if latest_rank is not None:
            value = f"#{int(latest_rank)}"
    if value in (None, ""):
        return ""
    free_rank = qimai_data.get("free_rank", snapshot.get("free_rank", ""))
    if not qimai_series_has_required_source(api_urls, "rank") and normalize_rank_value(
        value
    ) == normalize_rank_value(free_rank):
        return ""
    return value


def qimai_average_value(
    qimai_data: dict[str, Any],
    snapshot: dict[str, Any],
    average_key: str,
    series_key: str,
    api_urls: list[str],
    required_api: str,
) -> Any:
    series = qimai_data.get(series_key, [])
    if not qimai_series_has_required_source(api_urls, required_api):
        return ""
    if isinstance(series, list) and looks_like_qimai_activity_series(series):
        return ""
    return qimai_data.get(average_key, snapshot.get(average_key, ""))


def normalize_rank_value(value: Any) -> str:
    text = str(value or "").strip()
    match = re.search(r"\d+", text)
    return match.group(0) if match else text


def latest_qimai_series_value(series: Any) -> float | None:
    if not isinstance(series, list):
        return None
    for point in reversed(series):
        if not isinstance(point, dict):
            continue
        value = safe_float(point.get("value"))
        if value is not None and not looks_like_timestamp_number(value):
            return value
    return None


def qimai_series_has_required_source(api_urls: list[str], required_api: str) -> bool:
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


def looks_like_qimai_activity_series(series: list[dict[str, Any]]) -> bool:
    values = [point.get("value") for point in series if isinstance(point, dict)]
    dates = [str(point.get("date", "")) for point in series if isinstance(point, dict)]
    return values == [2, 72600, -2493] and dates == ["2026-01-29", "2026-02-10", "2026-04-10"]


def looks_like_timestamp_number(value: Any) -> bool:
    if isinstance(value, bool):
        return False
    try:
        number = abs(float(value))
    except (TypeError, ValueError):
        return False
    return (1_000_000_000 <= number <= 4_102_444_800) or (
        1_000_000_000_000 <= number <= 4_102_444_800_000
    )
