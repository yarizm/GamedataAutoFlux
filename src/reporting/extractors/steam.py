"""Steam reporting extractor."""

from __future__ import annotations

import re
from datetime import date, datetime, timedelta, timezone
from typing import Any

from src.reporting.extractors.common import (
    extract_time,
    first_number,
    first_present,
    format_percent,
    parse_date_only,
    parse_datetime_value,
    safe_float,
    safe_int,
    truncate,
)


def extract_steam(data: dict[str, Any], result: Any) -> None:
    """Extract structured report rows from Steam payloads."""
    game_name = data.get("game_name", "")
    snapshot = data.get("snapshot", {})
    steam_api = data.get("steam_api", {}) if isinstance(data.get("steam_api"), dict) else {}
    if not snapshot and "content" in data:
        content = data["content"]
        if isinstance(content, dict):
            snapshot = content.get("snapshot", {})
            game_name = game_name or content.get("game_name", "")
            steam_api = (
                content.get("steam_api", steam_api)
                if isinstance(content.get("steam_api"), dict)
                else steam_api
            )

    reviews_data = data.get("reviews", {}) or steam_api.get("reviews", {})
    if not isinstance(reviews_data, dict):
        reviews_data = {}
    overall_summary = (
        reviews_data.get("overall_summary", {})
        if isinstance(reviews_data.get("overall_summary"), dict)
        else {}
    )
    recent_30d_summary = (
        reviews_data.get("recent_30d_summary", {})
        if isinstance(reviews_data.get("recent_30d_summary"), dict)
        else {}
    )
    review_trend_90d = (
        reviews_data.get("review_trend_90d", [])
        if isinstance(reviews_data.get("review_trend_90d"), list)
        else []
    )
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
        steamdb_overall_positive_rate = steamdb_overall_positive_rate_value(steamdb)
        steamdb_review_score_text = steamdb_review_score_text_value(steamdb)
        charts_for_review = steamdb.get("charts", {})
        if isinstance(charts_for_review, dict):
            steamdb_review_trend = steamdb_user_review_trend(charts_for_review)
            if steamdb_review_trend:
                review_trend_90d = steamdb_review_trend
                review_trend_summary = {
                    "days": 90,
                    "complete": len(steamdb_review_trend) >= 90,
                    "reviews_fetched": int(
                        sum(safe_float(row.get("total")) or 0 for row in steamdb_review_trend)
                    ),
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
        overall_positive_rate = first_present(
            overall_summary.get("review_score_percent"),
            reviews_data.get("review_score_percent"),
        )
    recent_positive_rate = review_rate_from_trend(review_trend_90d, days=30)
    if recent_positive_rate in (None, "") and not steamdb_present:
        recent_positive_rate = recent_30d_summary.get("review_score_percent")

    overview_row = {
        "游戏名": game_name or snapshot.get("name", "未知"),
        "数据来源": "Steam",
        "当前在线": safe_int(snapshot.get("current_players")),
        "评论总量": safe_int(snapshot.get("total_reviews")),
        "好评率": steamdb_review_score_text
        or ("" if steamdb_present else snapshot.get("review_score", "")),
        "整体好评率": format_percent(overall_positive_rate),
        "近期好评率(30 Days)": format_percent(recent_positive_rate),
        "3个月好评率趋势图": review_trend_summary_text(review_trend_90d, review_trend_summary),
        "价格": snapshot.get("price", ""),
        "标签": ", ".join(snapshot.get("tags", []))
        if isinstance(snapshot.get("tags"), list)
        else "",
        "开发商": snapshot.get("developer", ""),
        "发行商": snapshot.get("publisher", ""),
        "采集时间": extract_time(data),
    }

    if isinstance(steamdb, dict):
        overview_row["SteamDB月峰值"] = safe_int(steamdb.get("monthly_peak"))
        overview_row["SteamDB日均在线"] = safe_int(steamdb.get("daily_avg"))

        charts = steamdb.get("charts", {})
        if isinstance(charts, dict):
            ccu_excluded_date = steam_ccu_excluded_date(data, steamdb)
            followers = charts.get("followers_history", [])
            if isinstance(followers, list) and followers:
                follower_gain = series_gain_last_days(followers, 7)
                overview_row["Steam关注增量(7日)"] = (
                    follower_gain if follower_gain is not None else ""
                )
            wishlist = charts.get("wishlist_history", [])
            wishlist_gain = (
                series_gain_last_days(wishlist, 7) if isinstance(wishlist, list) else None
            )
            follower_gain = (
                series_gain_last_days(followers, 7) if isinstance(followers, list) else None
            )
            overview_row["WishList Activity(7d Gain)/Follower(7d Gain)"] = (
                format_wishlist_follower_gain(
                    wishlist_gain,
                    follower_gain,
                )
            )
            overview_row["7日ccu peak"] = max_peak_last_days(
                charts, 7, excluded_date=ccu_excluded_date
            )
            overview_row["30日CCU peak"] = max_peak_last_days(
                charts, 30, excluded_date=ccu_excluded_date
            )
            overview_row["3个月ccu趋势"] = ccu_trend_summary(
                charts, excluded_date=ccu_excluded_date
            )
            overview_row["steam畅销榜"] = extract_steam_top_sellers_rank(steamdb, charts)

        result.steam_player_peaks.extend(
            extract_steam_peak_rows(data, steamdb, game_name, snapshot)
        )
        result.steam_monthly_peaks.extend(
            extract_steam_monthly_rows(data, steamdb, game_name, snapshot)
        )
        result.events.extend(extract_steamdb_event_rows(data, steamdb, game_name, snapshot))

    result.overview.append(overview_row)
    append_steam_review_trend(result, game_name or snapshot.get("name", ""), review_trend_90d)

    if isinstance(reviews_data, dict):
        items = reviews_data.get("items", []) or reviews_data.get("reviews", [])
        if isinstance(items, list):
            for review in items[:100]:
                if not isinstance(review, dict):
                    continue
                result.reviews.append(
                    {
                        "游戏名": game_name or snapshot.get("name", ""),
                        "数据来源": "Steam",
                        "作者": review.get("author", {}).get("steamid", "")
                        if isinstance(review.get("author"), dict)
                        else "",
                        "评分": "好评" if review.get("voted_up") else "差评",
                        "评论内容": truncate(
                            review.get("review", review.get("review_text", "")), 500
                        ),
                        "游戏时长(h)": round(
                            review.get("author", {}).get("playtime_forever", 0) / 60, 1
                        )
                        if isinstance(review.get("author"), dict)
                        else "",
                        "点赞数": safe_int(review.get("votes_up")),
                        "日期": review.get("timestamp_created", ""),
                    }
                )

    news = data.get("news", {}) or steam_api.get("news", {})
    news_items = news.get("items", []) if isinstance(news, dict) else news
    if isinstance(news_items, list):
        for article in news_items[:50]:
            if not isinstance(article, dict):
                continue
            event_row = build_steam_news_event(data, article, game_name, snapshot)
            result.events.append(event_row)
            result.trends.append(
                {
                    "关键词": game_name,
                    "类型": "Steam新闻",
                    "日期": event_row.get("日期", ""),
                    "标题": event_row.get("标题", ""),
                    "热度值": "",
                }
            )


def extract_steam_peak_rows(
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
    if (
        isinstance(online_history, dict)
        and isinstance(online_history.get("records"), list)
        and len(online_history.get("records") or []) >= 90
    ):
        records = online_history["records"]
    if not records:
        records = charts.get("online_history_daily_precise_90d") or []
    if (
        not records
        and isinstance(online_history, dict)
        and isinstance(online_history.get("records"), list)
    ):
        records = online_history["records"]
    if not records:
        records = charts.get("online_history_daily_precise_30d") or []
    if not records:
        records = (
            charts.get("online_history_monthly_peak_1y") or charts.get("online_history_1y") or []
        )
    if not isinstance(records, list):
        return []

    rows: list[dict[str, Any]] = []
    app_id = data.get("app_id") or snapshot.get("app_id", "")
    excluded_date = steam_ccu_excluded_date(data, steamdb)
    for record in records:
        if not isinstance(record, dict):
            continue
        date_value = record.get("date") or record.get("month") or record.get("label")
        peak_value = first_number(
            record, "peak_players", "peak", "players", "max_players", "daily_peak_players"
        )
        if date_value in (None, "") or peak_value is None:
            continue
        if is_excluded_steam_ccu_day(record, excluded_date):
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
                or (
                    online_history.get("requested_slice")
                    if isinstance(online_history, dict)
                    else ""
                ),
            }
        )
    return rows


def extract_steam_monthly_rows(
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
        peak_value = first_number(record, "peak_value", "peak_players", "peak", "players")
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


def append_steam_review_trend(result: Any, game_name: str, trend_rows: list[dict[str, Any]]) -> None:
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


def steamdb_user_review_trend(charts: dict[str, Any]) -> list[dict[str, Any]]:
    rows = charts.get("user_reviews_history_90d") or charts.get("user_reviews_history") or []
    if not isinstance(rows, list):
        return []
    normalized: list[dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        date_value = row.get("date") or row.get("timestamp")
        positive = safe_float(row.get("positive"))
        negative = safe_float(row.get("negative"))
        total = safe_float(row.get("total"))
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


def steamdb_overall_positive_rate_value(steamdb: dict[str, Any]) -> float | None:
    charts = steamdb.get("charts", {}) if isinstance(steamdb.get("charts"), dict) else {}
    for container in (charts, steamdb.get("info", {}), steamdb):
        if not isinstance(container, dict):
            continue
        direct = first_present(
            container.get("steamdb_rating_percent"),
            container.get("review_score_percent"),
            container.get("positive_reviews_percent"),
        )
        parsed = safe_float(direct)
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
        parsed = parse_steamdb_rating_from_text(text)
        if parsed is not None:
            return parsed
    trend = steamdb_user_review_trend(charts)
    if trend:
        return review_rate_from_trend(trend, days=len(trend))
    return None


def steamdb_review_score_text_value(steamdb: dict[str, Any]) -> str:
    rate = steamdb_overall_positive_rate_value(steamdb)
    if rate is None:
        return ""
    return f"{rate:.2f}% (SteamDB)"


def parse_steamdb_rating_from_text(text: str) -> float | None:
    normalized = re.sub(r"\s+", " ", str(text or "")).strip()
    patterns = (
        r"([0-9]{1,3}(?:\.[0-9]+)?)\s*%\s+SteamDB Rating",
        r"([0-9]{1,3}(?:\.[0-9]+)?)\s*%\s+[0-9][0-9,.\s]*[KMBkmb]?\s+reviews\b",
    )
    for pattern in patterns:
        match = re.search(pattern, normalized, re.IGNORECASE)
        if not match:
            continue
        parsed = safe_float(match.group(1))
        if parsed is not None:
            return parsed
    return None


def review_trend_summary_text(rows: list[dict[str, Any]], summary: dict[str, Any]) -> str:
    if not rows:
        return ""
    days = int(summary.get("days") or len(rows) or 90)
    total_reviews = summary.get("total_reviews")
    reviews_fetched = summary.get("reviews_fetched")
    complete = summary.get("complete")
    if complete is False and total_reviews not in (None, "") and reviews_fetched not in (None, ""):
        return f"{len(rows)}/{days} days (incomplete {reviews_fetched}/{total_reviews} reviews)"
    return f"{len(rows)}/{days} days"


def review_rate_from_trend(rows: list[dict[str, Any]], *, days: int) -> float | None:
    if not isinstance(rows, list) or not rows:
        return None
    sorted_rows = sorted(
        (row for row in rows if isinstance(row, dict)),
        key=lambda row: str(row.get("date", "")),
    )
    if is_cumulative_review_series(sorted_rows):
        window = sorted_rows[-days:]
        if len(window) >= 2:
            first = window[0]
            last = window[-1]
            positive_delta = (safe_float(last.get("positive")) or 0) - (
                safe_float(first.get("positive")) or 0
            )
            total_delta = (safe_float(last.get("total")) or 0) - (
                safe_float(first.get("total")) or 0
            )
            if positive_delta >= 0 and total_delta > 0:
                return round((positive_delta / total_delta) * 100, 2)
        latest_rate = safe_float(sorted_rows[-1].get("positive_rate"))
        return round(latest_rate, 2) if latest_rate is not None else None

    positive_total = 0.0
    review_total = 0.0
    for row in sorted_rows[-days:]:
        total = safe_float(row.get("total"))
        positive = safe_float(row.get("positive"))
        if total is None or total <= 0 or positive is None:
            continue
        positive_total += positive
        review_total += total
    if review_total <= 0:
        return None
    return round((positive_total / review_total) * 100, 2)


def is_cumulative_review_series(rows: list[dict[str, Any]]) -> bool:
    if any(row.get("metric") == "cumulative" for row in rows):
        return True
    comparable = [(safe_float(row.get("positive")), safe_float(row.get("total"))) for row in rows]
    comparable = [pair for pair in comparable if pair[0] is not None and pair[1] is not None]
    if len(comparable) < 3:
        return False
    positives = [pair[0] for pair in comparable]
    totals = [pair[1] for pair in comparable]
    return positives == sorted(positives) and totals == sorted(totals)


def max_peak_last_days(
    charts: dict[str, Any],
    days: int,
    *,
    excluded_date: date | None = None,
) -> int | float | str:
    online_history = charts.get("online_history", {})
    daily = (
        charts.get("online_history_daily_precise_90d")
        or charts.get("online_history_daily_precise_30d")
        or []
    )
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
        day = parse_date_only(record.get("date") or record.get("timestamp"))
        peak = first_number(record, "peak_players", "peak", "players", "max_players")
        if day is None or peak is None:
            continue
        if excluded_date is not None and day == excluded_date:
            continue
        if day >= cutoff:
            values.append(peak)
    return max(values) if values else ""


def ccu_trend_summary(charts: dict[str, Any], *, excluded_date: date | None = None) -> str:
    daily = (
        charts.get("online_history_daily_precise_90d")
        or charts.get("online_history_daily_precise_30d")
        or []
    )
    if isinstance(daily, list) and daily:
        complete_daily = [
            row
            for row in daily
            if not (isinstance(row, dict) and is_excluded_steam_ccu_day(row, excluded_date))
        ]
        return f"{min(len(complete_daily), 90)} daily points"
    monthly = charts.get("online_history_monthly_peak_1y") or charts.get("online_history_1y") or []
    if isinstance(monthly, list) and monthly:
        return f"{min(len(monthly), 3)} monthly points"
    return ""


def extract_steam_top_sellers_rank(steamdb: dict[str, Any], charts: dict[str, Any]) -> str:
    top_sellers = steamdb.get("top_sellers")
    if isinstance(top_sellers, dict):
        rank = top_sellers.get("rank")
        if rank not in (None, ""):
            return f"#{rank}"
        if top_sellers.get("matched") is False:
            return "未进入SteamDB当前全球畅销榜Top 100"
        if top_sellers.get("error"):
            return "Steam畅销榜采集失败"

    fallback = first_present(
        charts.get("steam_top_sellers_rank"),
        charts.get("top_sellers_rank"),
        charts.get("sales_rank"),
        charts.get("global_top_sellers"),
        charts.get("rank"),
    )
    if fallback not in (None, ""):
        return str(fallback)
    return "未采集"


def steam_ccu_excluded_date(data: dict[str, Any], steamdb: dict[str, Any]) -> date:
    candidates: list[Any] = [
        extract_time(data),
        data.get("collected_at"),
        steamdb.get("collected_at") if isinstance(steamdb, dict) else None,
    ]
    source_meta = data.get("source_meta", {}) if isinstance(data.get("source_meta"), dict) else {}
    if isinstance(source_meta, dict):
        candidates.append(source_meta.get("collected_at"))
    steamdb_meta = (
        steamdb.get("source_meta", {}) if isinstance(steamdb.get("source_meta"), dict) else {}
    )
    if isinstance(steamdb_meta, dict):
        candidates.append(steamdb_meta.get("collected_at"))

    for value in candidates:
        parsed = parse_datetime_value(value)
        if parsed is not None:
            return parsed.date()
    return datetime.now(timezone.utc).date()


def is_excluded_steam_ccu_day(record: dict[str, Any], excluded_date: date | None) -> bool:
    if excluded_date is None:
        return False
    record_date = parse_date_only(record.get("date") or record.get("timestamp"))
    return record_date == excluded_date


def series_gain_last_days(series: list[Any], days: int) -> int | float | None:
    points: list[tuple[datetime, int | float]] = []
    for item in series:
        if not isinstance(item, dict):
            continue
        dt = parse_datetime_value(item.get("timestamp") or item.get("date") or item.get("month"))
        value = first_number(item, "peak_players", "value", "followers", "wishlist", "players", "peak")
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


def format_wishlist_follower_gain(
    wishlist_gain: int | float | None,
    follower_gain: int | float | None,
) -> str:
    wishlist_text = "" if wishlist_gain is None else str(wishlist_gain)
    follower_text = "" if follower_gain is None else str(follower_gain)
    if not wishlist_text and not follower_text:
        return ""
    return f"Wishlist {wishlist_text or 'N/A'} / Follower {follower_text or 'N/A'}"


def build_steam_news_event(
    data: dict[str, Any],
    article: dict[str, Any],
    game_name: str,
    snapshot: dict[str, Any],
) -> dict[str, Any]:
    title = str(article.get("title", "") or "")
    return {
        "游戏名": game_name or snapshot.get("name", ""),
        "App ID": data.get("app_id") or snapshot.get("app_id", ""),
        "日期": format_event_time(article.get("date")),
        "事件类型": classify_event_title(title),
        "标题": title,
        "摘要": truncate(article.get("contents", ""), 800),
        "来源": "Steam官方新闻",
        "作者/来源名": article.get("author") or article.get("feed_name", ""),
        "URL": article.get("url", ""),
        "原始ID": article.get("gid", ""),
    }


def extract_steamdb_event_rows(
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
            updated_at = update.get("updated_at") or format_event_time(update.get("timestamp_unix"))
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


def format_event_time(value: Any) -> str:
    if value in (None, ""):
        return ""
    if isinstance(value, (int, float)):
        try:
            return datetime.fromtimestamp(float(value), tz=timezone.utc).strftime(
                "%Y-%m-%d %H:%M:%S UTC"
            )
        except (OSError, OverflowError, ValueError):
            return str(value)
    return str(value)


def classify_event_title(title: str) -> str:
    lowered = title.lower()
    if any(keyword in lowered for keyword in ("update", "patch", "bug", "fix", "optimization", "hotfix")):
        return "版本更新"
    if any(keyword in lowered for keyword in ("event", "season", "activity", "festival")):
        return "活动"
    return "公告/新闻"
