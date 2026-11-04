from datetime import datetime, timezone

from autoflux_plugin_steam.steam.steam_api_client import _build_review_trend_from_histogram
from autoflux_plugin_steam.steam.steamdb_scraper import (
    _extract_review_stats_from_text,
    _parse_sales_text,
)
from src.reporting.data_extractor import extract_from_records


def test_extract_review_stats_from_steamdb_text() -> None:
    text = "Delta Force\n58.07%\n180k reviews\n105.2k 58.2% positive reviews"

    stats = _extract_review_stats_from_text(text)

    assert stats["steamdb_rating_percent"] == 58.07
    assert stats["review_score_percent"] == 58.07
    assert stats["total_reviews"] == 180000
    assert stats["positive_reviews"] == 105200


def test_report_uses_steamdb_review_rate_and_trend() -> None:
    record = {
        "game_name": "Delta Force",
        "snapshot": {
            "name": "Delta Force",
            "current_players": 100,
            "total_reviews": 999,
            "review_score": "10% (Official)",
        },
        "steam_api": {
            "reviews": {
                "review_score_percent": 10,
                "overall_summary": {"review_score_percent": 10},
                "recent_30d_summary": {"review_score_percent": 11},
                "review_trend_90d": [
                    {"date": "2026-01-01", "positive": 1, "total": 10, "positive_rate": 10}
                ],
            }
        },
        "steamdb": {
            "source": "steamdb_playwright",
            "info": {"steamdb_rating_percent": 58.07},
            "charts": {
                "user_reviews_history_90d": [
                    {
                        "date": "2026-04-28",
                        "positive": 50,
                        "negative": 50,
                        "total": 100,
                        "metric": "bucket",
                        "source": "steamdb_user_reviews_history",
                    },
                    {
                        "date": "2026-04-29",
                        "positive": 80,
                        "negative": 70,
                        "total": 150,
                        "metric": "bucket",
                        "source": "steamdb_user_reviews_history",
                    },
                ]
            },
        },
    }

    extracted = extract_from_records([record])
    overview = extracted.overview[0]

    assert overview["好评率"] == "58.07% (SteamDB)"
    assert overview["整体好评率"] == "58.07%"
    assert overview["近期好评率(30 Days)"] == "52.0%"
    assert len([row for row in extracted.trends if row["类型"] == "Steam好评率(90天)"]) == 2


def test_review_trend_survives_invalid_end_date() -> None:
    # 只有 end_date 且无法解析时回退到今天，不能对 None 做日期运算
    for end_date in ("not-a-timestamp", 10**20):
        rows = _build_review_trend_from_histogram(
            {"results": {"recent": [], "rollups": [], "end_date": end_date}},
            days=3,
        )
        assert [row["total"] for row in rows] == [0, 0, 0]
        assert rows[-1]["date"] == datetime.now(timezone.utc).date().isoformat()


def test_review_trend_uses_valid_end_date() -> None:
    rows = _build_review_trend_from_histogram(
        {"results": {"recent": [], "rollups": [], "end_date": 1_700_000_000}},
        days=2,
    )
    assert rows[-1]["date"] == "2023-11-14"


def test_parse_sales_text_extracts_all_currencies() -> None:
    parsed = _parse_sales_text("Lowest $19.99 €12,99 £9.99 ¥98 R$ 29,99 -50 %", "https://steamdb.info/sales/")
    assert parsed["prices"] == ["$19.99", "€12,99", "£9.99", "¥98", "R$ 29,99"]
    assert parsed["discounts"] == ["-50 %"]
