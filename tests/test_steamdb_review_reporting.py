from src.collectors.steam.steamdb_scraper import _extract_review_stats_from_text
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
