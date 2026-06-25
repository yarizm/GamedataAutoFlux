from src.reporting.data_extractor import extract_from_records


def test_report_extracts_steam_events_and_top_sellers() -> None:
    record = {
        "game_name": "Delta Force",
        "app_id": "2507950",
        "snapshot": {"name": "Delta Force", "app_id": "2507950"},
        "news": {
            "items": [
                {
                    "title": "Season Update Live",
                    "date": 1772409600,
                    "contents": "Big seasonal content update",
                    "author": "Steam Team",
                    "url": "https://store.steampowered.com/news/app/2507950/view/1",
                    "gid": "news-1",
                }
            ]
        },
        "steamdb": {
            "source": "steamdb_playwright",
            "top_sellers": {"rank": 7},
            "charts": {
                "update_history": [
                    {
                        "patch_id": "patch-1",
                        "patchnote_url": "https://steamdb.info/patchnotes/patch-1/",
                        "updated_at_relative": "2 hours ago",
                        "timestamp_raw": "2026-03-01 10:00 UTC",
                    }
                ]
            },
        },
    }

    extracted = extract_from_records([record])
    overview = extracted.overview[0]
    event_titles = {item["标题"] for item in extracted.events}
    steam_news = [item for item in extracted.trends if item.get("类型") == "Steam新闻"]

    assert overview["steam畅销榜"] == "#7"
    assert "Season Update Live" in event_titles
    assert "SteamDB Patch patch-1" in event_titles
    assert any(item["事件类型"] == "版本更新" for item in extracted.events)
    assert any(item["事件类型"] == "SteamDB版本更新" for item in extracted.events)
    assert len(steam_news) == 1
    assert steam_news[0]["标题"] == "Season Update Live"
