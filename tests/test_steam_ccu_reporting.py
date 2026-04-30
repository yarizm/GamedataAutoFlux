from src.reporting.data_extractor import extract_from_records


def test_report_excludes_collection_day_from_steam_ccu() -> None:
    record = {
        "game_name": "Delta Force",
        "app_id": "2507950",
        "source_meta": {"collected_at": "2026-04-30T18:00:00Z"},
        "snapshot": {"name": "Delta Force", "app_id": "2507950"},
        "steamdb": {
            "source": "steamdb_playwright",
            "charts": {
                "online_history_daily_precise_90d": [
                    {"date": "2026-04-28", "peak_players": 100},
                    {"date": "2026-04-29", "peak_players": 120},
                    {"date": "2026-04-30", "peak_players": 9999},
                ]
            },
        },
    }

    extracted = extract_from_records([record])
    row_values = [set(row.values()) for row in extracted.steam_player_peaks]
    overview = extracted.overview[0]

    assert len(extracted.steam_player_peaks) == 2
    assert any("2026-04-28" in values and 100 in values for values in row_values)
    assert any("2026-04-29" in values and 120 in values for values in row_values)
    assert not any("2026-04-30" in values or 9999 in values for values in row_values)
    assert 120 in overview.values()
    assert 9999 not in overview.values()
    assert "2 daily points" in overview.values()
