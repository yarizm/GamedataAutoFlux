from src.collectors.qimai_collector import (
    _find_app_rank_in_payloads,
    _normalize_series,
    _sanitize_qimai_metric_series,
    _series_from_export_rows,
)
from src.reporting.data_extractor import extract_from_records


def test_qimai_download_export_rows_to_series():
    rows = [
        {"日期": "2026-01-29", "iPhone下载量预估": "12,345", "iPad下载量预估": "100"},
        {"日期": "2026-01-30", "iPhone下载量预估": "10,000", "iPad下载量预估": "80"},
    ]

    assert _series_from_export_rows(rows, "download") == [
        {"date": "2026-01-29", "value": 12345},
        {"date": "2026-01-30", "value": 10000},
    ]


def test_qimai_comment_export_sums_star_columns():
    rows = [
        {"日期": "01月29日", "一星": "10", "二星": "20", "三星": "30", "四星": "40", "五星": "50"},
    ]

    series = _series_from_export_rows(rows, "comment")

    assert len(series) == 1
    assert series[0]["date"].endswith("-01-29")
    assert series[0]["value"] == 150


def test_qimai_rank_export_uses_rank_column():
    rows = [
        {"日期": "2026-04-27", "畅销榜排名": "3", "免费榜排名": "1"},
        {"日期": "2026-04-28", "畅销榜排名": "4", "免费榜排名": "2"},
    ]

    assert _series_from_export_rows(rows, "rank") == [
        {"date": "2026-04-27", "value": 3},
        {"date": "2026-04-28", "value": 4},
    ]


def test_qimai_chart_source_is_accepted_by_report_extractor():
    record = {
        "collector": "qimai",
        "game_name": "Delta Force",
        "qimai": {
            "api_urls": ["qimai_chart://download"],
            "downloads_trend_90d": [
                {"date": "2026-04-27", "value": 1000},
                {"date": "2026-04-28", "value": 1200},
            ],
            "downloads_avg_30d": 1100,
        },
        "snapshot": {"name": "Delta Force"},
    }

    data = extract_from_records([record])

    assert [row["值"] for row in data.trends if row["指标"] == "Downloads"] == [1000, 1200]
    overview = data.overview[0]
    assert overview["Qimai downloads avg 30d"] == 1100


def test_report_extractor_drops_qimai_rank_timestamp_values():
    record = {
        "collector": "qimai",
        "game_name": "Delta Force",
        "qimai": {
            "api_urls": ["qimai_chart://rank"],
            "ios_grossing_rank_trend": [
                {"date": "2026-01-29", "value": 1_769_702_400_000},
                {"date": "2026-01-30", "value": 4},
            ],
        },
        "snapshot": {"name": "Delta Force"},
    }

    data = extract_from_records([record])

    assert [
        (row["日期"], row["值"])
        for row in data.trends
        if row["指标"] == "iOS grossing rank"
    ] == [("2026-01-30", 4)]


def test_report_extractor_uses_latest_rank_trend_for_grossing_rank():
    record = {
        "collector": "qimai",
        "game_name": "Delta Force",
        "qimai": {
            "api_urls": ["https://api.qimai.cn/app/rankMore"],
            "ios_grossing_rank_trend": [
                {"date": "2026-04-27", "value": 41},
                {"date": "2026-04-28", "value": 50},
            ],
        },
        "snapshot": {"name": "Delta Force"},
    }

    data = extract_from_records([record])

    assert data.overview[0]["Qimai grossing rank CN"] == "#50"


def test_qimai_rank_chart_arrays_do_not_use_timestamp_as_value():
    timestamp = 1_769_702_400_000

    assert _normalize_series([[3, timestamp], [timestamp + 86_400_000, 4], ["2026-01-31", timestamp + 172_800_000, 5]]) == [
        {"date": "2026-01-29", "value": 3},
        {"date": "2026-01-30", "value": 4},
        {"date": "2026-01-31", "value": 5},
    ]


def test_qimai_rank_dict_uses_rank_when_value_is_timestamp():
    timestamp = 1_769_702_400_000

    assert _normalize_series([
        {"date": "2026-01-29", "value": timestamp, "rank": 3},
        {"date": "2026-01-30", "value": timestamp + 86_400_000, "ranking": 4},
    ]) == [
        {"date": "2026-01-29", "value": 3},
        {"date": "2026-01-30", "value": 4},
    ]


def test_qimai_public_rank_payload_can_find_app_rank():
    payloads = [
        {
            "url": "https://api.qimai.cn/rank/index",
            "payload": {
                "data": [
                    {"appid": "111", "rank": 1, "name": "Other"},
                    {"appid": "1642894547", "rank": 3, "name": "Delta Force"},
                ]
            },
        }
    ]

    assert _find_app_rank_in_payloads(payloads, "1642894547", "Delta Force") == 3


def test_qimai_rank_series_sanitizer_drops_timestamp_values():
    bad_series = [
        {"date": "2026-01-29", "value": 1_769_702_400_000},
        {"date": "2026-01-30", "value": 3},
    ]

    assert _sanitize_qimai_metric_series("ios_grossing_rank_trend", bad_series) == [
        {"date": "2026-01-30", "value": 3}
    ]
