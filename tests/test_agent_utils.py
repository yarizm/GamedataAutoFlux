"""Tests for shared utility functions in src.services._utils."""

from src.services._utils import (
    build_record_summary,
    compute_record_completeness,
    extract_record_identity,
    first_str,
    nested_get,
    normalize_key,
    source_label,
    detect_collector,
    record_group,
    max_iso,
    parse_date_prefix,
    replace_date_prefix,
    roll_time_params,
)


class TestNestedGet:
    def test_basic(self):
        data = {"a": {"b": {"c": 42}}}
        assert nested_get(data, "a", "b", "c") == 42

    def test_missing_key(self):
        data = {"a": {"b": {}}}
        assert nested_get(data, "a", "x") is None

    def test_non_dict_intermediate(self):
        assert nested_get({"a": 1}, "a", "b") is None

    def test_top_level(self):
        assert nested_get({"x": 1}, "x") == 1


class TestFirstStr:
    def test_returns_first_non_empty(self):
        assert first_str(None, "", "hello", "world") == "hello"

    def test_all_empty_returns_empty(self):
        assert first_str(None, "", None) == ""

    def test_single_value(self):
        assert first_str("only") == "only"


class TestNormalizeKey:
    def test_alphanumeric(self):
        assert normalize_key("Hello World") == "hello-world"

    def test_chinese(self):
        result = normalize_key("原神")
        assert result and result != "unknown"

    def test_empty(self):
        assert normalize_key("") == "unknown"


class TestDetectCollector:
    def test_steam_discussions(self):
        assert detect_collector({"discussions": {}}) == "steam_discussions"

    def test_steam(self):
        assert detect_collector({"steamdb": {}, "news": []}) == "steam"

    def test_taptap(self):
        assert detect_collector({"reviews_summary": {}}) == "taptap"

    def test_gtrends(self):
        assert detect_collector({"trend_history": []}) == "gtrends"

    def test_events(self):
        assert detect_collector({"events": []}) == "events"

    def test_monitor(self):
        assert detect_collector({"monitor_metrics": {}}) == "monitor"

    def test_unknown(self):
        assert detect_collector({}) == "unknown"


class TestSourceLabel:
    def test_known(self):
        assert source_label("steam") == "Steam"
        assert source_label("taptap") == "TapTap"

    def test_unknown(self):
        assert source_label("custom") == "custom"


class TestExtractRecordIdentity:
    def test_steam(self, sample_record_steam):
        identity = extract_record_identity(sample_record_steam)
        assert identity is not None
        assert identity["game_name"] == "Counter-Strike 2"
        assert identity["app_id"] == "730"
        assert identity["collector"] == "steam"

    def test_taptap(self, sample_record_taptap):
        identity = extract_record_identity(sample_record_taptap)
        assert identity is not None
        assert identity["game_name"] == "原神"
        assert identity["app_id"] == "100"

    def test_partial(self, sample_record_partial):
        identity = extract_record_identity(sample_record_partial)
        assert identity is not None
        assert identity["game_name"] == "Partial Game"
        assert identity["app_id"] == ""

    def test_empty(self, sample_record_empty):
        assert extract_record_identity(sample_record_empty) is None


class TestRecordGroup:
    def test_with_group(self):
        from src.storage.base import StorageRecord
        r = StorageRecord(key="k", data={}, metadata={"group_id": "g1", "group_name": "Group 1"}, source="s")
        g = record_group(r)
        assert g["group_id"] == "g1"
        assert g["group_name"] == "Group 1"

    def test_without_group(self):
        from src.storage.base import StorageRecord
        r = StorageRecord(key="k", data={}, metadata={}, source="s")
        g = record_group(r)
        assert g["group_id"] == ""
        assert g["group_name"] == ""


class TestComputeCompleteness:
    def test_full(self, sample_record_steam):
        assert compute_record_completeness(sample_record_steam) == "full"

    def test_partial(self, sample_record_partial):
        assert compute_record_completeness(sample_record_partial) == "partial"

    def test_empty(self, sample_record_empty):
        assert compute_record_completeness(sample_record_empty) == "empty"

    def test_non_dict_data(self):
        from src.storage.base import StorageRecord
        r = StorageRecord(key="k", data=None, metadata={}, source="s")
        assert compute_record_completeness(r) == "empty"


class TestBuildRecordSummary:
    def test_steam(self, sample_record_steam):
        summary = build_record_summary(sample_record_steam.data)
        assert summary["current_players"] == 1000000
        assert summary["total_reviews"] == 500000
        assert summary["review_score"] == 88

    def test_empty(self):
        assert build_record_summary({}) == {}

    def test_non_dict(self):
        assert build_record_summary(None) == {}


class TestMaxIso:
    def test_first_none(self):
        assert max_iso(None, "2025-01-01T00:00:00") == "2025-01-01T00:00:00"

    def test_second_none(self):
        assert max_iso("2025-01-01T00:00:00", None) == "2025-01-01T00:00:00"

    def test_both_valid(self):
        assert max_iso("2025-01-01T00:00:00", "2025-02-01T00:00:00") == "2025-02-01T00:00:00"

    def test_both_none(self):
        assert max_iso(None, None) is None


class TestDateHelpers:
    def test_parse_valid(self):
        d = parse_date_prefix("2025-01-15")
        assert d is not None
        assert d.year == 2025
        assert d.month == 1
        assert d.day == 15

    def test_parse_valid_datetime(self):
        d = parse_date_prefix("2025-01-15T10:00:00")
        assert d is not None

    def test_parse_invalid(self):
        assert parse_date_prefix("not a date") is None
        assert parse_date_prefix("") is None

    def test_replace_short(self):
        from datetime import date
        result = replace_date_prefix("2025-01-15", date(2025, 6, 1))
        assert result == "2025-06-01"

    def test_replace_with_suffix(self):
        from datetime import date
        result = replace_date_prefix("2025-01-15T10:00:00", date(2025, 6, 1))
        assert result == "2025-06-01T10:00:00"

    def test_roll_time_params(self):
        params = {"start_date": "2025-01-01", "end_date": "2025-01-31"}
        roll_time_params(params)
        assert params["start_date"] != "2025-01-01"
        assert params["end_date"] != "2025-01-31"
        # End date should be today
        from datetime import date
        assert params["end_date"] == date.today().isoformat()
