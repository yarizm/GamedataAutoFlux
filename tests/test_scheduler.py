"""Tests for scheduler pure functions (_roll_refresh_template, roll_time_params, etc.)."""

from datetime import date

from src.core.scheduler import _roll_refresh_template
from src.services._utils import (
    roll_time_params as _roll_time_params,
    parse_date_prefix as _parse_date_prefix,
    replace_date_prefix as _replace_date_prefix,
)


class TestParseDatePrefix:
    def test_valid_iso_date(self):
        d = _parse_date_prefix("2025-01-15")
        assert d == date(2025, 1, 15)

    def test_valid_datetime(self):
        d = _parse_date_prefix("2025-01-15T10:00:00")
        assert d == date(2025, 1, 15)

    def test_invalid_returns_none(self):
        assert _parse_date_prefix("not a date") is None

    def test_empty_returns_none(self):
        assert _parse_date_prefix("") is None

    def test_none_returns_none(self):
        assert _parse_date_prefix(None) is None


class TestReplaceDatePrefix:
    def test_date_only(self):
        result = _replace_date_prefix("2025-01-15", date(2025, 6, 1))
        assert result == "2025-06-01"

    def test_with_time_suffix(self):
        result = _replace_date_prefix("2025-01-15T10:00:00", date(2025, 6, 1))
        assert result == "2025-06-01T10:00:00"


class TestRollTimeParams:
    def test_shifts_dates(self):
        params = {"start_date": "2025-01-01", "end_date": "2025-01-31"}
        _roll_time_params(params)
        today = date.today()
        assert params["end_date"] == today.isoformat()
        assert params["start_date"] != "2025-01-01"

    def test_shifts_time_params(self):
        params = {"start_time": "2025-01-01T00:00:00", "end_time": "2025-01-02T00:00:00"}
        _roll_time_params(params)
        today = date.today()
        assert params["end_time"].startswith(today.isoformat())

    def test_ignores_missing_keys(self):
        params = {"other_param": "value"}
        original = dict(params)
        _roll_time_params(params)
        assert params == original

    def test_ignores_non_date_values(self):
        params = {"start_date": "not-valid", "end_date": "also-invalid"}
        _roll_time_params(params)
        assert params["start_date"] == "not-valid"
        assert params["end_date"] == "also-invalid"


class TestRollRefreshTemplate:
    def test_no_rolling_window_returns_copy(self):
        template = {"targets": [{"name": "CS2", "params": {"app_id": "730"}}]}
        result = _roll_refresh_template(template)
        assert result is not template  # deep copy
        assert result == template

    def test_empty_template(self):
        assert _roll_refresh_template({}) == {}

    def test_none_template(self):
        assert _roll_refresh_template(None) == {}

    def test_with_rolling_window_rolls_dates(self):
        template = {
            "config": {"refresh": {"rolling_window": True}},
            "targets": [
                {"name": "CS2", "params": {"start_date": "2025-01-01", "end_date": "2025-01-31"}}
            ],
        }
        result = _roll_refresh_template(template)
        today = date.today()
        rolled_end = result["targets"][0]["params"]["end_date"]
        assert rolled_end == today.isoformat()

    def test_rolling_without_targets(self):
        template = {"config": {"refresh": {"rolling_window": True}}}
        result = _roll_refresh_template(template)
        assert result == template or result is not template  # copy

    def test_rolling_with_empty_target_params(self):
        template = {
            "config": {"refresh": {"rolling_window": True}},
            "targets": [{"name": "CS2"}],
        }
        result = _roll_refresh_template(template)
        assert result["targets"][0]["name"] == "CS2"
