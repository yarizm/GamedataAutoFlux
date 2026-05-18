"""测试采集器错误分类模块"""

from src.core.errors import ErrorCode, classify_exception, error_summary


class TestErrorCodeEnum:
    def test_all_codes_have_label(self):
        for code in ErrorCode:
            assert code.chinese_label, f"{code} missing chinese_label"

    def test_all_codes_have_suggestion(self):
        for code in ErrorCode:
            assert code.suggestion, f"{code} missing suggestion"

    def test_severity_is_warning_or_error(self):
        for code in ErrorCode:
            assert code.severity in ("warning", "error"), f"{code} bad severity: {code.severity}"


class TestClassifyException:
    def test_missing_credentials(self):
        assert classify_exception(ValueError("missing API key")) == ErrorCode.missing_credentials
        assert (
            classify_exception(Exception("credentials not found")) == ErrorCode.missing_credentials
        )
        assert classify_exception(Exception("Unauthorized")) == ErrorCode.missing_credentials

    def test_login_required(self):
        assert (
            classify_exception(Exception("session expired, please login"))
            == ErrorCode.login_required
        )
        assert classify_exception(Exception("cookie invalid")) == ErrorCode.login_required

    def test_anti_bot_blocked(self):
        assert classify_exception(Exception("cloudflare detected")) == ErrorCode.anti_bot_blocked
        assert classify_exception(Exception("HTTP 403 Forbidden")) == ErrorCode.anti_bot_blocked
        assert (
            classify_exception(Exception("access denied by bot protection"))
            == ErrorCode.anti_bot_blocked
        )

    def test_rate_limited(self):
        assert classify_exception(Exception("rate limit exceeded")) == ErrorCode.rate_limited
        assert classify_exception(Exception("HTTP 429 too many requests")) == ErrorCode.rate_limited

    def test_network_unreachable(self):
        assert (
            classify_exception(ConnectionError("connection refused"))
            == ErrorCode.network_unreachable
        )
        assert classify_exception(TimeoutError("timeout")) == ErrorCode.network_unreachable
        assert classify_exception(OSError("getaddrinfo failed")) == ErrorCode.network_unreachable

    def test_site_structure_changed(self):
        assert (
            classify_exception(Exception("parse error: element not found"))
            == ErrorCode.site_structure_changed
        )
        assert (
            classify_exception(Exception("CSS selector failed")) == ErrorCode.site_structure_changed
        )

    def test_empty_data(self):
        assert classify_exception(Exception("no data found")) == ErrorCode.empty_data
        assert classify_exception(Exception("HTTP 404 not found")) == ErrorCode.empty_data

    def test_unknown_fallback(self):
        assert classify_exception(Exception("some weird unexpected error")) == ErrorCode.unknown


class TestErrorSummary:
    def test_ok_summary(self):
        s = error_summary(ErrorCode.network_unreachable, "timeout connecting")
        assert s["code"] == "network_unreachable"
        assert s["label"] == "网络不可达"
        assert s["suggestion"]
        assert s["severity"] == "error"
        assert s["detail"] == "timeout connecting"

    def test_warning_severity(self):
        assert error_summary(ErrorCode.empty_data)["severity"] == "warning"
        assert error_summary(ErrorCode.rate_limited)["severity"] == "warning"


class TestCollectResultSummary:
    def test_success_result_summary(self):
        from src.collectors.base import CollectTarget, CollectResult

        result = CollectResult(
            target=CollectTarget(name="CS2"),
            success=True,
            data={"players": 1000},
        )
        s = result.to_summary()
        assert s["success"] is True
        assert s["status"] == "ok"
        assert s["target"] == "CS2"

    def test_failed_result_summary_with_error_code(self):
        from src.collectors.base import CollectTarget, CollectResult

        result = CollectResult(
            target=CollectTarget(name="CS2"),
            success=False,
            error="timeout",
            error_code=ErrorCode.network_unreachable.value,
        )
        s = result.to_summary()
        assert s["success"] is False
        assert s["status"] == "error"
        assert s["error_code"] == "network_unreachable"
        assert s["error_label"] == "网络不可达"
        assert s["severity"] == "error"
        assert "suggestion" in s

    def test_failed_result_summary_no_code_fallback(self):
        from src.collectors.base import CollectTarget, CollectResult

        result = CollectResult(
            target=CollectTarget(name="CS2"),
            success=False,
            error="some error",
        )
        s = result.to_summary()
        assert s["status"] == "error"
        assert s["error_code"] == "unknown"
        assert s["error_label"] == "未知错误"
