from src.collectors.dynamic_playwright_collector import (
    DynamicPlaywrightCollector,
    _safe_log_text,
)


def test_dynamic_playwright_safe_log_text_redacts_embedded_secrets() -> None:
    safe = _safe_log_text(
        "url=https://example.com/?api_key=secret-key; token=secret-token; "
        "Authorization: Bearer abcdefghijklmnop"
    )

    assert "secret-key" not in safe
    assert "secret-token" not in safe
    assert "abcdefghijklmnop" not in safe
    assert "api_key=[REDACTED]" in safe
    assert "token=[REDACTED]" in safe
    assert "Authorization=[REDACTED]" in safe


def test_dynamic_playwright_validate_config_redacts_invalid_mode_log(monkeypatch) -> None:
    captured: list[str] = []
    monkeypatch.setattr(
        "src.collectors.dynamic_playwright_collector.logger.error",
        lambda message: captured.append(str(message)),
    )

    collector = DynamicPlaywrightCollector()

    assert (
        collector.validate_config(
            {
                "url": "https://example.com",
                "extraction_mode": "token=secret-token",
                "fields": {},
            }
        )
        is False
    )
    rendered = " ".join(captured)
    assert "secret-token" not in rendered
    assert "token=[REDACTED]" in rendered
