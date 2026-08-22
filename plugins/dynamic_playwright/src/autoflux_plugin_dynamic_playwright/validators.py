"""Configuration safety rules owned by the Dynamic Playwright plugin."""

from src.core.collector_validators import (
    CollectorConfigIssue,
    register_collector_config_validator,
)
from src.core.url_safety import validate_dynamic_browser_config


def _validate(config: dict) -> list[CollectorConfigIssue]:
    try:
        validate_dynamic_browser_config(config)
    except ValueError as exc:
        return [
            CollectorConfigIssue(
                code="unsafe_dynamic_playwright_config",
                field="url",
                message=str(exc),
            )
        ]
    return []


register_collector_config_validator(
    "dynamic_playwright",
    _validate,
    owner="autoflux-plugin-dynamic-playwright",
)
