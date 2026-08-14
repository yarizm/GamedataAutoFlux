"""Configuration safety rules owned by the Dynamic Playwright plugin."""

from fastapi import HTTPException

from src.core.collector_validators import (
    CollectorConfigIssue,
    register_collector_config_validator,
)
from src.web.safety import validate_dynamic_playwright_config


def _validate(config: dict) -> list[CollectorConfigIssue]:
    try:
        validate_dynamic_playwright_config(config)
    except HTTPException as exc:
        return [
            CollectorConfigIssue(
                code="unsafe_dynamic_playwright_config",
                field="url",
                message=str(exc.detail or "Dynamic browser collector config is unsafe."),
            )
        ]
    return []


register_collector_config_validator(
    "dynamic_playwright",
    _validate,
    owner="autoflux-plugin-dynamic-playwright",
)
