"""Utilities for redacting secrets before returning or storing metadata."""

from __future__ import annotations

from copy import deepcopy
from typing import Any


SENSITIVE_KEY_TOKENS = (
    "cookie",
    "authorization",
    "api_key",
    "apikey",
    "token",
    "password",
    "secret",
)


def redact_sensitive(value: Any) -> Any:
    """Return a deep copy with likely secret values replaced by a marker."""
    cloned = deepcopy(value)
    return _redact_in_place(cloned)


def _redact_in_place(value: Any) -> Any:
    if isinstance(value, dict):
        for key in list(value.keys()):
            if _is_sensitive_key(key):
                value[key] = "[REDACTED]"
            else:
                value[key] = _redact_in_place(value[key])
        return value
    if isinstance(value, list):
        return [_redact_in_place(item) for item in value]
    return value


def _is_sensitive_key(key: Any) -> bool:
    key_text = str(key or "").lower()
    return any(token in key_text for token in SENSITIVE_KEY_TOKENS)
