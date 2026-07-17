"""Utilities for redacting secrets before returning or storing metadata."""

from __future__ import annotations

from copy import deepcopy
import json
import re
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

# Pagination / resume cursors contain "token" as a substring but are not secrets.
# Without this, YouTube checkpoint `page_token` is wiped on every append.
_NON_SENSITIVE_KEY_EXACT = frozenset(
    {
        "page_token",
        "next_page_token",
        "start_page_token",
        "pagetoken",
        "nextpagetoken",
    }
)

_TEXT_SECRET_PATTERN = re.compile(
    r"(?P<key_quote>['\"]?)"
    r"\b(?P<key>[A-Za-z0-9_-]*(?:api[_-]?key|apikey|token|password|secret|cookie|authorization)[A-Za-z0-9_-]*)\b"
    r"(?P=key_quote)"
    r"\s*[:=]\s*"
    r"(?P<quote>['\"]?)"
    r"(?P<value>[^'\"\n,;&}\]]+)"
    r"(?P=quote)?",
    re.IGNORECASE,
)
_AUTH_CREDENTIAL_PATTERN = re.compile(
    r"\b(?P<scheme>Bearer|Basic)\s+(?P<credential>[A-Za-z0-9._~+/=-]{8,})",
    re.IGNORECASE,
)


def redact_sensitive(value: Any) -> Any:
    """Return a deep copy with likely secret values replaced by a marker."""
    cloned = deepcopy(value)
    return _redact_in_place(cloned)


def redact_sensitive_text(text: str) -> str:
    """Redact likely secrets embedded in plain text or JSON-formatted strings."""
    raw = str(text or "")
    try:
        parsed = json.loads(raw)
    except (TypeError, ValueError):
        parsed = None

    if isinstance(parsed, (dict, list)):
        return json.dumps(redact_sensitive(parsed), ensure_ascii=False, default=str)

    redacted = _TEXT_SECRET_PATTERN.sub(_replace_text_secret, raw)
    return _AUTH_CREDENTIAL_PATTERN.sub(
        lambda match: f"{match.group('scheme')} [REDACTED]",
        redacted,
    )


def _replace_text_secret(match: re.Match[str]) -> str:
    if str(match.group("value") or "").strip().startswith("[REDACTED"):
        return match.group(0)
    return f"{match.group('key')}=[REDACTED]"


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
    if isinstance(value, str):
        return redact_sensitive_text(value)
    return value


def _is_sensitive_key(key: Any) -> bool:
    key_text = str(key or "").lower().strip()
    if not key_text:
        return False
    if key_text in _NON_SENSITIVE_KEY_EXACT:
        return False
    return any(token in key_text for token in SENSITIVE_KEY_TOKENS)
