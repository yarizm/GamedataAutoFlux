"""Redaction helpers shared by Agent runtime, history, and SSE surfaces."""

from __future__ import annotations

import json
from typing import Any

from langchain_core.messages import AIMessage, BaseMessage

from src.core.sensitive import redact_sensitive, redact_sensitive_text


def redact_stream_value(value: Any) -> Any:
    safe_value = redact_sensitive(value)
    return _redact_stream_text_values(safe_value)


def redact_stream_event(event: dict[str, Any]) -> dict[str, Any]:
    redacted = redact_stream_value(event)
    if isinstance(redacted, dict):
        return redacted
    return {"type": "message", "content": redacted}


def redact_stream_text(text: str) -> str:
    raw = str(text or "")
    try:
        parsed = json.loads(raw)
    except (TypeError, ValueError):
        parsed = None
    if isinstance(parsed, (dict, list)):
        return json.dumps(redact_stream_value(parsed), ensure_ascii=False, default=str)

    return redact_sensitive_text(raw)


def redact_message_content(content: Any) -> Any:
    redacted = redact_stream_value(content)
    if isinstance(redacted, (str, list)):
        return redacted
    return json.dumps(redacted, ensure_ascii=False, default=str)


def redact_history_message(message: BaseMessage) -> BaseMessage:
    safe_content = redact_message_content(getattr(message, "content", ""))
    if hasattr(message, "model_copy"):
        return message.model_copy(update={"content": safe_content})
    try:
        message.content = safe_content
    except Exception:
        return AIMessage(content=str(safe_content))
    return message


def _redact_stream_text_values(value: Any) -> Any:
    if isinstance(value, str):
        return redact_stream_text(value)
    if isinstance(value, list):
        return [_redact_stream_text_values(item) for item in value]
    if isinstance(value, tuple):
        return [_redact_stream_text_values(item) for item in value]
    if isinstance(value, dict):
        return {key: _redact_stream_text_values(child) for key, child in value.items()}
    return value
