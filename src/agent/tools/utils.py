"""
Agent 工具公共方法
"""

import json
from typing import Any

from src.core.sensitive import redact_sensitive, redact_sensitive_text


def _normalize_for_json(obj: Any) -> Any:
    if hasattr(obj, "model_dump"):
        return obj.model_dump(mode="json")
    if isinstance(obj, list):
        return [_normalize_for_json(item) for item in obj]
    if isinstance(obj, tuple):
        return [_normalize_for_json(item) for item in obj]
    if isinstance(obj, dict):
        return {key: _normalize_for_json(value) for key, value in obj.items()}
    return obj


def _safe_json(obj: Any) -> str:
    """序列化为 JSON 字符串，处理 Pydantic 模型与 datetime"""
    obj = redact_sensitive(_normalize_for_json(obj))
    return json.dumps(obj, ensure_ascii=False, default=str)


def _safe_error_text(error: Any) -> str:
    """Return a redacted one-line error string for tool responses and logs."""
    return redact_sensitive_text(str(error or ""))


def _format_result(
    status: str,
    summary: str,
    data: Any = None,
    *,
    record_count: int | None = None,
    warnings: list[str] | None = None,
    suggestion: str = "",
    max_data_length: int = 4000,
) -> str:
    """构建结构化的工具返回结果，提供 summary/data/suggestion 三层可读性"""
    result: dict[str, Any] = {
        "status": status,
        "summary": summary,
    }
    if record_count is not None:
        result["record_count"] = record_count
    if warnings:
        result["warnings"] = warnings
    if suggestion:
        result["suggestion"] = suggestion

    if data is not None:
        safe_data = redact_sensitive(_normalize_for_json(data))
        serialized = _safe_json(safe_data)
        if len(serialized) > max_data_length:
            result["data_truncated"] = True
            result["summary"] = summary + "（数据量过大，已截断，请进一步查询）"
            result["data"] = serialized[:max_data_length] + " ...[Data Truncated]"
        else:
            result["data"] = safe_data

    return json.dumps(redact_sensitive(result), ensure_ascii=False, default=str)
