"""
Agent 工具公共方法
"""
import json
from typing import Any

def _safe_json(obj: Any) -> str:
    """序列化为 JSON 字符串，处理 Pydantic 模型与 datetime"""
    if hasattr(obj, "model_dump"):
        obj = obj.model_dump(mode="json")
    elif isinstance(obj, list):
        obj = [
            item.model_dump(mode="json") if hasattr(item, "model_dump") else item for item in obj
        ]
    return json.dumps(obj, ensure_ascii=False, default=str)


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
        serialized = _safe_json(data)
        if len(serialized) > max_data_length:
            result["data_truncated"] = True
            result["summary"] = summary + "（数据量过大，已截断，请进一步查询）"
            result["data"] = serialized[:max_data_length] + " ...[Data Truncated]"
        else:
            result["data"] = data

    return json.dumps(result, ensure_ascii=False, default=str)
