"""
统一采集器错误分类。

为所有采集器提供结构化的错误码，方便任务日志、WebUI 和 Agent 工具
统一展示错误类型、中文说明和修复建议。
"""

from __future__ import annotations

from enum import Enum
from typing import Any


class ErrorCode(str, Enum):
    missing_credentials = "missing_credentials"
    login_required = "login_required"
    anti_bot_blocked = "anti_bot_blocked"
    network_unreachable = "network_unreachable"
    site_structure_changed = "site_structure_changed"
    empty_data = "empty_data"
    rate_limited = "rate_limited"
    unknown = "unknown"

    @property
    def chinese_label(self) -> str:
        _labels: dict[ErrorCode, str] = {
            ErrorCode.missing_credentials: "凭证缺失",
            ErrorCode.login_required: "需要登录",
            ErrorCode.anti_bot_blocked: "反爬拦截",
            ErrorCode.network_unreachable: "网络不可达",
            ErrorCode.site_structure_changed: "站点结构变化",
            ErrorCode.empty_data: "空数据",
            ErrorCode.rate_limited: "频率限制",
            ErrorCode.unknown: "未知错误",
        }
        return _labels.get(self, "未知错误")

    @property
    def suggestion(self) -> str:
        _suggestions: dict[ErrorCode, str] = {
            ErrorCode.missing_credentials: "检查 .env 或 settings.yaml 中对应 API Key / Cookie 是否已配置",
            ErrorCode.login_required: "运行对应登录脚本（如 steamdb_login.py、qimai_login.py）重新获取 Cookie",
            ErrorCode.anti_bot_blocked: "等待 10-30 分钟后重试，或切换代理/网络环境",
            ErrorCode.network_unreachable: "检查网络连接、代理设置和防火墙规则",
            ErrorCode.site_structure_changed: "目标站点可能改版，需检查并更新采集规则",
            ErrorCode.empty_data: "该时间段目标可能无数据，尝试扩大时间范围或检查数据源状态",
            ErrorCode.rate_limited: "降低采集频率，增加请求间隔或等待冷却时间",
            ErrorCode.unknown: "查看详细日志排查原因",
        }
        return _suggestions.get(self, "查看详细日志排查原因")

    @property
    def severity(self) -> str:
        _severity: dict[ErrorCode, str] = {
            ErrorCode.missing_credentials: "error",
            ErrorCode.login_required: "error",
            ErrorCode.anti_bot_blocked: "error",
            ErrorCode.network_unreachable: "error",
            ErrorCode.site_structure_changed: "error",
            ErrorCode.empty_data: "warning",
            ErrorCode.rate_limited: "warning",
            ErrorCode.unknown: "error",
        }
        return _severity.get(self, "error")


def classify_exception(exc: Exception) -> ErrorCode:
    """根据异常类型和消息自动推断错误码"""
    msg = str(exc).lower()

    # 按优先级匹配
    if any(kw in msg for kw in ("api key", "apikey", "credentials", "unauthorized", "token", "未配置")):
        return ErrorCode.missing_credentials
    if any(kw in msg for kw in ("login", "cookie", "session expired", "not authenticated", "sign in")):
        return ErrorCode.login_required
    if any(kw in msg for kw in ("captcha", "cloudflare", "bot", "blocked", "forbidden", "access denied", "403")):
        return ErrorCode.anti_bot_blocked
    if any(kw in msg for kw in ("rate limit", "too many requests", "429", "throttle")):
        return ErrorCode.rate_limited
    if any(kw in msg for kw in (
        "timeout", "connection", "network", "unreachable", "dns", "resolve",
        "refused", "nodata", "socket", "getaddrinfo", "connect error",
    )):
        return ErrorCode.network_unreachable
    if any(kw in msg for kw in ("parse", "structure", "xpath", "selector", "element not found", "css", "html changed")):
        return ErrorCode.site_structure_changed
    if any(kw in msg for kw in ("empty", "no data", "not found", "no result", "404", "null")):
        return ErrorCode.empty_data

    return ErrorCode.unknown


def error_summary(error_code: ErrorCode, error_message: str | None = None) -> dict[str, Any]:
    """生成结构化的错误摘要，供 CollectResult.to_summary() 和 Agent 工具结果使用"""
    return {
        "code": error_code.value,
        "label": error_code.chinese_label,
        "suggestion": error_code.suggestion,
        "severity": error_code.severity,
        "detail": error_message or "",
    }
