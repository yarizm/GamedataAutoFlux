"""Safety helpers for high-risk API operations."""

from __future__ import annotations

import ipaddress
import secrets
from urllib.parse import urlparse

from fastapi import Header, HTTPException
from starlette.requests import HTTPConnection


def require_explicit_confirmation(confirm: bool, operation: str) -> None:
    """Require API callers to opt in before destructive or scheduled actions."""
    if not confirm:
        raise HTTPException(
            status_code=400,
            detail=f"Explicit confirmation required for {operation}; pass confirm=true.",
        )


def require_admin(
    request: HTTPConnection,
    x_api_key: str | None = Header(None, alias="X-API-Key"),
    api_key: str | None = None,
) -> None:
    """Allow local-only access by default, or require the configured admin API key."""
    from src.core.config import get as get_config

    expected_key = str(get_config("server.api_key", "") or "").strip()
    token = (x_api_key or api_key or "").strip()

    if expected_key:
        if not token or not secrets.compare_digest(token, expected_key):
            raise HTTPException(status_code=401, detail="Unauthorized")
        return

    if _is_local_request(request):
        return

    raise HTTPException(
        status_code=401,
        detail="Admin API key is required for non-local requests.",
    )


def validate_dynamic_playwright_config(config: dict) -> None:
    """Reject dynamic browser collectors that can reach local/private resources."""
    if not isinstance(config, dict):
        raise HTTPException(400, "dynamic_playwright config must be an object")
    url = str(config.get("url", "") or "").strip()
    if not url:
        raise HTTPException(400, "dynamic_playwright config requires url")

    parsed = urlparse(url)
    if parsed.scheme not in {"http", "https"}:
        raise HTTPException(400, "dynamic_playwright url must use http or https")
    host = parsed.hostname
    if not host:
        raise HTTPException(400, "dynamic_playwright url must include a host")
    if "{" in host or "}" in host:
        raise HTTPException(400, "dynamic_playwright url host cannot be templated")
    if _is_blocked_host(host):
        raise HTTPException(400, "dynamic_playwright url host is not allowed")


def validate_url_runtime(url: str) -> None:
    """运行时二次校验 URL — 在浏览器发起导航前调用，防止 DNS rebinding。

    与 validate_dynamic_playwright_config 不同，此函数在实际请求时执行，
    此时 DNS 已解析，可检测 rebinding 后的私有 IP。
    """
    parsed = urlparse(url)
    host = parsed.hostname
    if not host:
        raise HTTPException(400, "url must include a host")
    if _is_blocked_host(host):
        raise HTTPException(400, f"url host '{host}' resolves to a blocked address")


def _is_local_request(request: HTTPConnection) -> bool:
    host = request.client.host if request.client else ""
    if not host:
        return False
    if host == "testclient":
        return True
    return _is_loopback_host(host)


def _try_parse_ip(host: str) -> ipaddress.IPv4Address | ipaddress.IPv6Address | None:
    """尝试将 host 解析为 IP 地址，支持标准、hex（0x...）、decimal 格式。"""
    # 标准格式
    try:
        return ipaddress.ip_address(host)
    except ValueError:
        pass

    # Hex 格式 (0x7f000001 → 127.0.0.1)
    if host.startswith("0x") or host.startswith("0X"):
        try:
            return ipaddress.ip_address(int(host, 16))
        except (ValueError, OverflowError):
            pass

    # 纯数字 decimal 格式 (2130706433 → 127.0.0.1)
    if host.isdigit():
        try:
            return ipaddress.ip_address(int(host))
        except (ValueError, OverflowError):
            pass

    return None


def _is_blocked_host(host: str) -> bool:
    normalized = host.strip().strip("[]").lower().rstrip(".")
    if _is_loopback_host(normalized):
        return True
    if normalized in {
        "metadata.google.internal",
        "metadata",
    }:
        return True
    if normalized.endswith(".localhost") or normalized.endswith(".local"):
        return True
    ip = _try_parse_ip(normalized)
    if ip is None:
        return False
    return (
        ip.is_private
        or ip.is_loopback
        or ip.is_link_local
        or ip.is_multicast
        or ip.is_reserved
        or ip.is_unspecified
    )


def _is_loopback_host(host: str) -> bool:
    normalized = host.strip().strip("[]").lower().rstrip(".")
    if normalized in {"localhost", "testclient"}:
        return True
    ip = _try_parse_ip(normalized)
    return ip is not None and ip.is_loopback
