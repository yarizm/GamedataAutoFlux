"""Safety helpers for high-risk API operations.

纯校验逻辑（字面量/IP 网段判断、DNS 解析）在 `src.core.url_safety`，
本模块只做 FastAPI/HTTPException 的薄包装。
"""

from __future__ import annotations

import secrets
from urllib.parse import urlparse

from fastapi import Header, HTTPException
from starlette.requests import HTTPConnection

from src.core.url_safety import blocked_reason_for_host, is_loopback_host


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
    """Web 层包装：核心校验（`src.core.url_safety`）+ HTTPException 语义。"""
    from src.core.url_safety import validate_dynamic_browser_config

    try:
        validate_dynamic_browser_config(config)
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc


def validate_url_runtime(url: str) -> None:
    """运行时 URL 校验（字面量层，同步）。

    只覆盖 IP 字面量与保留名；对普通域名的 DNS rebinding 防护必须走
    `src.core.url_safety.NavigationUrlGuard`（异步解析后逐地址检查），
    本函数保留给无法 await 的同步调用点。
    """
    parsed = urlparse(url)
    host = parsed.hostname
    if not host:
        raise HTTPException(400, "url must include a host")
    reason = blocked_reason_for_host(host)
    if reason:
        raise HTTPException(400, f"url host '{host}' is blocked: {reason}")


def _is_local_request(request: HTTPConnection) -> bool:
    host = request.client.host if request.client else ""
    if not host:
        return False
    if host == "testclient":
        return True
    return is_loopback_host(host)
