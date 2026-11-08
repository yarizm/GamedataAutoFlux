"""WebUI 登录：用管理员 API key 换取 HttpOnly 会话 Cookie。

浏览器无法给 WebSocket、``<a href>`` 下载、``window.open`` 加自定义头，
所以配置了 ``server.api_key`` 时 WebUI 靠 Cookie 鉴权（SameSite=Strict + Origin 校验防 CSRF）。
脚本 / Worker 继续用 ``X-API-Key`` 头。key 校验与失败限流统一在 ``src.web.safety.check_presented_key``。
"""

from __future__ import annotations

from fastapi import APIRouter, HTTPException, Request, Response, status
from pydantic import BaseModel, Field

from src.core.app_settings import ServerSettings, get_server_settings
from src.web.safety import (
    check_access,
    check_presented_key,
    is_cross_site_request,
    session_cookie_name,
)
from src.web.session_tokens import (
    SESSION_TTL_SECONDS,
    issue_session_token,
    revoke_session_token,
)

router = APIRouter(prefix="/auth", tags=["auth"])


class LoginRequest(BaseModel):
    api_key: str = Field(..., min_length=1, max_length=512)


@router.get("/status")
async def auth_status(request: Request) -> dict[str, bool]:
    """只反映登录 Cookie / 本机免 key 状态。

    不读 X-API-Key：否则它就成了一个能无限次试 key 的校验接口。
    """
    server = get_server_settings()
    return {
        "auth_required": bool(server.api_key.strip()),
        "authenticated": check_access(request, None, server) is None,
    }


@router.post("/login")
async def login(req: LoginRequest, request: Request, response: Response) -> dict[str, bool]:
    server = get_server_settings()
    _reject_cross_site(request, server)
    admin_key = server.api_key.strip()
    if not admin_key:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "API key auth is not enabled.")

    rejection = check_presented_key(request, req.api_key, [admin_key])
    if rejection is not None:
        raise HTTPException(*rejection)

    response.set_cookie(
        session_cookie_name(server),
        issue_session_token(admin_key),
        max_age=SESSION_TTL_SECONDS,
        httponly=True,
        samesite="strict",
        secure=_wants_secure_cookie(request),
        path="/",
    )
    return {"authenticated": True}


@router.post("/logout")
async def logout(request: Request, response: Response) -> dict[str, bool]:
    """清 Cookie 并把当前 token 写入吊销表：之后重放这枚 Cookie 一律 401。"""
    server = get_server_settings()
    _reject_cross_site(request, server)
    cookie_name = session_cookie_name(server)
    admin_key = server.api_key.strip()
    if admin_key:
        revoke_session_token(request.cookies.get(cookie_name), admin_key)
    response.delete_cookie(
        cookie_name,
        path="/",
        secure=_wants_secure_cookie(request),
        httponly=True,
        samesite="strict",
    )
    return {"authenticated": False}


def _reject_cross_site(request: Request, server: ServerSettings) -> None:
    if is_cross_site_request(request, server):
        raise HTTPException(status.HTTP_403_FORBIDDEN, "Cross-site requests are not allowed.")


def _wants_secure_cookie(request: Request) -> bool:
    """浏览器经 HTTPS 访问时给 Cookie 加 Secure。

    不受信的 TLS 代理后面 request.url.scheme 是 http，所以也看 X-Forwarded-Proto 和 Origin。
    这两个头能被伪造，但伪造只会让 Cookie 多带 Secure，是更安全的方向。
    """
    if request.url.scheme == "https":
        return True
    forwarded_proto = request.headers.get("x-forwarded-proto", "")
    if any(part.strip().lower() == "https" for part in forwarded_proto.split(",")):
        return True
    return request.headers.get("origin", "").strip().lower().startswith("https://")
