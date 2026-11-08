"""Safety helpers for high-risk API operations.

纯校验逻辑（字面量/IP 网段判断、DNS 解析）在 `src.core.url_safety`，
本模块只做 FastAPI/HTTPException 的薄包装，以及管理端 / Worker 接口鉴权。
会话 token 的签发与校验在 `src.web.session_tokens`。
"""

from __future__ import annotations

import secrets
import threading
import time
from collections import deque
from collections.abc import Iterable
from urllib.parse import urlparse, urlsplit

from fastapi import Header, HTTPException, WebSocketException, status
from starlette.requests import HTTPConnection

from src.core.app_settings import ServerSettings, get_server_settings
from src.core.url_safety import blocked_reason_for_host, is_loopback_host
from src.web.session_tokens import verify_session_token


def require_explicit_confirmation(confirm: bool, operation: str) -> None:
    """Require API callers to opt in before destructive or scheduled actions."""
    if not confirm:
        raise HTTPException(
            status_code=400,
            detail=f"Explicit confirmation required for {operation}; pass confirm=true.",
        )


# 反向代理 / CDN 会把远程请求变成来自 127.0.0.1 的连接；带这些头说明请求经过了代理，
# 不能再按"本机请求"免鉴权。不加任何头的代理和 TCP 隧道拦不住，见 require_admin 的说明。
PROXY_HEADERS = (
    "forwarded",
    "x-forwarded-for",
    "x-forwarded-host",
    "x-forwarded-proto",
    "x-forwarded-port",
    "x-forwarded-prefix",
    "x-real-ip",
    "via",
    "cf-connecting-ip",
    "true-client-ip",
    "x-client-ip",
)

SESSION_COOKIE_PREFIX = "autoflux_session_"

# key 校验失败限流（登录表单、管理接口 X-API-Key、Worker key 共用一个计数）
KEY_FAILURE_LIMIT = 10
KEY_FAILURE_WINDOW_SECONDS = 300
KEY_FAILURE_MAX_CLIENTS = 10_000

_DEFAULT_PORTS = {"http": 80, "https": 443}
_SAME_ORIGIN_FETCH_SITES = frozenset({"same-origin", "none"})
_CROSS_SITE_FETCH_SITES = frozenset({"same-site", "cross-site"})

Rejection = tuple[int, str]


class KeyFailureTracker:
    """按客户端 IP 记录 key 校验失败的滑动窗口计数，表大小有上限。

    - 只在失败时写入；查询不插入条目，过期时间戳在查询时顺手清掉。
    - 条目按"最近一次失败时间"排序（每次失败移到末尾），满了先清过期、再淘汰最久没失败的，
      正在爆破的 IP 总在末尾，不会被别人刷新条目挤掉。
    - 成功不清零：否则同一出口 IP 上合法 Worker 每 3 秒一次的成功请求会不断替攻击者归零。
    - require_admin / require_worker 是同步依赖，FastAPI 在线程池里执行，所以用线程锁。
    """

    def __init__(
        self,
        limit: int = KEY_FAILURE_LIMIT,
        window_seconds: float = KEY_FAILURE_WINDOW_SECONDS,
        max_clients: int = KEY_FAILURE_MAX_CLIENTS,
    ) -> None:
        self._limit = limit
        self._window = window_seconds
        self._max_clients = max_clients
        self._failures: dict[str, deque[float]] = {}
        self._lock = threading.Lock()

    def is_locked(self, client: str) -> bool:
        cutoff = time.monotonic() - self._window
        with self._lock:
            failures = self._failures.get(client)
            if failures is None:
                return False
            while failures and failures[0] <= cutoff:
                failures.popleft()
            if not failures:
                del self._failures[client]
                return False
            return len(failures) >= self._limit

    def record_failure(self, client: str) -> None:
        now = time.monotonic()
        with self._lock:
            failures = self._failures.pop(client, None)
            if failures is None:
                self._make_room(now - self._window)
                failures = deque(maxlen=self._limit)
            failures.append(now)
            self._failures[client] = failures

    def _make_room(self, cutoff: float) -> None:
        while self._failures:
            oldest = next(iter(self._failures))
            if self._failures[oldest][-1] > cutoff:
                break
            del self._failures[oldest]
        while len(self._failures) >= self._max_clients:
            del self._failures[next(iter(self._failures))]


_key_failures = KeyFailureTracker()


def require_admin(
    request: HTTPConnection,
    x_api_key: str | None = Header(None, alias="X-API-Key"),
) -> None:
    """管理端鉴权（HTTP 与 WebSocket 共用；WebSocket 被拒一律 close 1008）。

    - 配置了 ``server.api_key``：``X-API-Key`` 头匹配，或带有效的登录 Cookie（WebUI 经
      ``/api/auth/login`` 换取，只在同源请求中接受）。不支持查询参数传 key——会进访问日志和浏览器历史。
    - 未配置：只放行本机 loopback 连接，并要求没有代理转发头、Host 头是 loopback（防 DNS rebinding）、
      不是跨站请求（防恶意网页对 localhost 发起 CSRF / 跨站 WebSocket）。

      免 key 模式信任一切能连到 loopback 端口的连接，这一点无法从请求本身区分：nginx 默认的
      ``proxy_pass`` 会把 Host 改成 127.0.0.1:8000 且不加 X-Forwarded-For；frp、``ssh -R``、
      netsh portproxy 这类 TCP 隧道的 Host 由远程客户端随意填写。**经反向代理或隧道暴露服务时
      必须配置 server.api_key。**

    key 校验失败按客户端 IP 限流，见 ``check_presented_key``。
    """
    _enforce(request, check_access(request, x_api_key, get_server_settings()))


def require_worker(
    request: HTTPConnection,
    x_api_key: str | None = Header(None, alias="X-API-Key"),
) -> None:
    """Worker 协议接口鉴权：额外接受 ``server.worker_api_key``（它访问不了任何管理接口）；
    其余规则与 require_admin 完全相同（管理员 key / 登录 Cookie / 本机免 key）。"""
    _enforce(
        request,
        check_access(request, x_api_key, get_server_settings(), accept_worker_key=True),
    )


def admin_rejection(
    request: HTTPConnection,
    x_api_key: str | None,
    *,
    enforce_same_origin: bool = True,
) -> Rejection | None:
    """返回管理端拒绝原因 ``(status_code, detail)``；``None`` 表示放行。

    ``enforce_same_origin=False`` 只给页面导航（GET /）用：顶层导航不构成 CSRF，
    从其他站点点链接打开应用时 ``Sec-Fetch-Site`` 是 cross-site，不能因此拒绝。
    其余检查（key、Cookie、本机/代理/Host）照做。
    """
    return check_access(
        request, x_api_key, get_server_settings(), enforce_same_origin=enforce_same_origin
    )


def check_access(
    request: HTTPConnection,
    x_api_key: str | None,
    server: ServerSettings,
    *,
    accept_worker_key: bool = False,
    enforce_same_origin: bool = True,
) -> Rejection | None:
    """鉴权主流程，``server`` 由请求入口取一次往下传。

    1. 带了 ``X-API-Key`` 且本接口有可接受的 key：只看 key（对 → 放行；错 → 401 并计入失败；
       该 IP 已被锁定 → 429）。key 由脚本显式添加，浏览器跨站请求带不上，不做同源检查。
    2. 配置了 ``server.api_key``：看登录 Cookie，并挡住跨站请求。
    3. 没有任何 key 可比对：走本机免 key 规则。
    """
    admin_key = server.api_key.strip()
    accepted = [admin_key] if admin_key else []
    worker_key = server.worker_api_key.strip()
    if accept_worker_key and worker_key:
        accepted.append(worker_key)

    token = (x_api_key or "").strip()
    if token and accepted:
        return check_presented_key(request, token, accepted)
    if admin_key:
        return _session_rejection(
            request, server, admin_key, enforce_same_origin=enforce_same_origin
        )
    return _local_bypass_rejection(request, server, enforce_same_origin=enforce_same_origin)


def check_presented_key(
    request: HTTPConnection, presented: str, accepted: Iterable[str]
) -> Rejection | None:
    """校验调用方出示的 key（X-API-Key 头或登录表单）。所有 key 比对都必须走这里。

    每次失败都计入按客户端 IP（``client_ip``）的失败计数；``KEY_FAILURE_WINDOW_SECONDS`` 内达到
    ``KEY_FAILURE_LIMIT`` 次后，该 IP 任何带 key 的尝试不论对错都返回 429（WebSocket close 1008）。

    取舍：计数按 TCP 对端地址，不信任可伪造的 X-Forwarded-For，所以反代后面、NAT 出口后面的
    客户端共享一个计数，攻击者能让同一出口上的脚本/Worker 暂时 429。为缩小这个 DoS 面，锁定只拦
    "带 key 的尝试"：持有有效会话 Cookie 的请求和本机免 key 请求不受影响。
    """
    client = client_ip(request)
    if _key_failures.is_locked(client):
        return (
            status.HTTP_429_TOO_MANY_REQUESTS,
            "Too many failed API key attempts; try again later.",
        )
    token = presented.strip()
    if token and any(_keys_match(token, key) for key in accepted):
        return None
    _key_failures.record_failure(client)
    return status.HTTP_401_UNAUTHORIZED, "Invalid API key."


def client_ip(request: HTTPConnection) -> str:
    """TCP 对端地址：本机判断与 key 失败限流统一用它（不读 X-Forwarded-For，可伪造）。"""
    return request.client.host if request.client else ""


def is_api_key_auth_enabled() -> bool:
    return bool(get_server_settings().api_key.strip())


def session_cookie_name(server: ServerSettings) -> str:
    """Cookie 不区分端口：名字带上端口，同一主机上的多个实例才不会互相覆盖登录态。"""
    return f"{SESSION_COOKIE_PREFIX}{server.port}"


def remote_access_warning(server: ServerSettings) -> str | None:
    """监听非 loopback 地址却没配 api_key 时给出启动告警文案。"""
    if server.api_key.strip() or is_loopback_host(server.host):
        return None
    return (
        f"server.host={server.host!r} 不是 loopback 地址，但未配置 server.api_key："
        "远程访问管理接口和 WebUI 会被拒绝（只有本机 loopback 连接能免 key 访问）。"
        "需要远程访问，或经反向代理/隧道暴露服务时，请配置 server.api_key。"
    )


def is_cross_site_request(request: HTTPConnection, server: ServerSettings | None = None) -> bool:
    """请求是否由其他站点的页面发起（CSRF / 跨站 WebSocket 劫持）。

    1. Origin 在 ``server.cors_origins`` 白名单（不含 ``*``）里 → 可信。必须先于 Sec-Fetch-Site
       判断：白名单里的跨站前端发来的请求，Sec-Fetch-Site 本来就是 cross-site。
    2. 有 ``Sec-Fetch-Site``：same-origin / none 放行，same-site / cross-site 拒绝
       （本机其他端口的页面是 same-site，同样拒绝）。
    3. 没有 Sec-Fetch-Site（非安全上下文如 http://<局域网 IP> 的页面浏览器不发，或老浏览器）：
       Origin 为 ``null`` → 跨站；有 Origin 就和 Host 头比较 hostname + port（端口缺省按 Origin 的
       scheme 取 80/443；不比较 scheme——不受信的 TLS 代理后面 request.url.scheme 是 http）；
       没有 Origin → 不算跨站（非浏览器客户端，或浏览器同源 GET / 导航）。
    """
    if server is None:
        server = get_server_settings()
    raw_origin = request.headers.get("origin")
    origin = raw_origin.strip().rstrip("/").lower() if raw_origin is not None else None
    if origin and origin != "null" and origin in _trusted_origins(server):
        return False

    fetch_site = request.headers.get("sec-fetch-site", "").strip().lower()
    if fetch_site in _SAME_ORIGIN_FETCH_SITES:
        return False
    if fetch_site in _CROSS_SITE_FETCH_SITES:
        return True

    if origin is None:
        return False
    if origin == "null":
        return True
    return not _origin_matches_host(origin, request.headers.get("host", ""))


def _trusted_origins(server: ServerSettings) -> set[str]:
    return {
        item.strip().rstrip("/").lower()
        for item in server.cors_origins
        if item.strip() not in ("", "*")
    }


def _origin_matches_host(origin: str, host_header: str) -> bool:
    try:
        parsed_origin = urlsplit(origin)
        default_port = _DEFAULT_PORTS.get(parsed_origin.scheme)
        origin_host = parsed_origin.hostname
        origin_port = parsed_origin.port
        parsed_host = urlsplit(f"//{host_header.strip()}")
        request_host = parsed_host.hostname
        request_port = parsed_host.port
    except ValueError:  # 非法端口等
        return False
    if default_port is None or not origin_host or not request_host:
        return False
    origin_port = default_port if origin_port is None else origin_port
    request_port = default_port if request_port is None else request_port
    return (origin_host, origin_port) == (request_host, request_port)


def _session_rejection(
    request: HTTPConnection,
    server: ServerSettings,
    admin_key: str,
    *,
    enforce_same_origin: bool,
) -> Rejection | None:
    token = request.cookies.get(session_cookie_name(server))
    if verify_session_token(token, admin_key) is None:
        return status.HTTP_401_UNAUTHORIZED, "Unauthorized"
    # Cookie 由浏览器自动携带，必须挡住跨站请求（CSRF / 跨站 WebSocket）。
    if enforce_same_origin and is_cross_site_request(request, server):
        return status.HTTP_403_FORBIDDEN, "Cross-site requests are not allowed."
    return None


def _local_bypass_rejection(
    request: HTTPConnection, server: ServerSettings, *, enforce_same_origin: bool
) -> Rejection | None:
    """未配置 api_key 时的本机免 key 规则；返回拒绝原因，``None`` 表示放行。"""
    if not is_loopback_host(client_ip(request)):
        return status.HTTP_401_UNAUTHORIZED, "Admin API key is required for non-local requests."
    if any(name in request.headers for name in PROXY_HEADERS):
        return status.HTTP_401_UNAUTHORIZED, "Admin API key is required for proxied requests."
    if not _has_loopback_host_header(request):
        return (
            status.HTTP_403_FORBIDDEN,
            "Host header must be a loopback address when no admin API key is configured.",
        )
    if enforce_same_origin and is_cross_site_request(request, server):
        return status.HTTP_403_FORBIDDEN, "Cross-site requests are not allowed."
    return None


def _has_loopback_host_header(request: HTTPConnection) -> bool:
    try:
        host = urlsplit(f"//{request.headers.get('host', '').strip()}").hostname
    except ValueError:
        return False
    return bool(host) and is_loopback_host(host)


def _keys_match(token: str, expected: str) -> bool:
    # compare_digest 对含非 ASCII 字符的 str 会抛 TypeError，统一按 bytes 比较。
    return secrets.compare_digest(token.encode("utf-8"), expected.encode("utf-8"))


def _enforce(request: HTTPConnection, rejection: Rejection | None) -> None:
    if rejection is None:
        return
    status_code, detail = rejection
    if request.scope.get("type") == "websocket":
        raise WebSocketException(code=status.WS_1008_POLICY_VIOLATION, reason=detail)
    raise HTTPException(status_code=status_code, detail=detail)


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
