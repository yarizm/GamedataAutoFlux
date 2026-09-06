"""URL/host 安全校验核心逻辑（无 Web 框架依赖）。

分两层：
- 字面量层（同步）：hostname 是 IP 字面量（标准/hex/decimal）或保留名
  （localhost、metadata、*.local 等）时直接判定；
- 解析层（异步）：对普通域名实际执行 DNS A/AAAA 解析，任一结果落在
  private/loopback/link-local/reserved/metadata 网段即拦截——这是防
  DNS rebinding 的必要条件，浏览器自己的解析发生在校验之后，不能替代。

Web 层（HTTPException 包装）在 `src.web.safety`，采集器插件直接用本模块，
避免业务侧反向依赖 `src.web`。
"""

from __future__ import annotations

import asyncio
import ipaddress
import socket
from urllib.parse import urlparse

_BLOCKED_HOSTNAMES = {
    "localhost",
    "testclient",
    "metadata.google.internal",
    "metadata",
}

_BLOCKED_HOSTNAME_SUFFIXES = (
    ".localhost",
    ".local",
)


def try_parse_ip(
    host: str,
) -> ipaddress.IPv4Address | ipaddress.IPv6Address | None:
    """尝试将 host 解析为 IP 地址，支持标准、hex（0x...）、decimal 格式。"""
    normalized = host.strip().strip("[]")
    # 标准格式
    try:
        return ipaddress.ip_address(normalized)
    except ValueError:
        pass

    # Hex 格式 (0x7f000001 → 127.0.0.1)
    if normalized.startswith("0x") or normalized.startswith("0X"):
        try:
            return ipaddress.ip_address(int(normalized, 16))
        except (ValueError, OverflowError):
            pass

    # 纯数字 decimal 格式 (2130706433 → 127.0.0.1)
    if normalized.isdigit():
        try:
            return ipaddress.ip_address(int(normalized))
        except (ValueError, OverflowError):
            pass

    return None


def blocked_reason_for_ip(ip_text: str) -> str | None:
    """IP 落在禁用网段时返回原因，否则 None。IPv4-mapped IPv6 会解包后再判。"""
    try:
        ip = ipaddress.ip_address(ip_text.strip().strip("[]"))
    except ValueError:
        return None
    if isinstance(ip, ipaddress.IPv6Address) and ip.ipv4_mapped is not None:
        ip = ip.ipv4_mapped
    if ip.is_loopback:
        return f"{ip} is loopback"
    if ip.is_private:
        return f"{ip} is private"
    if ip.is_link_local:
        return f"{ip} is link-local"
    if ip.is_reserved:
        return f"{ip} is reserved"
    if ip.is_multicast:
        return f"{ip} is multicast"
    if ip.is_unspecified:
        return f"{ip} is unspecified"
    return None


def is_loopback_host(host: str) -> bool:
    normalized = host.strip().strip("[]").lower().rstrip(".")
    if normalized in {"localhost", "testclient"}:
        return True
    ip = try_parse_ip(normalized)
    return ip is not None and ip.is_loopback


def blocked_reason_for_host(host: str) -> str | None:
    """字面量层校验：不发起 DNS，只挡确定的禁用目标。"""
    normalized = host.strip().strip("[]").lower().rstrip(".")
    if not normalized:
        return "empty host"
    if normalized in _BLOCKED_HOSTNAMES:
        return f"{normalized} is a blocked hostname"
    if normalized.endswith(_BLOCKED_HOSTNAME_SUFFIXES):
        return f"{normalized} is a blocked hostname suffix"
    ip = try_parse_ip(normalized)
    if ip is not None:
        return blocked_reason_for_ip(str(ip))
    return None


async def _getaddrinfo(host: str, port: int | None) -> list[tuple]:
    """DNS 解析薄封装，独立成函数便于测试替换。"""
    loop = asyncio.get_running_loop()
    return await loop.getaddrinfo(
        host, port or None, family=socket.AF_UNSPEC, type=socket.SOCK_STREAM
    )


async def resolve_host_reason(host: str, port: int | None = None) -> str | None:
    """解析层校验：解析 host 的全部 A/AAAA 记录并逐个检查网段。"""
    try:
        infos = await _getaddrinfo(host, port)
    except (socket.gaierror, OSError) as exc:
        return f"DNS resolution failed for {host}: {exc}"
    seen: set[str] = set()
    for info in infos:
        addr = info[4][0] if len(info) >= 4 and info[4] else ""
        if not addr or addr in seen:
            continue
        seen.add(addr)
        reason = blocked_reason_for_ip(addr)
        if reason:
            return f"{host} resolves to blocked address: {reason}"
    if not seen:
        return f"DNS returned no addresses for {host}"
    return None


def validate_dynamic_browser_config(config) -> None:
    """Dynamic browser 采集配置校验（字面量层，Web 层与插件共用）。

    不合法时抛 ValueError；HTTPException 包装只在 `src.web.safety`。
    """
    if not isinstance(config, dict):
        raise ValueError("dynamic_playwright config must be an object")
    url = str(config.get("url", "") or "").strip()
    if not url:
        raise ValueError("dynamic_playwright config requires url")

    parsed = urlparse(url)
    if parsed.scheme not in {"http", "https"}:
        raise ValueError("dynamic_playwright url must use http or https")
    host = parsed.hostname
    if not host:
        raise ValueError("dynamic_playwright url must include a host")
    if "{" in host or "}" in host:
        raise ValueError("dynamic_playwright url host cannot be templated")
    reason = blocked_reason_for_host(host)
    if reason:
        raise ValueError(f"dynamic_playwright url host is not allowed: {reason}")


class NavigationUrlGuard:
    """导航/子资源 URL 守卫：字面量 + DNS 校验，**不做任何结论缓存**。

    DNS rebinding 的核心是"解析结果随时间变化"——缓存第一次的安全结论
    正好把防护窗口关掉。因此每次 `check_url` 都重新解析：初始导航与
    每一条被拦截的请求（redirect/子资源）独立判定，代价是每请求一次
    DNS 查询（按页面请求数，可接受）。
    """

    async def check_url(self, url: str) -> str | None:
        """返回 None 表示安全，否则返回拦截原因（每次调用都重新解析）。"""
        try:
            parsed = urlparse(str(url or ""))
            port = parsed.port
        except ValueError as exc:
            return f"unparseable url: {exc}"
        if parsed.scheme not in {"http", "https"}:
            return f"scheme {parsed.scheme!r} is not allowed"
        host = parsed.hostname
        if not host:
            return "url must include a host"

        literal = blocked_reason_for_host(host)
        if literal:
            return literal

        # 不缓存：同 host 的下一次请求可能解析到不同地址（rebinding）
        return await resolve_host_reason(host, port)

    async def enforce(self, url: str) -> None:
        """不安全时抛 ValueError，由调用方转为采集失败。"""
        reason = await self.check_url(url)
        if reason:
            raise ValueError(f"blocked url: {url} ({reason})")
