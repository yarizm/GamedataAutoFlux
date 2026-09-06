"""SSRF 校验核心逻辑测试（修复清单 P0：DNS 解析层 / redirect / 子资源拦截）。

覆盖：
- 字面量层：IPv4/IPv6/hex/decimal、保留名、IPv4-mapped IPv6；
- 解析层：monkeypatch `_getaddrinfo` 模拟 DNS，含 rebinding（域名→内网 IP）；
- NavigationUrlGuard：scheme/host 约束、同 host DNS 结论缓存；
- 采集器 `_route_guard`：redirect 后目标与子资源请求同样被拦截（伪 route 对象，
  无需真实浏览器）。
"""

from types import SimpleNamespace

import pytest

from src.core import url_safety
from src.core.url_safety import (
    NavigationUrlGuard,
    blocked_reason_for_host,
    blocked_reason_for_ip,
    resolve_host_reason,
)


def _fake_resolver(mapping: dict[str, list[str]]):
    """按 host 返回预置地址的 `_getaddrinfo` 替身，记录调用次数。"""
    calls: list[str] = []

    async def _fake(host: str, port):
        calls.append(host)
        addrs = mapping.get(host)
        if addrs is None:
            raise OSError("no address")
        return [(0, 0, 0, "", (addr, port or 80)) for addr in addrs]

    _fake.calls = calls
    return _fake


# ---------- 字面量层 ----------


def test_blocked_reason_for_ip_covers_private_and_special_ranges():
    assert blocked_reason_for_ip("10.1.2.3")
    assert blocked_reason_for_ip("127.0.0.1")
    assert blocked_reason_for_ip("192.168.0.10")
    assert blocked_reason_for_ip("169.254.1.1")  # link-local
    assert blocked_reason_for_ip("0.0.0.0")  # unspecified
    assert blocked_reason_for_ip("::1")  # IPv6 loopback
    assert blocked_reason_for_ip("fe80::1")  # IPv6 link-local
    assert blocked_reason_for_ip("fc00::5")  # IPv6 unique-local
    assert blocked_reason_for_ip("::ffff:10.0.0.7")  # IPv4-mapped IPv6


def test_blocked_reason_for_ip_allows_public_addresses():
    assert blocked_reason_for_ip("93.184.216.34") is None
    assert blocked_reason_for_ip("2606:2800:220:1:248:1893:25c8:1946") is None


def test_blocked_reason_for_host_literal_forms():
    assert blocked_reason_for_host("localhost")
    assert blocked_reason_for_host("LOCALHOST.")  # 大小写与尾点
    assert blocked_reason_for_host("0x7f000001")  # hex IP
    assert blocked_reason_for_host("2130706433")  # decimal IP
    assert blocked_reason_for_host("169.254.169.254")  # metadata IP
    assert blocked_reason_for_host("metadata.google.internal")
    assert blocked_reason_for_host("printer.local")
    assert blocked_reason_for_host("[::1]")  # 带方括号的 IPv6


def test_blocked_reason_for_host_passes_unknown_domains_without_dns():
    """字面量层不做 DNS：普通域名留给解析层。"""
    assert blocked_reason_for_host("example.com") is None


# ---------- 解析层（DNS） ----------


async def test_resolve_host_reason_allows_public_resolution(monkeypatch):
    monkeypatch.setattr(
        url_safety, "_getaddrinfo", _fake_resolver({"ok.example.com": ["93.184.216.34"]})
    )
    assert await resolve_host_reason("ok.example.com") is None


async def test_resolve_host_reason_blocks_private_resolution(monkeypatch):
    """DNS rebinding：公网域名解析到内网 IP 必须拦截。"""
    monkeypatch.setattr(
        url_safety, "_getaddrinfo", _fake_resolver({"rebind.example.com": ["10.0.0.8"]})
    )
    reason = await resolve_host_reason("rebind.example.com")
    assert reason is not None and "blocked" in reason


async def test_resolve_host_reason_blocks_when_any_record_is_private(monkeypatch):
    monkeypatch.setattr(
        url_safety,
        "_getaddrinfo",
        _fake_resolver({"mixed.example.com": ["93.184.216.34", "192.168.1.1"]}),
    )
    reason = await resolve_host_reason("mixed.example.com")
    assert reason is not None


async def test_resolve_host_reason_blocks_ipv6_loopback_resolution(monkeypatch):
    monkeypatch.setattr(
        url_safety, "_getaddrinfo", _fake_resolver({"v6.example.com": ["::1"]})
    )
    reason = await resolve_host_reason("v6.example.com")
    assert reason is not None


async def test_resolve_host_reason_reports_dns_failure(monkeypatch):
    monkeypatch.setattr(url_safety, "_getaddrinfo", _fake_resolver({}))
    reason = await resolve_host_reason("nx.example.com")
    assert reason is not None and "DNS resolution failed" in reason


# ---------- NavigationUrlGuard ----------


async def test_guard_enforce_allows_public_url(monkeypatch):
    monkeypatch.setattr(
        url_safety, "_getaddrinfo", _fake_resolver({"example.com": ["93.184.216.34"]})
    )
    await NavigationUrlGuard().enforce("https://example.com/games/1")


async def test_guard_enforce_raises_on_rebinding_host(monkeypatch):
    monkeypatch.setattr(
        url_safety, "_getaddrinfo", _fake_resolver({"evil.example.com": ["127.0.0.1"]})
    )
    with pytest.raises(ValueError, match="blocked url"):
        await NavigationUrlGuard().enforce("http://evil.example.com/x")


async def test_guard_rejects_non_http_schemes_without_dns():
    guard = NavigationUrlGuard()
    assert (await guard.check_url("file:///etc/passwd")) is not None
    assert (await guard.check_url("ftp://example.com/file")) is not None


async def test_guard_rejects_literal_loopback_without_dns():
    guard = NavigationUrlGuard()
    assert (await guard.check_url("http://127.0.0.1:8000/")) is not None
    assert (await guard.check_url("http://[::1]/")) is not None


async def test_guard_does_not_cache_and_re_resolves_every_request(monkeypatch):
    """安全结论不得缓存：同 host 每次请求都重新解析（rebinding 窗口）。"""
    resolver = _fake_resolver({"example.com": ["93.184.216.34"]})
    monkeypatch.setattr(url_safety, "_getaddrinfo", resolver)
    guard = NavigationUrlGuard()

    assert (await guard.check_url("https://example.com/a")) is None
    assert (await guard.check_url("https://example.com/b")) is None
    assert len(resolver.calls) == 2  # 每次请求一次解析


async def test_stateful_dns_rebinding_second_lookup_blocked(monkeypatch):
    """状态式 resolver：第一次公网 → 安全；第二次解析到回环 → 必须拦截。"""
    calls: list[str] = []

    async def _stateful(host: str, port):
        calls.append(host)
        addr = "93.184.216.34" if len(calls) == 1 else "127.0.0.1"
        return [(0, 0, 0, "", (addr, port or 80))]

    monkeypatch.setattr(url_safety, "_getaddrinfo", _stateful)
    guard = NavigationUrlGuard()

    assert (await guard.check_url("http://evil.example.com/page1")) is None
    reason = await guard.check_url("http://evil.example.com/page2")
    assert reason is not None and "blocked" in reason
    assert len(calls) == 2  # 第二次真的重新解析了


async def test_guard_tolerates_invalid_port():
    guard = NavigationUrlGuard()
    reason = await guard.check_url("http://example.com:99999/")
    assert reason is not None


# ---------- 采集器请求拦截（redirect / 子资源） ----------


class _FakeRoute:
    def __init__(self, url: str):
        self.request = SimpleNamespace(url=url)
        self.aborted = False
        self.continued = False

    async def abort(self) -> None:
        self.aborted = True

    async def continue_(self) -> None:
        self.continued = True


def _collector_with_dns(monkeypatch, mapping):
    from autoflux_plugin_dynamic_playwright.collector import DynamicPlaywrightCollector

    monkeypatch.setattr(url_safety, "_getaddrinfo", _fake_resolver(mapping))
    return DynamicPlaywrightCollector()


async def test_route_guard_continues_public_requests(monkeypatch):
    collector = _collector_with_dns(
        monkeypatch, {"site.example.com": ["93.184.216.34"]}
    )
    route = _FakeRoute("https://site.example.com/page")
    await collector._route_guard(route)
    assert route.continued and not route.aborted


async def test_route_guard_aborts_redirect_to_internal_host(monkeypatch):
    """公网页面 302 跳内网域名：redirect 后的请求必须被 abort。"""
    collector = _collector_with_dns(
        monkeypatch,
        {"site.example.com": ["93.184.216.34"], "intranet.local.lan": ["10.9.8.7"]},
    )
    first = _FakeRoute("https://site.example.com/page")
    await collector._route_guard(first)
    redirected = _FakeRoute("http://intranet.local.lan/admin")
    await collector._route_guard(redirected)
    assert redirected.aborted and not redirected.continued


async def test_route_guard_aborts_private_subresource(monkeypatch):
    """页面里的内网子资源（图片/接口）同样被拦截。"""
    collector = _collector_with_dns(
        monkeypatch, {"site.example.com": ["93.184.216.34"], "db.internal": ["172.16.0.9"]}
    )
    route = _FakeRoute("http://db.internal/metrics")
    await collector._route_guard(route)
    assert route.aborted


async def test_route_guard_aborts_after_dns_rebinds_mid_session(monkeypatch):
    """初始 enforce 安全后 DNS 变更：后续请求必须被 abort(无缓存可依)。"""
    calls: list[str] = []

    async def _stateful(host: str, port):
        calls.append(host)
        addr = "93.184.216.34" if len(calls) <= 1 else "127.0.0.1"
        return [(0, 0, 0, "", (addr, port or 80))]

    monkeypatch.setattr(url_safety, "_getaddrinfo", _stateful)
    from autoflux_plugin_dynamic_playwright.collector import DynamicPlaywrightCollector

    collector = DynamicPlaywrightCollector()

    first = _FakeRoute("http://site.example.com/page")
    await collector._route_guard(first)
    assert first.continued and not first.aborted

    second = _FakeRoute("http://site.example.com/asset")  # 同 host,DNS 已变
    await collector._route_guard(second)
    assert second.aborted and not second.continued


async def test_route_guard_passes_through_non_network_schemes(monkeypatch):
    """data:/blob: 不出浏览器，直接放行。"""
    collector = _collector_with_dns(monkeypatch, {})
    route = _FakeRoute("data:image/png;base64,AAAA")
    await collector._route_guard(route)
    assert route.continued and not route.aborted
