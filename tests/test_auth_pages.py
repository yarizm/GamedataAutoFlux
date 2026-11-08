"""GET / 的服务端登录门禁。

门禁在服务端做，三条前端加载路径（Vite dev / 构建产物 / 旧版 static 脚本）都被覆盖：
未通过管理端鉴权时不下发 SPA，配了 server.api_key 给登录页（401），
免 key 模式下的远程/代理访问给「远程访问未开启」说明页（403，无表单）。
"""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from src.web.app import app

LOCAL = {"base_url": "http://127.0.0.1:8000", "client": ("127.0.0.1", 50000)}
# 独立的远程 IP，避免和其他测试的登录失败限流计数互相影响。
REMOTE = {"base_url": "http://192.168.1.20:8000", "client": ("198.51.100.23", 50000)}

SPA_MARKER = 'id="tab-plugins"'
LOGIN_FORM_MARKER = 'id="login-form"'


def _configure_api_key(monkeypatch, value: str) -> None:
    import src.core.config as config

    original_get = config.get

    def fake_get(key, default=None):
        if key == "server.api_key":
            return value
        return original_get(key, default)

    # get_app_settings() 不缓存，覆盖即时生效。
    monkeypatch.setattr(config, "get", fake_get)


def _assert_spa(response) -> None:
    assert response.status_code == 200, response.text[:500]
    assert SPA_MARKER in response.text
    assert LOGIN_FORM_MARKER not in response.text


def _assert_no_escaped_quotes(html: str) -> None:
    # Edit 工具曾把 HTML 属性引号写成 \" 字面量，浏览器会因此找不到元素 ID。
    assert '\\"' not in html


@pytest.mark.parametrize(
    "headers",
    [
        {},
        # 从其他站点点链接打开：顶层导航不是 CSRF，不能因 Sec-Fetch-Site 拒绝。
        {"Sec-Fetch-Site": "cross-site", "Sec-Fetch-Mode": "navigate"},
    ],
)
def test_keyless_local_browser_gets_spa(headers) -> None:
    with TestClient(app, **LOCAL) as client:
        response = client.get("/", headers=headers)
    _assert_spa(response)


@pytest.mark.parametrize(
    ("client_kwargs", "headers"),
    [
        (REMOTE, {}),
        # 本机反向代理/隧道转发的请求，同样按远程处理。
        (LOCAL, {"X-Forwarded-For": "203.0.113.10"}),
    ],
)
def test_keyless_remote_access_gets_remote_disabled_page(client_kwargs, headers) -> None:
    with TestClient(app, **client_kwargs) as client:
        response = client.get("/", headers=headers)

    assert response.status_code == 403
    html = response.text
    assert 'data-mode="remote_disabled"' in html
    assert "server.api_key" in html
    # 没有可用的登录方式，不能给出必然失败的表单。
    assert "<form" not in html
    assert "<input" not in html
    assert SPA_MARKER not in html
    assert response.headers["cache-control"] == "no-store"
    _assert_no_escaped_quotes(html)


@pytest.mark.parametrize("client_kwargs", [REMOTE, LOCAL])
def test_api_key_mode_without_session_gets_login_page(monkeypatch, client_kwargs) -> None:
    _configure_api_key(monkeypatch, "page-gate-secret")
    with TestClient(app, **client_kwargs) as client:
        response = client.get("/")

    assert response.status_code == 401
    html = response.text
    assert 'data-mode="login"' in html
    assert '<form id="login-form">' in html
    assert (
        '<input type="password" id="login-api-key" name="api_key" '
        'autocomplete="current-password" maxlength="512" required autofocus>'
    ) in html
    assert '<p id="login-error" class="error" role="alert"></p>' in html
    assert '<button type="submit" id="login-submit"' in html
    assert "fetch('/api/auth/status'" in html
    assert "fetch('/api/auth/login'" in html
    assert "gamedata-autoflux.language" in html
    # 模板里绝不能出现密钥，也不依赖 dist 构建产物。
    assert "page-gate-secret" not in html
    assert "/static/dist/" not in html
    assert SPA_MARKER not in html
    assert response.headers["cache-control"] == "no-store"
    _assert_no_escaped_quotes(html)


def test_login_then_index_serves_spa_and_logout_returns_to_login(monkeypatch) -> None:
    _configure_api_key(monkeypatch, "page-gate-secret")
    with TestClient(app, **REMOTE) as client:
        assert client.get("/").status_code == 401

        wrong = client.post("/api/auth/login", json={"api_key": "wrong-key"})
        assert wrong.status_code == 401
        assert client.get("/").status_code == 401

        login = client.post("/api/auth/login", json={"api_key": "page-gate-secret"})
        assert login.status_code == 200, login.text
        # 不硬编码 Cookie 名：靠 TestClient 的 cookie jar 自动携带。
        _assert_spa(client.get("/"))
        _assert_spa(client.get("/", headers={"Sec-Fetch-Site": "cross-site"}))

        logout = client.post("/api/auth/logout")
        assert logout.status_code == 200, logout.text
        assert client.get("/").status_code == 401
