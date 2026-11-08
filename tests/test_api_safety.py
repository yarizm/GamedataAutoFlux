import pytest
from fastapi.testclient import TestClient

from src.web.app import app


def test_destructive_data_record_delete_requires_confirmation() -> None:
    with TestClient(app) as client:
        response = client.delete("/api/data/records/missing-record")

    assert response.status_code == 400
    assert "confirm=true" in response.json()["detail"]


def test_destructive_data_category_delete_requires_confirmation() -> None:
    with TestClient(app) as client:
        response = client.delete("/api/data/games/missing-game")

    assert response.status_code == 400
    assert "confirm=true" in response.json()["detail"]


def test_destructive_pipeline_delete_requires_confirmation() -> None:
    with TestClient(app) as client:
        response = client.delete("/api/pipelines/missing-pipeline")

    assert response.status_code == 400
    assert "confirm=true" in response.json()["detail"]


def test_destructive_cron_delete_requires_confirmation() -> None:
    with TestClient(app) as client:
        response = client.delete("/api/cron-jobs/missing-job")

    assert response.status_code == 400
    assert "confirm=true" in response.json()["detail"]


def test_destructive_task_delete_requires_confirmation() -> None:
    with TestClient(app) as client:
        response = client.delete("/api/tasks/missing-task")

    assert response.status_code == 400
    assert "confirm=true" in response.json()["detail"]


def test_destructive_report_delete_requires_confirmation() -> None:
    with TestClient(app) as client:
        response = client.delete("/api/reports/missing-report")

    assert response.status_code == 400
    assert "confirm=true" in response.json()["detail"]


def test_local_admin_api_allowed_without_api_key() -> None:
    with TestClient(app) as client:
        response = client.get("/api/components")

    assert response.status_code == 200


def test_non_local_admin_api_requires_api_key_when_unconfigured() -> None:
    with TestClient(app, client=("203.0.113.10", 50000)) as client:
        response = client.get("/api/components")

    assert response.status_code == 401


def test_health_remains_public_for_non_local_requests() -> None:
    with TestClient(app, client=("203.0.113.10", 50000)) as client:
        response = client.get("/api/health")

    assert response.status_code == 200


def test_configured_admin_api_key_is_required(monkeypatch) -> None:
    import src.core.config as config

    original_get = config.get

    def fake_get(key, default=None):
        if key == "server.api_key":
            return "secret-key"
        return original_get(key, default)

    monkeypatch.setattr(config, "get", fake_get)

    with TestClient(app, client=("203.0.113.10", 50000)) as client:
        missing = client.get("/api/components")
        wrong = client.get("/api/components", headers={"X-API-Key": "wrong"})
        ok = client.get("/api/components", headers={"X-API-Key": "secret-key"})

    assert missing.status_code == 401
    assert wrong.status_code == 401
    assert ok.status_code == 200


def test_provider_config_redacts_and_preserves_api_key(tmp_path, monkeypatch) -> None:
    import src.core.config as config

    settings_path = tmp_path / "settings.yaml"
    settings_path.write_text(
        """
app:
  debug: false
server:
  host: "127.0.0.1"
  port: 8000
  api_key: ""
agent:
  enabled: false
database:
  provider: "sqlalchemy"
  sqlalchemy_url: "${DATABASE_URL}"
llm:
  provider: qwen
  qwen:
    model: qwen-max
    base_url: https://example.test/v1
    api_key: real-secret
    temperature: 0.3
    max_tokens: 2000
""",
        encoding="utf-8",
    )
    monkeypatch.setattr(config, "_DEFAULT_SETTINGS_FILE", settings_path)
    config._settings = None
    config._settings_validation = None
    config._raw_sections = None

    with TestClient(app) as client:
        loaded = client.get("/api/agent/providers/config")
        saved = client.put(
            "/api/agent/providers/config",
            json={
                "provider": "qwen",
                "items": [
                    {
                        "key": "qwen",
                        "model": "qwen-plus",
                        "base_url": "https://example.test/v2",
                        "api_key": "",
                        "temperature": 0.3,
                        "max_tokens": 2000,
                    }
                ],
            },
        )

    assert loaded.status_code == 200
    provider = loaded.json()["providers"][0]
    assert provider["api_key"] == ""
    assert provider["has_api_key"] is True
    assert saved.status_code == 200
    text = settings_path.read_text(encoding="utf-8")
    assert "api_key: real-secret" in text
    assert "model: qwen-plus" in text


def test_dynamic_playwright_rejects_private_or_non_http_urls() -> None:
    payload = {
        "name": "bad_dynamic",
        "steps": [
            {
                "type": "collector",
                "name": "dynamic_playwright",
                "config": {"url": "http://127.0.0.1:8000/private", "fields": {}},
            }
        ],
    }
    with TestClient(app) as client:
        private_resp = client.post("/api/pipelines", json=payload)
        payload["steps"][0]["config"]["url"] = "file:///etc/passwd"
        file_resp = client.post("/api/pipelines", json=payload)

    assert private_resp.status_code == 400
    assert file_resp.status_code == 400


def test_dynamic_playwright_allows_external_https_url() -> None:
    payload = {
        "name": "safe_dynamic_https",
        "steps": [
            {
                "type": "collector",
                "name": "dynamic_playwright",
                "config": {"url": "https://example.com/games/{app_id}", "fields": {}},
            }
        ],
    }
    with TestClient(app) as client:
        response = client.post("/api/pipelines", json=payload)

    assert response.status_code == 200


def test_data_limit_zero_is_rejected() -> None:
    with TestClient(app) as client:
        response = client.get("/api/data/games?limit=0")

    assert response.status_code == 422


def test_report_template_delete_requires_confirmation() -> None:
    with TestClient(app) as client:
        response = client.delete("/api/reports/templates/missing-template")

    assert response.status_code == 400
    assert "confirm=true" in response.json()["detail"]


REMOTE = ("203.0.113.10", 50000)
# 状态修改型探针：过了鉴权会停在 confirm 校验（400），不会真的触发重启。
PROBE = "/api/plugin-manager/apply-restart"


def _fake_server_config(monkeypatch, **server) -> None:
    """覆盖 server.<key> 配置（走 config.get 接缝，类型化配置每次现取）。"""
    import src.core.config as config

    overrides = {f"server.{key}": value for key, value in server.items()}
    original_get = config.get

    def fake_get(key, default=None):
        if key in overrides:
            return overrides[key]
        return original_get(key, default)

    monkeypatch.setattr(config, "get", fake_get)


def _fake_api_key(monkeypatch, value: str, worker_value: str = "") -> None:
    _fake_server_config(monkeypatch, api_key=value, worker_api_key=worker_value)


def _cookie_name() -> str:
    from src.core.app_settings import get_server_settings
    from src.web.safety import session_cookie_name

    return session_cookie_name(get_server_settings())


def _set_cookie(response, name: str):
    from http.cookies import SimpleCookie

    jar = SimpleCookie()
    for header in response.headers.get_list("set-cookie"):
        jar.load(header)
    return jar[name]


def test_websocket_requires_admin_for_non_local_clients() -> None:
    from starlette.websockets import WebSocketDisconnect

    with TestClient(app, client=REMOTE) as client:
        with pytest.raises(WebSocketDisconnect) as exc_info:
            with client.websocket_connect("/api/ws/tasks"):
                pass
    assert exc_info.value.code == 1008


def test_websocket_rejects_cross_site_origin_on_localhost() -> None:
    from starlette.websockets import WebSocketDisconnect

    with TestClient(app) as client:
        with pytest.raises(WebSocketDisconnect) as exc_info:
            with client.websocket_connect(
                "/api/ws/tasks", headers={"Origin": "https://evil.example"}
            ):
                pass
    assert exc_info.value.code == 1008


def test_websocket_allows_local_keyless_same_origin() -> None:
    with TestClient(app, base_url="http://127.0.0.1:8000") as client:
        with client.websocket_connect(
            "/api/ws/tasks", headers={"Origin": "http://127.0.0.1:8000"}
        ) as ws:
            ws.send_text("ping")
            assert ws.receive_text() == "pong"


def test_websocket_accepts_login_cookie_only_from_same_origin(monkeypatch) -> None:
    from starlette.websockets import WebSocketDisconnect

    _fake_api_key(monkeypatch, "secret-key")
    origin = "http://192.168.1.10:8000"
    with TestClient(app, base_url=origin, client=REMOTE) as client:
        login = client.post(
            "/api/auth/login", json={"api_key": "secret-key"}, headers={"Origin": origin}
        )
        assert login.status_code == 200
        # 浏览器 WebSocket 握手必带 Origin
        with client.websocket_connect("/api/ws/tasks", headers={"Origin": origin}) as ws:
            ws.send_text("ping")
            assert ws.receive_text() == "pong"
        with pytest.raises(WebSocketDisconnect) as exc_info:
            with client.websocket_connect(
                "/api/ws/tasks", headers={"Origin": "https://evil.example"}
            ):
                pass
    assert exc_info.value.code == 1008


@pytest.mark.parametrize(
    "headers",
    [
        {"Origin": "https://evil.example"},
        {"Origin": "null"},
        {"Sec-Fetch-Site": "cross-site"},
        # 本机其他端口上的页面（如 localhost:5500 的静态服务器）是 same-site，不是同源
        {"Origin": "http://localhost:5500", "Sec-Fetch-Site": "same-site", "Host": "localhost:8000"},
        {"Origin": "http://localhost:5500", "Host": "localhost:8000"},
        # Vite 代理（changeOrigin）把 Host 改成 :8000、Origin 仍是 :5173：不在白名单就拒绝
        {"Origin": "http://localhost:5173", "Host": "localhost:8000"},
        {"Host": "evil.example:8000"},  # DNS rebinding
        {"Host": "testclient.:8000"},  # TestClient 的占位主机名（含末尾点变体）不是 loopback
        {"Host": "testserver"},
        {"Host": "0.0.0.0:8000"},
        # 本机反向代理 / CDN 转发的远程请求
        {"X-Forwarded-For": "203.0.113.10"},
        {"Forwarded": "for=203.0.113.10"},
        {"X-Forwarded-Proto": "https"},
        {"X-Real-IP": "203.0.113.10"},
        {"Via": "1.1 proxy"},
        {"CF-Connecting-IP": "203.0.113.10"},
    ],
)
def test_local_keyless_access_rejects_csrf_rebinding_and_proxy(headers) -> None:
    with TestClient(app) as client:
        response = client.post(PROBE, headers=headers)
    assert response.status_code in (401, 403)


@pytest.mark.parametrize(
    "headers",
    [
        {},
        {"Origin": "http://127.0.0.1:8000", "Host": "127.0.0.1:8000"},
        {"Origin": "HTTP://LocalHost:8000/", "Host": "localhost:8000"},
        {"Origin": "http://[::1]:8000", "Host": "[::1]:8000"},
        {"Origin": "http://localhost:8000", "Sec-Fetch-Site": "same-origin", "Host": "localhost:8000"},
        {"Sec-Fetch-Site": "same-origin", "Host": "localhost:8000"},
        {"Sec-Fetch-Site": "none", "Host": "localhost:8000"},  # 地址栏直接访问
    ],
)
def test_local_keyless_access_allows_same_origin(headers) -> None:
    with TestClient(app) as client:
        response = client.post(PROBE, headers=headers)
    # 过了鉴权，停在 confirm 校验
    assert response.status_code == 400


def test_starlette_testclient_placeholders_are_not_local() -> None:
    """生产鉴权不认 TestClient 的占位值 testserver / testclient。"""
    with TestClient(app, base_url="http://testserver", client=("testclient", 50000)) as client:
        assert client.get("/api/components").status_code == 401
    with TestClient(app, base_url="http://testserver") as client:
        assert client.get("/api/components").status_code == 403


def test_other_local_port_page_is_rejected_with_and_without_api_key(monkeypatch) -> None:
    """localhost:5500 上的页面不能对 :8000 发简单请求（multipart 上传 wheel = 代码执行）。"""
    evil = {"Origin": "http://localhost:5500", "Sec-Fetch-Site": "same-site"}
    with TestClient(app, base_url="http://localhost:8000") as client:
        assert client.post(PROBE, headers=evil).status_code == 403

    _fake_api_key(monkeypatch, "secret-key")
    with TestClient(app, base_url="http://localhost:8000") as client:
        assert client.post("/api/auth/login", json={"api_key": "secret-key"}, headers=evil).status_code == 403
        login = client.post(
            "/api/auth/login",
            json={"api_key": "secret-key"},
            headers={"Origin": "http://localhost:8000", "Sec-Fetch-Site": "same-origin"},
        )
        assert login.status_code == 200
        assert client.post(PROBE, headers=evil).status_code == 403
        assert client.post(PROBE, headers={"Origin": "http://localhost:8000"}).status_code == 400


def test_remote_same_origin_browser_login_and_cookie_requests(monkeypatch) -> None:
    """局域网 IP 打开 WebUI：同源登录、带 Cookie 的 POST 都要放行。

    http://<局域网 IP> 是非安全上下文，浏览器不发 Sec-Fetch-Site，只能比较 Origin 和 Host。
    """
    _fake_api_key(monkeypatch, "secret-key")
    origin = "http://192.168.1.10:8000"
    with TestClient(app, base_url=origin, client=("192.168.1.20", 50000)) as client:
        login = client.post(
            "/api/auth/login",
            json={"api_key": "secret-key"},
            headers={"Origin": origin, "Sec-Fetch-Site": "same-origin"},
        )
        assert login.status_code == 200
        assert client.get("/api/auth/status").json() == {
            "auth_required": True,
            "authenticated": True,
        }
        assert client.post(PROBE, headers={"Origin": origin}).status_code == 400
        assert (
            client.post(PROBE, headers={"Origin": origin, "Sec-Fetch-Site": "same-origin"}).status_code
            == 400
        )
        # 同一个 Cookie，从其他来源发起就拒绝
        assert client.post(PROBE, headers={"Origin": "http://192.168.1.10:5500"}).status_code == 403
        assert client.post(PROBE, headers={"Origin": origin, "Sec-Fetch-Site": "cross-site"}).status_code == 403


def test_https_domain_behind_tls_proxy_counts_as_same_origin(monkeypatch) -> None:
    """TLS 在代理终止：应用看到 http + Host: autoflux.example.com，浏览器 Origin 是 https。"""
    _fake_api_key(monkeypatch, "secret-key")
    name = _cookie_name()
    origin = "https://autoflux.example.com"
    with TestClient(app, base_url="http://autoflux.example.com", client=REMOTE) as client:
        login = client.post("/api/auth/login", json={"api_key": "secret-key"}, headers={"Origin": origin})
        assert login.status_code == 200
        morsel = _set_cookie(login, name)
        assert morsel["secure"]
        cookie = {"Cookie": f"{name}={morsel.value}"}
        assert client.post(PROBE, headers={"Origin": origin, **cookie}).status_code == 400
        assert client.post(PROBE, headers={"Origin": "https://evil.example", **cookie}).status_code == 403

        proxied = client.post(
            "/api/auth/login", json={"api_key": "secret-key"}, headers={"X-Forwarded-Proto": "https"}
        )
        assert proxied.status_code == 200
        assert _set_cookie(proxied, name)["secure"]


def test_cors_whitelisted_origin_is_trusted_even_when_cross_site(monkeypatch) -> None:
    trusted = "https://dashboard.example"
    cross_site = {"Origin": trusted, "Sec-Fetch-Site": "cross-site"}
    _fake_server_config(monkeypatch, cors_origins=[trusted, "*"])
    with TestClient(app) as client:
        assert client.post(PROBE, headers=cross_site).status_code == 400
        # 白名单里的 "*" 不代表信任任意来源
        assert (
            client.post(
                PROBE, headers={"Origin": "https://evil.example", "Sec-Fetch-Site": "cross-site"}
            ).status_code
            == 403
        )

    _fake_server_config(monkeypatch, api_key="secret-key", cors_origins=[trusted])
    # 跨站前端是 https 页面，API 也得是 https（否则混合内容），登录 Cookie 带 Secure
    with TestClient(app, base_url="https://api.example", client=REMOTE) as client:
        login = client.post("/api/auth/login", json={"api_key": "secret-key"}, headers=cross_site)
        assert login.status_code == 200
        assert client.post(PROBE, headers=cross_site).status_code == 400


def test_api_key_with_non_ascii_is_rejected_not_500(monkeypatch) -> None:
    _fake_api_key(monkeypatch, "secret-key")
    with TestClient(app, client=REMOTE) as client:
        # httpx 不接受非 ASCII 的 str 头，按 UTF-8 字节发送；服务端按 latin-1 解码成非 ASCII 字符串
        response = client.get("/api/components", headers={"X-API-Key": "密钥".encode()})
    assert response.status_code == 401


def test_export_files_require_admin_and_only_serve_export_artifacts(tmp_path, monkeypatch) -> None:
    import src.web.routes.youtube_export as youtube_export

    monkeypatch.setattr(youtube_export, "EXPORT_DIR", tmp_path)
    export_name = "export_youtube_profiles_20260101_000000.xlsx"
    (tmp_path / export_name).write_bytes(b"xlsx")
    (tmp_path / "secret.json").write_text("{}", encoding="utf-8")

    with TestClient(app, client=("203.0.113.10", 50000)) as client:
        assert client.get(f"/api/files/export/{export_name}").status_code == 401

    with TestClient(app) as client:
        ok = client.get(f"/api/files/export/{export_name}")
        assert ok.status_code == 200
        assert ok.content == b"xlsx"
        assert client.get("/api/files/export/secret.json").status_code == 404
        assert client.get("/api/files/export/..%2Fpyproject.toml").status_code == 404


def test_dynamic_playwright_rejects_hex_encoded_loopback() -> None:
    """hex 编码的 127.0.0.1 (0x7f000001) 应被拦截。"""
    payload = {
        "name": "hex_ssrf",
        "steps": [
            {
                "type": "collector",
                "name": "dynamic_playwright",
                "config": {"url": "http://0x7f000001/", "fields": {}},
            }
        ],
    }
    with TestClient(app) as client:
        response = client.post("/api/pipelines", json=payload)
    assert response.status_code == 400


def test_dynamic_playwright_rejects_decimal_encoded_loopback() -> None:
    """decimal 编码的 127.0.0.1 (2130706433) 应被拦截。"""
    payload = {
        "name": "dec_ssrf",
        "steps": [
            {
                "type": "collector",
                "name": "dynamic_playwright",
                "config": {"url": "http://2130706433/", "fields": {}},
            }
        ],
    }
    with TestClient(app) as client:
        response = client.post("/api/pipelines", json=payload)
    assert response.status_code == 400


def test_validate_url_runtime_blocks_private_ip() -> None:
    """运行时校验应拦截解析到私有 IP 的 URL。"""
    from src.web.safety import validate_url_runtime
    from fastapi import HTTPException

    try:
        validate_url_runtime("http://127.0.0.1/path")
        assert False, "Should have raised HTTPException"
    except HTTPException as e:
        assert e.status_code == 400
        assert "blocked" in e.detail.lower()


def test_validate_url_runtime_allows_public_url() -> None:
    """运行时校验应放行公网 URL。"""
    from src.web.safety import validate_url_runtime

    # 不应抛异常
    validate_url_runtime("https://example.com/path")


def test_get_raw_section_uses_cache(tmp_path, monkeypatch) -> None:
    """get_raw_section 应缓存结果，不重复读磁盘。"""
    import src.core.config as config

    settings_path = tmp_path / "settings.yaml"
    settings_path.write_text(
        "llm:\n  provider: qwen\n  qwen:\n    model: qwen-max\n    api_key: secret\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(config, "_DEFAULT_SETTINGS_FILE", settings_path)

    # 清除缓存
    config._raw_sections = None

    result1 = config.get_raw_section("llm")
    assert result1["qwen"]["api_key"] == "secret"

    # 修改文件内容（模拟外部写入）
    settings_path.write_text(
        "llm:\n  provider: qwen\n  qwen:\n    model: qwen-max\n    api_key: changed\n",
        encoding="utf-8",
    )

    # 缓存命中，不应读到新值
    result2 = config.get_raw_section("llm")
    assert result2["qwen"]["api_key"] == "secret"

    # 清除缓存后应读到新值
    config.invalidate_raw_section_cache()
    result3 = config.get_raw_section("llm")
    assert result3["qwen"]["api_key"] == "changed"

    # 清理：确保不泄漏到其他测试
    config.invalidate_raw_section_cache()


def test_api_key_query_param_is_not_accepted(monkeypatch) -> None:
    _fake_api_key(monkeypatch, "secret-key")
    with TestClient(app, client=REMOTE) as client:
        assert client.get("/api/components", params={"api_key": "secret-key"}).status_code == 401


def test_login_cookie_grants_access_and_logout_revokes(monkeypatch) -> None:
    import json

    from src.web.session_tokens import get_auth_dir

    _fake_api_key(monkeypatch, "secret-key")
    name = _cookie_name()
    with TestClient(app, client=REMOTE) as client:
        status = client.get("/api/auth/status").json()
        assert status == {"auth_required": True, "authenticated": False}
        assert client.get("/api/components").status_code == 401

        login = client.post("/api/auth/login", json={"api_key": "secret-key"})
        assert login.status_code == 200
        morsel = _set_cookie(login, name)
        assert morsel["httponly"] and morsel["samesite"].lower() == "strict"
        assert not morsel["secure"]  # 纯 http 访问不加 Secure
        token = client.cookies.get(name)
        assert token == morsel.value

        assert client.get("/api/auth/status").json()["authenticated"] is True
        assert client.get("/api/components").status_code == 200
        # Cookie 鉴权必须挡住跨站请求
        assert (
            client.get("/api/components", headers={"Origin": "https://evil.example"}).status_code
            == 403
        )

        assert client.post("/api/auth/logout").status_code == 200
        assert client.get("/api/components").status_code == 401
        # 重放登出前的 Cookie：已写入吊销表
        replay = {"Cookie": f"{name}={token}"}
        assert client.get("/api/components", headers=replay).status_code == 401
        assert client.get("/api/auth/status", headers=replay).json()["authenticated"] is False

    revoked = json.loads((get_auth_dir() / "revoked_sessions.json").read_text(encoding="utf-8"))
    assert token.split(".")[2] in revoked


def test_login_rejects_wrong_key_rate_limits_and_cross_site(monkeypatch) -> None:
    from src.web.safety import KEY_FAILURE_LIMIT

    _fake_api_key(monkeypatch, "secret-key")
    with TestClient(app, client=REMOTE) as client:
        assert (
            client.post(
                "/api/auth/login",
                json={"api_key": "secret-key"},
                headers={"Origin": "https://evil.example"},
            ).status_code
            == 403
        )
        for _ in range(KEY_FAILURE_LIMIT):
            assert client.post("/api/auth/login", json={"api_key": "wrong"}).status_code == 401
        # 超过失败上限后，即使 key 正确也先拒绝
        assert client.post("/api/auth/login", json={"api_key": "secret-key"}).status_code == 429


def test_login_requires_configured_api_key() -> None:
    with TestClient(app) as client:
        response = client.post("/api/auth/login", json={"api_key": "anything"})
        assert response.status_code == 400
        assert client.get("/api/auth/status").json() == {
            "auth_required": False,
            "authenticated": True,
        }


def test_api_key_header_brute_force_is_rate_limited(monkeypatch) -> None:
    from starlette.websockets import WebSocketDisconnect

    from src.web.safety import KEY_FAILURE_LIMIT

    _fake_api_key(monkeypatch, "secret-key")
    with TestClient(app, client=REMOTE) as client:
        assert client.post("/api/auth/login", json={"api_key": "secret-key"}).status_code == 200
        for _ in range(KEY_FAILURE_LIMIT):
            assert client.get("/api/components", headers={"X-API-Key": "guess"}).status_code == 401
        # 第 N+1 次起，不论对错都 429；登录同样被锁
        assert client.get("/api/components", headers={"X-API-Key": "guess"}).status_code == 429
        assert client.get("/api/components", headers={"X-API-Key": "secret-key"}).status_code == 429
        assert client.post("/api/auth/login", json={"api_key": "secret-key"}).status_code == 429
        with pytest.raises(WebSocketDisconnect) as exc_info:
            with client.websocket_connect("/api/ws/tasks", headers={"X-API-Key": "secret-key"}):
                pass
        assert exc_info.value.code == 1008
        # 持有有效会话 Cookie 的请求不受锁定影响
        assert client.get("/api/components").status_code == 200

    # 计数按客户端 IP，其他 IP 不受影响
    with TestClient(app, client=("198.51.100.7", 50000)) as other:
        assert other.get("/api/components", headers={"X-API-Key": "secret-key"}).status_code == 200


def test_key_failures_are_shared_by_worker_key_and_login(monkeypatch) -> None:
    from src.web.safety import KEY_FAILURE_LIMIT

    _fake_api_key(monkeypatch, "admin-key", worker_value="worker-key")
    register = {"worker_id": "worker-rate-limit", "capabilities": []}
    with TestClient(app, client=REMOTE) as client:
        half = KEY_FAILURE_LIMIT // 2
        for _ in range(half):
            response = client.post(
                "/api/workers/register", json=register, headers={"X-API-Key": "guess"}
            )
            assert response.status_code == 401
        for _ in range(KEY_FAILURE_LIMIT - half):
            assert client.post("/api/auth/login", json={"api_key": "guess"}).status_code == 401
        locked = client.post(
            "/api/workers/register", json=register, headers={"X-API-Key": "worker-key"}
        )
        assert locked.status_code == 429


def test_auth_status_ignores_api_key_header(monkeypatch) -> None:
    from src.web.safety import KEY_FAILURE_LIMIT

    _fake_api_key(monkeypatch, "secret-key")
    with TestClient(app, client=REMOTE) as client:
        expected = {"auth_required": True, "authenticated": False}
        for _ in range(KEY_FAILURE_LIMIT + 2):
            assert client.get("/api/auth/status", headers={"X-API-Key": "guess"}).json() == expected
        assert client.get("/api/auth/status", headers={"X-API-Key": "secret-key"}).json() == expected
        # status 上的尝试既不给答案，也不计入失败
        assert client.get("/api/components", headers={"X-API-Key": "secret-key"}).status_code == 200


def test_key_failure_tracker_is_bounded() -> None:
    from src.web.safety import KeyFailureTracker

    tracker = KeyFailureTracker(limit=2, window_seconds=300, max_clients=3)
    for client in ("a", "b", "c", "d"):
        tracker.record_failure(client)
    assert not tracker.is_locked("zzz")  # 查询不插入条目
    tracker.record_failure("b")
    tracker.record_failure("e")
    # 满了淘汰最久没失败的：a、c 被挤掉，正在失败的 b 保留并被锁定
    assert tracker.is_locked("b")
    assert len(tracker._failures) == 3
    assert "a" not in tracker._failures and "c" not in tracker._failures

    expired = KeyFailureTracker(limit=2, window_seconds=0, max_clients=10)
    expired.record_failure("x")
    expired.record_failure("x")
    assert not expired.is_locked("x")
    assert "x" not in expired._failures


def test_rotating_api_key_invalidates_existing_cookie(monkeypatch) -> None:
    _fake_api_key(monkeypatch, "secret-key")
    with TestClient(app, client=REMOTE) as client:
        assert client.post("/api/auth/login", json={"api_key": "secret-key"}).status_code == 200
        assert client.get("/api/components").status_code == 200
        _fake_api_key(monkeypatch, "rotated-key")
        assert client.get("/api/components").status_code == 401


def test_session_cookie_name_is_bound_to_server_port(monkeypatch) -> None:
    from src.core.app_settings import ServerSettings, get_server_settings
    from src.web.safety import session_cookie_name
    from src.web.session_tokens import issue_session_token

    assert session_cookie_name(ServerSettings(port=9000)) == "autoflux_session_9000"

    _fake_api_key(monkeypatch, "secret-key")
    port = get_server_settings().port
    token = issue_session_token("secret-key")
    with TestClient(app, client=REMOTE) as client:
        for name in (f"autoflux_session_{port + 1}", "autoflux_session"):
            response = client.get("/api/components", headers={"Cookie": f"{name}={token}"})
            assert response.status_code == 401
        ok = client.get("/api/components", headers={"Cookie": f"autoflux_session_{port}={token}"})
        assert ok.status_code == 200


def test_session_token_expiry_skew_and_key_binding(monkeypatch) -> None:
    import time

    from src.web.session_tokens import (
        SESSION_TTL_SECONDS,
        is_valid_session_token,
        issue_session_token,
    )

    token = issue_session_token("secret-key")
    assert is_valid_session_token(token, "secret-key")
    assert not is_valid_session_token(token, "rotated-key")
    assert not is_valid_session_token(token, "")
    assert not is_valid_session_token(token, "secret-key", now=time.time() + SESSION_TTL_SECONDS + 1)
    assert not is_valid_session_token("not-a-token", "secret-key")
    assert not is_valid_session_token(None, "secret-key")
    future = issue_session_token("secret-key", now=time.time() + 3600)
    assert not is_valid_session_token(future, "secret-key")
    slightly_ahead = issue_session_token("secret-key", now=time.time() + 30)
    assert is_valid_session_token(slightly_ahead, "secret-key")
    tampered = token[:-1] + ("0" if token[-1] != "0" else "1")
    assert not is_valid_session_token(tampered, "secret-key")


def test_session_token_cannot_be_forged_with_api_key_alone() -> None:
    import hashlib
    import hmac
    import time

    from src.web.session_tokens import is_valid_session_token

    api_key = "secret-key"
    issued_at = int(time.time())
    nonce = "A" * 24
    key_digest = hashlib.sha256(api_key.encode()).hexdigest()
    message = f"v1|{issued_at}|{nonce}|{key_digest}".encode()
    forged = f"v1.{issued_at}.{nonce}.{hmac.new(api_key.encode(), message, hashlib.sha256).hexdigest()}"
    assert not is_valid_session_token(forged, api_key)

    # 旧格式：HMAC(api_key, 已知明文)
    legacy_message = f"autoflux-admin-session|{issued_at}".encode()
    legacy_sig = hmac.new(api_key.encode(), legacy_message, hashlib.sha256).hexdigest()
    assert not is_valid_session_token(f"{issued_at}.{legacy_sig}", api_key)


def test_session_secret_is_persisted_in_auth_dir(tmp_path, monkeypatch) -> None:
    import os
    import stat

    from src.web.session_tokens import get_auth_dir, is_valid_session_token, issue_session_token

    token = issue_session_token("secret-key")
    auth_dir = get_auth_dir()
    secret_file = auth_dir / "session_secret"
    assert len(bytes.fromhex(secret_file.read_text(encoding="ascii").strip())) == 32
    if os.name == "posix":
        assert stat.S_IMODE(secret_file.stat().st_mode) == 0o600

    # 换一个目录 = 换一把密钥；切回来后从文件读回原密钥
    monkeypatch.setenv("AUTOFLUX_AUTH_DIR", str(tmp_path / "other-auth"))
    assert not is_valid_session_token(token, "secret-key")
    monkeypatch.setenv("AUTOFLUX_AUTH_DIR", str(auth_dir))
    assert is_valid_session_token(token, "secret-key")


@pytest.mark.parametrize(
    "raw_value",
    [
        b"\xb2.x",  # Starlette 按 latin-1 解码成 '²'：str.isdigit() 为 True，int() 抛异常
        b"v1.\xb2.AAAAAAAAAAAAAAAAAAAAAAAA." + b"0" * 64,
        b"9" * 5000 + b".x",  # 超过 int() 位数上限
        b"v1." + b"9" * 5000 + b".AAAAAAAAAAAAAAAAAAAAAAAA." + b"0" * 64,
        b"v1.1.AAAA.AAAA",
        b"",
    ],
)
def test_malformed_session_cookie_is_rejected_not_500(monkeypatch, raw_value) -> None:
    from starlette.requests import HTTPConnection

    from src.web.safety import admin_rejection

    _fake_api_key(monkeypatch, "secret-key")
    # TestClient/httpx 会把非 ASCII 头重新编码，这里直接构造原始字节的 scope
    scope = {
        "type": "http",
        "method": "GET",
        "path": "/api/components",
        "raw_path": b"/api/components",
        "query_string": b"",
        "scheme": "http",
        "server": ("127.0.0.1", 8000),
        "client": REMOTE,
        "headers": [
            (b"host", b"127.0.0.1:8000"),
            (b"cookie", _cookie_name().encode() + b"=" + raw_value),
        ],
    }
    assert admin_rejection(HTTPConnection(scope), None) == (401, "Unauthorized")


def test_oversized_numeric_session_cookie_does_not_break_endpoints(monkeypatch) -> None:
    _fake_api_key(monkeypatch, "secret-key")
    cookie = {"Cookie": f"{_cookie_name()}={'9' * 5000}.x"}
    with TestClient(app, client=REMOTE) as client:
        status = client.get("/api/auth/status", headers=cookie)
        assert status.status_code == 200
        assert status.json()["authenticated"] is False
        assert client.get("/api/components", headers=cookie).status_code == 401


def test_worker_api_key_only_reaches_worker_protocol(monkeypatch) -> None:
    _fake_api_key(monkeypatch, "admin-key", worker_value="worker-key")
    worker_headers = {"X-API-Key": "worker-key"}
    with TestClient(app, client=REMOTE) as client:
        registered = client.post(
            "/api/workers/register",
            json={"worker_id": "worker-scope-test", "capabilities": []},
            headers=worker_headers,
        )
        assert registered.status_code == 200, registered.text
        heartbeat = client.post(
            "/api/workers/worker-scope-test/heartbeat",
            json={"status": "online"},
            headers=worker_headers,
        )
        assert heartbeat.status_code == 200, heartbeat.text

        # worker key 不能访问管理接口
        assert client.get("/api/workers", headers=worker_headers).status_code == 401
        assert client.get("/api/workers/worker-scope-test", headers=worker_headers).status_code == 401
        assert client.get("/api/components", headers=worker_headers).status_code == 401
        # 无 key 不能访问 worker 协议接口；admin key 可以
        assert (
            client.post(
                "/api/workers/register", json={"worker_id": "anon", "capabilities": []}
            ).status_code
            == 401
        )
        assert client.get("/api/workers", headers={"X-API-Key": "admin-key"}).status_code == 200


def test_remote_access_warning_when_listening_publicly_without_api_key() -> None:
    from src.core.app_settings import ServerSettings
    from src.web.safety import remote_access_warning

    assert remote_access_warning(ServerSettings(host="0.0.0.0")) is not None
    assert remote_access_warning(ServerSettings(host="192.168.1.10")) is not None
    assert remote_access_warning(ServerSettings(host="127.0.0.1")) is None
    assert remote_access_warning(ServerSettings(host="::1")) is None
    assert remote_access_warning(ServerSettings(host="0.0.0.0", api_key="k")) is None


def test_startup_logs_remote_access_warning(monkeypatch) -> None:
    from loguru import logger

    import src.web.app as web_app

    _fake_server_config(monkeypatch, host="0.0.0.0")
    # lifespan 里的 configure_logging 会 logger.remove() 掉测试 sink
    monkeypatch.setattr(web_app, "configure_logging", lambda: None)
    messages: list[str] = []
    sink_id = logger.add(lambda message: messages.append(str(message)), level="WARNING")
    try:
        with TestClient(app):
            pass
    finally:
        logger.remove(sink_id)
    assert any("server.api_key" in message for message in messages)


def test_server_settings_do_not_depend_on_other_sections(monkeypatch) -> None:
    """运行时其他段（scheduler 等）被改坏，不能把鉴权拖成 500。"""
    from pydantic import ValidationError

    import src.core.config as config
    from src.core.app_settings import get_app_settings, get_server_settings

    original_get = config.get

    def fake_get(key, default=None):
        if key == "scheduler.max_concurrent_tasks":
            return "not-a-number"
        return original_get(key, default)

    monkeypatch.setattr(config, "get", fake_get)
    with pytest.raises(ValidationError):
        get_app_settings()
    assert get_server_settings().port > 0
    # 不进 lifespan（启动时本来就要整份配置合法），只看运行中的鉴权路径
    client = TestClient(app, client=REMOTE)
    assert client.get("/api/auth/status").status_code == 200
    assert client.get("/api/components").status_code == 401
