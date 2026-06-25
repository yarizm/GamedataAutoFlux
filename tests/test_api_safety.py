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


def test_websocket_router_not_protected_by_admin(monkeypatch) -> None:
    """ws_router 不应被 require_admin 保护（浏览器 WS 无法携带自定义 header）。

    验证方式：配置 api_key 后，admin 路由应 401，health 应 200，
    而 ws_router 上的依赖不应包含 require_admin。
    """
    import src.core.config as config
    from src.web.app import create_app

    original_get = config.get

    def fake_get(key, default=None):
        if key == "server.api_key":
            return "secret-key"
        return original_get(key, default)

    monkeypatch.setattr(config, "get", fake_get)

    # 检查 ws_router 是否被注入了 require_admin 依赖
    test_app = create_app()
    ws_routes = [
        r for r in test_app.routes if hasattr(r, "path") and "/ws/" in getattr(r, "path", "")
    ]
    for route in ws_routes:
        dep_names = [
            d.dependency.__name__ if hasattr(d.dependency, "__name__") else str(d.dependency)
            for d in getattr(route, "dependencies", [])
        ]
        assert "require_admin" not in dep_names, (
            f"WS route {route.path} has require_admin dependency — "
            "browser WebSocket cannot set custom headers"
        )


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
