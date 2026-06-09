from types import SimpleNamespace

import pytest
from langchain_core.messages import AIMessage, HumanMessage

import src.agent.agent as agent_module
import src.web.app as web_app
import src.web.routes.agent as agent_routes
from src.agent.agent import AgentService, _redact_stream_text, _redact_stream_value
from src.agent.tools import ALL_TOOLS
from src.web.app import app
from fastapi.testclient import TestClient


def test_agent_status_summary_is_lightweight() -> None:
    service = AgentService(session_service=object())

    summary = service.get_status_summary()

    assert summary["initialized"] is False
    assert summary["active_tool_count"] == len(ALL_TOOLS)
    assert summary["base_tools"] == [tool.name for tool in ALL_TOOLS]
    assert "get_agent_status" in summary["active_tools"]
    assert summary["missing_base_tools"] == []
    assert summary["extra_active_tools"] == []
    assert summary["tool_groups"]["reports"]["count"] >= 3
    assert "get_agent_status" in summary["tool_groups"]["system"]["tools"]
    assert summary["mcp_tools"] == []
    assert summary["session_count"] == 0


def test_agent_status_summary_reports_tool_inventory_drift() -> None:
    service = AgentService(session_service=object())
    service._agent_executor = SimpleNamespace(
        tools=[
            SimpleNamespace(name="list_tasks"),
            SimpleNamespace(name="browser_snapshot"),
            SimpleNamespace(name="custom_debug_tool"),
        ]
    )

    summary = service.get_status_summary()

    assert summary["active_tools"] == [
        "list_tasks",
        "browser_snapshot",
        "custom_debug_tool",
    ]
    assert "get_agent_status" in summary["missing_base_tools"]
    assert summary["extra_active_tools"] == ["browser_snapshot", "custom_debug_tool"]
    assert summary["tool_groups"]["tasks"]["tools"] == ["list_tasks"]
    assert summary["tool_groups"]["browser"]["tools"] == ["browser_snapshot"]
    assert summary["tool_groups"]["other"]["tools"] == ["custom_debug_tool"]


def test_agent_status_summary_includes_session_health_metrics(monkeypatch) -> None:
    service = AgentService(session_service=object())
    active_provider = service.get_active_provider()
    service.get_available_providers = lambda: [
        {"key": active_provider, "label": "Active", "model": "active-model"}
    ]
    service._session_timeout = 60
    service._histories = {
        "active": [HumanMessage(content="hello"), AIMessage(content="ok")],
        "stale": [HumanMessage(content="old")],
    }
    service._sessions_timestamps = {
        "active": 990.0,
        "stale": 900.0,
        "orphan": 100.0,
    }
    monkeypatch.setattr(agent_module.time, "time", lambda: 1000.0)

    summary = service.get_status_summary()

    assert summary["provider_available"] is True
    assert summary["available_provider_count"] == 1
    assert summary["available_providers"][0]["key"] == active_provider
    assert summary["session_count"] == 2
    assert summary["history_message_count"] == 3
    assert summary["average_messages_per_session"] == 1.5
    assert summary["newest_session_age_seconds"] == 10
    assert summary["oldest_session_age_seconds"] == 100
    assert summary["stale_session_count"] == 1


def test_get_agent_status_tool_is_registered() -> None:
    assert "get_agent_status" in {tool.name for tool in ALL_TOOLS}
    assert "precheck_report" in {tool.name for tool in ALL_TOOLS}
    assert "list_reports" in {tool.name for tool in ALL_TOOLS}


def test_agent_status_api_returns_runtime_summary() -> None:
    with TestClient(app) as client:
        response = client.get("/api/agent/status")

    assert response.status_code == 200
    payload = response.json()
    assert payload["active_tool_count"] >= len(ALL_TOOLS)
    assert "get_agent_status" in payload["active_tools"]


def test_agent_chat_sse_redacts_logged_exception(monkeypatch) -> None:
    captured: list[str] = []

    class FailingAgentService:
        _mcp_manager = None

        async def ainvoke(self, message, session_id):
            raise RuntimeError("agent failed api_key=secret-key; token=secret-token")
            yield {}

    monkeypatch.setattr(web_app, "get_agent_service", lambda: FailingAgentService())
    monkeypatch.setattr(agent_routes.logger, "error", lambda message: captured.append(str(message)))

    with TestClient(app) as client:
        response = client.post(
            "/api/agent/chat",
            json={"message": "hello", "session_id": "redact-log"},
        )

    rendered_logs = " ".join(captured)
    assert response.status_code == 200
    assert "secret-key" not in rendered_logs
    assert "secret-token" not in rendered_logs
    assert "api_key=[REDACTED]" in rendered_logs
    assert "token=[REDACTED]" in rendered_logs
    assert "secret-key" not in response.text
    assert "secret-token" not in response.text


def test_set_agent_provider_error_response_is_redacted(monkeypatch) -> None:
    class FailingAgentService:
        def set_provider(self, provider):
            raise ValueError("unknown provider api_key=provider-secret")

    monkeypatch.setattr(web_app, "get_agent_service", lambda: FailingAgentService())

    with TestClient(app) as client:
        response = client.post("/api/agent/providers", json={"provider": "missing"})

    assert response.status_code == 400
    assert "provider-secret" not in response.text
    assert "api_key=[REDACTED]" in response.text


@pytest.mark.asyncio
async def test_agent_provider_config_tolerates_invalid_numeric_values(monkeypatch) -> None:
    import src.core.config as config_module

    monkeypatch.setattr(
        config_module,
        "get_raw_section",
        lambda section: {
            "provider": "qwen",
            "qwen": {
                "model": "qwen-max",
                "api_key": "plain-secret",
                "temperature": "",
                "max_tokens": "bad-number",
            },
        },
    )
    monkeypatch.setattr(
        config_module,
        "get",
        lambda key, default=None: "qwen" if key == "llm.provider" else default,
    )

    payload = await agent_routes.get_llm_providers_config()

    rendered = str(payload)
    provider = payload["providers"][0]
    assert provider["temperature"] == 0.3
    assert provider["max_tokens"] == 2000
    assert provider["api_key"] == ""
    assert provider["has_api_key"] is True
    assert "plain-secret" not in rendered


def test_agent_stream_redacts_sensitive_structured_payloads() -> None:
    payload = {
        "api_key": "secret-key",
        "headers": {"Authorization": "Bearer secret-token"},
        "query": "token=plain-secret",
        "nested": [{"cookie": "session-secret"}],
    }

    redacted = _redact_stream_value(payload)
    rendered = str(redacted)

    assert "secret-key" not in rendered
    assert "secret-token" not in rendered
    assert "plain-secret" not in rendered
    assert "session-secret" not in rendered
    assert "[REDACTED]" in rendered


def test_agent_stream_redacts_sensitive_text_and_json_strings() -> None:
    text = _redact_stream_text("api_key=secret-key; token: secret-token")
    json_text = _redact_stream_text('{"api_key": "secret-key", "value": "ok"}')

    assert "secret-key" not in text
    assert "secret-token" not in text
    assert "secret-key" not in json_text
    assert "ok" in json_text
