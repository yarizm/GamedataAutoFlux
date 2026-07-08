import json
from types import SimpleNamespace
from typing import Any, Sequence

import pytest
from langchain.agents import create_agent
from langchain_core.language_models.fake_chat_models import FakeMessagesListChatModel
from langchain_core.messages import AIMessage, HumanMessage
from langchain_core.tools import BaseTool, tool

import src.agent.agent as agent_module
import src.web.app as web_app
import src.web.routes.agent as agent_routes
from src.agent.agent import AgentService, _redact_stream_text, _redact_stream_value
from src.agent.runtime import LangGraphAgentRuntime
from src.agent.schemas import ChatRequest
from src.agent.tools import ALL_TOOLS
from src.agent.tools.system import GetAgentStatusTool
from src.web.app import app
from fastapi.testclient import TestClient


class ImmediateSessionService:
    def __init__(self) -> None:
        self._max_sessions = 50
        self.saved_histories = None
        self.deleted_sessions = []

    async def save_histories(self, histories, timestamps, last_save_time, force=False):
        self.saved_histories = histories
        return 0

    async def delete_sessions(self, session_ids):
        self.deleted_sessions.extend(session_ids)


class ColdStartHistorySessionService:
    def __init__(self) -> None:
        self._max_sessions = 50
        self.load_calls = 0
        self.deleted_sessions = []

    async def load_histories(self):
        self.load_calls += 1
        return (
            {
                "loaded-thread": [
                    HumanMessage(content="password=persisted-secret"),
                    AIMessage(content='{"token": "persisted-token", "value": "ready"}'),
                ]
            },
            {"loaded-thread": 123.0},
        )

    async def save_histories(self, histories, timestamps, last_save_time, force=False):
        return last_save_time

    async def delete_sessions(self, session_ids):
        self.deleted_sessions.extend(session_ids)


class FlakyLoadHistorySessionService:
    def __init__(self) -> None:
        self._max_sessions = 50
        self.load_calls = 0

    async def load_histories(self):
        self.load_calls += 1
        if self.load_calls == 1:
            raise RuntimeError("temporary storage failure")
        return (
            {
                "retry-thread": [
                    HumanMessage(content="hello"),
                    AIMessage(content="world"),
                ]
            },
            {"retry-thread": 321.0},
        )

    async def save_histories(self, histories, timestamps, last_save_time, force=False):
        return last_save_time


def test_agent_status_summary_is_lightweight() -> None:
    service = AgentService(session_service=object())

    summary = service.get_status_summary()

    assert summary["initialized"] is False
    assert summary["runtime_backend"] in {"langchain_classic", "langgraph_agent"}
    assert summary["active_tool_count"] == len(ALL_TOOLS)
    assert summary["base_tools"] == [tool.name for tool in ALL_TOOLS]
    assert "get_agent_status" in summary["active_tools"]
    assert summary["missing_base_tools"] == []
    assert summary["extra_active_tools"] == []
    assert summary["tool_groups"]["reports"]["count"] >= 3
    assert "get_agent_status" in summary["tool_groups"]["system"]["tools"]
    assert summary["mcp_tools"] == []
    assert summary["session_count"] == 0
    assert summary["thread_count"] == 0
    assert summary["thread_checkpoint_backend"] in {"disabled", "memory", "file"}
    assert summary["thread_checkpoint_storage_path"] is None
    assert summary["thread_checkpointing_enabled"] is False
    assert summary["status_health"] == "ok"
    assert summary["status_warnings"] == []
    assert summary["history_load_failed"] is False
    assert summary["pending_history_recovery_thread_count"] == 0
    assert summary["history_recovery_warnings"] == []


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
    assert summary["average_messages_per_thread"] == 1.5
    assert summary["newest_session_age_seconds"] == 10
    assert summary["newest_thread_age_seconds"] == 10
    assert summary["oldest_session_age_seconds"] == 100
    assert summary["stale_session_count"] == 1
    assert summary["thread_count"] == 2
    assert summary["stale_thread_count"] == 1
    assert summary["thread_checkpoint_backend"] in {"disabled", "memory", "file"}
    assert summary["thread_checkpoint_storage_path"] is None


def test_agent_reload_config_rebuilds_runtime_backend(monkeypatch) -> None:
    current_backend = {"value": "langchain_classic"}

    def fake_get_config(key, default=None):
        if key == "agent.runtime_backend":
            return current_backend["value"]
        if key == "agent.max_iterations":
            return 10
        if key == "agent.session_timeout_minutes":
            return 60
        if key == "agent.system_prompt":
            return "system"
        if key == "llm.provider":
            return "qwen"
        if key == "llm":
            return {"qwen": {"model": "qwen-max"}}
        return default

    monkeypatch.setattr(agent_module, "get_config", fake_get_config)
    monkeypatch.setattr("src.agent.runtime.get_config", fake_get_config)

    service = AgentService(session_service=object())
    assert service.get_status_summary()["runtime_backend"] == "langchain_classic"

    current_backend["value"] = "langgraph_agent"
    service.reload_config()

    assert service.get_status_summary()["runtime_backend"] == "langgraph_agent"


def test_agent_status_summary_reports_effective_agent_type_for_langgraph(monkeypatch) -> None:
    def fake_get_config(key, default=None):
        if key == "agent.runtime_backend":
            return "langgraph_agent"
        if key == "agent.agent_type":
            return "react"
        if key == "agent.max_iterations":
            return 10
        if key == "agent.session_timeout_minutes":
            return 60
        if key == "agent.system_prompt":
            return "system"
        if key == "llm.provider":
            return "qwen"
        if key == "llm":
            return {"qwen": {"model": "qwen-max"}}
        return default

    monkeypatch.setattr(agent_module, "get_config", fake_get_config)
    monkeypatch.setattr("src.agent.runtime.get_config", fake_get_config)

    service = AgentService(session_service=object())
    summary = service.get_status_summary()

    assert summary["runtime_backend"] == "langgraph_agent"
    assert summary["configured_agent_type"] == "react"
    assert summary["effective_agent_type"] == "openai_tools"
    assert summary["agent_type"] == "openai_tools"
    assert summary["legacy_react_parser_enabled"] is False
    assert summary["agent_type_compatibility_warnings"]
    assert summary["status_health"] == "warning"
    assert summary["status_warnings"] == summary["agent_type_compatibility_warnings"]


def test_agent_defaults_to_langgraph_runtime_when_backend_not_configured(monkeypatch) -> None:
    def fake_get_config(key, default=None):
        if key == "agent.max_iterations":
            return 10
        if key == "agent.session_timeout_minutes":
            return 60
        if key == "agent.system_prompt":
            return "system"
        if key == "llm.provider":
            return "qwen"
        if key == "llm":
            return {"qwen": {"model": "qwen-max"}}
        return default

    monkeypatch.setattr(agent_module, "get_config", fake_get_config)
    monkeypatch.setattr("src.agent.runtime.get_config", fake_get_config)
    monkeypatch.setattr(
        "src.agent.checkpointer.get_config",
        lambda key, default=None: default,
    )

    service = AgentService(session_service=object())
    summary = service.get_status_summary()

    assert summary["runtime_backend"] == "langgraph_agent"


def test_agent_set_provider_resets_langgraph_runtime(monkeypatch) -> None:
    def fake_get_config(key, default=None):
        if key == "agent.runtime_backend":
            return "langgraph_agent"
        if key == "agent.agent_type":
            return "openai_tools"
        if key == "agent.max_iterations":
            return 10
        if key == "agent.session_timeout_minutes":
            return 60
        if key == "agent.system_prompt":
            return "system"
        if key == "llm.provider":
            return "qwen"
        if key == "llm":
            return {
                "qwen": {"model": "qwen-max"},
                "deepseek": {"model": "deepseek-chat"},
            }
        return default

    monkeypatch.setattr(agent_module, "get_config", fake_get_config)
    monkeypatch.setattr("src.agent.runtime.get_config", fake_get_config)
    monkeypatch.setattr(
        "src.agent.checkpointer.get_config",
        lambda key, default=None: default,
    )

    service = AgentService(session_service=object())
    service._initialized = True
    service._agent_executor = object()
    service._mcp_manager = object()

    service.set_provider("deepseek")

    summary = service.get_status_summary()
    assert summary["runtime_backend"] == "langgraph_agent"
    assert summary["provider"] == "deepseek"
    assert service._initialized is False
    assert service._agent_executor is None
    assert service._mcp_manager is None


def test_agent_reset_runtime_rebuilds_langgraph_checkpointer(monkeypatch, tmp_path) -> None:
    current_backend = {"value": "memory"}
    file_path = tmp_path / "agent-checkpoints.json"

    def fake_runtime_config(key, default=None):
        if key == "agent.runtime_backend":
            return "langgraph_agent"
        if key == "agent.agent_type":
            return "openai_tools"
        if key == "agent.max_iterations":
            return 10
        if key == "agent.session_timeout_minutes":
            return 60
        if key == "agent.system_prompt":
            return "system"
        if key == "llm.provider":
            return "qwen"
        if key == "llm":
            return {"qwen": {"model": "qwen-max"}}
        return default

    def fake_checkpointer_config(key, default=None):
        if key == "agent.langgraph_checkpointer.backend":
            return current_backend["value"]
        if key == "agent.langgraph_checkpointer.file_path":
            return str(file_path)
        return default

    monkeypatch.setattr(agent_module, "get_config", fake_runtime_config)
    monkeypatch.setattr("src.agent.runtime.get_config", fake_runtime_config)
    monkeypatch.setattr("src.agent.checkpointer.get_config", fake_checkpointer_config)

    service = AgentService(session_service=object())
    summary = service.get_status_summary()
    assert summary["runtime_backend"] == "langgraph_agent"
    assert summary["thread_checkpoint_backend"] == "memory"
    assert summary["thread_checkpoint_storage_path"] is None

    current_backend["value"] = "file"
    service.reset_runtime()

    updated = service.get_status_summary()
    assert updated["runtime_backend"] == "langgraph_agent"
    assert updated["thread_checkpoint_backend"] == "file"
    assert updated["thread_checkpoint_storage_path"] == str(file_path)
    assert updated["thread_checkpointing_enabled"] is True


@pytest.mark.asyncio
async def test_langgraph_status_summary_reports_injected_mcp_tools(monkeypatch) -> None:
    class FakeMcpManager:
        def __init__(self) -> None:
            self._is_running = False
            self.start_calls = 0
            self.tools = [SimpleNamespace(name="browser_snapshot")]

        async def start(self) -> None:
            self.start_calls += 1
            self._is_running = True

        def get_langchain_tools(self):
            return self.tools

    class FakeExecutor:
        def __init__(self, tools):
            self.tools = list(tools)

    def fake_get_config(key, default=None):
        if key == "agent.runtime_backend":
            return "langgraph_agent"
        if key == "agent.agent_type":
            return "openai_tools"
        if key == "agent.playwright_mcp.enabled":
            return True
        if key == "agent.max_iterations":
            return 10
        if key == "agent.session_timeout_minutes":
            return 60
        if key == "agent.system_prompt":
            return "system"
        if key == "llm.provider":
            return "qwen"
        if key == "llm":
            return {"qwen": {"model": "qwen-max"}}
        return default

    monkeypatch.setattr(agent_module, "get_config", fake_get_config)
    monkeypatch.setattr("src.agent.runtime.get_config", fake_get_config)
    monkeypatch.setattr(
        "src.agent.checkpointer.get_config",
        lambda key, default=None: default,
    )
    monkeypatch.setattr(
        LangGraphAgentRuntime,
        "ensure_initialized",
        lambda self, **kwargs: (
            setattr(self, "initialized", True),
            setattr(self, "llm", object()),
            setattr(self, "agent_executor", FakeExecutor(kwargs["base_tools"])),
            setattr(self, "active_tools", list(kwargs["base_tools"])),
        ),
    )
    monkeypatch.setattr(
        LangGraphAgentRuntime,
        "_build_executor",
        lambda self, llm, tools, max_iterations: FakeExecutor(tools),
    )

    service = AgentService(session_service=object())
    fake_manager = FakeMcpManager()
    monkeypatch.setattr(service, "_create_mcp_manager", lambda: fake_manager)
    service._runtime._create_mcp_manager = lambda: fake_manager

    await service._async_ensure_initialized()

    summary = service.get_status_summary()
    assert summary["runtime_backend"] == "langgraph_agent"
    assert summary["mcp_enabled"] is True
    assert summary["mcp_running"] is True
    assert summary["mcp_tool_count"] == 1
    assert summary["mcp_tools"] == ["browser_snapshot"]
    assert "browser_snapshot" in summary["active_tools"]
    assert fake_manager.start_calls == 1


def test_get_agent_status_tool_is_registered() -> None:
    assert "get_agent_status" in {tool.name for tool in ALL_TOOLS}
    assert "precheck_report" in {tool.name for tool in ALL_TOOLS}
    assert "list_reports" in {tool.name for tool in ALL_TOOLS}


@pytest.mark.asyncio
async def test_get_agent_status_tool_loads_persisted_histories_on_demand(monkeypatch) -> None:
    session_service = ColdStartHistorySessionService()
    service = AgentService(session_service=session_service)
    monkeypatch.setattr(web_app, "get_agent_service", lambda: service)

    payload = json.loads(await GetAgentStatusTool()._arun())

    assert payload["status"] == "ok"
    assert payload["data"]["histories_loaded"] is True
    assert payload["data"]["thread_count"] == 1
    assert payload["data"]["session_count"] == 1
    assert payload.get("warnings") is None
    assert session_service.load_calls == 1


@pytest.mark.asyncio
async def test_get_agent_status_tool_surfaces_warning_status(monkeypatch) -> None:
    session_service = FlakyLoadHistorySessionService()
    service = AgentService(session_service=session_service)
    monkeypatch.setattr(web_app, "get_agent_service", lambda: service)

    payload = json.loads(await GetAgentStatusTool()._arun())

    assert payload["status"] == "warning"
    assert payload["warnings"]
    assert "告警" in payload["summary"]
    assert payload["data"]["status_health"] == "warning"
    assert payload["data"]["history_load_failed"] is True
    assert session_service.load_calls == 1


def test_agent_status_api_returns_runtime_summary() -> None:
    with TestClient(app) as client:
        response = client.get("/api/agent/status")

    assert response.status_code == 200
    payload = response.json()
    assert payload["runtime_backend"] in {"langchain_classic", "langgraph_agent"}
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


def test_agent_chat_route_accepts_thread_id_alias(monkeypatch) -> None:
    captured = {"thread_id": None}

    class EchoAgentService:
        _mcp_manager = None

        async def ainvoke(self, message, session_id):
            captured["thread_id"] = session_id
            yield {"type": "final", "content": f"echo:{message}"}

    monkeypatch.setattr(web_app, "get_agent_service", lambda: EchoAgentService())

    with TestClient(app) as client:
        response = client.post(
            "/api/agent/chat",
            json={"message": "hello", "thread_id": "thread-route"},
        )

    assert response.status_code == 200
    assert captured["thread_id"] == "thread-route"
    assert 'data: {"type": "final", "content": "echo:hello"}' in response.text


def test_agent_chat_sse_supports_langgraph_runtime(monkeypatch) -> None:
    class BindToolsFakeModel(FakeMessagesListChatModel):
        def bind_tools(
            self,
            tools: Sequence[BaseTool | dict | type | Any],
            *,
            tool_choice: Any = None,
            **kwargs: Any,
        ):
            return self

    @tool
    def echo(text: str) -> str:
        """Echo text."""
        return f"ECHO:{text}"

    model = BindToolsFakeModel(
        responses=[
            AIMessage(
                content="",
                tool_calls=[{"name": "echo", "args": {"text": "hi"}, "id": "call_1", "type": "tool_call"}],
            ),
            AIMessage(content="done final"),
        ]
    )
    agent_graph = create_agent(model=model, tools=[echo], system_prompt="You are helpful")

    session_service = ImmediateSessionService()
    service = AgentService(session_service=session_service)
    graph_runtime = LangGraphAgentRuntime(
        build_openai_tools_system_prompt=lambda tools: "You are helpful",
        build_openai_tools_prompt=lambda tools: None,
        build_react_prompt=lambda tools: None,
        create_mcp_manager=lambda: None,
        handle_parsing_error=lambda exc: str(exc),
    )
    service._set_runtime_for_testing(graph_runtime)
    service._agent_executor = agent_graph

    async def initialized_noop() -> None:
        return None

    monkeypatch.setattr(service, "_async_ensure_initialized", initialized_noop)
    monkeypatch.setattr(web_app, "get_agent_service", lambda: service)

    with TestClient(app) as client:
        response = client.post(
            "/api/agent/chat",
            json={"message": "hello", "session_id": "graph-sse"},
        )

    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/event-stream")

    payloads = []
    for line in response.text.splitlines():
        if line.startswith("data: "):
            payloads.append(json.loads(line[6:]))

    assert any(event["type"] == "tool_call" and event["name"] == "echo" for event in payloads)
    assert any(
        event["type"] == "tool_result" and "ECHO:hi" in event["content"] for event in payloads
    )
    assert not any(event["type"] == "error" for event in payloads)
    final_events = [event for event in payloads if event["type"] == "final"]
    assert len(final_events) == 1
    assert final_events[0]["content"] == "done final"
    assert session_service.saved_histories is not None


def test_agent_history_and_sessions_api_support_langgraph_runtime(monkeypatch) -> None:
    class BindToolsFakeModel(FakeMessagesListChatModel):
        def bind_tools(
            self,
            tools: Sequence[BaseTool | dict | type | Any],
            *,
            tool_choice: Any = None,
            **kwargs: Any,
        ):
            return self

    @tool
    def echo(text: str) -> str:
        """Echo text."""
        return f"ECHO:{text}"

    model = BindToolsFakeModel(
        responses=[
            AIMessage(
                content="",
                tool_calls=[{"name": "echo", "args": {"text": "hi"}, "id": "call_1", "type": "tool_call"}],
            ),
            AIMessage(content="done final"),
        ]
    )
    agent_graph = create_agent(model=model, tools=[echo], system_prompt="You are helpful")

    session_service = ImmediateSessionService()
    service = AgentService(session_service=session_service)
    graph_runtime = LangGraphAgentRuntime(
        build_openai_tools_system_prompt=lambda tools: "You are helpful",
        build_openai_tools_prompt=lambda tools: None,
        build_react_prompt=lambda tools: None,
        create_mcp_manager=lambda: None,
        handle_parsing_error=lambda exc: str(exc),
    )
    service._set_runtime_for_testing(graph_runtime)
    service._agent_executor = agent_graph

    async def initialized_noop() -> None:
        return None

    monkeypatch.setattr(service, "_async_ensure_initialized", initialized_noop)
    monkeypatch.setattr(web_app, "get_agent_service", lambda: service)

    with TestClient(app) as client:
        chat_response = client.post(
            "/api/agent/chat",
            json={"message": "hello", "session_id": "graph-history"},
        )
        sessions_response = client.get("/api/agent/sessions")
        history_response = client.get("/api/agent/history", params={"session_id": "graph-history"})
        clear_response = client.delete("/api/agent/history", params={"session_id": "graph-history"})
        cleared_history_response = client.get(
            "/api/agent/history",
            params={"session_id": "graph-history"},
        )

    assert chat_response.status_code == 200
    assert sessions_response.status_code == 200
    assert history_response.status_code == 200
    assert clear_response.status_code == 200
    assert cleared_history_response.status_code == 200

    sessions_payload = sessions_response.json()
    history_payload = history_response.json()
    cleared_history_payload = cleared_history_response.json()

    assert "graph-history" in sessions_payload["sessions"]
    assert [message["role"] for message in history_payload["messages"]] == ["user", "assistant"]
    assert history_payload["messages"][0]["content"] == "hello"
    assert history_payload["messages"][1]["content"] == "done final"
    assert cleared_history_payload["messages"] == []
    assert session_service.deleted_sessions == ["graph-history"]


def test_agent_history_routes_accept_thread_id_alias(monkeypatch) -> None:
    service = AgentService(session_service=object())
    service._histories["thread-demo"] = [
        HumanMessage(content="hello"),
        AIMessage(content="world"),
    ]

    monkeypatch.setattr(web_app, "get_agent_service", lambda: service)

    with TestClient(app) as client:
        history_response = client.get("/api/agent/history", params={"thread_id": "thread-demo"})
        clear_response = client.delete("/api/agent/history", params={"thread_id": "thread-demo"})

    assert history_response.status_code == 200
    assert history_response.json()["session_id"] == "thread-demo"
    assert history_response.json()["thread_id"] == "thread-demo"
    assert [item["content"] for item in history_response.json()["messages"]] == ["hello", "world"]
    assert clear_response.status_code == 200
    assert clear_response.json()["thread_id"] == "thread-demo"
    assert service.get_thread_history("thread-demo") == []


def test_agent_read_only_routes_load_persisted_histories_on_demand(monkeypatch) -> None:
    session_service = ColdStartHistorySessionService()
    service = AgentService(session_service=session_service)

    monkeypatch.setattr(web_app, "get_agent_service", lambda: service)

    with TestClient(app) as client:
        sessions_response = client.get("/api/agent/sessions")
        history_response = client.get("/api/agent/history", params={"thread_id": "loaded-thread"})
        status_response = client.get("/api/agent/status")

    assert sessions_response.status_code == 200
    assert history_response.status_code == 200
    assert status_response.status_code == 200
    assert service._histories_loaded is True
    assert session_service.load_calls == 1
    assert sessions_response.json()["threads"] == ["loaded-thread"]
    assert history_response.json()["thread_id"] == "loaded-thread"
    assert history_response.json()["messages"] == [
        {"role": "user", "content": "password=[REDACTED]"},
        {"role": "assistant", "content": '{"token": "[REDACTED]", "value": "ready"}'},
    ]
    status_payload = status_response.json()
    assert status_payload["histories_loaded"] is True
    assert status_payload["history_load_failed"] is False
    assert status_payload["pending_history_recovery_thread_count"] == 0
    assert status_payload["history_recovery_warnings"] == []
    assert status_payload["thread_count"] == 1
    assert status_payload["session_count"] == 1


def test_agent_read_only_routes_retry_history_load_after_failure(monkeypatch) -> None:
    session_service = FlakyLoadHistorySessionService()
    service = AgentService(session_service=session_service)

    monkeypatch.setattr(web_app, "get_agent_service", lambda: service)

    with TestClient(app) as client:
        first_status_response = client.get("/api/agent/status")
        second_status_response = client.get("/api/agent/status")

    assert first_status_response.status_code == 200
    assert second_status_response.status_code == 200
    assert first_status_response.json()["histories_loaded"] is False
    assert first_status_response.json()["status_health"] == "warning"
    assert first_status_response.json()["status_warnings"]
    assert first_status_response.json()["history_load_failed"] is True
    assert first_status_response.json()["pending_history_recovery_thread_count"] == 0
    assert first_status_response.json()["history_recovery_warnings"]
    assert first_status_response.json()["thread_count"] == 0
    assert second_status_response.json()["histories_loaded"] is True
    assert second_status_response.json()["status_health"] == "ok"
    assert second_status_response.json()["status_warnings"] == []
    assert second_status_response.json()["history_load_failed"] is False
    assert second_status_response.json()["thread_count"] == 1
    assert second_status_response.json()["session_count"] == 1
    assert session_service.load_calls == 2


def test_agent_chat_request_accepts_thread_id_alias() -> None:
    payload = ChatRequest.model_validate({"message": "hello", "thread_id": "t-1"})

    assert payload.session_id == "t-1"
    assert payload.thread_id == "t-1"


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


def test_reset_runtime_singletons_clears_agent_chain() -> None:
    original_agent_service = web_app._agent_service
    original_agent_session_service = web_app._agent_session_service
    original_task_service = web_app._task_service
    original_worker_registry = web_app._worker_registry
    original_session_registry = web_app._session_registry
    try:
        web_app._agent_service = object()
        web_app._agent_session_service = object()
        web_app._task_service = object()
        web_app._worker_registry = object()
        web_app._session_registry = object()

        web_app._reset_runtime_singletons(reset_agent=True, reset_agent_session=True)
    finally:
        cleared_agent_service = web_app._agent_service
        cleared_agent_session_service = web_app._agent_session_service
        cleared_task_service = web_app._task_service
        cleared_worker_registry = web_app._worker_registry
        cleared_session_registry = web_app._session_registry
        web_app._agent_service = original_agent_service
        web_app._agent_session_service = original_agent_session_service
        web_app._task_service = original_task_service
        web_app._worker_registry = original_worker_registry
        web_app._session_registry = original_session_registry

    assert cleared_agent_service is None
    assert cleared_agent_session_service is None
    assert cleared_task_service is None
    assert cleared_worker_registry is None
    assert cleared_session_registry is None


def test_get_agent_service_rebuilds_after_session_service_replaced(monkeypatch) -> None:
    original_agent_service = web_app._agent_service
    original_agent_session_service = web_app._agent_session_service

    class SessionServiceA:
        pass

    class SessionServiceB:
        pass

    class FakeAgentService:
        def __init__(self, session_service):
            self.session_service = session_service

    try:
        monkeypatch.setattr(web_app, "get_config", lambda key, default=None: True)
        monkeypatch.setattr("src.agent.agent.AgentService", FakeAgentService)

        web_app._agent_service = None
        web_app._agent_session_service = SessionServiceA()
        first = web_app.get_agent_service()

        web_app._reset_runtime_singletons(reset_agent=True)
        web_app._agent_session_service = SessionServiceB()
        second = web_app.get_agent_service()
    finally:
        web_app._agent_service = original_agent_service
        web_app._agent_session_service = original_agent_session_service

    assert first is not second
    assert first.session_service.__class__ is SessionServiceA
    assert second.session_service.__class__ is SessionServiceB


def test_get_agent_service_defaults_to_langgraph_runtime(monkeypatch) -> None:
    original_agent_service = web_app._agent_service
    original_agent_session_service = web_app._agent_session_service

    class FakeSessionService:
        pass

    def fake_app_get_config(key, default=None):
        if key == "agent.enabled":
            return True
        return default

    def fake_agent_get_config(key, default=None):
        if key == "agent.max_iterations":
            return 10
        if key == "agent.session_timeout_minutes":
            return 60
        if key == "agent.system_prompt":
            return "system"
        if key == "llm.provider":
            return "qwen"
        if key == "llm":
            return {"qwen": {"model": "qwen-max"}}
        return default

    try:
        monkeypatch.setattr(web_app, "get_config", fake_app_get_config)
        monkeypatch.setattr(agent_module, "get_config", fake_agent_get_config)
        monkeypatch.setattr("src.agent.runtime.get_config", fake_agent_get_config)
        monkeypatch.setattr(
            "src.agent.checkpointer.get_config",
            lambda key, default=None: default,
        )
        web_app._agent_service = None
        web_app._agent_session_service = FakeSessionService()

        service = web_app.get_agent_service()
    finally:
        web_app._agent_service = original_agent_service
        web_app._agent_session_service = original_agent_session_service

    assert service is not None
    assert service.get_status_summary()["runtime_backend"] == "langgraph_agent"


def test_app_lifespan_smoke_uses_default_langgraph_runtime(monkeypatch) -> None:
    class BindToolsFakeModel(FakeMessagesListChatModel):
        def bind_tools(
            self,
            tools: Sequence[BaseTool | dict | type | Any],
            *,
            tool_choice: Any = None,
            **kwargs: Any,
        ):
            return self

    @tool
    def echo(text: str) -> str:
        """Echo text."""
        return f"ECHO:{text}"

    model = BindToolsFakeModel(
        responses=[
            AIMessage(
                content="",
                tool_calls=[{"name": "echo", "args": {"text": "hi"}, "id": "call_1", "type": "tool_call"}],
            ),
            AIMessage(content="done final"),
        ]
    )

    import src.core.config as config_module

    original_get = config_module.get

    def fake_get_config(key, default=None):
        if key == "agent.runtime_backend":
            return "langgraph_agent"
        if key == "agent.agent_type":
            return "openai_tools"
        if key == "agent.playwright_mcp.enabled":
            return False
        if key == "agent.max_iterations":
            return 10
        if key == "agent.session_timeout_minutes":
            return 60
        if key == "agent.system_prompt":
            return "system"
        if key == "llm.provider":
            return "local"
        if key == "llm.local.api_key":
            return ""
        if key == "llm.local.base_url":
            return ""
        if key == "llm.local.model":
            return "fake-local"
        if key == "llm.local.temperature":
            return 0.3
        if key == "llm.local.max_tokens":
            return 2000
        if key == "llm":
            return {"local": {"model": "fake-local"}}
        return original_get(key, default)

    monkeypatch.setattr(agent_module, "get_config", fake_get_config)
    monkeypatch.setattr("src.agent.runtime.get_config", fake_get_config)
    monkeypatch.setattr("src.core.config.get", fake_get_config)
    monkeypatch.setattr(web_app, "get_config", fake_get_config)
    monkeypatch.setattr(agent_module, "ALL_TOOLS", [echo])
    monkeypatch.setattr(
        "src.agent.runtime.ChatOpenAI",
        lambda **kwargs: model,
    )

    with TestClient(web_app.create_app()) as client:
        status_response = client.get("/api/agent/status")
        chat_response = client.post(
            "/api/agent/chat",
            json={"message": "hello", "thread_id": "lifespan-graph"},
        )
        history_response = client.get("/api/agent/history", params={"thread_id": "lifespan-graph"})

    assert status_response.status_code == 200
    assert status_response.json()["runtime_backend"] == "langgraph_agent"
    assert chat_response.status_code == 200
    assert history_response.status_code == 200

    payloads = []
    for line in chat_response.text.splitlines():
        if line.startswith("data: "):
            payloads.append(json.loads(line[6:]))

    assert any(event["type"] == "tool_call" and event["name"] == "echo" for event in payloads)
    assert any(event["type"] == "tool_result" and "ECHO:hi" in event["content"] for event in payloads)
    final_events = [event for event in payloads if event["type"] == "final"]
    assert len(final_events) == 1
    assert final_events[0]["content"] == "done final"
    assert [item["content"] for item in history_response.json()["messages"]] == ["hello", "done final"]
