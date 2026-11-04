"""PlaywrightMcpManager 与 mcp SDK 的契约测试：起一个真实的 stdio MCP 进程，不依赖 Node 和网络。

背景：mcp 2.x 把 inputSchema/isError/mimeType 改成 snake_case 后，`_fetch_tools` 里的
AttributeError 被 except 吞掉，Agent 的浏览器工具全部消失且只留一行错误日志。只有走真实 SDK
类型才能发现这种问题，所以这里不用 mock。
"""

from __future__ import annotations

import sys
import textwrap

import pytest

from src.agent.agent_status_summary import build_agent_status_summary
from src.agent.mcp_client import PlaywrightMcpManager

_FAKE_SERVER = textwrap.dedent(
    """
    from mcp.server.mcpserver import Image, MCPServer
    from mcp.server.mcpserver.exceptions import ToolError

    server = MCPServer("fake")


    @server.tool()
    def echo(text: str, times: int = 1, note: str | None = None) -> str:
        assert note is None, "None 参数应在发送前被过滤"
        return text * times


    @server.tool()
    def boom() -> str:
        raise ToolError("kaboom")


    @server.tool()
    def picture() -> Image:
        return Image(data=b"not-really-png", format="png")


    server.run()
    """
)


@pytest.fixture
def manager(monkeypatch: pytest.MonkeyPatch, tmp_path) -> PlaywrightMcpManager:
    script = tmp_path / "fake_mcp_server.py"
    script.write_text(_FAKE_SERVER, encoding="utf-8")
    config = {
        "agent.playwright_mcp.enabled": True,
        "agent.playwright_mcp.command": sys.executable,
        "agent.playwright_mcp.args": [str(script)],
    }
    monkeypatch.setattr(
        "src.agent.mcp_client.get_config", lambda key, default=None: config.get(key, default)
    )
    return PlaywrightMcpManager()


async def test_manager_loads_tools_and_maps_results(manager: PlaywrightMcpManager) -> None:
    await manager.start()
    try:
        assert manager._is_running is True
        tools = {tool.name: tool for tool in manager.get_langchain_tools()}
        assert set(tools) == {"echo", "boom", "picture"}

        # JSON schema → 参数模型：必填/选填都要保留
        echo_fields = tools["echo"].args_schema.model_fields
        assert echo_fields["text"].is_required()
        assert not echo_fields["times"].is_required()

        assert await tools["echo"].ainvoke({"text": "ab", "times": 2}) == "abab"

        # isError 结果要计入连续失败，而不是当成功返回
        error = await tools["boom"].ainvoke({})
        assert error.startswith("MCP 工具错误")
        assert "kaboom" in error
        assert tools["boom"]._consecutive_failures == 1

        # 图片内容只回传 mime type，不把 base64 塞进上下文
        image = await tools["picture"].ainvoke({})
        assert "image/png" in image
        assert "not-really-png" not in image
    finally:
        await manager.stop()

    assert manager._is_running is False
    assert manager.get_langchain_tools() == []


def _status_summary(*, mcp_running: bool, mcp_tool_names: list[str]) -> dict:
    return build_agent_status_summary(
        provider="p",
        model="m",
        available_providers=[{"key": "p"}],
        effective_agent_type="openai_tools",
        configured_agent_type="openai_tools",
        legacy_react_parser_enabled=False,
        compatibility_warnings=[],
        history_recovery_warnings=[],
        initialized=True,
        runtime_backend="langgraph_agent",
        thread_checkpoint_backend="disabled",
        thread_checkpoint_storage_path=None,
        max_iterations=10,
        session_timeout_seconds=60,
        histories_loaded=True,
        history_load_failed=False,
        pending_history_recovery_thread_count=0,
        session_count=0,
        thread_count=0,
        session_metrics={},
        base_tool_names=[],
        active_tool_names=[],
        mcp_enabled=True,
        mcp_running=mcp_running,
        mcp_tool_names=mcp_tool_names,
    )


def test_status_warns_when_mcp_runs_without_tools() -> None:
    summary = _status_summary(mcp_running=True, mcp_tool_names=[])

    assert summary["status_health"] == "warning"
    assert any("MCP" in warning for warning in summary["status_warnings"])


@pytest.mark.parametrize(
    ("mcp_running", "mcp_tool_names"),
    [(True, ["browser_click"]), (False, [])],
)
def test_status_stays_ok_otherwise(mcp_running: bool, mcp_tool_names: list[str]) -> None:
    summary = _status_summary(mcp_running=mcp_running, mcp_tool_names=mcp_tool_names)

    assert summary["status_health"] == "ok"
    assert summary["status_warnings"] == []
