from src.agent.agent_runtime_facade import (
    build_agent_openai_tools_system_prompt,
    build_agent_runtime,
)
from src.agent.runtime import LangGraphAgentRuntime, recursion_limit_from_max_iterations


def test_runtime_facade_builds_system_prompt() -> None:
    system_prompt = "You are helpful"
    tools: list = []

    system_text = build_agent_openai_tools_system_prompt(system_prompt, tools)

    assert isinstance(system_text, str)
    assert system_prompt in system_text


def test_runtime_facade_build_runtime_returns_langgraph_runtime() -> None:
    runtime = build_agent_runtime(
        system_prompt="You are helpful",
        create_mcp_manager=lambda: None,
    )

    assert isinstance(runtime, LangGraphAgentRuntime)
    assert runtime.backend_name == "langgraph_agent"
    assert runtime.input_mode == "messages_graph"
    assert hasattr(runtime, "ensure_initialized")
    assert hasattr(runtime, "ensure_async")


def test_recursion_limit_from_max_iterations() -> None:
    assert recursion_limit_from_max_iterations(10) == 25
    assert recursion_limit_from_max_iterations(1) == 10
    assert recursion_limit_from_max_iterations(40) == 85
