from langchain_core.prompts import ChatPromptTemplate
from loguru import logger

from src.agent.agent_runtime_facade import (
    build_agent_openai_tools_prompt,
    build_agent_openai_tools_system_prompt,
    build_agent_react_prompt,
    build_agent_runtime,
    handle_agent_parsing_error,
)


def test_runtime_facade_builds_prompt_variants() -> None:
    system_prompt = "You are helpful"
    tools: list = []

    system_text = build_agent_openai_tools_system_prompt(system_prompt, tools)
    openai_prompt = build_agent_openai_tools_prompt(system_prompt, tools)
    react_prompt = build_agent_react_prompt(system_prompt, tools)

    assert isinstance(system_text, str)
    assert system_prompt in system_text
    assert isinstance(openai_prompt, ChatPromptTemplate)
    assert isinstance(react_prompt, ChatPromptTemplate)


def test_runtime_facade_handle_parsing_error_logs_and_returns_response(monkeypatch) -> None:
    captured: list[str] = []
    monkeypatch.setattr(logger, "warning", lambda message, *args: captured.append(message))

    next_count, response = handle_agent_parsing_error(
        current_count=1,
        error=ValueError("bad token=secret-token"),
        redact_stream_text=lambda text: text.replace("secret-token", "[REDACTED]"),
    )

    assert next_count >= 1
    assert isinstance(response, str)
    assert captured


def test_runtime_facade_build_runtime_returns_runtime_instance() -> None:
    runtime = build_agent_runtime(
        system_prompt="You are helpful",
        create_mcp_manager=lambda: None,
        handle_parsing_error=lambda exc: str(exc),
    )

    assert runtime is not None
    assert hasattr(runtime, "ensure_initialized")
    assert hasattr(runtime, "ensure_async")
