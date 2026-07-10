"""Helpers for Agent prompt/runtime construction and parser error handling."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from langchain_core.prompts import ChatPromptTemplate
from langchain_core.tools import BaseTool
from loguru import logger

from src.agent.agent_prompting import (
    build_openai_tools_prompt,
    build_openai_tools_system_prompt,
    build_react_prompt,
    next_parsing_error_response,
)
from src.agent.runtime import BaseAgentRuntime, create_runtime
from src.core.sensitive import redact_sensitive_text


def build_agent_openai_tools_system_prompt(
    system_prompt: str,
    tools: list[BaseTool],
) -> str:
    return build_openai_tools_system_prompt(system_prompt, tools)


def build_agent_openai_tools_prompt(
    system_prompt: str,
    tools: list[BaseTool],
) -> ChatPromptTemplate:
    return build_openai_tools_prompt(system_prompt, tools)


def build_agent_react_prompt(
    system_prompt: str,
    tools: list[BaseTool],
) -> ChatPromptTemplate:
    return build_react_prompt(system_prompt, tools)


def handle_agent_parsing_error(
    *,
    current_count: int,
    error: Exception,
    redact_stream_text: Callable[[str], str],
) -> tuple[int, str]:
    next_count, response = next_parsing_error_response(
        current_count,
        error,
        redact_stream_text=redact_stream_text,
    )
    logger.warning(
        "Agent JSON parsing error (count {}): {}",
        current_count + 1,
        redact_sensitive_text(str(error)),
    )
    return next_count, response


def build_agent_runtime(
    *,
    system_prompt: str,
    create_mcp_manager: Callable[[], Any],
    handle_parsing_error: Callable[[Exception], str],
) -> BaseAgentRuntime:
    return create_runtime(
        build_openai_tools_system_prompt=lambda tools: build_agent_openai_tools_system_prompt(
            system_prompt, tools
        ),
        build_openai_tools_prompt=lambda tools: build_agent_openai_tools_prompt(
            system_prompt, tools
        ),
        build_react_prompt=lambda tools: build_agent_react_prompt(system_prompt, tools),
        create_mcp_manager=create_mcp_manager,
        handle_parsing_error=handle_parsing_error,
    )
