"""Helpers for Agent runtime construction."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from langchain_core.tools import BaseTool

from src.agent.agent_prompting import build_openai_tools_system_prompt
from src.agent.runtime import BaseAgentRuntime, create_runtime


def build_agent_openai_tools_system_prompt(
    system_prompt: str,
    tools: list[BaseTool],
) -> str:
    return build_openai_tools_system_prompt(system_prompt, tools)


def build_agent_runtime(
    *,
    system_prompt: str,
    create_mcp_manager: Callable[[], Any],
) -> BaseAgentRuntime:
    return create_runtime(
        build_openai_tools_system_prompt=lambda tools: build_agent_openai_tools_system_prompt(
            system_prompt, tools
        ),
        create_mcp_manager=create_mcp_manager,
    )
