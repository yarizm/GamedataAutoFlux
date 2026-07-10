"""Pure helpers for Agent provider selection and runtime config refresh."""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class AgentRuntimeConfigSnapshot:
    provider_override: str | None
    max_iterations: int
    session_timeout: int
    system_prompt: str


def resolve_active_provider(
    provider_override: str | None,
    *,
    default_provider: str,
) -> str:
    provider = str(provider_override or default_provider or "").strip()
    return provider or "qwen"


def discover_available_providers(llm_config: Mapping[str, Any]) -> list[dict[str, str]]:
    providers: list[dict[str, str]] = []
    for key, cfg in llm_config.items():
        if key == "provider" or not isinstance(cfg, Mapping):
            continue
        model = cfg.get("model")
        if not model:
            continue
        providers.append(
            {
                "key": str(key),
                "label": str(key).capitalize(),
                "model": str(model),
            }
        )
    return providers


def validate_provider_selection(
    provider_name: str,
    available_providers: Sequence[Mapping[str, Any]],
) -> None:
    provider_keys = {str(item.get("key", "")) for item in available_providers}
    if provider_name not in provider_keys:
        raise ValueError(f"Unknown provider: {provider_name}")


def build_runtime_config_snapshot(
    *,
    provider_name: str | None,
    available_providers: Sequence[Mapping[str, Any]],
    config_get: Callable[[str, Any], Any],
    default_system_prompt_text: str,
) -> AgentRuntimeConfigSnapshot:
    provider_override = None
    if provider_name is not None:
        validate_provider_selection(provider_name, available_providers)
        provider_override = provider_name

    return AgentRuntimeConfigSnapshot(
        provider_override=provider_override,
        max_iterations=int(config_get("agent.max_iterations", 10)),
        session_timeout=int(config_get("agent.session_timeout_minutes", 60)) * 60,
        system_prompt=str(config_get("agent.system_prompt", default_system_prompt_text)),
    )
