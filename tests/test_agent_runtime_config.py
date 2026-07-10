import pytest

from src.agent.agent_runtime_config import (
    build_runtime_config_snapshot,
    discover_available_providers,
    resolve_active_provider,
    validate_provider_selection,
)


def test_resolve_active_provider_prefers_override_then_default() -> None:
    assert resolve_active_provider("deepseek", default_provider="qwen") == "deepseek"
    assert resolve_active_provider(None, default_provider="qwen") == "qwen"
    assert resolve_active_provider("", default_provider="") == "qwen"


def test_discover_available_providers_skips_invalid_entries() -> None:
    providers = discover_available_providers(
        {
            "provider": "qwen",
            "qwen": {"model": "qwen-max"},
            "deepseek": {"model": "deepseek-chat"},
            "broken": {"base_url": "http://localhost"},
            "other": "ignored",
        }
    )

    assert providers == [
        {"key": "qwen", "label": "Qwen", "model": "qwen-max"},
        {"key": "deepseek", "label": "Deepseek", "model": "deepseek-chat"},
    ]


def test_validate_provider_selection_rejects_unknown_provider() -> None:
    with pytest.raises(ValueError, match="Unknown provider: missing"):
        validate_provider_selection(
            "missing",
            [{"key": "qwen", "label": "Qwen", "model": "qwen-max"}],
        )


def test_build_runtime_config_snapshot_uses_current_config_values() -> None:
    config_values = {
        "agent.max_iterations": 12,
        "agent.session_timeout_minutes": 90,
        "agent.system_prompt": "custom-system",
    }

    snapshot = build_runtime_config_snapshot(
        provider_name="deepseek",
        available_providers=[
            {"key": "qwen", "label": "Qwen", "model": "qwen-max"},
            {"key": "deepseek", "label": "Deepseek", "model": "deepseek-chat"},
        ],
        config_get=lambda key, default=None: config_values.get(key, default),
        default_system_prompt_text="default-system",
    )

    assert snapshot.provider_override == "deepseek"
    assert snapshot.max_iterations == 12
    assert snapshot.session_timeout == 5400
    assert snapshot.system_prompt == "custom-system"


def test_build_runtime_config_snapshot_allows_resetting_provider_override() -> None:
    snapshot = build_runtime_config_snapshot(
        provider_name=None,
        available_providers=[{"key": "qwen", "label": "Qwen", "model": "qwen-max"}],
        config_get=lambda key, default=None: default,
        default_system_prompt_text="default-system",
    )

    assert snapshot.provider_override is None
    assert snapshot.max_iterations == 10
    assert snapshot.session_timeout == 3600
    assert snapshot.system_prompt == "default-system"
