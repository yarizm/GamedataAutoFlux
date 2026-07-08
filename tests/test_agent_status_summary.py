from src.agent.agent_status_summary import (
    build_agent_status_summary,
    build_history_recovery_warnings,
    describe_tool_action,
    summarize_tool_groups,
)


def test_build_history_recovery_warnings_reports_failure_and_pending_threads() -> None:
    warnings = build_history_recovery_warnings(
        history_load_failed=True,
        pending_recovery_thread_count=2,
    )

    assert len(warnings) == 2
    assert "temporarily unavailable" in warnings[0]
    assert "2 thread(s)" in warnings[1]


def test_describe_tool_action_returns_human_readable_description() -> None:
    description = describe_tool_action(
        "create_dynamic_pipeline",
        {
            "url": "https://example.com/game/cs2",
            "pipeline_name": "example_cs2",
        },
    )

    assert "https://example.com/game/cs2" in description
    assert "example_cs2" in description


def test_summarize_tool_groups_buckets_tools_by_domain() -> None:
    summary = summarize_tool_groups(
        ["list_tasks", "browser_snapshot", "custom_debug_tool", "get_agent_status"]
    )

    assert summary["tasks"]["tools"] == ["list_tasks"]
    assert summary["browser"]["tools"] == ["browser_snapshot"]
    assert summary["system"]["tools"] == ["get_agent_status"]
    assert summary["other"]["tools"] == ["custom_debug_tool"]


def test_build_agent_status_summary_preserves_existing_contract() -> None:
    summary = build_agent_status_summary(
        provider="qwen",
        model="qwen-max",
        available_providers=[{"key": "qwen", "label": "Qwen", "model": "qwen-max"}],
        effective_agent_type="openai_tools",
        configured_agent_type="react",
        legacy_react_parser_enabled=False,
        compatibility_warnings=["react ignored"],
        history_recovery_warnings=["pending recovery"],
        initialized=True,
        runtime_backend="langgraph_agent",
        thread_checkpoint_backend="file",
        thread_checkpoint_storage_path="data/checkpoints.json",
        max_iterations=10,
        session_timeout_seconds=3600,
        histories_loaded=True,
        history_load_failed=False,
        pending_history_recovery_thread_count=1,
        session_count=2,
        thread_count=2,
        session_metrics={"history_message_count": 4},
        base_tool_names=["list_tasks", "get_agent_status"],
        active_tool_names=["list_tasks", "custom_debug_tool"],
        mcp_enabled=True,
        mcp_running=False,
        mcp_tool_names=["browser_snapshot"],
    )

    assert summary["provider_available"] is True
    assert summary["available_provider_count"] == 1
    assert summary["status_health"] == "warning"
    assert summary["status_warnings"] == ["react ignored", "pending recovery"]
    assert summary["thread_checkpointing_enabled"] is True
    assert summary["history_message_count"] == 4
    assert summary["missing_base_tools"] == ["get_agent_status"]
    assert summary["extra_active_tools"] == ["custom_debug_tool"]
    assert summary["mcp_tool_count"] == 1
