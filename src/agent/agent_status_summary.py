"""Pure helpers for Agent status summaries and tool descriptions."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from src.agent.agent_history_state import summarize_session_metrics


def build_history_recovery_warnings(
    *,
    history_load_failed: bool,
    pending_recovery_thread_count: int,
) -> list[str]:
    warnings: list[str] = []
    if history_load_failed:
        warnings.append(
            "Persisted thread history is temporarily unavailable; history loading will be retried and new conversations are buffered in memory."
        )
    if pending_recovery_thread_count:
        warnings.append(
            f"{pending_recovery_thread_count} thread(s) are waiting to merge buffered conversation history back into persistent storage."
        )
    return warnings


def describe_tool_action(tool_name: str, args: Mapping[str, Any]) -> str:
    descriptions = {
        "precheck_report": f"预检报告数据覆盖情况，模板: {args.get('template', 'general_game')}",
        "list_reports": f"查看最近生成的报告列表，数量上限: {args.get('limit', 20)}",
        "generate_report": f"准备生成报告，分析目标: {str(args.get('prompt', ''))[:80]}",
        "get_report_content": f"获取报告 {args.get('report_id', '')} 的详细内容",
        "list_tasks": "查看当前任务列表",
        "get_task_detail": f"查看任务 {args.get('task_id', '')} 的详情",
        "create_task": f"创建采集任务: {args.get('name', '')}",
        "cancel_task": f"取消任务 {args.get('task_id', '')}",
        "list_pipeline_templates": "查看可用的 Pipeline 模板",
        "list_pipelines": "查看已创建的 Pipeline",
        "create_pipeline": f"创建 Pipeline: {args.get('name', '')}",
        "create_dynamic_pipeline": (
            f"为 {args.get('url', '')} 创建动态 Pipeline: "
            f"{args.get('pipeline_name', '')}"
        ),
        "delete_pipeline": f"删除 Pipeline: {args.get('name', '')}",
        "list_cron_jobs": "查看定时任务列表",
        "create_cron_job": f"创建定时任务: {args.get('name', '')}",
        "delete_cron_job": f"删除定时任务: {args.get('name', '')}",
        "list_data_games": "浏览已采集的游戏数据",
        "search_data": f"搜索数据: {args.get('query', '')}",
        "get_system_stats": "查看系统运行状态",
        "get_agent_status": "查看 AI Agent 当前模型、工具和会话状态",
        "resolve_steam_app_id": f"搜索 Steam App ID: {args.get('game_name', '')}",
        "verify_steam_app_id": f"验证 Steam App ID: {args.get('app_id', '')}",
        "search_game_identifiers": f"自动搜索游戏平台标识符: {args.get('game_name', '')}",
        "verify_game_identifier": (
            f"验证 {args.get('platform', '')} 标识符: {args.get('identifier', '')}"
        ),
        "review_collection_results": f"复查采集结果: {args.get('task_id', '')}",
        "browser_navigate": f"正在浏览器中打开 URL: {args.get('url', '')}",
        "browser_snapshot": "正在获取页面快照",
        "browser_evaluate": "正在页面中执行提取脚本",
    }
    return descriptions.get(tool_name, f"调用工具 {tool_name}")


def summarize_tool_groups(tool_names: Sequence[str]) -> dict[str, dict[str, Any]]:
    groups: dict[str, dict[str, Any]] = {}
    for tool_name in tool_names:
        group = _tool_group_name(tool_name)
        bucket = groups.setdefault(group, {"count": 0, "tools": []})
        bucket["count"] += 1
        bucket["tools"].append(tool_name)
    return dict(sorted(groups.items()))


def build_agent_status_summary(
    *,
    provider: str,
    model: str,
    available_providers: Sequence[Mapping[str, Any]],
    effective_agent_type: str,
    configured_agent_type: str,
    legacy_react_parser_enabled: bool,
    compatibility_warnings: Sequence[str],
    history_recovery_warnings: Sequence[str],
    initialized: bool,
    runtime_backend: str,
    thread_checkpoint_backend: str,
    thread_checkpoint_storage_path: str | None,
    max_iterations: int,
    session_timeout_seconds: int,
    histories_loaded: bool,
    history_load_failed: bool,
    pending_history_recovery_thread_count: int,
    session_count: int,
    thread_count: int,
    session_metrics: Mapping[str, Any],
    base_tool_names: Sequence[str],
    active_tool_names: Sequence[str],
    mcp_enabled: bool,
    mcp_running: bool,
    mcp_tool_names: Sequence[str],
) -> dict[str, Any]:
    compatibility_warnings_list = list(compatibility_warnings)
    history_recovery_warnings_list = list(history_recovery_warnings)
    status_warnings = compatibility_warnings_list + history_recovery_warnings_list
    available_provider_list = list(available_providers)
    base_tool_list = list(base_tool_names)
    active_tool_list = list(active_tool_names)
    mcp_tool_list = list(mcp_tool_names)
    base_tool_set = set(base_tool_list)
    active_tool_set = set(active_tool_list)

    return {
        "provider": provider,
        "model": model,
        "provider_available": provider in {item["key"] for item in available_provider_list},
        "available_provider_count": len(available_provider_list),
        "available_providers": available_provider_list,
        "agent_type": effective_agent_type,
        "configured_agent_type": configured_agent_type,
        "effective_agent_type": effective_agent_type,
        "legacy_react_parser_enabled": legacy_react_parser_enabled,
        "agent_type_compatibility_warnings": compatibility_warnings_list,
        "status_health": "warning" if status_warnings else "ok",
        "status_warnings": status_warnings,
        "initialized": initialized,
        "runtime_backend": runtime_backend,
        "thread_checkpoint_backend": thread_checkpoint_backend,
        "thread_checkpoint_storage_path": thread_checkpoint_storage_path,
        "thread_checkpointing_enabled": thread_checkpoint_backend != "disabled",
        "max_iterations": max_iterations,
        "session_timeout_seconds": session_timeout_seconds,
        "histories_loaded": histories_loaded,
        "history_load_failed": history_load_failed,
        "pending_history_recovery_thread_count": pending_history_recovery_thread_count,
        "history_recovery_warnings": history_recovery_warnings_list,
        "session_count": session_count,
        "thread_count": thread_count,
        **dict(session_metrics),
        "base_tool_count": len(base_tool_list),
        "active_tool_count": len(active_tool_list),
        "base_tools": base_tool_list,
        "active_tools": active_tool_list,
        "tool_groups": summarize_tool_groups(active_tool_list),
        "missing_base_tools": sorted(base_tool_set - active_tool_set),
        "extra_active_tools": sorted(active_tool_set - base_tool_set),
        "mcp_enabled": mcp_enabled,
        "mcp_running": mcp_running,
        "mcp_tool_count": len(mcp_tool_list),
        "mcp_tools": mcp_tool_list,
    }


def summarize_agent_runtime_status(
    *,
    provider: str,
    model: str,
    available_providers: Sequence[Mapping[str, Any]],
    runtime: Any,
    base_tools: Sequence[Any],
    mcp_manager: Any | None,
    histories: Mapping[str, list[Any]],
    timestamps: Mapping[str, float],
    session_timeout_seconds: int,
    max_iterations: int,
    histories_loaded: bool,
    history_load_failed: bool,
    pending_history_recovery_thread_count: int,
    mcp_enabled: bool,
    initialized: bool,
) -> dict[str, Any]:
    executor_tools = runtime.get_active_tools(list(base_tools))
    base_tool_names = [tool.name for tool in base_tools]
    active_tool_names = [tool.name for tool in executor_tools] or base_tool_names
    mcp_running = bool(mcp_manager and getattr(mcp_manager, "_is_running", False))
    mcp_tools = mcp_manager.get_langchain_tools() if mcp_running else []
    mcp_tool_names = [tool.name for tool in mcp_tools]
    history_recovery_warnings = build_history_recovery_warnings(
        history_load_failed=history_load_failed,
        pending_recovery_thread_count=pending_history_recovery_thread_count,
    )
    session_metrics = summarize_session_metrics(
        histories,
        timestamps,
        timeout_seconds=session_timeout_seconds,
    )

    return build_agent_status_summary(
        provider=provider,
        model=model,
        available_providers=available_providers,
        effective_agent_type="openai_tools",
        configured_agent_type="openai_tools",
        legacy_react_parser_enabled=False,
        compatibility_warnings=[],
        history_recovery_warnings=history_recovery_warnings,
        initialized=initialized,
        runtime_backend=getattr(runtime, "backend_name", "langgraph_agent"),
        thread_checkpoint_backend=getattr(runtime, "thread_checkpoint_backend", "disabled"),
        thread_checkpoint_storage_path=getattr(runtime, "thread_checkpoint_storage_path", None),
        max_iterations=max_iterations,
        session_timeout_seconds=session_timeout_seconds,
        histories_loaded=histories_loaded,
        history_load_failed=history_load_failed,
        pending_history_recovery_thread_count=pending_history_recovery_thread_count,
        session_count=len(histories),
        thread_count=len(histories),
        session_metrics=session_metrics,
        base_tool_names=base_tool_names,
        active_tool_names=active_tool_names,
        mcp_enabled=mcp_enabled,
        mcp_running=mcp_running,
        mcp_tool_names=mcp_tool_names,
    )


def _tool_group_name(tool_name: str) -> str:
    name = str(tool_name or "").lower()
    if "report" in name:
        return "reports"
    if "task" in name or name == "review_collection_results":
        return "tasks"
    if "pipeline" in name:
        return "pipelines"
    if "cron" in name:
        return "cron"
    if name in {"list_data_games", "search_data", "get_data_record_content"}:
        return "data"
    if "identifier" in name or "steam_app_id" in name:
        return "identifiers"
    if name.startswith("browser_"):
        return "browser"
    if name in {"get_system_stats", "get_agent_status", "launch_steamdb_browser"}:
        return "system"
    if "semantic" in name:
        return "semantic_search"
    return "other"
