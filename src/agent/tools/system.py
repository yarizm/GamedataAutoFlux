"""
系统概览工具
"""

from langchain_core.tools import BaseTool

from src.agent.tools.utils import _format_result, _safe_error_text


class GetSystemStatsTool(BaseTool):
    name: str = "get_system_stats"
    description: str = "获取系统概览统计信息：任务总数、运行中数量、定时任务数等"

    async def _arun(self) -> str:
        from src.web.app import scheduler

        stats = scheduler.get_stats()
        total = stats.get("total_tasks", 0)
        running = stats.get("running_tasks", 0)
        return _format_result(
            "ok",
            f"系统概览: {total} 个任务记录，{running} 个运行中",
            stats,
            record_count=total,
            suggestion="使用 list_tasks 查看任务列表，使用 list_data_games 浏览数据",
        )

    def _run(self) -> str:
        raise NotImplementedError("Use _arun")


class GetAgentStatusTool(BaseTool):
    name: str = "get_agent_status"
    description: str = "查看 AI Agent 当前运行状态，包括模型、工具数量、MCP 浏览器工具和会话数量。"

    async def _arun(self) -> str:
        from src.web.app import get_agent_service

        agent_service = get_agent_service()
        if not agent_service:
            return _format_result("error", "Agent 服务未启用")
        await agent_service.ensure_histories_loaded()
        status = agent_service.get_status_summary()
        tool_count = status.get("active_tool_count", 0)
        mcp_state = "运行中" if status.get("mcp_running") else "未运行"
        warnings = list(status.get("status_warnings", []) or [])
        result_status = "warning" if status.get("status_health") == "warning" else "ok"
        summary_suffix = f"，存在 {len(warnings)} 条告警" if warnings else ""
        return _format_result(
            result_status,
            f"Agent 当前使用 {status.get('provider')} / {status.get('model')}，可用工具 {tool_count} 个，MCP {mcp_state}{summary_suffix}",
            status,
            record_count=tool_count,
            warnings=warnings or None,
            suggestion="如需切换模型，请在 Agent 设置中选择 provider；如需网页探索，请先确保 MCP 工具运行。",
            max_data_length=8000,
        )

    def _run(self) -> str:
        raise NotImplementedError("Use _arun")


class CheckSystemReadinessTool(BaseTool):
    name: str = "check_system_readiness"
    description: str = (
        "检查系统级采集就绪状态（配置与依赖、会话敏感采集源摘要）。"
        "不会执行 deep probe / 外网探测。用户问系统能不能采、环境是否就绪时使用。"
    )

    async def _arun(self) -> str:
        from src.core.diagnostics import (
            build_config_diagnostics,
            build_session_diagnostics_overview,
        )

        try:
            config = build_config_diagnostics()
            sessions = build_session_diagnostics_overview()
            status = "ok"
            for part in (config, sessions):
                st = str(part.get("status") or "").lower()
                if st == "error":
                    status = "error"
                elif st == "warning" and status == "ok":
                    status = "warning"
            return _format_result(
                status,
                f"系统就绪检查完成，状态={status}（未执行深度探测）",
                {"config": config, "sessions": sessions},
                suggestion="如需 live 深度探测，请到系统检查页开启 Deep Probe。",
                max_data_length=8000,
            )
        except Exception as e:
            return _format_result("error", f"系统就绪检查失败: {_safe_error_text(e)}")

    def _run(self) -> str:
        raise NotImplementedError("Use _arun")


class CheckCollectorReadinessTool(BaseTool):
    name: str = "check_collector_readiness"
    description: str = (
        "检查单个采集器（如 qimai/steam/youtube）的本地会话与相关配置是否就绪。"
        "不会执行 deep probe。用户问某个数据源能不能采、是否已登录时使用。"
    )

    async def _arun(self, collector_id: str = "") -> str:
        from src.core.diagnostics import build_collector_session_diagnostics

        cid = str(collector_id or "").strip()
        if not cid:
            return _format_result("error", "collector_id 不能为空")
        try:
            payload = build_collector_session_diagnostics(cid)
            status = str(payload.get("status") or "ok")
            return _format_result(
                status if status in ("ok", "warning", "error") else "ok",
                f"采集器 {cid} 就绪检查完成，状态={status}",
                payload,
                suggestion="若状态为 error，请根据 checks 修复登录态或配置；深度探测请到系统检查页。",
                max_data_length=8000,
            )
        except Exception as e:
            return _format_result("error", f"采集器就绪检查失败: {_safe_error_text(e)}")

    def _run(self, collector_id: str = "") -> str:
        raise NotImplementedError("Use _arun")


class LaunchSteamDBBrowserTool(BaseTool):
    name: str = "launch_steamdb_browser"
    description: str = "一键启动用于 SteamDB 采集的浏览器，并开放 CDP 端口。系统检查报错找不到 SteamDB 浏览器时，可以通过此工具自动打开。"

    async def _arun(self) -> str:
        import subprocess
        import asyncio
        from src.core.config import get as get_config
        from src.core.diagnostics import build_steamdb_launch_command

        cdp_port = get_config("steam.steamdb.cdp_port", 9222)
        cmd = build_steamdb_launch_command()

        try:
            subprocess.Popen(cmd)

            success = await asyncio.to_thread(self._wait_for_cdp, cdp_port)

            if success:
                return _format_result(
                    "ok",
                    "SteamDB 浏览器启动成功，CDP 端口已就绪。",
                    {},
                    suggestion="你可以继续进行需要 SteamDB 登录态的采集任务了。",
                )
            else:
                return _format_result(
                    "warning",
                    "SteamDB 浏览器拉起命令已执行，但未能在 10 秒内检测到 CDP 端口开放，可能启动较慢或者遇到错误。请稍后再检查系统状态。",
                    {},
                )
        except Exception as e:
            return _format_result(
                "error",
                f"启动 SteamDB 浏览器失败: {_safe_error_text(e)}",
                {},
            )

    def _run(self) -> str:
        raise NotImplementedError("Use _arun")

    @staticmethod
    def _wait_for_cdp(port: int, timeout: int = 10) -> bool:
        import time
        import urllib.request

        deadline = time.time() + timeout
        url = f"http://127.0.0.1:{port}/json/version"
        while time.time() < deadline:
            try:
                with urllib.request.urlopen(url, timeout=1) as response:
                    if response.status == 200:
                        return True
            except Exception:
                time.sleep(0.5)
        return False
