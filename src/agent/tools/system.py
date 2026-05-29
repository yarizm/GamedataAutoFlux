"""
系统概览工具
"""

from langchain_core.tools import BaseTool

from src.agent.tools.utils import _format_result


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
                f"启动 SteamDB 浏览器失败: {e}",
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
