"""
FastAPI 应用入口

WebUI 后端，提供任务管理、Pipeline 配置和报告生成 API。
"""

from __future__ import annotations

import asyncio
import importlib
import pkgutil
import sys
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from loguru import logger

from src.core.config import load_settings, get as get_config
from src.core.logging_config import configure_logging
from src.core.scheduler import Scheduler
from src.reporting.generator import ReportGenerator


# 全局调度器实例
scheduler = Scheduler()
report_generator = ReportGenerator()

# 模板引擎
_WEB_DIR = Path(__file__).parent
templates = Jinja2Templates(directory=str(_WEB_DIR / "templates"))


def _configure_windows_event_loop_policy() -> None:
    """在 Windows 下尽早切到 Proactor loop，兼容 Playwright 子进程。"""
    if sys.platform != "win32":
        return
    if not hasattr(asyncio, "WindowsProactorEventLoopPolicy"):
        return

    current_policy = asyncio.get_event_loop_policy()
    if current_policy.__class__.__name__ != "WindowsProactorEventLoopPolicy":
        asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())


_configure_windows_event_loop_policy()


def _auto_discover_plugins():
    """自动发现并导入所有已注册的插件（采集器、处理器、存储）"""
    plugin_packages = ["src.collectors", "src.processors", "src.storage"]
    for package_name in plugin_packages:
        try:
            package = importlib.import_module(package_name)
            package_path = Path(package.__file__).parent
            for _, module_name, _ in pkgutil.iter_modules([str(package_path)]):
                if module_name == "base":
                    continue
                full_name = f"{package_name}.{module_name}"
                try:
                    importlib.import_module(full_name)
                    logger.debug(f"插件已加载: {full_name}")
                except Exception as e:
                    logger.warning(f"插件加载失败: {full_name} - {e}")
        except Exception as e:
            logger.warning(f"包扫描失败: {package_name} - {e}")


@asynccontextmanager
async def lifespan(app: FastAPI):
    """应用生命周期管理"""
    # 启动
    load_settings()
    configure_logging()
    loop_name = asyncio.get_running_loop().__class__.__name__
    logger.info(f"当前 asyncio 事件循环: {loop_name}")
    logger.info("GamedataAutoFlux 启动中...")
    _auto_discover_plugins()
    await scheduler.start()
    logger.info("GamedataAutoFlux 启动完成 ✓")

    yield

    # 关闭
    logger.info("GamedataAutoFlux 关闭中...")
    await scheduler.stop()
    logger.info("GamedataAutoFlux 已关闭")


def create_app() -> FastAPI:
    """创建 FastAPI 应用"""
    app = FastAPI(
        title="GamedataAutoFlux",
        description="游戏行业数据监控与分析工作流",
        version="0.1.0",
        lifespan=lifespan,
    )

    # 注册路由
    from src.web.routes.tasks import router as tasks_router
    from src.web.routes.pipelines import router as pipelines_router
    from src.web.routes.reports import router as reports_router

    app.include_router(tasks_router, prefix="/api")
    app.include_router(pipelines_router, prefix="/api")
    app.include_router(reports_router, prefix="/api")

    # 注册页面路由
    from src.web.routes.pages import router as pages_router
    app.include_router(pages_router)

    # 挂载静态文件
    static_dir = _WEB_DIR / "static"
    static_dir.mkdir(parents=True, exist_ok=True)
    app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")

    return app


# 应用实例
app = create_app()


def main():
    """命令行入口"""
    import uvicorn

    host = get_config("server.host", "0.0.0.0")
    port = get_config("server.port", 8000)
    debug = get_config("app.debug", False)

    uvicorn.run(
        "src.web.app:app",
        host=host,
        port=port,
        reload=debug,
        log_level="info",
    )


if __name__ == "__main__":
    main()
