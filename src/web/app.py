"""
FastAPI 应用入口

WebUI 后端，提供任务管理、Pipeline 配置和报告生成 API。
"""

from __future__ import annotations

import asyncio
import importlib
import pkgutil
import sys
import threading
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import Depends, FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from loguru import logger

from src.core.config import load_settings, get as get_config
from src.core.logging_config import configure_logging
from src.core.scheduler import Scheduler
from src.reporting.generator import ReportGenerator


# 全局调度器实例
scheduler = None
report_generator = None

# Agent 服务单例（延迟初始化）
_agent_service = None
_agent_session_service = None  # 在 lifespan 中初始化


_agent_service_lock = threading.Lock()

def get_agent_service():
    """获取 Agent 服务实例，未启用时返回 None"""
    global _agent_service
    if not get_config("agent.enabled", True):
        return None
    
    if _agent_service is None:
        with _agent_service_lock:
            if _agent_service is None:
                if _agent_session_service is None:
                    logger.debug("Agent 会话服务尚未初始化，跳过")
                    return None
                try:
                    from src.agent.agent import AgentService
                    _agent_service = AgentService(session_service=_agent_session_service)
                    logger.info("Agent 服务已初始化")
                except Exception as e:
                    logger.warning(f"Agent 服务初始化失败: {e}")
                    return None
    return _agent_service


# Service layer singletons (lazy init)
_task_service = None
_task_service_lock = threading.Lock()


def get_task_service():
    global _task_service
    if _task_service is None:
        with _task_service_lock:
            if _task_service is None:
                from src.services.task_service import TaskService
                _task_service = TaskService(scheduler=scheduler)
    return _task_service


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

    global scheduler, report_generator
    if scheduler is None:
        scheduler = Scheduler()
    if report_generator is None:
        report_generator = ReportGenerator()

    # 发现并注册组件（需要在任何 get_storage() 等工厂函数调用前执行）
    _auto_discover_plugins()

    # 初始化共享 DB session factory
    from src.storage.session_factory import init_shared_session_factory

    session_factory = await init_shared_session_factory()

    # 初始化全局存储
    from src.storage.factory import get_storage
    
    app.state.storage = get_storage()
    await app.state.storage.initialize()

    # 创建 Agent 会话持久化服务
    from src.services.agent_session_service import AgentSessionService

    global _agent_session_service
    _agent_session_service = AgentSessionService(
        session_factory=session_factory,
        session_timeout=get_config("agent.session_timeout_minutes", 60) * 60,
        max_sessions=50,
    )


    # 注入 repositories 到 scheduler
    from src.services.sqlalchemy_task_repository import SQLAlchemyTaskRepository
    from src.services.sqlalchemy_cron_repository import SQLAlchemyCronRepository
    from src.services.sqlalchemy_pipeline_repository import SQLAlchemyPipelineRepository

    scheduler._task_repo = SQLAlchemyTaskRepository(session_factory)
    scheduler._cron_repo = SQLAlchemyCronRepository(session_factory)
    scheduler._pipeline_repo = SQLAlchemyPipelineRepository(session_factory)

    # 注册事件 hooks
    from src.core.events import event_bus
    from src.core.hooks import ReportGenerationHook, AlertHook, WebSocketBroadcastHook
    from src.services.alert_service import AlertService
    from src.web.routes.ws import manager

    scheduler._event_bus = event_bus
    event_bus.on("task_completed", ReportGenerationHook(report_generator).handle)
    event_bus.on("task_completed", AlertHook(AlertService.get_instance()).handle)
    event_bus.on("task_updated", WebSocketBroadcastHook(manager).handle)

    await scheduler.start()
    logger.info("GamedataAutoFlux 启动完成 ✓")

    yield

    # 关闭
    logger.info("GamedataAutoFlux 关闭中...")
    await scheduler.stop()

    # 注销所有 EventBus handlers，防止重复注册
    from src.core.events import event_bus

    event_bus.clear()

    agent_svc = get_agent_service()
    if agent_svc and agent_svc._mcp_manager:
        await agent_svc._mcp_manager.stop()

    # 关闭全局存储并重置单例
    import src.storage.factory

    if hasattr(app.state, "storage") and app.state.storage:
        await app.state.storage.close()
    src.storage.factory._global_storage = None

    # 关闭共享 DB session factory
    from src.storage.session_factory import close_shared_session_factory

    await close_shared_session_factory()

    logger.info("GamedataAutoFlux 已关闭")


def create_app() -> FastAPI:
    """创建 FastAPI 应用"""
    app = FastAPI(
        title="GamedataAutoFlux",
        description="游戏行业数据监控与分析工作流",
        version="0.1.0",
        lifespan=lifespan,
    )

    from fastapi.middleware.cors import CORSMiddleware
    
    cors_origins = get_config(
        "server.cors_origins", ["http://localhost:8000", "http://127.0.0.1:8000"]
    )
    if isinstance(cors_origins, str):
        cors_origins = [cors_origins]
        
    app.add_middleware(
        CORSMiddleware,
        allow_origins=cors_origins,
        allow_credentials=True if cors_origins != ["*"] else False,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # 注册路由
    from src.web.routes.tasks import router as tasks_router
    from src.web.routes.pipelines import router as pipelines_router
    from src.web.routes.reports import router as reports_router
    from src.web.routes.data import router as data_router
    from src.web.routes.ws import router as ws_router
    from src.web.routes.agent import router as agent_router
    from src.web.routes.health import router as health_router
    from src.web.safety import require_admin

    admin_dependencies = [Depends(require_admin)]
    app.include_router(tasks_router, prefix="/api", dependencies=admin_dependencies)
    app.include_router(pipelines_router, prefix="/api", dependencies=admin_dependencies)
    app.include_router(reports_router, prefix="/api", dependencies=admin_dependencies)
    app.include_router(data_router, prefix="/api", dependencies=admin_dependencies)
    app.include_router(ws_router, prefix="/api")
    app.include_router(agent_router, prefix="/api", dependencies=admin_dependencies)
    app.include_router(health_router, prefix="/api")

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

    # Windows 下必须在 uvicorn.run 之前设置 ProactorEventLoop，
    # 否则 Playwright MCP 子进程的 subprocess_exec 会抛 NotImplementedError。
    _configure_windows_event_loop_policy()

    host = get_config("server.host", "127.0.0.1")
    port = get_config("server.port", 8000)
    debug = get_config("app.debug", False)

    uvicorn.run(
        "src.web.app:app",
        host=host,
        port=port,
        reload=debug,
        log_level="info",
        loop="asyncio",
    )


if __name__ == "__main__":
    main()
