"""
FastAPI 应用入口

WebUI 后端，提供任务管理、Pipeline 配置和报告生成 API。

全局服务单例归属 `src.bootstrap.container`（composition root）；
本模块通过 `__getattr__` 委托保持 `src.web.app.scheduler` 等
历史引用兼容，Web 层自身不拥有业务单例。
"""

from __future__ import annotations

import asyncio
import sys
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import Depends, FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from loguru import logger

from src.bootstrap import container
from src.bootstrap.container import (  # noqa: F401 — Web 层兼容再导出
    _reset_runtime_singletons,
    get_agent_service,
    get_dag_repository,
    get_session_registry,
    get_task_service,
    get_worker_registry,
)
from src.core.config import load_settings, get as get_config
from src.core.logging_config import configure_logging

# 可变单例（scheduler / report_generator / agent 服务）委托给 container，
# 避免 `from ... import scheduler` 在单例重建后拿到旧引用
_CONTAINER_DELEGATED_ATTRS = frozenset(
    {"scheduler", "report_generator", "_agent_service", "_agent_session_service"}
)


def __getattr__(name: str):
    if name in _CONTAINER_DELEGATED_ATTRS:
        return getattr(container, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


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
    """Load core components and validate installed plugin entry points."""
    from src.core.plugin_system import plugin_manager
    from src.plugin_manager.environment import prepare_managed_environment
    from src.plugin_manager.store import plugin_state_store

    managed_site_packages = prepare_managed_environment()
    disabled_distributions = {
        item.distribution
        for item in plugin_state_store.list_managed_plugins()
        if item.desired_state == "disabled"
    }
    statuses = plugin_manager.load_installed(
        disabled_distributions=disabled_distributions,
    )
    if managed_site_packages is not None:
        plugin_state_store.mark_runtime_reconciled(
            status.name for status in statuses if status.state == "active"
        )
        plugin_state_store.mark_generation_reconciled()


@asynccontextmanager
async def lifespan(app: FastAPI):
    """应用生命周期管理"""
    # 启动
    load_settings()
    configure_logging()
    loop_name = asyncio.get_running_loop().__class__.__name__
    logger.info(f"当前 asyncio 事件循环: {loop_name}")
    logger.info("GamedataAutoFlux 启动中...")

    scheduler, report_generator = container.ensure_core_services()
    _reset_runtime_singletons(reset_agent=True)

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

    container.set_agent_session_service(
        AgentSessionService(
            session_factory=session_factory,
            session_timeout=get_config("agent.session_timeout_minutes", 60) * 60,
            max_sessions=50,
        )
    )

    # 挂载 repositories 到 scheduler（public lifecycle API）
    from src.services.sqlalchemy_task_repository import SQLAlchemyTaskRepository
    from src.services.sqlalchemy_cron_repository import SQLAlchemyCronRepository
    from src.services.sqlalchemy_pipeline_repository import SQLAlchemyPipelineRepository

    scheduler.attach_persistence(
        task_repo=SQLAlchemyTaskRepository(session_factory),
        cron_repo=SQLAlchemyCronRepository(session_factory),
        pipeline_repo=SQLAlchemyPipelineRepository(session_factory),
    )

    # 注册事件 hooks
    from src.core.events import event_bus
    from src.core.hooks import (
        AlertHook,
        ReportGenerationHook,
        WebSocketBroadcastHook,
        WebSocketTaskEventHook,
    )
    from src.core.ws_broadcast import set_broadcaster
    from src.services.alert_service import AlertService
    from src.web.routes.ws import manager

    scheduler.attach_persistence(event_bus=event_bus)
    set_broadcaster(manager.broadcast)
    event_bus.on(
        "task_completed", ReportGenerationHook(report_generator, scheduler=scheduler).handle
    )
    event_bus.on("task_completed", AlertHook(AlertService.get_instance()).handle)
    event_bus.on("task_updated", WebSocketBroadcastHook(manager).handle)
    event_bus.on("task_event", WebSocketTaskEventHook(manager).handle)

    await scheduler.start()
    from src.plugin_manager.operations import PluginOperationService
    from src.plugin_manager.references import PluginReferenceScanner

    app.state.plugin_operations = PluginOperationService(
        reference_scanner=PluginReferenceScanner(
            scheduler=scheduler,
            dag_repository=get_dag_repository(),
        )
    )
    await app.state.plugin_operations.start()
    # 自动迁移检测：旧 pipeline 快照转 graph，只转不删
    try:
        from src.services.pipeline_dag_migration import migrate_pipelines_to_dag

        migration = await migrate_pipelines_to_dag(session_factory)
        if migration["migrated"]:
            logger.info(
                f"自动迁移 {len(migration['migrated'])} 个 pipeline 到 DAG: {migration['migrated']}"
            )
        if migration["failed"]:
            logger.warning(f"自动迁移失败的 pipeline: {migration['failed']}")
    except Exception as exc:
        logger.warning(f"自动迁移检测失败（不阻断启动）: {exc}")

    _reset_runtime_singletons(reset_agent=True)
    logger.info("GamedataAutoFlux 启动完成 ✓")

    yield

    # 关闭
    logger.info("GamedataAutoFlux 关闭中...")
    if getattr(app.state, "plugin_operations", None) is not None:
        await app.state.plugin_operations.stop()
    await scheduler.stop()

    # 注销所有 EventBus handlers 与 WS 广播端口，防止重复注册
    from src.core.events import event_bus
    from src.core.ws_broadcast import set_broadcaster as _clear_broadcaster

    event_bus.clear()
    _clear_broadcaster(None)

    agent_svc = container._agent_service
    mcp_manager = getattr(agent_svc, "_mcp_manager", None)
    if mcp_manager:
        await mcp_manager.stop()

    # 关闭全局存储并重置单例
    import src.storage.factory

    _reset_runtime_singletons(reset_agent=True, reset_agent_session=True)

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
    from src.web.routes.workers import router as workers_router
    from src.web.routes.targets import router as targets_router
    from src.web.routes.youtube_export import router as youtube_export_router
    from src.web.routes.plugin_manager import router as plugin_manager_router
    from src.web.safety import require_admin

    admin_dependencies = [Depends(require_admin)]
    app.include_router(tasks_router, prefix="/api", dependencies=admin_dependencies)
    app.include_router(pipelines_router, prefix="/api", dependencies=admin_dependencies)
    app.include_router(reports_router, prefix="/api", dependencies=admin_dependencies)
    app.include_router(data_router, prefix="/api", dependencies=admin_dependencies)
    app.include_router(ws_router, prefix="/api")
    app.include_router(agent_router, prefix="/api", dependencies=admin_dependencies)
    app.include_router(workers_router, prefix="/api", dependencies=admin_dependencies)
    app.include_router(targets_router, prefix="/api", dependencies=admin_dependencies)
    app.include_router(youtube_export_router, prefix="/api", dependencies=admin_dependencies)
    app.include_router(plugin_manager_router, prefix="/api", dependencies=admin_dependencies)
    app.include_router(health_router, prefix="/api")

    # 注册页面路由
    from src.web.routes.pages import router as pages_router

    app.include_router(pages_router)

    # 挂载静态文件
    static_dir = _WEB_DIR / "static"
    static_dir.mkdir(parents=True, exist_ok=True)
    app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")

    # YouTube export files
    tmp_dir = Path(__file__).parent.parent.parent / "tmp"
    tmp_dir.mkdir(parents=True, exist_ok=True)
    app.mount("/api/files/export", StaticFiles(directory=str(tmp_dir)), name="export_files")

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
