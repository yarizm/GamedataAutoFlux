"""Application Container —— 全局服务单例的唯一归属地。

FastAPI（src.web.app）、Agent 工具、Worker 等共用这一套 getter；
业务代码从本模块获取组件，**禁止 import `src.web`**（Web 层是装配的
消费者，不是提供者）。构造时序：

- `ensure_core_services()` 创建 scheduler / report_generator（进程内首个调用方负责，通常在 app lifespan）；
- DB session factory 就绪后由 lifespan 调 `scheduler.attach_persistence(...)` 挂仓储；
- Agent 会话服务在 DB 初始化后通过 `set_agent_session_service(...)` 注入。
"""

from __future__ import annotations

import threading

from loguru import logger

from src.core.app_settings import get_app_settings

from src.core.scheduler import Scheduler
from src.reporting.generator import ReportGenerator

# 核心服务（lifespan / 首个调用方经 ensure_core_services 创建）
scheduler: Scheduler | None = None
report_generator: ReportGenerator | None = None

# Agent 服务单例（延迟初始化；session 服务在 lifespan 中注入）
_agent_service = None
_agent_session_service = None
_agent_service_lock = threading.Lock()

# Service layer 单例（lazy init）
_task_service = None
_task_service_lock = threading.Lock()
_worker_registry = None
_worker_registry_lock = threading.Lock()
_session_registry = None
_session_registry_lock = threading.Lock()
_dag_repo = None
_dag_repo_lock = threading.Lock()
_pipeline_service = None
_pipeline_service_lock = threading.Lock()
_cron_service = None
_cron_service_lock = threading.Lock()
_worker_service = None
_worker_service_lock = threading.Lock()
_event_bus = None
_event_bus_lock = threading.Lock()


def ensure_core_services() -> tuple[Scheduler, ReportGenerator]:
    """创建（如缺失）并返回 scheduler 与 report_generator。"""
    global scheduler, report_generator
    if scheduler is None:
        scheduler = Scheduler()
    if report_generator is None:
        report_generator = ReportGenerator()
    return scheduler, report_generator


def _reset_runtime_singletons(
    *,
    reset_agent: bool = False,
    reset_agent_session: bool = False,
) -> None:
    global _task_service, _worker_registry, _session_registry, _agent_service
    global _agent_session_service, _dag_repo, _pipeline_service, _cron_service, _worker_service

    _task_service = None
    _worker_registry = None
    _session_registry = None
    _dag_repo = None
    _pipeline_service = None
    _cron_service = None
    _worker_service = None
    global _event_bus
    _event_bus = None
    if reset_agent:
        _agent_service = None
    if reset_agent_session:
        _agent_session_service = None


def set_agent_session_service(session_service) -> None:
    """lifespan 在 DB 初始化后注入 Agent 会话持久化服务。"""
    global _agent_session_service
    _agent_session_service = session_service


def get_agent_service():
    """获取 Agent 服务实例，未启用时返回 None"""
    global _agent_service
    if not get_app_settings().agent.enabled:
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


def get_task_service():
    global _task_service
    if _task_service is None:
        with _task_service_lock:
            if _task_service is None:
                from src.services.task_service import TaskService

                _task_service = TaskService(
                    scheduler=scheduler,
                    get_session_registry=lambda: get_session_registry(),
                )
    return _task_service


def get_worker_registry():
    global _worker_registry
    if _worker_registry is None:
        with _worker_registry_lock:
            if _worker_registry is None:
                task_store = scheduler.task_store if scheduler is not None else None
                if task_store is not None:
                    from src.services.worker_registry import StorageWorkerRegistry

                    _worker_registry = StorageWorkerRegistry(task_store)
                else:
                    from src.services.worker_registry import InMemoryWorkerRegistry

                    _worker_registry = InMemoryWorkerRegistry()
    return _worker_registry


def get_session_registry():
    global _session_registry
    if _session_registry is None:
        with _session_registry_lock:
            if _session_registry is None:
                task_store = scheduler.task_store if scheduler is not None else None
                if task_store is not None:
                    from src.services.session_registry import StorageSessionRegistry

                    _session_registry = StorageSessionRegistry(task_store)
                else:
                    from src.services.session_registry import InMemorySessionRegistry

                    _session_registry = InMemorySessionRegistry()
    return _session_registry


def get_dag_repository():
    """获取 DAG 仓储单例（lazy init，复用共享 session factory）。"""
    global _dag_repo
    if _dag_repo is None:
        with _dag_repo_lock:
            if _dag_repo is None:
                from src.services.sqlalchemy_dag_repository import SQLAlchemyDAGRepository
                from src.storage.session_factory import get_session_factory

                _dag_repo = SQLAlchemyDAGRepository(get_session_factory())
    return _dag_repo


def get_pipeline_service():
    """Pipeline 业务服务（注册表所有者，由 Scheduler 组合持有）。"""
    if scheduler is None:
        raise RuntimeError("core services not initialized; call ensure_core_services()")
    return scheduler.pipeline_service


def get_cron_service():
    """Cron 业务门面。"""
    global _cron_service
    if _cron_service is None:
        with _cron_service_lock:
            if _cron_service is None:
                from src.services.cron_service import CronService

                if scheduler is None:
                    raise RuntimeError("core services not initialized; call ensure_core_services()")
                _cron_service = CronService(lambda: scheduler)
    return _cron_service


def get_event_bus():
    """进程级 EventBus 单例（原 src.core.events 模块级单例收编至此）。"""
    global _event_bus
    if _event_bus is None:
        with _event_bus_lock:
            if _event_bus is None:
                from src.core.events import EventBus

                _event_bus = EventBus()
    return _event_bus


def get_worker_service():
    """Worker 业务门面。"""
    global _worker_service
    if _worker_service is None:
        with _worker_service_lock:
            if _worker_service is None:
                from src.services.worker_service import WorkerService

                if scheduler is None:
                    raise RuntimeError("core services not initialized; call ensure_core_services()")
                _worker_service = WorkerService(lambda: scheduler)
    return _worker_service
