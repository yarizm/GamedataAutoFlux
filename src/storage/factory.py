"""
存储工厂

根据 settings.yaml 中的 database.provider 获取对应的存储引擎单例。

当前架构使用单一数据库后端，所有调用共享同一个存储实例。
``name`` 参数保留用于未来多存储后端扩展（如独立的 vector store）。
"""

import threading

from loguru import logger

from src.core.config import get_settings
from src.core.registry import registry
from src.storage.base import BaseStorage

_global_storage: BaseStorage | None = None
_global_storage_lock = threading.Lock()


def get_storage(name: str | None = None) -> BaseStorage:
    """获取全局共享的存储引擎实例（单例）。

    所有调用方（路由、Agent 工具、Pipeline）共享同一个存储实例，
    底层连接池由 SQLAlchemy engine 统一管理。

    Args:
        name: 存储名称。当前架构下仅用于日志追踪，不影响返回的实例。
              未来可用于区分不同的存储后端（如 "vector"、"cache" 等）。

    Returns:
        全局共享的 BaseStorage 实例
    """
    global _global_storage
    if _global_storage is not None:
        return _global_storage

    with _global_storage_lock:
        if _global_storage is not None:
            return _global_storage

        settings = get_settings()
        db_config = settings.get("database", {})
        provider = db_config.get("provider", "sqlalchemy")

        store_cls = registry.get("storage", provider)
        _global_storage = store_cls(db_config)
        if name:
            logger.debug(
                f"get_storage(name={name!r}) → 返回全局默认存储 ({provider})，"
                f"当前架构共享同一实例"
            )
    return _global_storage


def reset_storage() -> None:
    """重置全局存储单例（仅测试用）。"""
    global _global_storage
    with _global_storage_lock:
        _global_storage = None
