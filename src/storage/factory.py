import threading

from src.core.config import get_settings
from src.core.registry import registry
from src.storage.base import BaseStorage

_global_storage = None
_global_storage_lock = threading.Lock()

def get_storage(name: str | None = None) -> BaseStorage:
    """根据 settings.yaml 中的 database 配置动态获取对应的存储引擎实例（单例）。

    Args:
        name: 存储名称（当前未使用，保留用于未来多存储后端扩展）。
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
    return _global_storage
