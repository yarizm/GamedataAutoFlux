from src.core.config import get_settings
from src.core.registry import registry
from src.storage.base import BaseStorage


_global_storage = None

def get_storage() -> BaseStorage:
    """根据 settings.yaml 中的 database 配置动态获取对应的存储引擎实例（单例）。"""
    global _global_storage
    if _global_storage is not None:
        return _global_storage

    settings = get_settings()
    db_config = settings.get("database", {})
    provider = db_config.get("provider", "sqlalchemy")

    store_cls = registry.get("storage", provider)
    _global_storage = store_cls(db_config)
    return _global_storage
