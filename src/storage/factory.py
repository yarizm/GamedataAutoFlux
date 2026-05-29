from src.core.config import get_settings
from src.core.registry import registry
from src.storage.base import BaseStorage


def get_storage() -> BaseStorage:
    """根据 settings.yaml 中的 database 配置动态获取对应的存储引擎实例。"""
    settings = get_settings()
    db_config = settings.get("database", {})
    provider = db_config.get("provider", "sqlalchemy")

    store_cls = registry.get("storage", provider)
    return store_cls(db_config)
