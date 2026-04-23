"""
全局配置管理

通过 YAML 文件加载配置，支持环境变量覆盖。
使用单例模式保证全局配置一致性。
"""

from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Any

import yaml
from loguru import logger


_ROOT_DIR = Path(__file__).resolve().parent.parent.parent
_CONFIG_DIR = _ROOT_DIR / "config"
_DEFAULT_SETTINGS_FILE = _CONFIG_DIR / "settings.yaml"

_settings: dict[str, Any] | None = None


def _resolve_env_vars(value: Any) -> Any:
    """递归解析配置值中的 ${ENV_VAR} 占位符"""
    if isinstance(value, str):
        pattern = re.compile(r"\$\{([^}]+)}")
        matches = pattern.findall(value)
        for var_name in matches:
            env_val = os.environ.get(var_name, "")
            value = value.replace(f"${{{var_name}}}", env_val)
        return value
    elif isinstance(value, dict):
        return {k: _resolve_env_vars(v) for k, v in value.items()}
    elif isinstance(value, list):
        return [_resolve_env_vars(item) for item in value]
    return value


def load_settings(config_path: str | Path | None = None) -> dict[str, Any]:
    """
    加载配置文件。

    Args:
        config_path: 配置文件路径，默认使用 config/settings.yaml

    Returns:
        解析后的配置字典
    """
    global _settings

    path = Path(config_path) if config_path else _DEFAULT_SETTINGS_FILE

    if not path.exists():
        logger.warning(f"配置文件不存在: {path}，使用默认配置")
        _settings = {}
        return _settings

    with open(path, "r", encoding="utf-8") as f:
        raw = yaml.safe_load(f) or {}

    _settings = _resolve_env_vars(raw)
    logger.info(f"配置已加载: {path}")
    return _settings


def get_settings() -> dict[str, Any]:
    """获取当前配置（如未加载则自动加载默认配置）"""
    global _settings
    if _settings is None:
        load_settings()
    return _settings


def get(key: str, default: Any = None) -> Any:
    """
    获取嵌套配置项，使用点号分隔路径。

    示例:
        get("server.port", 8000)
        get("vector_store.provider", "stub")
    """
    settings = get_settings()
    keys = key.split(".")
    value = settings
    for k in keys:
        if isinstance(value, dict):
            value = value.get(k)
        else:
            return default
        if value is None:
            return default
    return value


def get_root_dir() -> Path:
    """获取项目根目录"""
    return _ROOT_DIR


def get_data_dir() -> Path:
    """获取数据目录，自动创建"""
    data_dir = _ROOT_DIR / "data"
    data_dir.mkdir(parents=True, exist_ok=True)
    return data_dir
