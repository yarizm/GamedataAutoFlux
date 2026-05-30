"""
全局配置管理

通过 YAML 文件加载配置，支持环境变量覆盖。
使用单例模式保证全局配置一致性。
"""

from __future__ import annotations

import io
import os
import re
from pathlib import Path
from typing import Any

import yaml
from loguru import logger


# 项目启动时自动加载 .env 文件
def _load_dotenv() -> None:
    """尝试加载项目根目录的 .env 文件到环境变量"""
    try:
        from dotenv import load_dotenv as _load

        env_file = Path(__file__).resolve().parent.parent.parent / ".env"
        if env_file.exists():
            _load(env_file)
            logger.info(f".env 文件已加载: {env_file}")
    except Exception:
        pass


_load_dotenv()


_ROOT_DIR = Path(__file__).resolve().parent.parent.parent
_CONFIG_DIR = _ROOT_DIR / "config"
_DEFAULT_SETTINGS_FILE = _CONFIG_DIR / "settings.yaml"

_settings: dict[str, Any] | None = None
_settings_validation: dict[str, Any] | None = None


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
    global _settings, _settings_validation

    path = Path(config_path) if config_path else _DEFAULT_SETTINGS_FILE

    if not path.exists():
        logger.warning(f"配置文件不存在: {path}，使用默认配置")
        _settings = {}
        _settings_validation = _validate_settings(_settings)
        return _settings

    try:
        with open(path, "r", encoding="utf-8") as f:
            raw = yaml.safe_load(f) or {}
    except yaml.YAMLError as exc:
        logger.error(f"配置文件解析失败: {exc}")
        raw = {}

    _settings = _resolve_env_vars(raw)
    _settings_validation = _validate_settings(_settings)
    if not _settings_validation["valid"]:
        issue_text = "; ".join(
            f"{issue['path']}: {issue['message']}" for issue in _settings_validation["issues"]
        )
        logger.warning(f"settings.yaml validation warnings: {issue_text}")
    logger.info(f"配置已加载: {path}")
    return _settings


def get_settings() -> dict[str, Any]:
    """获取当前配置（如未加载则自动加载默认配置）"""
    global _settings
    if _settings is None:
        load_settings()
    return _settings


def get_settings_validation() -> dict[str, Any]:
    """Return the latest settings.yaml validation summary."""
    global _settings_validation
    if _settings_validation is None:
        load_settings()
    return _settings_validation or {"valid": True, "issues": [], "normalized": {}}


def _validate_settings(settings: dict[str, Any]) -> dict[str, Any]:
    try:
        from src.core.config_schema import validate_settings_payload
    except ImportError as exc:
        return {
            "valid": False,
            "issues": [
                {
                    "path": "config_schema",
                    "message": f"Unable to import settings schema: {exc}",
                    "type": "schema_import_error",
                }
            ],
            "normalized": {},
        }
    return validate_settings_payload(settings)


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
    
    if value is not None and default is not None:
        if isinstance(default, bool) and not isinstance(value, bool):
            if isinstance(value, str):
                value = value.lower() in ("true", "1", "yes", "on")
            elif isinstance(value, int):
                value = value != 0
            else:
                value = bool(value)
        elif isinstance(default, int) and not isinstance(value, int):
            try:
                value = int(value)
            except (ValueError, TypeError):
                pass
        elif isinstance(default, float) and not isinstance(value, float):
            try:
                value = float(value)
            except (ValueError, TypeError):
                pass

    return value


def get_root_dir() -> Path:
    """获取项目根目录"""
    return _ROOT_DIR


def get_data_dir() -> Path:
    """获取数据目录，自动创建"""
    data_dir = _ROOT_DIR / "data"
    data_dir.mkdir(parents=True, exist_ok=True)
    return data_dir


def get_raw_section(key: str) -> dict[str, Any]:
    """读取原始配置 section（不解析 ${ENV_VAR}），返回原始占位符文本"""
    path = _DEFAULT_SETTINGS_FILE
    if not path.exists():
        return {}

    with open(path, "r", encoding="utf-8") as f:
        raw = yaml.safe_load(f) or {}

    value = raw.get(key)
    return value if isinstance(value, dict) else {}


def save_section(key: str, value: Any, config_path: str | Path | None = None) -> None:
    """将某个顶级 section 写回 settings.yaml，保留其他 section 和注释。

    通过替换 YAML 文本中对应 section 来实现，尽可能保留原始格式。
    """
    global _settings, _settings_validation

    path = Path(config_path) if config_path else _DEFAULT_SETTINGS_FILE

    if not path.exists():
        logger.warning(f"配置文件不存在: {path}，无法保存")
        return

    original = path.read_text(encoding="utf-8")

    buf = io.StringIO()
    yaml.dump({key: value}, buf, default_flow_style=False, allow_unicode=True, sort_keys=False)
    dumped = buf.getvalue()

    # 去掉顶层 key 那一行（由 yaml.dump 生成），剩余行作为 section body
    dumped_lines = dumped.splitlines(True)
    if dumped_lines and dumped_lines[0].startswith(key + ":"):
        dumped_body = "".join(dumped_lines[1:])
    else:
        dumped_body = dumped

    new_content = _replace_top_level_section(original, key, dumped_body)

    if new_content != original:
        path.write_text(new_content, encoding="utf-8")
        # 刷新内存缓存，下次 get 时重新加载
        _settings = None
        _settings_validation = None
        logger.info(f"配置 section '{key}' 已保存到 {path}")
    else:
        logger.warning(f"未找到 section '{key}' 或内容未变化，跳过保存")


def _replace_top_level_section(original: str, key: str, dumped_body: str) -> str:
    """替换顶层 YAML section，允许 section 内包含空行或 block scalar。"""
    lines = original.splitlines(keepends=True)
    start = None
    for index, line in enumerate(lines):
        if re.match(rf"^{re.escape(key)}\s*:", line):
            start = index
            break

    replacement = [f"{key}:\n", dumped_body]
    if start is None:
        suffix = "" if original.endswith(("\n", "\r\n")) or not original else "\n"
        return original + suffix + "".join(replacement)

    end = len(lines)
    for index in range(start + 1, len(lines)):
        line = lines[index]
        if line.strip() and not line.startswith((" ", "\t", "#")):
            end = index
            break

    return "".join(lines[:start] + replacement + lines[end:])
