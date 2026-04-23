"""
日志配置加载。

将 config/logging.yaml 转换为 Loguru 配置，并允许 settings.yaml 覆盖关键项。
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

import yaml
from loguru import logger

from src.core.config import get as get_config
from src.core.config import get_root_dir


def configure_logging(config_path: str | Path | None = None) -> None:
    """加载 logging.yaml 并应用到 Loguru。"""
    root_dir = get_root_dir()
    path = Path(config_path) if config_path else root_dir / "config" / "logging.yaml"

    handlers: list[dict[str, Any]] = []
    if path.exists():
        with open(path, "r", encoding="utf-8") as f:
            raw = yaml.safe_load(f) or {}
        handlers = raw.get("handlers", [])

    if not handlers:
        handlers = [_default_stdout_handler()]

    logger.remove()
    for handler in handlers:
        logger.add(**_normalize_handler(handler, root_dir))


def _normalize_handler(handler: dict[str, Any], root_dir: Path) -> dict[str, Any]:
    normalized = dict(handler)
    sink = normalized.get("sink")

    if sink == "ext://sys.stdout":
        normalized["sink"] = sys.stdout
    elif isinstance(sink, str) and not sink.startswith("ext://"):
        sink_path = Path(sink)
        if not sink_path.is_absolute():
            sink_path = root_dir / sink_path
        sink_path.parent.mkdir(parents=True, exist_ok=True)
        normalized["sink"] = str(sink_path)

    if "level" in normalized:
        normalized["level"] = get_config("logging.level", normalized["level"])
    if "rotation" in normalized:
        normalized["rotation"] = get_config("logging.rotation", normalized["rotation"])
    if "retention" in normalized:
        normalized["retention"] = get_config("logging.retention", normalized["retention"])

    return normalized


def _default_stdout_handler() -> dict[str, Any]:
    return {
        "sink": sys.stdout,
        "level": get_config("logging.level", "INFO"),
        "format": "<green>{time:HH:mm:ss}</green> | <level>{level:<8}</level> | <cyan>{message}</cyan>",
    }
