"""每条第三方 import 都必须能在锁定环境里解析（包括函数内的延迟 import）。

回归：`src/processors/vectorizer.py` 用 `langchain_text_splitters`，但依赖里没声明，只是碰巧被
langchain-community 间接带入。延迟 import 不会在启动时报错，缺依赖要到运行那条路径才发现。
"""

from __future__ import annotations

import ast
import importlib.util
import sys
from collections.abc import Iterator
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
_SKIP_PARTS = {"__pycache__", "node_modules", "data"}
# 只在特定操作系统存在的标准库模块，调用处都按平台分支
_PLATFORM_ONLY = {"fcntl", "msvcrt", "winreg", "_winapi"}


def _python_files() -> Iterator[Path]:
    for directory in ("src", "plugins", "scripts"):
        for path in (ROOT / directory).rglob("*.py"):
            if not _SKIP_PARTS.intersection(path.relative_to(ROOT).parts):
                yield path


def _imported_modules(path: Path) -> Iterator[tuple[str, int]]:
    tree = ast.parse(path.read_text(encoding="utf-8"))
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                yield alias.name, node.lineno
        elif isinstance(node, ast.ImportFrom) and node.level == 0 and node.module:
            yield node.module, node.lineno


def _is_first_party(module: str) -> bool:
    return module == "src" or module.startswith(("src.", "tests.", "autoflux_plugin_"))


def _script_siblings() -> set[str]:
    # scripts/ 下的脚本可以互相 import 同目录模块
    return {path.stem for path in (ROOT / "scripts").glob("*.py")}


def test_third_party_imports_resolve() -> None:
    siblings = _script_siblings()
    missing: dict[str, str] = {}
    checked: dict[str, bool] = {}
    for path in _python_files():
        for module, lineno in _imported_modules(path):
            top_level = module.split(".", 1)[0]
            if _is_first_party(module) or top_level in sys.stdlib_module_names:
                continue
            if top_level in _PLATFORM_ONLY or top_level in siblings:
                continue
            if top_level not in checked:
                checked[top_level] = importlib.util.find_spec(top_level) is not None
            if not checked[top_level]:
                relative = path.relative_to(ROOT).as_posix()
                missing.setdefault(top_level, f"{relative}:{lineno}")

    assert not missing, f"依赖未声明或未安装: {missing}"
