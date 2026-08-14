"""Safe subprocess defaults for managed plugin operations."""

from __future__ import annotations

import subprocess
import sys
from typing import Any


def managed_subprocess_options() -> dict[str, Any]:
    """Detach managed child processes from interactive service consoles.

    On Windows, console control events such as Ctrl+C are delivered to every
    process sharing the console. Plugin builds run in background operations,
    so they must not inherit those events or an interactive stdin handle.
    """

    options: dict[str, Any] = {"stdin": subprocess.DEVNULL}
    if sys.platform == "win32":
        options["creationflags"] = subprocess.CREATE_NO_WINDOW
    return options
