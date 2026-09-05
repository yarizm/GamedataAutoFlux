"""Cross-process ownership for the mutable plugin-manager control plane."""

from __future__ import annotations

import os
import threading
from pathlib import Path
from typing import BinaryIO

LOCK_FILENAME = ".operation.lock"
_owned_paths: set[Path] = set()
_owned_guard = threading.RLock()


def _lock_handle(handle: BinaryIO) -> None:
    handle.seek(0)
    if os.name == "nt":
        import msvcrt

        msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
        return

    import fcntl

    fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)


def _unlock_handle(handle: BinaryIO) -> None:
    handle.seek(0)
    if os.name == "nt":
        import msvcrt

        msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
        return

    import fcntl

    fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


class PluginManagerProcessLock:
    """Hold exclusive mutation ownership until the operation service stops."""

    def __init__(self, manager_dir: Path) -> None:
        self.path = (manager_dir / LOCK_FILENAME).resolve()
        self._handle: BinaryIO | None = None

    def acquire(self) -> bool:
        with _owned_guard:
            if self.path in _owned_paths:
                return False
            self.path.parent.mkdir(parents=True, exist_ok=True)
            handle = self.path.open("a+b")
            if self.path.stat().st_size == 0:
                handle.write(b"0")
                handle.flush()
            try:
                _lock_handle(handle)
            except (BlockingIOError, OSError):
                handle.close()
                return False
            _owned_paths.add(self.path)
            self._handle = handle
            return True

    def release(self) -> None:
        handle = self._handle
        if handle is None:
            return
        with _owned_guard:
            try:
                _unlock_handle(handle)
            finally:
                handle.close()
                self._handle = None
                _owned_paths.discard(self.path)


def plugin_manager_lock_available(manager_dir: Path) -> bool:
    """Probe another process's lock without creating control-plane state."""

    path = (manager_dir / LOCK_FILENAME).resolve()
    with _owned_guard:
        if path in _owned_paths:
            return True
    if not path.is_file():
        return True
    try:
        handle = path.open("r+b")
    except OSError:
        return False
    try:
        _lock_handle(handle)
    except (BlockingIOError, OSError):
        return False
    else:
        _unlock_handle(handle)
        return True
    finally:
        handle.close()
