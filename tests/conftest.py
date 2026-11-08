"""Shared test fixtures for GamedataAutoFlux."""

import os
import sys
import pytest
from datetime import datetime
from pathlib import Path


# The repository test suite exercises every first-party plugin without
# installing them into the active interpreter. Production discovery still uses
# package entry points; this is the explicit development-module escape hatch.
_PROJECT_ROOT = Path(__file__).resolve().parents[1]
_PLUGIN_NAMES = (
    "smart_web",
    "steam",
    "taptap",
    "gtrends",
    "qimai",
    "youtube",
    "official_site",
    "monitor",
    "dynamic_playwright",
)
for _plugin_name in reversed(_PLUGIN_NAMES):
    sys.path.insert(0, str(_PROJECT_ROOT / "plugins" / _plugin_name / "src"))

os.environ.setdefault(
    "AUTOFLUX_PLUGIN_MODULES",
    ",".join(f"autoflux_plugin_{name}" for name in _PLUGIN_NAMES if name != "smart_web"),
)

from src.core.task import Task, TaskTarget  # noqa: E402
from src.core.pipeline import Pipeline  # noqa: E402
from src.storage.base import StorageRecord  # noqa: E402


def _use_real_loopback_test_client_defaults() -> None:
    """TestClient 默认 Host ``testserver`` / 对端 ``testclient`` 不是真实本机值。

    生产鉴权不认这两个名字（本机免 key 只认 loopback 地址），所以测试默认改用真实本机值；
    要模拟远程客户端时照常显式传 ``client=``/``base_url=``。``websocket_connect`` 原本把相对路径
    固定拼到 ``ws://testserver``，这里改成跟 HTTP 请求一样基于 ``base_url``（浏览器也是连页面所在主机）。
    """
    from urllib.parse import urljoin

    from starlette.testclient import TestClient

    if getattr(TestClient, "_autoflux_loopback_defaults", False):
        return
    original_init = TestClient.__init__
    original_websocket_connect = TestClient.websocket_connect

    def __init__(self, *args, **kwargs):
        kwargs.setdefault("base_url", "http://127.0.0.1")
        kwargs.setdefault("client", ("127.0.0.1", 50000))
        original_init(self, *args, **kwargs)

    def websocket_connect(self, url, *args, **kwargs):
        if "://" not in url:
            url = urljoin(str(self.base_url).replace("http", "ws", 1), url)
        return original_websocket_connect(self, url, *args, **kwargs)

    TestClient.__init__ = __init__
    TestClient.websocket_connect = websocket_connect
    TestClient._autoflux_loopback_defaults = True


_use_real_loopback_test_client_defaults()


@pytest.fixture(autouse=True)
def isolated_auth_state(tmp_path, monkeypatch):
    """会话密钥/吊销表写到临时目录，key 失败限流计数每个测试从零开始。"""
    monkeypatch.setenv("AUTOFLUX_AUTH_DIR", str(tmp_path / "auth"))

    import src.web.safety as safety

    monkeypatch.setattr(safety, "_key_failures", safety.KeyFailureTracker())
    yield


@pytest.fixture(autouse=True)
def isolated_db_config(tmp_path, monkeypatch):
    """Ensure all tests use an isolated file database by default."""
    db_path = tmp_path / "test_autoflux.db"
    test_url = f"sqlite+aiosqlite:///{db_path.as_posix()}"
    monkeypatch.setenv("DATABASE_URL", test_url)
    monkeypatch.setenv("AUTOFLUX_PLUGIN_MANAGER_DIR", str(tmp_path / "plugin-manager"))

    from src.core.config import load_settings

    load_settings()

    from src.web.app import _auto_discover_plugins

    _auto_discover_plugins()

    import src.storage.factory

    src.storage.factory._global_storage = None

    import src.storage.session_factory

    src.storage.session_factory._engine = None
    src.storage.session_factory._session_factory = None

    yield


@pytest.fixture
def sample_record_steam():
    return StorageRecord(
        key="test:steam:0",
        data={
            "game_name": "Counter-Strike 2",
            "app_id": "730",
            "snapshot": {
                "current_players": 1000000,
                "total_reviews": 500000,
                "review_score": 88,
                "price": "Free",
            },
            "collector": "steam",
        },
        metadata={
            "collector": "steam",
            "source_task": {
                "pipeline_name": "steam_basic",
                "collector_name": "steam",
                "task_id": "task001",
                "task_name": "CS2 test",
                "target_params": {"app_id": "730"},
                "task_config": {},
            },
        },
        source="steam",
        stored_at=datetime(2025, 1, 15, 10, 0, 0),
        tags=["steam", "test"],
    )


@pytest.fixture
def sample_record_taptap():
    return StorageRecord(
        key="test:taptap:0",
        data={
            "game_name": "原神",
            "app_id": "100",
            "reviews_summary": {"total": 50000, "ratings_count": 12000},
            "collector": "taptap",
        },
        metadata={"collector": "taptap"},
        source="taptap",
        stored_at=datetime(2025, 1, 15, 11, 0, 0),
    )


@pytest.fixture
def sample_record_partial():
    return StorageRecord(
        key="test:partial:0",
        data={"game_name": "Partial Game", "collector": "steam"},
        metadata={},
        source="steam",
    )


@pytest.fixture
def sample_record_empty():
    return StorageRecord(
        key="test:empty:0",
        data={},
        metadata={},
        source="unknown",
    )


@pytest.fixture
def task_pending():
    return Task(
        id="task001",
        name="Test Task",
        pipeline_name="steam_basic",
        collector_name="steam",
        targets=[TaskTarget(name="CS2", target_type="game", params={"app_id": "730"})],
    )


@pytest.fixture
def task_completed():
    t = Task(
        id="task002",
        name="Completed Task",
        pipeline_name="steam_basic",
        collector_name="steam",
        targets=[TaskTarget(name="CS2", target_type="game", params={"app_id": "730"})],
    )
    t.start()
    t.complete({"success": True, "storage_count": 10})
    return t


@pytest.fixture
def task_failed():
    t = Task(id="task003", name="Failed Task", pipeline_name="taptap_basic")
    t.start()
    t.fail("Connection timeout")
    return t


@pytest.fixture
def pipeline_basic():
    p = Pipeline("test_pipeline")
    p.add_collector("steam", {"request_delay": 0.5})
    p.add_processor("cleaner")
    p.add_storage("sqlalchemy")
    return p
