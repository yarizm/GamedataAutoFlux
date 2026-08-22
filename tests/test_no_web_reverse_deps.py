"""架构收口验收（修复清单 P1）：业务代码不得 import `src.web`。

Web 层是装配结果的消费者；core / services / agent / reporting / worker /
bootstrap 与插件一律面向核心模块和 `src.bootstrap.container` 编程。
WebSocket 推送经 `src.core.ws_broadcast` 端口，由 Web 层注册发送函数。
"""

import re
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent

BUSINESS_DIRS = (
    "src/core",
    "src/services",
    "src/agent",
    "src/reporting",
    "src/worker",
    "src/bootstrap",
    "plugins",
)

ALLOWED_OFFENDERS: set[str] = set()

_WEB_IMPORT_RE = re.compile(r"^\s*(?:from\s+src\.web|import\s+src\.web)", re.MULTILINE)


def test_business_code_does_not_import_web_layer():
    offenders: list[str] = []
    for base in BUSINESS_DIRS:
        for py in (REPO / base).rglob("*.py"):
            rel = py.relative_to(REPO).as_posix()
            if rel in ALLOWED_OFFENDERS:
                continue
            if _WEB_IMPORT_RE.search(py.read_text(encoding="utf-8")):
                offenders.append(rel)
    assert offenders == [], f"业务代码出现 src.web 反向依赖: {offenders}"


def test_scheduler_exposes_public_persistence_api():
    """启动阶段注入必须走 public lifecycle API，不直写私有字段。"""
    from src.core.events import EventBus
    from src.core.scheduler import Scheduler

    scheduler = Scheduler()
    assert scheduler.task_store is None  # 启动前无 store

    class _FakeRepo:
        pass

    scheduler.attach_persistence(
        task_repo=_FakeRepo(),
        cron_repo=_FakeRepo(),
        pipeline_repo=_FakeRepo(),
        event_bus=EventBus(),
    )
    assert isinstance(scheduler._task_repo, _FakeRepo)
    assert isinstance(scheduler._cron_repo, _FakeRepo)
    assert isinstance(scheduler._pipeline_repo, _FakeRepo)
    assert isinstance(scheduler._event_bus, EventBus)
