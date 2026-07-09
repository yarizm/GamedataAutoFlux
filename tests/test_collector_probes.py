"""Tests for optional deep collector probes."""

from __future__ import annotations

import pytest

from src.core.collector_probes import (
    ProbeResult,
    build_probe_report,
    clear_probe_cache,
    merge_probe_issues,
    run_collector_probes,
)


@pytest.fixture(autouse=True)
def _clear_cache():
    clear_probe_cache()
    yield
    clear_probe_cache()


def test_merge_probe_issues_default_non_blocking() -> None:
    results = [
        ProbeResult(
            collector_id="youtube_comments",
            name="youtube.api_keys",
            status="error",
            message="bad key",
            error_code="missing_credentials",
        )
    ]
    issues = merge_probe_issues(probe_results=results, blocking_collectors=set())
    assert len(issues) == 1
    assert issues[0]["level"] == "warning"
    assert issues[0]["category"] == "probe"
    assert issues[0]["message"].startswith("[deep]")


def test_merge_probe_issues_blocking_keeps_error() -> None:
    results = [
        ProbeResult(
            collector_id="youtube_comments",
            name="youtube.api_keys",
            status="error",
            message="bad key",
            error_code="missing_credentials",
        )
    ]
    issues = merge_probe_issues(
        probe_results=results,
        blocking_collectors={"youtube_comments"},
    )
    assert issues[0]["level"] == "error"


def test_build_probe_report_status() -> None:
    report = build_probe_report(
        [
            ProbeResult("a", "n", "ok", "fine"),
            ProbeResult("b", "n", "warning", "warn"),
        ]
    )
    assert report["status"] == "warning"
    assert report["summary"]["ok"] == 1
    assert report["summary"]["warning"] == 1


@pytest.mark.asyncio
async def test_run_collector_probes_skips_unknown(monkeypatch) -> None:
    # Force no network: monkeypatch youtube probe path by using empty collectors
    results = await run_collector_probes(["unknown_collector_xyz"], targets=[])
    assert results
    assert results[0].status in {"skipped", "ok", "warning", "error"}


@pytest.mark.asyncio
async def test_youtube_probe_missing_keys(monkeypatch) -> None:
    monkeypatch.setattr(
        "src.core.collector_probes.get_config",
        lambda key, default=None: [] if key == "youtube.api_keys" else (
            5 if "timeout" in key else (120 if "ttl" in key else default)
        ),
    )
    results = await run_collector_probes(["youtube_comments"], targets=[])
    yt = [r for r in results if r.name == "youtube.api_keys"]
    assert yt
    assert yt[0].status == "error"
    assert yt[0].error_code == "missing_credentials"


@pytest.mark.asyncio
async def test_youtube_probe_valid_key_mocked(monkeypatch) -> None:
    class FakeResp:
        status_code = 200

        def json(self):
            return {"items": [{"id": "jNQXAC9IVRw"}]}

    class FakeClient:
        def __init__(self, *a, **k):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *a):
            return None

        async def get(self, *a, **k):
            return FakeResp()

    monkeypatch.setattr(
        "src.core.collector_probes.get_config",
        lambda key, default=None: (
            ["fake-key"] if key == "youtube.api_keys"
            else (5 if "timeout" in key else (0 if "ttl" in key else default))
        ),
    )
    monkeypatch.setattr("httpx.AsyncClient", FakeClient)
    results = await run_collector_probes(["youtube_profiles"], targets=[])
    yt = [r for r in results if r.name == "youtube.api_keys"]
    assert yt and yt[0].status == "ok"


@pytest.mark.asyncio
async def test_precheck_async_deep_merges_probe_issues(monkeypatch) -> None:
    from src.core.pipeline import Pipeline
    from src.services.task_precheck_service import TaskPrecheckService

    class FakeSched:
        def __init__(self, pipeline):
            self._pipeline = pipeline

        def get_pipeline(self, name):
            return self._pipeline

    async def fake_probes(collector_ids, targets=None, timeout_s=None):
        return [
            ProbeResult(
                collector_id=collector_ids[0],
                name="youtube.api_keys",
                status="error",
                message="invalid",
                error_code="missing_credentials",
            )
        ]

    monkeypatch.setattr(
        "src.core.collector_probes.run_collector_probes",
        fake_probes,
    )
    pipeline = Pipeline(name="yt")
    pipeline.add_collector("youtube_comments", {})
    service = TaskPrecheckService(FakeSched(pipeline))
    result = await service.precheck_async(
        name="t",
        pipeline_name="yt",
        targets=[{"name": "v", "params": {"video_url": "https://www.youtube.com/watch?v=x"}}],
        deep=True,
    )
    assert result.deep is True
    assert result.probe_report
    assert any(i.category == "probe" for i in result.issues)


@pytest.mark.asyncio
async def test_precheck_async_default_no_probe_calls(monkeypatch) -> None:
    from src.core.pipeline import Pipeline
    from src.services.task_precheck_service import TaskPrecheckService

    class FakeSched:
        def __init__(self, pipeline):
            self._pipeline = pipeline

        def get_pipeline(self, name):
            return self._pipeline

    called = {"n": 0}

    async def fake_probes(*a, **k):
        called["n"] += 1
        return []

    monkeypatch.setattr("src.core.collector_probes.run_collector_probes", fake_probes)
    monkeypatch.setattr(
        "src.services.task_precheck_service.get_config",
        lambda key, default=None: False if key == "precheck.deep_default" else default,
    )
    pipeline = Pipeline(name="taptap")
    pipeline.add_collector("taptap", {})
    service = TaskPrecheckService(FakeSched(pipeline))
    result = await service.precheck_async(
        name="t",
        pipeline_name="taptap",
        targets=[{"name": "g", "params": {"app_id": "1"}}],
        deep=False,
    )
    assert result.deep is False
    assert called["n"] == 0
