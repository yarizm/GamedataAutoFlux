"""Steam API client + collector review pagination resume (cursor) tests."""

from __future__ import annotations

import pytest

from src.collectors.base import CollectTarget
from src.collectors.steam.steam_api_client import SteamAPIClient
from src.collectors.steam_collector import SteamCollector
from src.core.collector_metadata import get_collector_metadata
from src.core.collector_resume import build_collector_cursor


@pytest.mark.asyncio
async def test_fetch_review_pages_resumes_from_cursor(monkeypatch):
    client = SteamAPIClient(request_delay=0)
    calls = []

    async def fake_request(url, params=None):
        calls.append(params.get("cursor"))
        if params.get("cursor") == "*":
            return {
                "query_summary": {"total_reviews": 300},
                "reviews": [
                    {
                        "recommendationid": "1",
                        "author": {},
                        "voted_up": True,
                        "review": "a",
                        "votes_up": 1,
                        "votes_funny": 0,
                        "timestamp_created": 1,
                        "language": "en",
                    }
                ],
                "cursor": "PAGE2",
            }
        if params.get("cursor") == "PAGE2":
            return {
                "query_summary": {},
                "reviews": [
                    {
                        "recommendationid": "2",
                        "author": {},
                        "voted_up": True,
                        "review": "b",
                        "votes_up": 2,
                        "votes_funny": 0,
                        "timestamp_created": 2,
                        "language": "en",
                    }
                ],
                "cursor": "PAGE3",
            }
        return {"reviews": [], "cursor": ""}

    monkeypatch.setattr(client, "_request", fake_request)
    reviews, summary, done, last = await client._fetch_review_pages(
        570, max_reviews=200, language="all", start_cursor="PAGE2", already_collected=1
    )
    assert calls[0] == "PAGE2"
    assert any(r.get("recommendationid") == "2" for r in reviews)
    assert last == "PAGE3"


@pytest.mark.asyncio
async def test_fetch_review_pages_seed_and_on_page(monkeypatch):
    client = SteamAPIClient(request_delay=0)
    seed = [
        {
            "recommendationid": "seed1",
            "author_steamid": "",
            "author_playtime": 0,
            "voted_up": True,
            "review_text": "seed",
            "votes_up": 0,
            "votes_funny": 0,
            "timestamp_created": 0,
            "language": "en",
        }
    ]
    page_hooks: list[tuple[str, int]] = []

    async def fake_request(url, params=None):
        cursor = params.get("cursor")
        if cursor == "PAGE2":
            return {
                "query_summary": {"total_reviews": 50},
                "reviews": [
                    {
                        "recommendationid": "2",
                        "author": {},
                        "voted_up": False,
                        "review": "b",
                        "votes_up": 0,
                        "votes_funny": 0,
                        "timestamp_created": 2,
                        "language": "en",
                    }
                ],
                "cursor": "PAGE3",
            }
        return {"reviews": [], "cursor": ""}

    async def on_page(*, cursor, reviews, query_summary):
        page_hooks.append((cursor, len(reviews)))

    monkeypatch.setattr(client, "_request", fake_request)
    reviews, summary, done, last = await client._fetch_review_pages(
        570,
        max_reviews=2,
        language="all",
        start_cursor="PAGE2",
        already_collected=1,
        seed_reviews=seed,
        on_page=on_page,
    )
    assert reviews[0]["recommendationid"] == "seed1"
    assert any(r.get("recommendationid") == "2" for r in reviews)
    assert len(reviews) == 2
    assert page_hooks and page_hooks[0][0] == "PAGE3"
    assert page_hooks[0][1] == 2
    assert last == "PAGE3"
    assert summary.get("total_reviews") == 50


@pytest.mark.asyncio
async def test_get_reviews_passes_resume_kwargs(monkeypatch):
    client = SteamAPIClient(request_delay=0)
    captured = {}

    async def fake_fetch(*args, **kwargs):
        captured.update(kwargs)
        return [], {"total_reviews": 0, "total_positive": 0, "total_negative": 0}, True, "X"

    async def fake_summary(*args, **kwargs):
        return {}

    monkeypatch.setattr(client, "_fetch_review_pages", fake_fetch)
    monkeypatch.setattr(client, "get_review_summary", fake_summary)
    await client.get_reviews(
        570,
        max_reviews=10,
        start_cursor="PAGE2",
        already_collected=3,
        seed_reviews=[{"recommendationid": "x"}],
    )
    assert captured.get("start_cursor") == "PAGE2"
    assert captured.get("already_collected") == 3
    assert captured.get("seed_reviews") == [{"recommendationid": "x"}]


def test_steam_metadata_l1_supports_checkpoint():
    meta = get_collector_metadata("steam")
    assert meta is not None
    assert meta.supports_checkpoint is True
    assert meta.recovery_level == "L1"


def _ok_steam_data(**overrides):
    data = {
        "source": "steam_api",
        "app_id": 570,
        "details": {"name": "Dota 2", "price": "Free"},
        "current_players": 100,
        "reviews": {
            "total_reviews": 10,
            "review_score_percent": 90,
            "review_score_desc": "Very Positive",
            "review_count_fetched": 2,
            "reviews": [
                {"recommendationid": "r1"},
                {"recommendationid": "r2"},
            ],
        },
        "achievements": [],
        "news": [],
    }
    data.update(overrides)
    return data


@pytest.mark.asyncio
async def test_collector_resumes_reviews_with_cursor_and_emits(monkeypatch):
    """Collector parses recovery cursor, passes resume kwargs, emits on_page checkpoint."""
    collector = SteamCollector(config={})
    emitted: list[dict] = []
    collect_kwargs: dict = {}

    async def fake_emit(cursor, state=None, stats=None):
        emitted.append({"cursor": cursor, "stats": stats or {}})

    async def fake_collect_all(app_id, **kwargs):
        collect_kwargs.update(kwargs)
        on_page = kwargs.get("on_page")
        if on_page is not None:
            await on_page(
                cursor="PAGE3",
                reviews=[{"recommendationid": "r2"}],
                query_summary={"total_reviews": 99},
            )
        return _ok_steam_data()

    class FakeAPI:
        async def collect_all(self, app_id, **kwargs):
            return await fake_collect_all(app_id, **kwargs)

    collector._steam_api = FakeAPI()
    collector._steamdb = None
    collector._firecrawl = None
    collector.config["_emit_checkpoint"] = fake_emit
    collector.config["recovery_checkpoint"] = {
        "cursor": build_collector_cursor(
            collector_id="steam",
            target_key="app:570",
            stage="api_reviews",
            payload={
                "app_id": "570",
                "completed_stages": ["resolve_app_id", "api_light"],
                "review_cursor": "PAGE2",
                "collected_count": 5,
                "max_reviews": 20,
                "partial_reviews": [],
                "steamdb_done": False,
            },
        )
    }

    target = CollectTarget(name="Dota 2", params={"app_id": "570", "skip_steamdb": True})
    result = await collector.collect(target)

    assert result.success is True
    assert collect_kwargs.get("start_cursor") == "PAGE2"
    # Empty partial → seed_reviews must be None (not []), already_collected used.
    assert collect_kwargs.get("seed_reviews") is None
    assert collect_kwargs.get("already_collected") == 5
    assert collect_kwargs.get("on_page") is not None
    assert emitted, "expected on_page / stage checkpoint emits"
    progress = [
        e
        for e in emitted
        if e["cursor"].get("stage") == "api_reviews"
        and e["cursor"].get("payload", {}).get("review_cursor") == "PAGE3"
    ]
    assert progress, f"missing api_reviews progress emit, got={emitted!r}"
    assert progress[0]["cursor"]["payload"]["collected_count"] == 6  # 5 + 1 new
    assert result.metadata.get("target_key") == "app:570"
    assert result.metadata.get("resume", {}).get("resumed") is True


@pytest.mark.asyncio
async def test_collector_uses_nonempty_seed_not_already_when_partial_present(monkeypatch):
    collector = SteamCollector(config={})
    collect_kwargs: dict = {}

    async def fake_collect_all(app_id, **kwargs):
        collect_kwargs.update(kwargs)
        return _ok_steam_data()

    class FakeAPI:
        async def collect_all(self, app_id, **kwargs):
            return await fake_collect_all(app_id, **kwargs)

    collector._steam_api = FakeAPI()
    collector._steamdb = None
    collector._firecrawl = None
    seed = [{"recommendationid": "seed1", "review_text": "x"}]
    collector.config["recovery_checkpoint"] = {
        "cursor": build_collector_cursor(
            collector_id="steam",
            target_key="app:730",
            stage="api_reviews",
            payload={
                "completed_stages": ["resolve_app_id", "api_light"],
                "review_cursor": "CUR",
                "collected_count": 1,
                "partial_reviews": seed,
                "steamdb_done": False,
            },
        )
    }
    target = CollectTarget(name="CS2", params={"app_id": "730", "skip_steamdb": True})
    result = await collector.collect(target)
    assert result.success is True
    assert collect_kwargs.get("start_cursor") == "CUR"
    assert collect_kwargs.get("seed_reviews") == seed
    assert collect_kwargs.get("already_collected") == 0


@pytest.mark.asyncio
async def test_collector_skips_steamdb_when_steamdb_done(monkeypatch):
    collector = SteamCollector(config={})
    scrape_called = {"n": 0}

    class FakeAPI:
        async def collect_all(self, app_id, **kwargs):
            return _ok_steam_data()

    class FakeSteamDB:
        async def scrape(self, *args, **kwargs):
            scrape_called["n"] += 1
            return {"source": "steamdb", "charts": {}}

    collector._steam_api = FakeAPI()
    collector._steamdb = FakeSteamDB()
    collector._firecrawl = None
    collector.config["recovery_checkpoint"] = {
        "cursor": build_collector_cursor(
            collector_id="steam",
            target_key="app:570",
            stage="steamdb",
            payload={
                "completed_stages": ["resolve_app_id", "api_light", "api_reviews"],
                "steamdb_done": True,
                "collected_count": 20,
                "review_cursor": "",
            },
        )
    }
    target = CollectTarget(name="Dota 2", params={"app_id": "570"})
    result = await collector.collect(target)
    assert result.success is True
    assert scrape_called["n"] == 0
    assert result.data is not None
    assert "steamdb" not in (result.data or {})


@pytest.mark.asyncio
async def test_collector_emits_cursor_on_api_failure(monkeypatch):
    collector = SteamCollector(config={})
    emitted: list[dict] = []

    async def fake_emit(cursor, state=None, stats=None):
        emitted.append(cursor)

    class FakeAPI:
        async def collect_all(self, app_id, **kwargs):
            raise RuntimeError("network down")

    collector._steam_api = FakeAPI()
    collector._steamdb = None
    collector._firecrawl = None
    collector.config["_emit_checkpoint"] = fake_emit
    collector.config["recovery_checkpoint"] = {
        "cursor": build_collector_cursor(
            collector_id="steam",
            target_key="app:570",
            stage="api_reviews",
            payload={
                "completed_stages": ["resolve_app_id", "api_light"],
                "review_cursor": "PAGE2",
                "collected_count": 3,
                "partial_reviews": [],
            },
        )
    }
    target = CollectTarget(name="Dota 2", params={"app_id": "570", "skip_steamdb": True})
    result = await collector.collect(target)
    assert result.success is False
    assert emitted, "failure path should emit last/resume cursor"
    last = emitted[-1]
    assert last.get("collector_id") == "steam"
    assert last.get("target_key") == "app:570"
    assert last.get("payload", {}).get("review_cursor") == "PAGE2"
    assert last.get("payload", {}).get("collected_count") == 3


@pytest.mark.asyncio
async def test_api_reviews_not_complete_when_reviews_payload_missing(monkeypatch):
    """Light keys OK but reviews=None → do not mark api_reviews complete."""
    collector = SteamCollector(config={})
    emitted: list[dict] = []

    async def fake_emit(cursor, state=None, stats=None):
        emitted.append({"cursor": cursor, "stats": stats or {}})

    class FakeAPI:
        async def collect_all(self, app_id, **kwargs):
            # Light success, reviews failed independently (collect_all sets None).
            return {
                "source": "steam_api",
                "app_id": 570,
                "details": {"name": "Dota 2"},
                "current_players": 100,
                "reviews": None,
                "achievements": [],
                "news": [],
            }

    collector._steam_api = FakeAPI()
    collector._steamdb = None
    collector._firecrawl = None
    collector.config["_emit_checkpoint"] = fake_emit

    target = CollectTarget(name="Dota 2", params={"app_id": "570", "skip_steamdb": True})
    result = await collector.collect(target)

    assert result.success is True
    assert emitted, "expected stage/progress checkpoint emits"
    stages_seen = [e["cursor"].get("payload", {}).get("completed_stages") or [] for e in emitted]
    for stages in stages_seen:
        assert "api_reviews" not in stages, f"api_reviews must stay incomplete, got={stages!r}"
    # api_light may still complete from light keys.
    final_stages = stages_seen[-1]
    assert "api_light" in final_stages
    assert "resolve_app_id" in final_stages


@pytest.mark.asyncio
async def test_api_reviews_not_complete_on_pure_error_blob(monkeypatch):
    """reviews={'error': ...} pure error blob → do not mark api_reviews complete."""
    collector = SteamCollector(config={})
    emitted: list[dict] = []

    async def fake_emit(cursor, state=None, stats=None):
        emitted.append(cursor)

    class FakeAPI:
        async def collect_all(self, app_id, **kwargs):
            return {
                "source": "steam_api",
                "app_id": 570,
                "details": {"name": "Dota 2"},
                "current_players": 1,
                "reviews": {"error": "rate limited"},
                "achievements": None,
                "news": None,
            }

    collector._steam_api = FakeAPI()
    collector._steamdb = None
    collector._firecrawl = None
    collector.config["_emit_checkpoint"] = fake_emit

    target = CollectTarget(name="Dota 2", params={"app_id": "570", "skip_steamdb": True})
    result = await collector.collect(target)
    assert result.success is True
    for cursor in emitted:
        stages = cursor.get("payload", {}).get("completed_stages") or []
        assert "api_reviews" not in stages


@pytest.mark.asyncio
async def test_incomplete_partial_does_not_enter_seed_mode(monkeypatch):
    """collected_count > len(partial) → count-only, not seed_reviews."""
    collector = SteamCollector(config={})
    collect_kwargs: dict = {}

    async def fake_collect_all(app_id, **kwargs):
        collect_kwargs.update(kwargs)
        return _ok_steam_data()

    class FakeAPI:
        async def collect_all(self, app_id, **kwargs):
            return await fake_collect_all(app_id, **kwargs)

    collector._steam_api = FakeAPI()
    collector._steamdb = None
    collector._firecrawl = None
    # Stale/short partial left from a prior count-only path (or truncation).
    short_partial = [{"recommendationid": "only-one"}]
    collector.config["recovery_checkpoint"] = {
        "cursor": build_collector_cursor(
            collector_id="steam",
            target_key="app:570",
            stage="api_reviews",
            payload={
                "completed_stages": ["resolve_app_id", "api_light"],
                "review_cursor": "PAGE5",
                "collected_count": 50,
                "partial_reviews": short_partial,
                "steamdb_done": False,
            },
        )
    }
    target = CollectTarget(
        name="Dota 2", params={"app_id": "570", "skip_steamdb": True, "max_reviews": 100}
    )
    result = await collector.collect(target)
    assert result.success is True
    assert collect_kwargs.get("start_cursor") == "PAGE5"
    assert collect_kwargs.get("seed_reviews") is None
    assert collect_kwargs.get("already_collected") == 50


@pytest.mark.asyncio
async def test_count_only_on_page_emits_empty_partial(monkeypatch):
    """Count-only resume: on_page must emit partial_reviews=[] (cursor+count only)."""
    collector = SteamCollector(config={})
    emitted: list[dict] = []
    collect_kwargs: dict = {}

    async def fake_emit(cursor, state=None, stats=None):
        emitted.append({"cursor": cursor, "stats": stats or {}})

    async def fake_collect_all(app_id, **kwargs):
        collect_kwargs.update(kwargs)
        on_page = kwargs.get("on_page")
        if on_page is not None:
            # Simulate newly fetched page only (not full history).
            await on_page(
                cursor="PAGE6",
                reviews=[{"recommendationid": "new1"}, {"recommendationid": "new2"}],
                query_summary={"total_reviews": 200},
            )
        return _ok_steam_data()

    class FakeAPI:
        async def collect_all(self, app_id, **kwargs):
            return await fake_collect_all(app_id, **kwargs)

    collector._steam_api = FakeAPI()
    collector._steamdb = None
    collector._firecrawl = None
    collector.config["_emit_checkpoint"] = fake_emit
    collector.config["recovery_checkpoint"] = {
        "cursor": build_collector_cursor(
            collector_id="steam",
            target_key="app:570",
            stage="api_reviews",
            payload={
                "completed_stages": ["resolve_app_id", "api_light"],
                "review_cursor": "PAGE5",
                "collected_count": 40,
                "partial_reviews": [],
                "steamdb_done": False,
            },
        )
    }
    target = CollectTarget(
        name="Dota 2", params={"app_id": "570", "skip_steamdb": True, "max_reviews": 100}
    )
    result = await collector.collect(target)
    assert result.success is True
    assert collect_kwargs.get("seed_reviews") is None
    assert collect_kwargs.get("already_collected") == 40

    progress = [
        e
        for e in emitted
        if e["cursor"].get("stage") == "api_reviews"
        and e["cursor"].get("payload", {}).get("review_cursor") == "PAGE6"
    ]
    assert progress, f"missing count-only progress emit, got={emitted!r}"
    payload = progress[0]["cursor"]["payload"]
    assert payload.get("partial_reviews") == []
    # already_collected(40) + newly listed(2)
    assert payload.get("collected_count") == 42
