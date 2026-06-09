from datetime import datetime

import pytest

from src.reporting.generator import ReportGenerator, _emit_report_progress, _safe_json
from src.services._utils import derive_collection_target_context
from src.storage.base import QueryResult, StorageRecord


def test_build_template_prompt_auto():
    generator = ReportGenerator()
    validation = {"available_collectors": ["steam", "qimai"]}

    prompt = generator._build_template_prompt(
        "Analyze this game", "auto", validation, custom_prompt="Focus on CN market"
    )

    assert "Analyze this game" in prompt
    assert "Available data sources: steam, qimai" in prompt
    assert "Focus on CN market" in prompt
    assert "Dynamically structure" in prompt


def test_report_context_json_redacts_sensitive_values():
    payload = _safe_json({"source_meta": {"api_key": "secret", "cookie": "sid"}}, max_chars=1000)

    assert "secret" not in payload
    assert "sid" not in payload
    assert "[REDACTED]" in payload


@pytest.mark.asyncio
async def test_emit_report_progress_redacts_message_and_extra(monkeypatch) -> None:
    captured: list[dict] = []

    async def fake_broadcast(message: dict) -> None:
        captured.append(message)

    monkeypatch.setattr("src.web.routes.ws.manager.broadcast", fake_broadcast)

    await _emit_report_progress(
        "progress-1",
        "llm_failed",
        0.5,
        "LLM failed api_key=message-secret",
        detail="token=extra-secret",
        nested={"api_key": "nested-secret"},
    )

    assert captured
    payload = captured[0]
    rendered = str(payload)
    assert payload["message"] == "LLM failed api_key=[REDACTED]"
    assert payload["detail"] == "token=[REDACTED]"
    assert payload["nested"]["api_key"] == "[REDACTED]"
    assert "message-secret" not in rendered
    assert "extra-secret" not in rendered
    assert "nested-secret" not in rendered


def test_collection_target_context_redacts_prompt_fallback() -> None:
    context = derive_collection_target_context(
        [],
        prompt="Generate report api_key=secret-key for Counter-Strike 2",
    )

    assert "secret-key" not in context["target_name"]
    assert "api_key=[REDACTED]" in context["target_name"]


def test_collection_target_context_redacts_record_keys_and_target_params() -> None:
    record = StorageRecord(
        key="record:api_key=key-secret",
        source="official_site",
        data={
            "collector": "official_site",
            "game_name": "Counter-Strike 2",
            "official_url": "https://example.test/news?api_key=url-secret",
        },
        metadata={
            "source_task": {
                "collector_name": "official_site",
                "target_params": {
                    "official_url": "https://example.test/news?api_key=task-secret",
                },
            }
        },
    )

    context = derive_collection_target_context([record])
    rendered = str(context)

    assert "key-secret" not in rendered
    assert "url-secret" not in rendered
    assert "task-secret" not in rendered
    assert context["source_record_keys"] == ["record:api_key=[REDACTED]"]
    assert context["official_url"] == "https://example.test/news?api_key=[REDACTED]"
    assert context["params_by_collector"]["official_site"] == {
        "official_url": "https://example.test/news?api_key=[REDACTED]",
    }


@pytest.mark.asyncio
async def test_render_report_fallback_note_redacts_provider_error(monkeypatch) -> None:
    generator = ReportGenerator()
    generator._llm_provider = "qwen"
    record = StorageRecord(
        key="record:steam",
        source="steam",
        data={"collector": "steam", "game_name": "Counter-Strike 2"},
        stored_at=datetime(2026, 1, 1, 12, 0, 0),
    )

    def fake_get_config(key: str, default=None):
        if key == "llm.qwen":
            return {"model": "qwen-max"}
        if key == "llm.qwen.fallback_to_stub":
            return True
        return default

    async def failing_render(*args, **kwargs):
        raise RuntimeError("provider failed api_key=llm-secret; token=note-secret")

    monkeypatch.setattr("src.reporting.generator.get_config", fake_get_config)
    monkeypatch.setattr(generator, "_render_report_with_openai_compatible", failing_render)

    content = await generator._render_report(
        "Analyze Counter-Strike 2",
        "",
        "default",
        [record],
    )

    assert "llm-secret" not in content
    assert "note-secret" not in content
    assert "api_key=[REDACTED]" in content
    assert "token=[REDACTED]" in content


def test_record_context_includes_dataset_coverage():
    generator = ReportGenerator()
    record = StorageRecord(
        key="task:steam:0",
        source="steam",
        data={
            "collector": "steam",
            "game_name": "Counter-Strike 2",
            "app_id": "730",
            "snapshot": {"current_players": 100, "total_reviews": 200},
        },
        metadata={"collector": "steam"},
        stored_at=datetime(2026, 1, 1, 12, 0, 0),
    )

    context = generator._build_record_context([record])

    assert "### Dataset Coverage" in context
    assert "sources: steam=1" in context
    assert "Counter-Strike 2" in context
    assert "current_players=100" in context


def test_record_context_redacts_sensitive_record_keys():
    generator = ReportGenerator()
    record = StorageRecord(
        key="upload:api_key=secret-key",
        source="steam",
        data={
            "collector": "steam",
            "game_name": "Counter-Strike 2",
            "snapshot": {"current_players": 100},
        },
        stored_at=datetime(2026, 1, 1, 12, 0, 0),
    )

    context = generator._build_record_context([record])

    assert "secret-key" not in context
    assert "upload:api_key=[REDACTED]" in context


def test_record_context_details_preserve_source_coverage():
    generator = ReportGenerator()
    records = [
        StorageRecord(
            key=f"record:steam:{index}",
            source="steam",
            data={"collector": "steam", "game_name": f"Steam Game {index}"},
            stored_at=datetime(2026, 1, 1, 12, 0, 0),
        )
        for index in range(12)
    ]
    records.extend(
        [
            StorageRecord(
                key="record:gtrends",
                source="gtrends",
                data={
                    "collector": "gtrends",
                    "game_name": "Counter-Strike 2",
                    "trend_history": [{"date": "2026-01-01", "value": 80}],
                },
                stored_at=datetime(2026, 1, 1, 12, 0, 0),
            ),
            StorageRecord(
                key="record:events",
                source="events",
                data={
                    "collector": "events",
                    "game_name": "Counter-Strike 2",
                    "events": [{"title": "Launch event"}],
                },
                stored_at=datetime(2026, 1, 1, 12, 0, 0),
            ),
        ]
    )

    context = generator._build_record_context(records)

    assert context.count("### Record ") == 12
    assert "- key: record:gtrends" in context
    assert "- key: record:events" in context


@pytest.mark.asyncio
async def test_generate_filters_report_history_from_explicit_records(monkeypatch):
    captured = {}
    report_history = StorageRecord(
        key="report:history",
        source="reporting",
        data={"title": "Generated report"},
        metadata={"kind": "report", "template": "default"},
        stored_at=datetime(2026, 1, 1, 12, 0, 0),
    )
    source_record = StorageRecord(
        key="record:steam",
        source="steam",
        data={"collector": "steam", "game_name": "Counter-Strike 2"},
        stored_at=datetime(2026, 1, 1, 12, 0, 0),
    )

    async def fake_render(self, prompt, data_source, template, records):
        captured["records"] = records
        return "ok"

    async def fake_save(self, report):
        captured["report"] = report

    monkeypatch.setattr(ReportGenerator, "_render_report", fake_render)
    monkeypatch.setattr(ReportGenerator, "_save_report", fake_save)

    report = await ReportGenerator().generate(
        prompt="generate report",
        records=[report_history, source_record],
        metadata={"selected_record_keys": ["report:history", "record:steam"]},
    )

    assert report.matched_records == 1
    assert [record.key for record in captured["records"]] == ["record:steam"]
    assert report.metadata["selected_record_keys"] == ["record:steam"]
    assert report.metadata["excluded_report_record_keys"] == ["report:history"]


@pytest.mark.asyncio
async def test_generate_records_data_quality_metadata(monkeypatch):
    captured = {}
    steam_record = StorageRecord(
        key="record:steam",
        source="steam",
        data={
            "collector": "steam",
            "game_name": "Counter-Strike 2",
            "app_id": "730",
            "snapshot": {"current_players": 100, "total_reviews": 200},
        },
        stored_at=datetime(2026, 1, 1, 12, 0, 0),
    )
    empty_record = StorageRecord(
        key="record:empty",
        source="monitor",
        data=None,
        stored_at=datetime(2026, 1, 1, 12, 0, 0),
    )

    async def fake_render(self, prompt, data_source, template, records):
        return "ok"

    async def fake_save(self, report):
        captured["report"] = report

    monkeypatch.setattr(ReportGenerator, "_render_report", fake_render)
    monkeypatch.setattr(ReportGenerator, "_save_report", fake_save)

    report = await ReportGenerator().generate(
        prompt="generate report",
        records=[steam_record, empty_record],
        metadata={"selected_record_keys": ["record:steam", "record:empty"]},
    )

    assert report.metadata["format"] == "markdown"
    assert report.metadata["source_record_count"] == 2
    assert report.metadata["usable_record_count"] == 1
    assert report.metadata["source_record_keys"] == ["record:steam", "record:empty"]
    assert report.metadata["usable_record_keys"] == ["record:steam"]
    assert report.metadata["empty_record_keys"] == ["record:empty"]
    assert report.metadata["source_coverage"] == {"steam": 1}
    assert report.metadata["record_completeness"] == {"full": 1, "empty": 1}
    assert report.metadata["source_freshness"]["oldest_record_at"].startswith("2026-01-01")
    assert report.metadata["source_freshness"]["newest_record_at"].startswith("2026-01-01")
    assert report.metadata["source_freshness"]["warning_days"] == 30
    assert report.metadata["selected_record_keys"] == ["record:steam", "record:empty"]
    assert report.metadata["target_context"]["target_name"] == "Counter-Strike 2"
    assert report.metadata["target_context"]["steam_app_id"] == "730"
    assert report.metadata["target_context"]["params_by_collector"]["monitor"] == {
        "app_id": "730"
    }
    assert captured["report"] == report


@pytest.mark.asyncio
async def test_generate_redacts_sensitive_report_metadata_keys(monkeypatch):
    record = StorageRecord(
        key="record:api_key=key-secret",
        source="official_site",
        data={
            "collector": "official_site",
            "game_name": "Counter-Strike 2",
            "official_url": "https://example.test/news?api_key=url-secret",
        },
        stored_at=datetime(2026, 1, 1, 12, 0, 0),
    )

    async def fake_render(self, prompt, data_source, template, records):
        return "ok"

    async def fake_save(self, report):
        return None

    monkeypatch.setattr(ReportGenerator, "_render_report", fake_render)
    monkeypatch.setattr(ReportGenerator, "_save_report", fake_save)

    report = await ReportGenerator().generate(
        prompt="generate report",
        records=[record],
        metadata={"selected_record_keys": [record.key]},
    )
    rendered = str(report.metadata)

    assert "key-secret" not in rendered
    assert "url-secret" not in rendered
    assert report.metadata["source_record_keys"] == ["record:api_key=[REDACTED]"]
    assert report.metadata["selected_record_keys"] == ["record:api_key=[REDACTED]"]
    assert report.metadata["target_context"]["source_record_keys"] == [
        "record:api_key=[REDACTED]"
    ]
    assert (
        report.metadata["target_context"]["official_url"]
        == "https://example.test/news?api_key=[REDACTED]"
    )


@pytest.mark.asyncio
async def test_generate_excel_records_data_quality_metadata(tmp_path, monkeypatch):
    captured = {}
    steam_record = StorageRecord(
        key="record:steam",
        source="steam",
        data={
            "collector": "steam",
            "game_name": "Counter-Strike 2",
            "app_id": "730",
            "snapshot": {"current_players": 100, "total_reviews": 200},
        },
        stored_at=datetime(2026, 1, 1, 12, 0, 0),
    )

    def fake_export_to_excel(**kwargs):
        captured["export"] = kwargs

    async def fake_save(self, report):
        captured["report"] = report

    monkeypatch.setattr("src.reporting.generator.export_to_excel", fake_export_to_excel)
    monkeypatch.setattr("src.reporting.generator.get_reports_dir", lambda: tmp_path)
    monkeypatch.setattr(ReportGenerator, "_save_report", fake_save)

    report = await ReportGenerator().generate_excel(
        prompt="generate report",
        records=[steam_record],
        params={"include_llm_analysis": False},
    )

    assert report.metadata["format"] == "excel"
    assert report.metadata["source_record_count"] == 1
    assert report.metadata["usable_record_count"] == 1
    assert report.metadata["source_coverage"] == {"steam": 1}
    assert report.metadata["record_completeness"] == {"full": 1}
    assert report.metadata["source_freshness"]["newest_record_at"].startswith("2026-01-01")
    assert report.metadata["sheets"]["overview"] >= 1
    assert report.metadata["target_context"]["target_name"] == "Counter-Strike 2"
    assert report.metadata["target_context"]["params_by_collector"]["monitor"] == {
        "app_id": "730"
    }
    assert captured["export"]["output_path"].parent == tmp_path
    assert captured["report"] == report


@pytest.mark.asyncio
async def test_generate_rejects_explicit_report_history_only_records():
    report_history = StorageRecord(
        key="report:history",
        source="reporting",
        data={"title": "Generated report"},
        metadata={"kind": "report", "template": "default"},
        stored_at=datetime(2026, 1, 1, 12, 0, 0),
    )

    with pytest.raises(ValueError, match="report history"):
        await ReportGenerator().generate(
            prompt="generate report",
            records=[report_history],
        )


@pytest.mark.asyncio
async def test_load_source_records_filters_data_source_labels(monkeypatch):
    official = StorageRecord(
        key="record:official",
        source="cleaner",
        data={"collector": "official_site", "game_name": "Example Game"},
        metadata={"source_task": {"collector_name": "official_site"}},
        stored_at=datetime(2026, 1, 1, 12, 0, 0),
    )
    taptap = StorageRecord(
        key="record:taptap",
        source="taptap",
        data={"collector": "taptap", "game_name": "Example Game"},
        stored_at=datetime(2026, 1, 1, 12, 0, 0),
    )
    store = _FakeReportStore(
        {
            "source:official website": [],
            "official website": [],
            "key:": [taptap, official],
        }
    )
    monkeypatch.setattr("src.reporting.generator.get_storage", lambda: store)

    records = await ReportGenerator()._load_source_records(
        prompt="Example Game",
        data_source="official website",
        params={"limit": 5},
    )

    assert [record.key for record in records] == ["record:official"]
    assert store.queries == ["source:official website", "official website", "key:"]


@pytest.mark.asyncio
async def test_load_source_records_prefers_exact_source_query(monkeypatch):
    record = StorageRecord(
        key="record:steam",
        source="steam",
        data={"collector": "steam", "game_name": "Counter-Strike 2"},
        stored_at=datetime(2026, 1, 1, 12, 0, 0),
    )
    store = _FakeReportStore({"source:steam": [record]})
    monkeypatch.setattr("src.reporting.generator.get_storage", lambda: store)

    records = await ReportGenerator()._load_source_records(
        prompt="Counter-Strike 2",
        data_source="steam",
        params={"limit": 5},
    )

    assert records == [record]
    assert store.queries == ["source:steam"]


@pytest.mark.asyncio
async def test_load_source_records_excludes_report_history_from_source_fallback(monkeypatch):
    report_history = StorageRecord(
        key="report:steam-history",
        source="reporting",
        data={"title": "Old Steam report", "data_source": "steam"},
        metadata={"kind": "report", "data_source": "steam"},
        stored_at=datetime(2026, 1, 1, 12, 0, 0),
    )
    source_record = StorageRecord(
        key="record:steam",
        source="steam",
        data={"collector": "steam", "game_name": "Counter-Strike 2"},
        stored_at=datetime(2026, 1, 1, 12, 0, 0),
    )
    store = _FakeReportStore(
        {
            "source:steam": [report_history],
            "steam": [report_history],
            "key:": [report_history, source_record],
        }
    )
    monkeypatch.setattr("src.reporting.generator.get_storage", lambda: store)

    records = await ReportGenerator()._load_source_records(
        prompt="Counter-Strike 2",
        data_source="steam",
        params={"limit": 5},
    )

    assert [record.key for record in records] == ["record:steam"]
    assert store.queries == ["source:steam", "steam", "key:"]


@pytest.mark.asyncio
async def test_load_source_records_coerces_record_limit(monkeypatch):
    record = StorageRecord(
        key="record:steam",
        source="steam",
        data={"collector": "steam", "game_name": "Counter-Strike 2"},
        stored_at=datetime(2026, 1, 1, 12, 0, 0),
    )
    store = _FakeReportStore({"source:steam": [record]})
    monkeypatch.setattr("src.reporting.generator.get_storage", lambda: store)

    records = await ReportGenerator()._load_source_records(
        prompt="Counter-Strike 2",
        data_source="steam",
        params={"limit": "not-a-number"},
    )

    assert records == [record]
    assert store.limits == [5]


@pytest.mark.asyncio
async def test_load_source_records_clamps_large_record_limit(monkeypatch):
    store = _FakeReportStore({"source:steam": []})
    monkeypatch.setattr("src.reporting.generator.get_storage", lambda: store)

    await ReportGenerator()._load_source_records(
        prompt="Counter-Strike 2",
        data_source="steam",
        params={"limit": 99999},
    )

    assert store.limits == [1000, 5000, 5000]


class _FakeReportStore:
    def __init__(self, records_by_query):
        self.records_by_query = records_by_query
        self.queries = []
        self.limits = []

    async def initialize(self):
        return None

    async def close(self):
        return None

    async def query(self, query: str, limit: int = 10, **kwargs):
        self.queries.append(query)
        self.limits.append(limit)
        records = list(self.records_by_query.get(query, []))[:limit]
        return QueryResult(records=records, total=len(records), query=query)
