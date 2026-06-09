import json
from datetime import datetime

import pytest

from src.agent.tools.reports import (
    GenerateReportTool,
    GetReportContentTool,
    ListReportsTool,
    PrecheckReportTool,
    _prepare_report_content,
)
from src.reporting.generator import GeneratedReport
from src.storage.base import StorageRecord


def test_prepare_report_content_redacts_text_secrets_and_truncates() -> None:
    content = "api_key=sk-secret\n" + ("x" * 50)

    safe_content, truncated = _prepare_report_content(content, max_chars=20)

    assert "sk-secret" not in safe_content
    assert "api_key=[REDACTED]" in safe_content
    assert truncated is True
    assert safe_content.endswith("[Report content truncated]")


def test_prepare_report_content_uses_shared_json_and_auth_redaction() -> None:
    content = (
        'Report raw context {"api_key": "json-key", "token": "json-token"}\n'
        "Authorization: Bearer abcdefghijklmnop\n"
        "Basic QWxhZGRpbjpvcGVuIHNlc2FtZQ=="
    )

    safe_content, truncated = _prepare_report_content(content, max_chars=500)

    assert truncated is False
    assert "json-key" not in safe_content
    assert "json-token" not in safe_content
    assert "abcdefghijklmnop" not in safe_content
    assert "QWxhZGRpbjpvcGVuIHNlc2FtZQ==" not in safe_content
    assert "api_key=[REDACTED]" in safe_content
    assert "token=[REDACTED]" in safe_content
    assert "Authorization=[REDACTED]" in safe_content
    assert "Basic [REDACTED]" in safe_content


@pytest.mark.asyncio
async def test_get_report_content_tool_redacts_and_truncates(monkeypatch) -> None:
    report = GeneratedReport(
        id="report-1",
        title="Sensitive report",
        prompt="prompt",
        data_source="steam",
        template="default",
        generated_at=datetime(2026, 1, 1, 12, 0, 0),
        matched_records=3,
        content="token: secret-token\n" + ("detail " * 2000),
        excel_path="data/excel_reports/report.xlsx",
        metadata={
            "source_record_count": 3,
            "usable_record_count": 2,
            "source_coverage": {"steam": 1, "gtrends": 1},
            "record_completeness": {"full": 2, "empty": 1},
            "empty_record_keys": ["record:empty"],
            "template_validation": {
                "status": "partial",
                "missing_collectors": ["monitor"],
            },
            "target_context": {
                "target_name": "Counter-Strike 2",
                "game_name": "Counter-Strike 2",
                "steam_app_id": "730",
                "params_by_collector": {
                    "monitor": {"app_id": "730"},
                    "gtrends": {},
                },
                "source_collectors": ["steam"],
                "source_record_keys": ["record:steam"],
                "source_record_count": 1,
            },
        },
    )
    fake_generator = _FakeReportGenerator(report)
    monkeypatch.setattr("src.web.app.report_generator", fake_generator)

    payload = json.loads(await GetReportContentTool()._arun("report-1"))

    assert payload["success"] is True
    assert payload["report_id"] == "report-1"
    assert payload["content_truncated"] is True
    assert payload["download_url"] == "/api/reports/report-1/download"
    assert "secret-token" not in payload["content"]
    assert "token=[REDACTED]" in payload["content"]
    assert len(payload["content"]) < 8200
    assert payload["matched_records"] == 3
    assert payload["usable_record_count"] == 2
    assert payload["source_coverage"] == {"steam": 1, "gtrends": 1}
    assert payload["template_status"] == "partial"
    assert payload["missing_collectors"] == ["monitor"]
    assert payload["empty_record_count"] == 1
    assert payload["quality_status"] == "partial"
    assert payload["regeneration_recommended"] is True
    assert any("Monitor" in risk for risk in payload["coverage_risks"])
    assert any(
        action["type"] == "collect_missing_sources"
        and action["recommended_tool"] == "precheck_report"
        for action in payload["follow_up_actions"]
    )
    assert payload["target_context"] == {
        "target_name": "Counter-Strike 2",
        "game_name": "Counter-Strike 2",
        "steam_app_id": "730",
        "source_collectors": ["steam"],
        "source_record_keys": ["record:steam"],
        "source_record_count": 1,
    }
    assert payload["next_best_action"]["collector"] == "monitor"
    assert payload["next_best_action"]["create_task_draft"]["targets"][0]["params"] == {
        "app_id": "730"
    }
    assert payload["suggested_collection_actions"][0]["can_execute_now"] is True
    assert "quality_warnings" in payload


@pytest.mark.asyncio
async def test_get_report_content_tool_marks_complete_quality(monkeypatch) -> None:
    report = GeneratedReport(
        id="report-complete",
        title="Complete report",
        prompt="prompt",
        data_source="steam",
        template="steam_game",
        generated_at=datetime(2026, 1, 1, 12, 0, 0),
        matched_records=2,
        content="ok",
        excel_path="data/excel_reports/report.xlsx",
        metadata={
            "source_record_count": 2,
            "usable_record_count": 2,
            "source_coverage": {"steam": 1, "gtrends": 1, "monitor": 1},
            "record_completeness": {"full": 2},
            "template_validation": {
                "status": "complete",
                "missing_collectors": [],
            },
        },
    )
    fake_generator = _FakeReportGenerator(report)
    monkeypatch.setattr("src.web.app.report_generator", fake_generator)

    payload = json.loads(await GetReportContentTool()._arun("report-complete"))

    assert payload["success"] is True
    assert payload["quality_status"] == "complete"
    assert payload["regeneration_recommended"] is False
    assert payload["coverage_risks"] == []
    assert payload["follow_up_actions"] == []


@pytest.mark.asyncio
async def test_get_report_content_tool_guides_empty_report_quality(monkeypatch) -> None:
    report = GeneratedReport(
        id="report-empty",
        title="Empty report",
        prompt="prompt",
        data_source="steam",
        template="steam_game",
        generated_at=datetime(2026, 1, 1, 12, 0, 0),
        matched_records=1,
        content="ok",
        excel_path="data/excel_reports/report.xlsx",
        metadata={
            "source_record_count": 1,
            "usable_record_count": 0,
            "source_coverage": {},
            "record_completeness": {"empty": 1},
            "empty_record_keys": ["record:empty"],
            "template_validation": {
                "status": "partial",
                "missing_collectors": ["steam"],
            },
        },
    )
    fake_generator = _FakeReportGenerator(report)
    monkeypatch.setattr("src.web.app.report_generator", fake_generator)

    payload = json.loads(await GetReportContentTool()._arun("report-empty"))

    assert payload["success"] is True
    assert payload["quality_status"] == "empty"
    assert payload["regeneration_recommended"] is True
    assert any("No usable source records" in risk for risk in payload["coverage_risks"])
    assert any(action["type"] == "replace_empty_records" for action in payload["follow_up_actions"])
    assert payload["follow_up_actions"][-1]["type"] == "regenerate_report"


@pytest.mark.asyncio
async def test_list_reports_tool_returns_quality_summary(monkeypatch) -> None:
    report = GeneratedReport(
        id="report-1",
        title="Sensitive report Bearer abcdefghijklmnop",
        prompt='{"token": "secret-token"} api_key=secret-key analyze Counter-Strike 2',
        data_source="steam",
        template="steam_game",
        generated_at=datetime(2026, 1, 1, 12, 0, 0),
        matched_records=3,
        content="ok",
        excel_path="data/excel_reports/report.xlsx",
        metadata={
            "format": "excel",
            "source_record_count": 3,
            "usable_record_count": 2,
            "source_record_keys": ["record:steam", "record:gtrends", "record:empty"],
            "selected_record_keys": ["record:steam", "record:gtrends", "record:empty"],
            "source_coverage": {"steam": 1, "gtrends": 1},
            "record_completeness": {"full": 2, "empty": 1},
            "empty_record_keys": ["record:empty"],
            "template_validation": {
                "status": "partial",
                "missing_collectors": ["monitor"],
            },
        },
    )
    fake_generator = _FakeListReportsGenerator([report])
    monkeypatch.setattr("src.web.app.report_generator", fake_generator)

    payload = json.loads(await ListReportsTool()._arun(limit=10))
    rendered = json.dumps(payload, ensure_ascii=False)

    assert payload["success"] is True
    assert payload["record_count"] == 1
    assert payload["limit"] == 10
    assert payload["scan_limit"] == 10
    assert fake_generator.limit == 10
    item = payload["reports"][0]
    assert item["id"] == "report-1"
    assert item["download_url"] == "/api/reports/report-1/download"
    assert item["generated_at"] == "2026-01-01T12:00:00"
    assert item["matched_records"] == 3
    assert item["quality"]["format"] == "excel"
    assert item["quality"]["usable_record_count"] == 2
    assert item["quality"]["source_coverage"] == {"steam": 1, "gtrends": 1}
    assert item["quality"]["template_status"] == "partial"
    assert item["quality"]["quality_status"] == "partial"
    assert item["quality"]["regeneration_recommended"] is True
    assert any("Monitor" in risk for risk in item["quality"]["coverage_risks"])
    assert item["quality"]["missing_collectors"] == ["monitor"]
    assert item["quality"]["empty_record_count"] == 1
    assert "quality_warnings" in item["quality"]
    assert "source_record_keys" not in item["quality"]
    assert "selected_record_keys" not in item["quality"]
    assert "secret-key" not in rendered
    assert "secret-token" not in rendered
    assert "abcdefghijklmnop" not in rendered
    assert "Bearer [REDACTED]" in rendered


@pytest.mark.asyncio
async def test_list_reports_tool_filters_reports(monkeypatch) -> None:
    matching_report = GeneratedReport(
        id="report-match",
        title="Counter-Strike 2 weekly report",
        prompt="Analyze Counter-Strike 2",
        data_source="steam",
        template="steam_game",
        generated_at=datetime(2026, 1, 2, 12, 0, 0),
        matched_records=2,
        content="ok",
        excel_path="",
        metadata={
            "format": "excel",
            "template_validation": {"status": "complete"},
        },
    )
    other_report = GeneratedReport(
        id="report-other",
        title="Mobile report",
        prompt="Analyze mobile game",
        data_source="taptap",
        template="taptap_game",
        generated_at=datetime(2026, 1, 1, 12, 0, 0),
        matched_records=1,
        content="ok",
        excel_path="",
        metadata={
            "format": "markdown",
            "template_validation": {"status": "partial"},
        },
    )
    fake_generator = _FakeListReportsGenerator([matching_report, other_report])
    monkeypatch.setattr("src.web.app.report_generator", fake_generator)

    payload = json.loads(
        await ListReportsTool()._arun(
            limit=5,
            query="counter-strike",
            data_source="steam",
            template="steam_game",
            quality_status="complete",
            report_format="excel",
        )
    )

    assert payload["success"] is True
    assert payload["record_count"] == 1
    assert payload["reports"][0]["id"] == "report-match"
    assert payload["scan_limit"] == 50
    assert fake_generator.limit == 50
    assert payload["filters"] == {
        "query": "counter-strike",
        "data_source": "steam",
        "template": "steam_game",
        "quality_status": "complete",
        "report_format": "excel",
    }


@pytest.mark.asyncio
async def test_list_reports_tool_filters_by_derived_empty_quality(monkeypatch) -> None:
    empty_report = GeneratedReport(
        id="report-empty",
        title="Empty source report",
        prompt="Analyze Counter-Strike 2",
        data_source="steam",
        template="steam_game",
        generated_at=datetime(2026, 1, 2, 12, 0, 0),
        matched_records=1,
        content="ok",
        excel_path="",
        metadata={
            "source_record_count": 1,
            "usable_record_count": 0,
            "source_coverage": {},
            "empty_record_keys": ["record:empty"],
            "template_validation": {
                "status": "partial",
                "missing_collectors": ["steam"],
            },
        },
    )
    partial_report = GeneratedReport(
        id="report-partial",
        title="Partial source report",
        prompt="Analyze Counter-Strike 2",
        data_source="steam",
        template="steam_game",
        generated_at=datetime(2026, 1, 1, 12, 0, 0),
        matched_records=1,
        content="ok",
        excel_path="",
        metadata={
            "source_record_count": 1,
            "usable_record_count": 1,
            "source_coverage": {"steam": 1},
            "template_validation": {
                "status": "partial",
                "missing_collectors": ["gtrends"],
            },
        },
    )
    fake_generator = _FakeListReportsGenerator([empty_report, partial_report])
    monkeypatch.setattr("src.web.app.report_generator", fake_generator)

    payload = json.loads(await ListReportsTool()._arun(limit=10, quality_status="empty"))

    assert payload["success"] is True
    assert payload["record_count"] == 1
    assert payload["reports"][0]["id"] == "report-empty"
    assert payload["reports"][0]["quality"]["quality_status"] == "empty"


@pytest.mark.asyncio
async def test_list_reports_tool_filters_by_source_coverage(monkeypatch) -> None:
    matching_report = GeneratedReport(
        id="report-source-coverage",
        title="Coverage report",
        prompt="Analyze Counter-Strike 2",
        data_source="",
        template="steam_game",
        generated_at=datetime(2026, 1, 2, 12, 0, 0),
        matched_records=2,
        content="ok",
        excel_path="",
        metadata={
            "format": "excel",
            "source_coverage": {"steam": 1, "gtrends": 1},
            "template_validation": {
                "status": "partial",
                "available_collectors": ["steam", "gtrends"],
                "missing_collectors": ["monitor"],
            },
        },
    )
    other_report = GeneratedReport(
        id="report-taptap",
        title="Mobile report",
        prompt="Analyze mobile game",
        data_source="",
        template="taptap_game",
        generated_at=datetime(2026, 1, 1, 12, 0, 0),
        matched_records=1,
        content="ok",
        excel_path="",
        metadata={
            "format": "excel",
            "source_coverage": {"taptap": 1},
            "template_validation": {
                "status": "complete",
                "available_collectors": ["taptap"],
            },
        },
    )
    fake_generator = _FakeListReportsGenerator([matching_report, other_report])
    monkeypatch.setattr("src.web.app.report_generator", fake_generator)

    payload = json.loads(
        await ListReportsTool()._arun(
            limit=5,
            data_source="steam",
            query="monitor",
        )
    )

    assert payload["success"] is True
    assert payload["record_count"] == 1
    assert payload["reports"][0]["id"] == "report-source-coverage"
    assert payload["reports"][0]["quality"]["source_coverage"] == {
        "steam": 1,
        "gtrends": 1,
    }


@pytest.mark.asyncio
async def test_precheck_report_tool_returns_readiness_guidance(monkeypatch) -> None:
    captured = {}
    source_record = StorageRecord(
        key="record:steam",
        source="steam",
        data={"collector": "steam", "game_name": "Counter-Strike 2", "app_id": "730"},
    )

    async def fake_load_precheck_records(request):
        captured["template"] = request.template
        captured["limit"] = request.params["limit"]
        captured["record_keys"] = list(request.record_keys)
        return [source_record]

    monkeypatch.setattr(
        "src.web.routes.reports._load_report_precheck_records",
        fake_load_precheck_records,
    )

    payload = json.loads(
        await PrecheckReportTool()._arun(
            prompt="generate Steam report",
            template="steam_game",
            record_keys=["record:steam"],
            limit=10,
        )
    )

    assert payload["success"] is True
    assert payload["status"] == "partial"
    assert payload["can_generate"] is True
    assert payload["should_collect_more"] is True
    assert payload["decision"] == "ask_user_generate_now_or_collect_first"
    assert payload["readiness"] == {
        "status": "partial",
        "can_generate": True,
        "should_collect_more": True,
        "decision": "ask_user_generate_now_or_collect_first",
    }
    assert payload["coverage_summary"]["selected_records"] == 1
    assert payload["coverage_summary"]["usable_records"] == 1
    assert payload["coverage_summary"]["missing_count"] == len(payload["missing_collectors"])
    assert payload["coverage_summary"]["missing_collectors"] == payload["missing_collectors"]
    assert set(payload["coverage_summary"]["missing_labels"]) == {
        item["collector_label"] for item in payload["suggested_collection_actions"]
    }
    assert payload["coverage_summary"]["available_collectors"] == ["steam"]
    assert payload["coverage_summary"]["source_counts"] == {"steam": 1}
    assert "partial data" in payload["agent_guidance"]
    assert payload["selected_records"] == 1
    assert payload["usable_records"] == 1
    assert payload["source_counts"] == {"steam": 1}
    assert "gtrends" in payload["missing_collectors"]
    assert payload["target_context"] == {
        "target_name": "Counter-Strike 2",
        "game_name": "Counter-Strike 2",
        "steam_app_id": "730",
        "source_collectors": ["steam"],
        "source_record_keys": ["record:steam"],
        "source_record_count": 1,
    }
    suggested_actions = {
        item["collector"]: item for item in payload["suggested_collection_actions"]
    }
    assert payload["suggested_collection_actions"][0]["collector"] == "steam_discussions"
    assert payload["next_best_action"]["type"] == "collect_missing_source"
    assert payload["next_best_action"]["collector"] == "steam_discussions"
    assert payload["next_best_action"]["can_execute_now"] is True
    assert payload["next_best_action"]["recommended_sequence"] == ["create_task"]
    assert suggested_actions["gtrends"]["collector"] == "gtrends"
    assert suggested_actions["gtrends"]["collector_label"] == "Google Trends"
    assert suggested_actions["gtrends"]["priority_label"] == "low"
    assert suggested_actions["gtrends"]["can_execute_now"] is True
    assert suggested_actions["gtrends"]["missing_params"] == []
    assert "Google Trends adds external demand context" in suggested_actions["gtrends"]["why"]
    assert suggested_actions["gtrends"]["next_tool"] == "create_task"
    assert suggested_actions["gtrends"]["recommended_sequence"] == ["create_task"]
    assert suggested_actions["gtrends"]["pipeline_name"] == "gtrends_basic"
    assert suggested_actions["gtrends"]["target_hint"] == (
        "Use the game name or search keyword as the target name."
    )
    assert suggested_actions["gtrends"]["create_task_draft"] == {
        "name": "Collect Google Trends data for report",
        "pipeline_name": "gtrends_basic",
        "targets": [
            {
                "name": "Counter-Strike 2",
                "target_type": "game",
                "params": {},
            }
        ],
        "collector_name": "gtrends",
        "config": {"batch_concurrency": 1},
    }
    assert suggested_actions["monitor"]["create_task_draft"]["targets"] == [
        {
            "name": "Counter-Strike 2",
            "target_type": "game",
            "params": {"app_id": "730"},
        }
    ]
    assert suggested_actions["monitor"]["recommended_sequence"] == ["create_task"]
    assert suggested_actions["monitor"]["identifier_status"] == "ready_from_selected_records"
    assert suggested_actions["monitor"]["can_execute_now"] is True
    assert suggested_actions["monitor"]["missing_params"] == []
    assert "identifier_first" not in suggested_actions["monitor"]
    assert suggested_actions["steam_discussions"]["create_task_draft"]["targets"] == [
        {
            "name": "Counter-Strike 2",
            "target_type": "game",
            "params": {"app_id": "730"},
        }
    ]
    assert suggested_actions["steam_discussions"]["recommended_sequence"] == ["create_task"]
    assert (
        suggested_actions["steam_discussions"]["identifier_status"]
        == "ready_from_selected_records"
    )
    assert suggested_actions["events"]["recommended_sequence"] == [
        "search_game_identifiers",
        "create_task",
    ]
    assert suggested_actions["events"]["identifier_first"] == "search_game_identifiers"
    assert suggested_actions["events"]["identifier_status"] == "needs_resolution"
    assert suggested_actions["events"]["can_execute_now"] is False
    assert suggested_actions["events"]["missing_params"] == ["official_url"]
    assert suggested_actions["events"]["create_task_draft"]["collector_name"] == "official_site"
    assert suggested_actions["events"]["create_task_draft"]["source_gap_collector"] == "events"
    assert captured == {
        "template": "steam_game",
        "limit": 10,
        "record_keys": ["record:steam"],
    }


@pytest.mark.asyncio
async def test_precheck_report_tool_uses_official_site_task_for_events_gap(
    monkeypatch,
) -> None:
    steam_record = StorageRecord(
        key="record:steam",
        source="steam",
        data={"collector": "steam", "game_name": "Counter-Strike 2", "app_id": "730"},
    )
    official_record = StorageRecord(
        key="record:official",
        source="official_site",
        data={
            "collector": "official_site",
            "game_name": "Counter-Strike 2",
            "official_url": "https://www.counter-strike.net/news",
        },
    )

    async def fake_load_precheck_records(request):
        return [steam_record, official_record]

    monkeypatch.setattr(
        "src.web.routes.reports._load_report_precheck_records",
        fake_load_precheck_records,
    )

    payload = json.loads(
        await PrecheckReportTool()._arun(
            prompt="generate Steam report",
            template="steam_game",
            record_keys=["record:steam", "record:official"],
        )
    )

    suggested_actions = {
        item["collector"]: item for item in payload["suggested_collection_actions"]
    }
    events_action = suggested_actions["events"]
    assert payload["target_context"]["official_url"] == "https://www.counter-strike.net/news"
    assert events_action["recommended_sequence"] == ["create_task"]
    assert events_action["identifier_status"] == "ready_from_selected_records"
    assert "identifier_first" not in events_action
    assert events_action["create_task_draft"] == {
        "name": "Collect Events data for report",
        "pipeline_name": "official_site_basic",
        "targets": [
            {
                "name": "Counter-Strike 2",
                "target_type": "game",
                "params": {"official_url": "https://www.counter-strike.net/news"},
            }
        ],
        "collector_name": "official_site",
        "config": {"batch_concurrency": 1},
        "source_gap_collector": "events",
    }


@pytest.mark.asyncio
async def test_precheck_report_tool_blocks_redacted_task_draft_params(
    monkeypatch,
) -> None:
    steam_record = StorageRecord(
        key="record:steam",
        source="steam",
        data={"collector": "steam", "game_name": "Counter-Strike 2", "app_id": "730"},
    )
    official_record = StorageRecord(
        key="record:api_key=key-secret",
        source="official_site",
        data={
            "collector": "official_site",
            "game_name": "Counter-Strike 2",
            "official_url": "https://www.counter-strike.net/news?api_key=url-secret",
        },
    )

    async def fake_load_precheck_records(request):
        return [steam_record, official_record]

    monkeypatch.setattr(
        "src.web.routes.reports._load_report_precheck_records",
        fake_load_precheck_records,
    )

    payload = json.loads(
        await PrecheckReportTool()._arun(
            prompt="generate Steam report",
            template="steam_game",
            record_keys=["record:steam", "record:api_key=key-secret"],
        )
    )
    rendered = json.dumps(payload, ensure_ascii=False)

    suggested_actions = {
        item["collector"]: item for item in payload["suggested_collection_actions"]
    }
    events_action = suggested_actions["events"]
    assert "key-secret" not in rendered
    assert "url-secret" not in rendered
    assert payload["target_context"]["source_record_keys"] == [
        "record:steam",
        "record:api_key=[REDACTED]",
    ]
    assert (
        payload["target_context"]["official_url"]
        == "https://www.counter-strike.net/news?api_key=[REDACTED]"
    )
    assert events_action["can_execute_now"] is False
    assert events_action["sensitive_params_redacted"] is True
    assert events_action["identifier_status"] == "needs_original_sensitive_params"
    assert events_action["next_tool"] == "search_game_identifiers"
    assert events_action["recommended_sequence"] == [
        "search_game_identifiers",
        "create_task",
    ]
    assert events_action["create_task_draft"]["targets"][0]["params"] == {
        "official_url": "https://www.counter-strike.net/news?api_key=[REDACTED]",
    }


@pytest.mark.asyncio
async def test_precheck_report_tool_uses_monitor_siteurl_from_selected_records(
    monkeypatch,
) -> None:
    source_record = StorageRecord(
        key="record:steam",
        source="steam",
        data={"collector": "steam", "game_name": "Counter-Strike 2"},
        metadata={
            "source_task": {
                "collector_name": "steam",
                "target_params": {"siteurl": "counter-strike_2"},
            }
        },
    )

    async def fake_load_precheck_records(request):
        return [source_record]

    monkeypatch.setattr(
        "src.web.routes.reports._load_report_precheck_records",
        fake_load_precheck_records,
    )

    payload = json.loads(
        await PrecheckReportTool()._arun(
            prompt="generate Steam report",
            template="steam_game",
            record_keys=["record:steam"],
        )
    )

    suggested_actions = {
        item["collector"]: item for item in payload["suggested_collection_actions"]
    }
    monitor_action = suggested_actions["monitor"]
    assert payload["target_context"]["siteurl"] == "counter-strike_2"
    assert monitor_action["recommended_sequence"] == ["create_task"]
    assert monitor_action["identifier_status"] == "ready_from_selected_records"
    assert "identifier_first" not in monitor_action
    assert monitor_action["create_task_draft"]["targets"] == [
        {
            "name": "Counter-Strike 2",
            "target_type": "game",
            "params": {"siteurl": "counter-strike_2"},
        }
    ]


@pytest.mark.asyncio
async def test_precheck_report_tool_falls_back_to_prompt_without_record_identity(
    monkeypatch,
) -> None:
    source_record = StorageRecord(
        key="record:steam",
        source="steam",
        data={"collector": "steam", "steamdb": {}},
    )

    async def fake_load_precheck_records(request):
        return [source_record]

    monkeypatch.setattr(
        "src.web.routes.reports._load_report_precheck_records",
        fake_load_precheck_records,
    )

    payload = json.loads(
        await PrecheckReportTool()._arun(
            prompt="generate Steam report",
            template="steam_game",
            record_keys=["record:steam"],
        )
    )

    suggested_actions = {
        item["collector"]: item for item in payload["suggested_collection_actions"]
    }
    assert payload["target_context"] == {
        "target_name": "generate Steam report",
        "source_collectors": ["steam"],
        "source_record_keys": ["record:steam"],
        "source_record_count": 1,
    }
    assert suggested_actions["gtrends"]["create_task_draft"]["targets"] == [
        {
            "name": "generate Steam report",
            "target_type": "game",
            "params": {},
        }
    ]
    assert suggested_actions["monitor"]["recommended_sequence"] == [
        "search_game_identifiers",
        "create_task",
    ]
    assert suggested_actions["monitor"]["identifier_first"] == "search_game_identifiers"
    assert suggested_actions["monitor"]["identifier_status"] == "needs_resolution"
    assert suggested_actions["monitor"]["can_execute_now"] is False
    assert suggested_actions["monitor"]["missing_params"] == ["app_id", "siteurl"]
    assert payload["next_best_action"]["collector"] == "gtrends"
    assert payload["next_best_action"]["can_execute_now"] is True


@pytest.mark.asyncio
async def test_precheck_report_tool_redacts_exception_text(monkeypatch) -> None:
    async def fail_load_precheck_records(request):
        raise RuntimeError("bad precheck: api_key=secret-key; token: secret-token")

    monkeypatch.setattr(
        "src.web.routes.reports._load_report_precheck_records",
        fail_load_precheck_records,
    )

    payload = json.loads(
        await PrecheckReportTool()._arun(
            prompt="generate report",
            record_keys=["record:api_key=secret-key"],
        )
    )

    assert payload["success"] is False
    assert "secret-key" not in payload["error"]
    assert "secret-token" not in payload["error"]
    assert "api_key=[REDACTED]" in payload["error"]
    assert "token=[REDACTED]" in payload["error"]


@pytest.mark.asyncio
async def test_generate_report_tool_redacts_missing_record_key(monkeypatch) -> None:
    fake_store = _MissingRecordStore()
    monkeypatch.setattr("src.storage.factory.get_storage", lambda: fake_store)

    payload = json.loads(
        await GenerateReportTool()._arun(
            prompt="generate report",
            record_keys=["upload:api_key=secret-key"],
        )
    )

    assert payload["success"] is False
    assert "secret-key" not in payload["error"]
    assert "api_key=[REDACTED]" in payload["error"]
    assert fake_store.closed is True


@pytest.mark.asyncio
async def test_generate_report_tool_rejects_report_history_record_keys(monkeypatch) -> None:
    report_record = StorageRecord(
        key="report:history",
        source="reporting",
        data={"title": "Generated report"},
        metadata={"kind": "report", "template": "default"},
    )
    fake_store = _RecordStore({report_record.key: report_record})
    monkeypatch.setattr("src.storage.factory.get_storage", lambda: fake_store)

    payload = json.loads(
        await GenerateReportTool()._arun(
            prompt="generate report",
            record_keys=[report_record.key],
        )
    )

    assert payload["success"] is False
    assert "report history" in payload["error"]
    assert payload["excluded_report_record_keys"] == ["report:history"]
    assert fake_store.closed is True


@pytest.mark.asyncio
async def test_generate_report_tool_filters_report_history_record_keys(monkeypatch) -> None:
    report_record = StorageRecord(
        key="report:history",
        source="reporting",
        data={"title": "Generated report"},
        metadata={"kind": "report", "template": "default"},
    )
    source_record = StorageRecord(
        key="record:steam",
        source="steam",
        data={"collector": "steam", "game_name": "Counter-Strike 2", "app_id": "730"},
    )
    fake_store = _RecordStore(
        {
            report_record.key: report_record,
            source_record.key: source_record,
        }
    )
    fake_generator = _FakeGenerateExcelReportGenerator()
    monkeypatch.setattr("src.storage.factory.get_storage", lambda: fake_store)
    monkeypatch.setattr("src.web.app.report_generator", fake_generator)

    payload = json.loads(
        await GenerateReportTool()._arun(
            prompt="generate report",
            record_keys=[report_record.key, source_record.key],
        )
    )

    assert payload["success"] is True
    assert payload["download_url"] == "/api/reports/report-generated/download"
    assert [record.key for record in fake_generator.records] == ["record:steam"]
    assert fake_generator.metadata == {
        "selected_record_keys": ["record:steam"],
        "excluded_report_record_keys": ["report:history"],
    }
    assert payload["matched_records"] == 1
    assert payload["usable_record_count"] == 1
    assert payload["source_coverage"] == {"steam": 1}
    assert payload["template_status"] == "partial"
    assert payload["missing_collectors"] == ["gtrends"]
    assert payload["excluded_report_record_keys"] == ["report:history"]
    assert payload["quality_status"] == "partial"
    assert payload["regeneration_recommended"] is True
    assert any("Google Trends" in risk for risk in payload["coverage_risks"])
    assert any(
        action["type"] == "collect_missing_sources"
        and action["missing_collectors"] == ["gtrends"]
        for action in payload["follow_up_actions"]
    )
    assert payload["target_context"]["target_name"] == "Counter-Strike 2"
    assert payload["target_context"]["steam_app_id"] == "730"
    assert payload["suggested_collection_actions"][0]["collector"] == "gtrends"
    assert payload["suggested_collection_actions"][0]["create_task_draft"]["targets"] == [
        {
            "name": "Counter-Strike 2",
            "target_type": "game",
            "params": {},
        }
    ]
    assert payload["next_best_action"]["collector"] == "gtrends"
    assert "quality_warnings" in payload


@pytest.mark.asyncio
async def test_generate_report_tool_redacts_generator_exception_text(monkeypatch) -> None:
    source_record = StorageRecord(
        key="record:steam",
        source="steam",
        data={"collector": "steam", "game_name": "Counter-Strike 2"},
    )
    fake_store = _RecordStore({source_record.key: source_record})
    fake_generator = _FailingGenerateExcelReportGenerator()
    monkeypatch.setattr("src.storage.factory.get_storage", lambda: fake_store)
    monkeypatch.setattr("src.web.app.report_generator", fake_generator)

    payload = json.loads(
        await GenerateReportTool()._arun(
            prompt="generate report",
            record_keys=[source_record.key],
        )
    )

    assert payload["success"] is False
    assert "secret-key" not in payload["error"]
    assert "secret-token" not in payload["error"]
    assert "api_key=[REDACTED]" in payload["error"]
    assert "token=[REDACTED]" in payload["error"]


class _FakeReportGenerator:
    def __init__(self, report: GeneratedReport) -> None:
        self.report = report

    async def get_report(self, report_id: str):
        if report_id == self.report.id:
            return self.report
        return None


class _FakeListReportsGenerator:
    def __init__(self, reports: list[GeneratedReport]) -> None:
        self.reports = reports
        self.limit = None

    async def list_reports(self, limit: int = 20):
        self.limit = limit
        return self.reports[:limit]


class _MissingRecordStore:
    def __init__(self) -> None:
        self.closed = False

    async def initialize(self) -> None:
        return None

    async def close(self) -> None:
        self.closed = True

    async def load(self, key: str):
        return None


class _RecordStore:
    def __init__(self, records_by_key: dict[str, StorageRecord]) -> None:
        self.records_by_key = records_by_key
        self.closed = False

    async def initialize(self) -> None:
        return None

    async def close(self) -> None:
        self.closed = True

    async def load(self, key: str):
        return self.records_by_key.get(key)


class _FakeGenerateExcelReportGenerator:
    def __init__(self) -> None:
        self.records: list[StorageRecord] = []
        self.metadata = None

    async def generate_excel(self, **kwargs):
        from src.services._utils import derive_collection_target_context

        self.records = list(kwargs["records"])
        self.metadata = kwargs.get("metadata")
        return GeneratedReport(
            id="report-generated",
            title="Generated report",
            prompt=kwargs["prompt"],
            data_source=kwargs.get("data_source", ""),
            template=kwargs.get("template", "default"),
            generated_at=datetime(2026, 1, 1, 12, 0, 0),
            matched_records=len(self.records),
            content="ok",
            excel_path="data/excel_reports/report.xlsx",
            metadata={
                **(self.metadata or {}),
                "source_record_count": len(self.records),
                "usable_record_count": len(self.records),
                "source_coverage": {"steam": len(self.records)},
                "record_completeness": {"full": len(self.records)},
                "target_context": derive_collection_target_context(
                    self.records,
                    prompt=kwargs["prompt"],
                    data_source=kwargs.get("data_source", ""),
                ),
                "template_validation": {
                    "status": "partial",
                    "missing_collectors": ["gtrends"],
                },
            },
        )


class _FailingGenerateExcelReportGenerator:
    async def generate_excel(self, **kwargs):
        raise RuntimeError("upstream failed: api_key=secret-key; token: secret-token")
