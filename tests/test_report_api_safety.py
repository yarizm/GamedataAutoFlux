from datetime import datetime

from src.reporting.data_extractor import extract_from_records
from src.reporting.generator import GeneratedReport, ReportSummary
from src.web.routes.reports import _to_report_response, _to_summary_response


def test_report_response_redacts_sensitive_content_and_metadata() -> None:
    report = GeneratedReport(
        id="report-sensitive",
        title="api_key=title-secret",
        content="Report body token=content-secret",
        prompt="Analyze api_key=prompt-secret",
        data_source="steam",
        template="general_game",
        generated_at=datetime(2026, 1, 1, 12, 0, 0),
        matched_records=1,
        metadata={"api_key": "metadata-secret", "nested": {"token": "nested-secret"}},
    )

    response = _to_report_response(report)
    rendered = response.model_dump_json()

    assert response.title == "api_key=[REDACTED]"
    assert response.content == "Report body token=[REDACTED]"
    assert response.prompt == "Analyze api_key=[REDACTED]"
    assert response.metadata == {"api_key": "[REDACTED]", "nested": {"token": "[REDACTED]"}}
    assert response.quality["quality_status"] == "unknown"
    assert "title-secret" not in rendered
    assert "content-secret" not in rendered
    assert "prompt-secret" not in rendered
    assert "metadata-secret" not in rendered
    assert "nested-secret" not in rendered


def test_report_summary_response_redacts_sensitive_display_fields() -> None:
    report = ReportSummary(
        id="report-summary-sensitive",
        title="api_key=title-secret",
        prompt="Analyze token=prompt-secret",
        data_source="steam",
        template="general_game",
        generated_at=datetime(2026, 1, 1, 12, 0, 0),
        matched_records=1,
        metadata={
            "format": "excel",
            "source_record_count": 1,
            "template_validation": {"status": "api_key=validation-secret"},
        },
    )

    response = _to_summary_response(report)
    rendered = response.model_dump_json()

    assert response.title == "api_key=[REDACTED]"
    assert response.prompt == "Analyze token=[REDACTED]"
    assert response.quality["template_status"] == "api_key=[REDACTED]"
    assert response.quality["quality_status"] == "api_key=[REDACTED]"
    assert "title-secret" not in rendered
    assert "prompt-secret" not in rendered
    assert "validation-secret" not in rendered


def test_extracted_raw_sources_are_redacted_for_excel_appendices() -> None:
    extracted = extract_from_records(
        [
            {
                "collector": "generic",
                "game_name": "Game token=name-secret",
                "api_key": "data-secret",
                "nested": {"cookie": "cookie-secret"},
            }
        ],
        record_keys=["record:api_key=key-secret"],
        metadata_list=[{"authorization": "Bearer metadata-secret"}],
    )

    rendered = str(extracted.raw_sources)
    raw_source = extracted.raw_sources[0]

    assert raw_source["key"] == "record:api_key=[REDACTED]"
    assert raw_source["game_name"] == "Game token=[REDACTED]"
    assert raw_source["metadata"]["authorization"] == "[REDACTED]"
    assert raw_source["data"]["api_key"] == "[REDACTED]"
    assert raw_source["data"]["nested"]["cookie"] == "[REDACTED]"
    assert "key-secret" not in rendered
    assert "name-secret" not in rendered
    assert "data-secret" not in rendered
    assert "cookie-secret" not in rendered
    assert "metadata-secret" not in rendered
