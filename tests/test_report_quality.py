from src.reporting.quality import assess_report_quality, build_report_quality_summary


def test_build_report_quality_summary_marks_partial_and_risks() -> None:
    summary = build_report_quality_summary(
        {
            "format": "excel",
            "source_record_count": 3,
            "usable_record_count": 2,
            "source_coverage": {"steam": 1, "gtrends": 1},
            "record_completeness": {"full": 2, "empty": 1},
            "empty_record_keys": ["record:empty"],
            "template_validation": {
                "status": "partial",
                "missing_collectors": ["monitor"],
            },
        }
    )

    assert summary["quality_status"] == "partial"
    assert summary["quality_summary"] == (
        "Report was generated with partial source coverage; missing sources: Monitor."
    )
    assert summary["regeneration_recommended"] is True
    assert summary["coverage_risks"] == [
        "Template coverage is missing required sources: Monitor.",
        "1 selected records had no usable data.",
    ]
    assert "follow_up_actions" not in summary


def test_assess_report_quality_can_include_agent_follow_up_actions() -> None:
    guidance = assess_report_quality(
        {
            "matched_records": 1,
            "source_record_count": 1,
            "usable_record_count": 0,
            "source_coverage": {},
            "template_status": "partial",
            "missing_collectors": ["steam"],
            "empty_record_count": 1,
        },
        include_follow_up_actions=True,
    )

    assert guidance["quality_status"] == "empty"
    assert guidance["regeneration_recommended"] is True
    action_types = [action["type"] for action in guidance["follow_up_actions"]]
    assert action_types == [
        "select_or_collect_source_records",
        "collect_missing_sources",
        "replace_empty_records",
        "regenerate_report",
    ]


def test_report_quality_marks_complete_but_stale_sources() -> None:
    guidance = assess_report_quality(
        {
            "source_record_count": 2,
            "usable_record_count": 2,
            "source_coverage": {"steam": 1, "gtrends": 1},
            "template_status": "complete",
            "source_freshness": {"max_age_days": 45, "warning_days": 30},
        },
        include_follow_up_actions=True,
    )

    assert guidance["quality_status"] == "stale"
    assert guidance["regeneration_recommended"] is True
    assert any("45 days old" in risk for risk in guidance["coverage_risks"])
    action_types = [action["type"] for action in guidance["follow_up_actions"]]
    assert action_types == ["refresh_stale_sources", "regenerate_report"]
