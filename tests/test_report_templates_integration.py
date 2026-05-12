"""Integration tests for report templates using fixture data."""

from src.reporting.report_templates import (
    ReportTemplate,
    REPORT_TEMPLATES,
    list_report_templates,
    get_report_template,
    is_structured_template,
    normalize_collector,
    validate_template_sources,
)


class TestReportTemplateModel:
    def test_to_dict(self):
        t = ReportTemplate(
            id="test_template",
            name="Test",
            description="A test template",
            required_collectors=("steam", "taptap"),
            optional_collectors=("qimai",),
            prompt_instruction="Do a report.",
        )
        d = t.to_dict()
        assert d["id"] == "test_template"
        assert d["name"] == "Test"
        assert d["required_collectors"] == ["steam", "taptap"]
        assert d["optional_collectors"] == ["qimai"]

    def test_defaults(self):
        t = ReportTemplate(id="t", name="n", description="d", required_collectors=())
        assert t.optional_collectors == ()
        assert t.prompt_instruction == ""


class TestReportTemplatesRegistry:
    def test_known_templates(self):
        assert "general_game" in REPORT_TEMPLATES
        assert "steam_game" in REPORT_TEMPLATES
        assert "taptap_game" in REPORT_TEMPLATES

    def test_general_game_requirements(self):
        t = REPORT_TEMPLATES["general_game"]
        assert "steam" in t.required_collectors
        assert "taptap" in t.required_collectors

    def test_steam_game_requirements(self):
        t = REPORT_TEMPLATES["steam_game"]
        assert "steam" in t.required_collectors
        assert "taptap" not in t.required_collectors

    def test_taptap_game_requirements(self):
        t = REPORT_TEMPLATES["taptap_game"]
        assert t.required_collectors == ("taptap",)


class TestListReportTemplates:
    def test_returns_list(self):
        templates = list_report_templates()
        assert isinstance(templates, list)
        assert len(templates) == 3

    def test_each_has_required_keys(self):
        for t in list_report_templates():
            for key in ("id", "name", "description", "required_collectors"):
                assert key in t


class TestGetReportTemplate:
    def test_known(self):
        t = get_report_template("steam_game")
        assert t is not None
        assert t.id == "steam_game"

    def test_unknown(self):
        assert get_report_template("nonexistent") is None


class TestIsStructuredTemplate:
    def test_known(self):
        assert is_structured_template("general_game") is True

    def test_unknown(self):
        assert is_structured_template("custom_template") is False


class TestNormalizeCollector:
    def test_known_alias(self):
        assert normalize_collector("google_trends") == "gtrends"
        assert normalize_collector("pytrends") == "gtrends"
        assert normalize_collector("steam_api") == "steam"
        assert normalize_collector("firecrawl") == "steam"
        assert normalize_collector("official_website") == "official_site"

    def test_unknown_passthrough(self):
        assert normalize_collector("custom_source") == "custom_source"

    def test_case_insensitive(self):
        assert normalize_collector("Steam_API") == "steam"

    def test_none_returns_unknown(self):
        assert normalize_collector(None) == "unknown"

    def test_empty_returns_unknown(self):
        assert normalize_collector("") == "unknown"


class TestValidateTemplateSources:
    def test_complete_template(self):
        result = validate_template_sources("taptap_game", {"taptap": 5})
        assert result["status"] == "complete"
        assert result["missing_collectors"] == []

    def test_partial_template(self):
        result = validate_template_sources("steam_game", {"steam": 2})
        assert result["status"] == "partial"
        assert "gtrends" in result["missing_collectors"]
        assert "monitor" in result["missing_collectors"]

    def test_unknown_template(self):
        result = validate_template_sources("custom", {"any_source": 10})
        assert result["status"] == "unchecked"
        assert result["known_template"] is False

    def test_normalizes_collector_names(self):
        result = validate_template_sources("steam_game", {
            "steam_api": 2,  # alias for steam
            "google_trends": 1,  # alias for gtrends
        })
        assert "steam" in result["available_collectors"]
        assert "gtrends" in result["available_collectors"]

    def test_zero_counts_filtered(self):
        result = validate_template_sources("steam_game", {
            "steam": 5,
            "gtrends": 0,
        })
        assert "gtrends" not in result["available_collectors"]

    def test_full_template_coverage(self):
        """Validate general_game template with all required sources."""
        result = validate_template_sources("general_game", {
            "steam": 5,
            "taptap": 3,
            "gtrends": 2,
            "monitor": 1,
            "events": 4,
            "steam_discussions": 2,
        })
        assert result["status"] == "complete"
        assert result["known_template"] is True
        assert len(result["missing_collectors"]) == 0


class TestCollectorLabels:
    def test_labels_exist(self):
        from src.reporting.report_templates import COLLECTOR_LABELS
        assert COLLECTOR_LABELS["steam"] == "Steam"
        assert COLLECTOR_LABELS["taptap"] == "TapTap"
        assert COLLECTOR_LABELS["gtrends"] == "Google Trends"
