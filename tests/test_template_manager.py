"""Tests for TemplateManager and YAML custom template loading."""

import yaml

from src.reporting.report_templates import (
    TemplateManager,
    ReportTemplate,
    get_report_template,
    validate_template_sources,
)


class TestTemplateManagerCustomYaml:
    def test_custom_yaml_loaded_alongside_builtins(self, tmp_path):
        """Custom YAML templates should be loaded alongside built-in templates."""
        template_dir = tmp_path / "templates"
        template_dir.mkdir(parents=True, exist_ok=True)
        yaml_file = template_dir / "custom_test.yaml"
        yaml_content = {
            "name": "Test Custom",
            "description": "Test Desc",
            "required_collectors": ["steam"],
            "prompt_instruction": "Test instruction",
        }
        with open(yaml_file, "w", encoding="utf-8") as f:
            yaml.dump(yaml_content, f)

        manager = TemplateManager(template_dir=template_dir)
        manager.load_all()

        # Should contain built-ins + custom
        template = manager.get_template("custom_test")
        assert template is not None
        assert template.name == "Test Custom"
        assert template.is_custom is True

        # Check built-in
        assert manager.get_template("general_game") is not None

    def test_missing_template_dir_does_not_error(self, tmp_path):
        """TemplateManager should not error when template directory doesn't exist."""
        template_dir = tmp_path / "nonexistent"
        manager = TemplateManager(template_dir=template_dir)
        manager.load_all()  # should not raise

        # Built-in templates should still be available
        assert manager.get_template("steam_game") is not None
        assert len(manager.list_templates()) >= 3  # at least the 3 built-ins

    def test_save_and_delete_custom_template(self, tmp_path):
        """save_template and delete_template should persist custom templates."""
        template_dir = tmp_path / "templates"
        template_dir.mkdir(parents=True, exist_ok=True)
        manager = TemplateManager(template_dir=template_dir)
        manager.load_all()

        # Save
        manager.save_template(
            "saved_test",
            {
                "name": "Saved Template",
                "description": "Saved via manager",
                "required_collectors": ["taptap"],
            },
        )
        template = manager.get_template("saved_test")
        assert template is not None
        assert template.name == "Saved Template"
        assert template.is_custom is True

        # Delete
        result = manager.delete_template("saved_test")
        assert result is True
        assert manager.get_template("saved_test") is None

    def test_cannot_delete_builtin_template(self, tmp_path):
        """Built-in templates should not be deletable."""
        template_dir = tmp_path / "templates"
        manager = TemplateManager(template_dir=template_dir)
        manager.load_all()

        result = manager.delete_template("general_game")
        assert result is False
        assert manager.get_template("general_game") is not None

    def test_malformed_yaml_skipped(self, tmp_path):
        """Malformed YAML should be skipped with error logged, not crash."""
        template_dir = tmp_path / "templates"
        template_dir.mkdir(parents=True, exist_ok=True)
        bad_file = template_dir / "bad.yaml"
        bad_file.write_text("{{{ not valid yaml", encoding="utf-8")

        manager = TemplateManager(template_dir=template_dir)
        manager.load_all()  # should not raise

        assert manager.get_template("bad") is None
        # Built-ins should still be present
        assert manager.get_template("general_game") is not None

    def test_custom_template_to_dict_includes_is_custom(self, tmp_path):
        """ReportTemplate.to_dict should include the is_custom field."""
        t = ReportTemplate(
            id="test",
            name="Test",
            description="Desc",
            required_collectors=("steam",),
            is_custom=True,
        )
        d = t.to_dict()
        assert d["is_custom"] is True

    def test_validates_custom_template_sources(self, tmp_path, monkeypatch):
        """validate_template_sources should work with custom templates."""
        from src.reporting import report_templates
        manager = TemplateManager(template_dir=tmp_path / "templates")
        manager.save_template(
            "custom_v2",
            {
                "name": "Custom V2",
                "description": "Test",
                "required_collectors": ["custom_source"],
            },
        )
        monkeypatch.setattr(report_templates, "_manager", manager)

        result = validate_template_sources("custom_v2", {"custom_source": 3})
        assert result["status"] == "complete"
        assert result["known_template"] is True
