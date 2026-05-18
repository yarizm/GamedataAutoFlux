"""Smoke tests — verify all core modules can be imported and instantiated."""

import pytest


def test_import_core_modules():
    from src.core.errors import ErrorCode, classify_exception

    assert ErrorCode.unknown.value == "unknown"
    assert callable(classify_exception)


def test_import_services():
    from src.services import (
        TaskService,
        DataService,
        ReportService,
    )

    assert TaskService is not None
    assert DataService is not None
    assert ReportService is not None


def test_import_collectors():
    from src.collectors.base import BaseCollector

    assert BaseCollector is not None


def test_import_storage():
    from src.storage.local_store import LocalStorage

    assert LocalStorage is not None


def test_import_reporting():
    from src.reporting.report_templates import list_report_templates

    templates = list_report_templates()
    assert isinstance(templates, list)
    assert len(templates) > 0


def test_import_agent():
    from src.agent.tools import ALL_TOOLS

    assert len(ALL_TOOLS) > 0


def test_import_web():
    from src.web.app import app
    from src.web.safety import require_explicit_confirmation

    assert app is not None
    with pytest.raises(Exception):
        require_explicit_confirmation(False, "test operation")


def test_error_code_completeness():
    from src.core.errors import ErrorCode

    for code in ErrorCode:
        assert code.chinese_label, f"{code} missing chinese_label"
        assert code.suggestion, f"{code} missing suggestion"
        assert code.severity in ("warning", "error"), f"{code} invalid severity"


def test_config_schema():
    from src.core.config_schema import validate_settings_payload

    result = validate_settings_payload({"server": {"host": "0.0.0.0", "port": 8000}})
    assert result.get("valid") is True
