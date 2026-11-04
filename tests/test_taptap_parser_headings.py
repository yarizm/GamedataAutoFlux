"""TapTap 页面标题的弯引号写法（What’s new）必须和直引号写法一样被识别。"""

import pytest
from autoflux_plugin_taptap.taptap.parser import (
    STOP_SECTION_HEADERS,
    UPDATES_HEADINGS,
    _parse_updates,
)


@pytest.mark.parametrize("heading", ["What's new", "What’s new"])
def test_parse_updates_recognizes_both_apostrophes(heading: str) -> None:
    lines = [heading, "2026-01-02", "Season two is live", "What’s happening", "unrelated"]

    updates = _parse_updates(lines)

    assert [item.get("published_at") for item in updates] == ["2026-01-02"]
    assert "Season two is live" in str(updates[0])
    assert "unrelated" not in str(updates)


def test_curly_apostrophe_headings_are_declared() -> None:
    assert "What’s new" in UPDATES_HEADINGS
    assert "What’s happening" in STOP_SECTION_HEADERS
