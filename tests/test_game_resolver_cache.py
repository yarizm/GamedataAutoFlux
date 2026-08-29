"""Game resolver cache failures are isolated and observable."""

import json
import time

from src.agent.schemas import IdentifierResult
from src.services import game_resolver


def test_load_cache_skips_corrupt_entries(monkeypatch, tmp_path) -> None:
    valid = IdentifierResult(
        platform="steam",
        identifier="123",
        identifier_type="steam_app_id",
        game_name="Example",
        source="cache",
    )
    path = tmp_path / "cache.json"
    path.write_text(
        json.dumps(
            {
                "steam:ok": [time.monotonic(), valid.model_dump(mode="json")],
                "steam:bad": ["not-a-timestamp", {"broken": True}],
                "malformed": {"unexpected": "shape"},
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(game_resolver, "GAME_RESOLVER_CACHE", path)
    monkeypatch.setattr(game_resolver, "_cache_loaded", False)
    monkeypatch.setattr(game_resolver, "_names_cache", {})

    game_resolver._load_cache()

    assert "steam:ok" in game_resolver._names_cache
    assert "steam:bad" not in game_resolver._names_cache
    assert "malformed" not in game_resolver._names_cache
