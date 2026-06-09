import json

import pytest

from src.agent.tools.identifiers import VerifyGameIdentifierTool
from src.agent.tools.semantic_search import SemanticSearchTool
from src.agent.tools.utils import _format_result, _safe_error_text, _safe_json


def test_format_result_redacts_sensitive_fields() -> None:
    payload = _format_result(
        "ok",
        "done",
        {"api_key": "secret", "nested": {"cookie": "session"}},
    )

    parsed = json.loads(payload)

    assert parsed["data"]["api_key"] == "[REDACTED]"
    assert parsed["data"]["nested"]["cookie"] == "[REDACTED]"
    assert "secret" not in payload
    assert "session" not in payload


def test_safe_json_redacts_nested_model_like_payloads() -> None:
    payload = _safe_json({"items": [{"token": "abc"}, {"value": 1}]})

    assert "abc" not in payload
    assert "[REDACTED]" in payload


def test_safe_json_redacts_sensitive_text_values() -> None:
    payload = _safe_json(
        {
            "error": "api_key=secret-key; token: secret-token",
            "header": "Bearer abcdefghijklmnop",
        }
    )

    assert "secret-key" not in payload
    assert "secret-token" not in payload
    assert "abcdefghijklmnop" not in payload
    assert "api_key=[REDACTED]" in payload
    assert "token=[REDACTED]" in payload
    assert "Bearer [REDACTED]" in payload


def test_safe_json_redacts_embedded_json_style_secret_text() -> None:
    payload = _safe_json(
        {
            "content": 'raw response {"api_key": "json-secret", "token": "token-secret"}',
        }
    )

    assert "json-secret" not in payload
    assert "token-secret" not in payload
    assert "api_key=[REDACTED]" in payload
    assert "token=[REDACTED]" in payload


def test_safe_json_keeps_redaction_marker_idempotent() -> None:
    payload = _safe_json({"content": "token=[REDACTED]"})

    assert "token=[REDACTED]" in payload
    assert "token=[REDACTED]]" not in payload


def test_safe_error_text_redacts_embedded_secrets() -> None:
    text = _safe_error_text("failed with api_key=secret-key; Bearer abcdefghijklmnop")

    assert text == "failed with api_key=[REDACTED]; Bearer [REDACTED]"
    assert "secret-key" not in text
    assert "abcdefghijklmnop" not in text


@pytest.mark.asyncio
async def test_semantic_search_embed_error_uses_structured_redacted_result(monkeypatch) -> None:
    class FakeEmbeddings:
        async def aembed_query(self, query):
            raise RuntimeError("embedding failed: api_key=embed-secret")

    monkeypatch.setattr("src.agent.tools.semantic_search.get_embeddings", lambda: FakeEmbeddings())

    payload = json.loads(await SemanticSearchTool()._arun("secret query"))

    assert payload["status"] == "error"
    assert "embed-secret" not in payload["summary"]
    assert "api_key=[REDACTED]" in payload["summary"]


@pytest.mark.asyncio
async def test_verify_game_identifier_redacts_exception_text(monkeypatch) -> None:
    class FakeResolver:
        async def setup(self):
            return None

        async def verify_identifier(self, platform, identifier, game_name):
            raise RuntimeError("verify failed: token=identifier-secret")

        async def teardown(self):
            return None

    monkeypatch.setattr("src.services.game_resolver.GameIdentifierResolver", FakeResolver)

    payload = json.loads(
        await VerifyGameIdentifierTool()._arun("steam", "730", "Counter-Strike 2")
    )

    assert payload["valid"] is False
    assert "identifier-secret" not in payload["error"]
    assert "token=[REDACTED]" in payload["error"]
