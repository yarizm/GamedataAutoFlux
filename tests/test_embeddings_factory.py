"""get_embeddings 工厂：DashScope 走 OpenAI 兼容接口，不依赖额外的 SDK 包。

回归：曾用 langchain_community 的 DashScopeEmbeddings，它需要未声明的 dashscope 包，构造时
ModuleNotFoundError 被吞成 None，语义检索和 vectorizer 因此一直不可用。
"""

from __future__ import annotations

from typing import Any

import pytest
from langchain_openai import OpenAIEmbeddings

from src.services._utils import get_embeddings


def _settings(monkeypatch: pytest.MonkeyPatch, qwen: dict[str, Any]) -> None:
    monkeypatch.setattr("src.core.config.get_settings", lambda: {"llm": {"qwen": qwen}})


def test_embeddings_send_plain_strings_in_small_batches(monkeypatch: pytest.MonkeyPatch) -> None:
    _settings(monkeypatch, {"api_key": "sk-test", "base_url": "https://example.invalid/v1"})

    embeddings = get_embeddings()

    assert isinstance(embeddings, OpenAIEmbeddings)
    assert embeddings.openai_api_base == "https://example.invalid/v1"

    batches: list[list[Any]] = []

    class FakeClient:
        def create(self, *, input: list[Any], **kwargs: Any) -> dict[str, Any]:  # noqa: A002
            batches.append(input)
            return {"data": [{"embedding": [0.5]} for _ in input]}

    embeddings.client = FakeClient()

    vectors = embeddings.embed_documents([f"文本 {index}" for index in range(12)])

    assert len(vectors) == 12
    assert [len(batch) for batch in batches] == [10, 2]
    # DashScope 不接受 token 数组
    assert all(isinstance(item, str) for batch in batches for item in batch)


def test_embeddings_default_to_dashscope_compatible_endpoint(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _settings(monkeypatch, {"api_key": "sk-test"})

    embeddings = get_embeddings()

    assert embeddings.openai_api_base == "https://dashscope.aliyuncs.com/compatible-mode/v1"
    assert embeddings.model == "text-embedding-v2"


@pytest.mark.parametrize("api_key", [None, "", "   ", "${DASHSCOPE_API_KEY}"])
def test_embeddings_unavailable_without_real_api_key(
    monkeypatch: pytest.MonkeyPatch, api_key: str | None
) -> None:
    # 不能退回到环境里的 OPENAI_API_KEY，否则会把别家的密钥发给 DashScope
    monkeypatch.setenv("OPENAI_API_KEY", "sk-should-not-be-used")
    _settings(monkeypatch, {"api_key": api_key})

    assert get_embeddings() is None
