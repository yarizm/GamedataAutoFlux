"""
Embedding 向量化处理器。

当前先提供可运行的 stub 版本，负责:
  - 从输入数据中提取可检索文本
  - 生成稳定的伪向量元数据，便于后续替换为真实 embedding
  - 保持 Pipeline 与向量存储接口稳定
"""

from __future__ import annotations

import hashlib
import json
import os
from typing import Any

import httpx
from loguru import logger

from src.core.config import get as get_config
from src.core.registry import registry
from src.processors.base import BaseProcessor, ProcessInput, ProcessOutput


@registry.register("processor", "embedding")
class EmbeddingProcessor(BaseProcessor):
    """
    Embedding 向量化处理器。

    支持的 provider:
      - stub: 默认实现，生成可重复的伪向量与文本摘要
      - qwen: 通过阿里云百炼 DashScope 文本向量接口生成真实向量
      - openai/local: 预留配置，当前回落到 stub
    """

    def __init__(self, config: dict[str, Any] | None = None):
        super().__init__(config)
        self.provider = self.config.get("provider") or get_config("embedding.provider", "stub")
        self.model_name = self._resolve_model_name()
        self.dimensions = int(self.config.get("dimensions") or self._resolve_dimensions())
        self.text_type = self.config.get("text_type") or self._resolve_text_type()
        self.output_type = self.config.get("output_type") or get_config(
            "embedding.qwen.output_type", "dense"
        )
        self.instruct = self.config.get("instruct") or get_config("embedding.qwen.instruct")
        self.timeout = float(self.config.get("timeout") or get_config("embedding.qwen.timeout", 30))
        self.fallback_to_stub = bool(
            self.config.get(
                "fallback_to_stub",
                get_config("embedding.qwen.fallback_to_stub", True),
            )
        )
        self.qwen_api_key = (
            self.config.get("api_key")
            or get_config("embedding.qwen.api_key", "")
            or os.getenv("DASHSCOPE_API_KEY", "")
        )
        self.qwen_base_url = (
            self.config.get("base_url")
            or get_config("embedding.qwen.base_url", "https://dashscope.aliyuncs.com/api/v1")
        ).rstrip("/")

    async def process(self, input_data: ProcessInput) -> ProcessOutput:
        logger.debug(f"[Embedding] provider={self.provider}, source={input_data.source}")

        extracted_text = self._extract_text(input_data.data)
        embedding, provider_used, extra_metadata = await self._generate_embedding([extracted_text])

        return ProcessOutput(
            data={
                "content": input_data.data,
                "embedding_text": extracted_text,
                "embedding": embedding[0],
            },
            metadata={
                **input_data.metadata,
                "embedded": provider_used != "stub",
                "embedding_provider": provider_used,
                "embedding_model": self.model_name,
                "embedding_dimensions": len(embedding[0]),
                **extra_metadata,
            },
            processor_name="embedding",
            success=True,
        )

    async def process_batch(self, inputs: list[ProcessInput]) -> list[ProcessOutput]:
        if self.provider != "qwen":
            return await super().process_batch(inputs)

        outputs: list[ProcessOutput] = []
        chunk_size = 10

        for start in range(0, len(inputs), chunk_size):
            chunk = inputs[start:start + chunk_size]
            texts = [self._extract_text(item.data) for item in chunk]
            embeddings, provider_used, extra_metadata = await self._generate_embedding(texts)

            for input_data, text, embedding in zip(chunk, texts, embeddings):
                outputs.append(
                    ProcessOutput(
                        data={
                            "content": input_data.data,
                            "embedding_text": text,
                            "embedding": embedding,
                        },
                        metadata={
                            **input_data.metadata,
                            "embedded": provider_used != "stub",
                            "embedding_provider": provider_used,
                            "embedding_model": self.model_name,
                            "embedding_dimensions": len(embedding),
                            **extra_metadata,
                        },
                        processor_name="embedding",
                        success=True,
                    )
                )

        return outputs

    def _resolve_model_name(self) -> str:
        if self.provider == "qwen":
            return self.config.get("model") or get_config("embedding.qwen.model", "text-embedding-v4")
        if self.provider == "openai":
            return self.config.get("model") or get_config("embedding.openai.model", "text-embedding-3-small")
        if self.provider == "local":
            return self.config.get("model_name") or get_config(
                "embedding.local.model_name",
                "sentence-transformers/all-MiniLM-L6-v2",
            )
        return "stub-hash-8d"

    def _extract_text(self, data: Any) -> str:
        if data is None:
            return ""
        if isinstance(data, str):
            return data.strip()
        if isinstance(data, dict):
            preferred_fields = ["name", "game_name", "title", "summary", "description", "short_description"]
            parts: list[str] = []
            for field in preferred_fields:
                value = data.get(field)
                if isinstance(value, str) and value.strip():
                    parts.append(value.strip())
            if "snapshot" in data and isinstance(data["snapshot"], dict):
                for value in data["snapshot"].values():
                    if isinstance(value, str) and value.strip():
                        parts.append(value.strip())
            if not parts:
                parts.append(json.dumps(data, ensure_ascii=False, default=str))
            return "\n".join(parts)
        if isinstance(data, list):
            return "\n".join(self._extract_text(item) for item in data[:10] if item is not None)
        return str(data)

    async def _generate_embedding(
        self,
        texts: list[str],
    ) -> tuple[list[list[float]], str, dict[str, Any]]:
        if self.provider == "qwen":
            try:
                response = await self._embed_with_qwen(texts)
                return (
                    response["embeddings"],
                    "qwen",
                    {
                        "embedding_text_type": self.text_type,
                        "embedding_output_type": self.output_type,
                        "embedding_request_id": response.get("request_id"),
                        "embedding_usage": response.get("usage", {}),
                    },
                )
            except Exception as exc:
                logger.warning(f"[Embedding] Qwen 调用失败，fallback={self.fallback_to_stub}: {exc}")
                if not self.fallback_to_stub:
                    raise
                return (
                    [self._build_stub_embedding(text) for text in texts],
                    "stub",
                    {
                        "embedding_fallback_reason": str(exc),
                        "embedding_text_type": self.text_type,
                    },
                )

        return ([self._build_stub_embedding(text) for text in texts], "stub", {})

    async def _embed_with_qwen(self, texts: list[str]) -> dict[str, Any]:
        if not self.qwen_api_key:
            raise ValueError(
                "Qwen embedding 需要 DASHSCOPE_API_KEY 或 embedding.qwen.api_key"
            )

        payload: dict[str, Any] = {
            "model": self.model_name,
            "input": {"texts": texts},
            "parameters": {
                "dimension": self.dimensions,
                "text_type": self.text_type,
                "output_type": self.output_type,
            },
        }
        if self.instruct:
            payload["parameters"]["instruct"] = self.instruct

        timeout = httpx.Timeout(self.timeout)
        headers = {
            "Authorization": f"Bearer {self.qwen_api_key}",
            "Content-Type": "application/json",
        }
        endpoint = f"{self.qwen_base_url}/services/embeddings/text-embedding/text-embedding"

        async with httpx.AsyncClient(timeout=timeout) as client:
            response = await client.post(endpoint, headers=headers, json=payload)
            if response.is_error:
                try:
                    error_payload = response.json()
                except Exception:
                    error_payload = response.text
                raise ValueError(
                    f"Qwen embedding request failed: status={response.status_code}, "
                    f"body={error_payload}"
                )
            data = response.json()

        if data.get("code"):
            raise ValueError(f"{data.get('code')}: {data.get('message', 'unknown error')}")

        embeddings = [
            item.get("embedding", [])
            for item in data.get("output", {}).get("embeddings", [])
        ]
        if len(embeddings) != len(texts):
            raise ValueError(
                f"Qwen embedding 返回数量异常: expected={len(texts)}, actual={len(embeddings)}"
            )

        return {
            "embeddings": embeddings,
            "usage": data.get("usage", {}),
            "request_id": data.get("request_id"),
        }

    def _resolve_dimensions(self) -> int:
        if self.provider == "qwen":
            return int(get_config("embedding.qwen.dimensions", 1024))
        return 8

    def _resolve_text_type(self) -> str:
        if self.provider == "qwen":
            return get_config("embedding.qwen.text_type", "document")
        return "document"

    def _build_stub_embedding(self, text: str) -> list[float]:
        """
        用哈希生成稳定的伪向量。

        这不是语义向量，但能保证:
          - 相同输入得到相同结果
          - 返回固定维度，便于后续平滑切换真实 provider
        """
        digest = hashlib.sha256(text.encode("utf-8")).digest()
        # 取前 8 个字节，映射到 [-1, 1]
        return [round((byte / 127.5) - 1, 6) for byte in digest[:8]]
