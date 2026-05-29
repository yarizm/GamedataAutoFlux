from typing import Any
from loguru import logger
from src.services._utils import get_embeddings

from langchain_text_splitters import RecursiveCharacterTextSplitter

from src.core.registry import registry
from src.processors.base import BaseProcessor, ProcessInput, ProcessOutput
from src.core.config import get_settings


@registry.register("processor", "vectorizer")
class VectorizerProcessor(BaseProcessor):
    """
    A pipeline processor that extracts text from the collected data
    and uses an embedding model (like Qwen via OpenAI compatible API)
    to generate vector representations. It saves the embedding in
    metadata['embedding'].
    """

    def __init__(self, config: dict[str, Any] | None = None):
        super().__init__(config)
        self.settings = get_settings()

        self.embeddings = get_embeddings()

    async def process(self, data: Any, context: dict[str, Any] | None = None) -> Any:
        # Not used since we override process_batch
        return data

    def _extract_text(self, data: Any) -> str:
        if isinstance(data, str):
            return data
        if isinstance(data, dict):
            return " ".join(self._extract_text(v) for v in data.values() if v)
        if isinstance(data, list):
            return " ".join(self._extract_text(v) for v in data if v)
        return str(data)

    async def process_batch(self, inputs: list[ProcessInput]) -> list[ProcessOutput]:
        results = []
        if not self.embeddings:
            logger.warning("[Vectorizer] Embeddings not configured. Skipping vectorization.")
            return [
                ProcessOutput(
                    success=True,
                    data=p_in.data,
                    metadata=p_in.metadata,
                    processor_name="vectorizer",
                )
                for p_in in inputs
            ]

        # Prepare valid texts and map them to their corresponding inputs
        valid_items = []
        texts_to_embed = []

        splitter = RecursiveCharacterTextSplitter(chunk_size=4000, chunk_overlap=200)

        for p_in in inputs:
            if not p_in.data:
                results.append(
                    ProcessOutput(
                        success=True,
                        data=p_in.data,
                        metadata=p_in.metadata,
                        processor_name="vectorizer",
                    )
                )
                continue

            text = self._extract_text(p_in.data).strip()
            if not text:
                results.append(
                    ProcessOutput(
                        success=True,
                        data=p_in.data,
                        metadata=p_in.metadata,
                        processor_name="vectorizer",
                    )
                )
                continue

            # If text is too long, chunk it and just take the first meaningful chunk for vectorization.
            # For full RAG, we would store chunks separately, but here we embed the document's main semantic core.
            if len(text) > 4000:
                chunks = splitter.split_text(text)
                text = chunks[0] if chunks else text[:4000]

            valid_items.append(p_in)
            texts_to_embed.append(text)

        if not texts_to_embed:
            return results

        # Batch API call
        try:
            logger.info(f"[Vectorizer] Embedding batch of {len(texts_to_embed)} documents...")
            # Split into mini-batches of 10 to avoid payload limits if needed
            vectors = []
            for i in range(0, len(texts_to_embed), 10):
                batch_texts = texts_to_embed[i : i + 10]
                batch_vectors = await self.embeddings.aembed_documents(batch_texts)
                vectors.extend(batch_vectors)

            for p_in, vector in zip(valid_items, vectors):
                meta = dict(p_in.metadata or {})
                meta["embedding"] = vector
                results.append(
                    ProcessOutput(
                        success=True, data=p_in.data, metadata=meta, processor_name="vectorizer"
                    )
                )
        except Exception as e:
            logger.error(f"[Vectorizer] Failed to embed batch: {e}")
            # Fallback to saving without embeddings
            for p_in in valid_items:
                results.append(
                    ProcessOutput(
                        success=True,
                        data=p_in.data,
                        metadata=p_in.metadata,
                        processor_name="vectorizer",
                    )
                )

        return results
