from typing import List
from langchain_openai import OpenAIEmbeddings
from app.core.config import settings


class EmbeddingService:
    """
    Production-safe embedding service using LangChain OpenAIEmbeddings.
    Must match the same model used by ingestion chunks.
    """

    # ✅ ต้องให้ตรงกับ ingestion (ดูใน embedder.py ของ ingestion ว่าใช้รุ่นไหน)
    MODEL = settings.OPENAI_EMBEDDING_MODEL #"text-embedding-3-small"

    _embedder = OpenAIEmbeddings(
        model=MODEL,
        api_key=settings.OPENAI_API_KEY,
    )

    @classmethod
    def embed(cls, text: str) -> List[float]:
        if not text or not text.strip():
            raise ValueError("EmbeddingService.embed(): text is empty")

        # ✅ LangChain API ที่ถูกต้อง
        vec = cls._embedder.embed_query(text.strip())

        # sanity check
        if not isinstance(vec, list) or not vec:
            raise RuntimeError("EmbeddingService.embed(): embedding is empty/invalid")

        return vec
