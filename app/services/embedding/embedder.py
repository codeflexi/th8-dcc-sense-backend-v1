from langchain_openai import OpenAIEmbeddings
from app.core.config import settings
from app.core.errors import ConfigError

class Embedder:
    def __init__(self):
        if not settings.OPENAI_API_KEY:
            raise ConfigError("Missing OPENAI_API_KEY")
        self._emb = OpenAIEmbeddings(model=settings.OPENAI_EMBEDDING_MODEL, api_key=settings.OPENAI_API_KEY)

    def embed_texts(self, texts: list[str]) -> list[list[float]]:
        return self._emb.embed_documents(texts)
