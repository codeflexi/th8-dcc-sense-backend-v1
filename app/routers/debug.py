from fastapi import APIRouter , Query   
from pydantic import BaseModel

from app.services.embedding.embedding_service import EmbeddingService
from app.repositories.vector_discovery_repo import VectorDiscoveryRepository

# router = APIRouter(prefix="/debug", tags=["debug"])
router = APIRouter()



class EmbedRequest(BaseModel):
    text: str


@router.post("/embedding")
def debug_embedding(req: EmbedRequest):
    """
    Debug-only endpoint.
    Purpose:
    - Verify embedding service works
    - Inspect vector length + sample values
    - Use the SAME embedding path as discovery
    """

    embedding = EmbeddingService.embed(req.text)

    return {
        "model": EmbeddingService.MODEL,
        "input_text": req.text,
        "embedding_dim": len(embedding),
        "embedding_preview": embedding[:10],  # ดูแค่ 10 ค่าแรก
        "ready_for_vector_rpc": True
    }

@router.get("/vector-search")
def debug_vector_search(
    q: str = Query(...),
    min_similarity: float = 0.3
):
    embedding = EmbeddingService.embed(q)

    repo = VectorDiscoveryRepository()
    hits = repo.discover_documents(
        query_embedding=embedding,
        min_similarity=min_similarity
    )

    return {
        "query": q,
        "embedding_dim": len(embedding),
        "hit_count": len(hits),
        "hits": hits
    }