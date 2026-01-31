from app.repositories.base import BaseRepository


class VectorDiscoveryRepository(BaseRepository):
    """
    Encapsulate vector-based document discovery (pgvector / rpc)
    """
    RPC_NAME = "dcc_vector_discover_documents_v1"
     
    def discover_documents(
        self,
        query_embedding: list[float],
        top_k_chunks: int = 50,
        top_k_docs: int = 15,
        min_similarity: float = 0.35,
        top_chunks_per_doc: int = 3,
    ) -> list[dict]:

        res = self.sb.rpc(
            self.RPC_NAME,
            {
                "query_embedding": query_embedding,
                "p_top_k_chunks": int(top_k_chunks),
                "p_top_k_docs": int(top_k_docs),
                "p_min_similarity": float(min_similarity),  # สำคัญ
                "p_top_chunks_per_doc": int(top_chunks_per_doc),
            }
        ).execute()

        return res.data or []
