from __future__ import annotations
from dataclasses import dataclass
from app.repositories.storage_repo import StorageRepository
from app.repositories.document_repo import DocumentRepository
from app.repositories.page_repo import PageRepository
from app.repositories.clause_repo import ClauseRepository
from app.repositories.price_repo import PriceItemRepository
from app.repositories.chunk_repo import ChunkRepository
from app.repositories.ingestion_repo import IngestionEventRepository, IngestionJobRepository
from app.services.parsing.page_reader import read_pages_with_llamaparse
from app.services.extraction.clause_extractor_llm import ClauseExtractor
from app.services.extraction.price_table_extractor import extract_price_rows_from_pages
from app.services.chunking.chunker import chunk_pages
from app.services.embedding.embedder import Embedder
from app.services.extraction.header_extractor_llm import HeaderExtractor
from app.repositories.document_header_repo import DocumentHeaderRepository
from app.services.extraction.header_deterministic_enricher import HeaderDeterministicEnricher
from app.services.semantic.semantic_extractor import SemanticExtractor
from app.services.semantic.semantic_validator import SemanticValidator
from app.services.semantic.semantic_to_header_mapper import (
    SemanticToHeaderMapper
)
from app.services.embedding.embedding_service import EmbeddingService




@dataclass
class IngestionCounters:
    pages_written: int = 0
    clauses_written: int = 0
    price_items_written: int = 0
    chunks_written: int = 0

class IngestionPipeline:
    def __init__(self):
        self.storage = StorageRepository()
        self.docs = DocumentRepository()
        self.pages = PageRepository()
        self.clauses = ClauseRepository()
        self.prices = PriceItemRepository()
        self.chunks = ChunkRepository()
        self.jobs = IngestionJobRepository()
        self.events = IngestionEventRepository()
        self.embedder = None
        self.clause_extractor = ClauseExtractor()
        
        # ✅ MISSING — ต้องมี
        self.document_header_extractor = HeaderExtractor()
        self.document_headers = DocumentHeaderRepository()
        self.enricher = HeaderDeterministicEnricher()
        self.semantic_extractor = SemanticExtractor()
        self.semantic_validator = SemanticValidator()
        self.semantic_mapper = SemanticToHeaderMapper()
        self.embed = EmbeddingService()  # Set to False to disable header extraction

    @staticmethod
    def _merge_non_null(base: dict, overlay: dict) -> dict:
        """
        Overlay values only if NOT None.
        Prevents semantic/deterministic from null-overwriting LLM.
        """
        out = dict(base)
        for k, v in overlay.items():
            if v is not None:
                out[k] = v
        return out

    async def run(self, *, job: dict, entity_id: str, entity_type: str, contract_id: str | None, filename: str, content_type: str, data: bytes):
        job_id = job["job_id"]
        document_id = job["document_id"]
        warnings: list[str] = []
        counters = IngestionCounters()

        # STEP 0: upload
        storage_key = f"{entity_id}/{document_id}/{filename}"
        self.events.append(job_id=job_id, document_id=document_id, event_type="DOC_UPLOAD_STARTED")
        self.storage.upload_bytes(storage_key=storage_key, data=data, content_type=content_type)
        self.docs.update_storage_key(document_id, storage_key)
        self.events.append(job_id=job_id, document_id=document_id, event_type="DOC_UPLOAD_OK", payload={"storage_key": storage_key})

        # STEP 1: parse pages with LlamaParse (markdown + metadata)
        self.events.append(job_id=job_id, document_id=document_id, event_type="DOC_PARSE_STARTED")
        pages = await read_pages_with_llamaparse(data, filename=filename)
        self.events.append(job_id=job_id, document_id=document_id, event_type="DOC_PARSE_OK", payload={"page_count": len(pages)})

        # STEP 2: persist pages (stable citation)
        page_rows = [{"document_id": document_id, "page_number": p["page_number"], "page_text": p.get("text","")} for p in pages]
        self.events.append(job_id=job_id, document_id=document_id, event_type="PAGES_WRITE_STARTED")
        
        # PRE-CLEAN (important order)
        self.clauses.delete_by_document(document_id=document_id)
        self.prices.delete_by_document(document_id=document_id)
        self.chunks.delete_by_document(document_id=document_id)
        #self.document_headers.delete_by_document(document_id=document_id)

        
        counters.pages_written = self.pages.replace_pages(document_id=document_id, pages=page_rows)
        self.events.append(job_id=job_id, document_id=document_id, event_type="PAGES_WRITTEN", payload={"count": counters.pages_written})

       
        # STEP 3: clauses (LLM structured)
        self.events.append(job_id=job_id, document_id=document_id, event_type="CLAUSE_EXTRACT_STARTED")
        clause_res = self.clause_extractor.extract_from_pages(pages)
        warnings.extend(clause_res.warnings)
        clause_rows = []
        if contract_id:
            for c in clause_res.clauses:
                page_id = self.pages.resolve_page_id(document_id=document_id, page_number=c["page_number"])
                clause_rows.append({
                    "contract_id": contract_id,
                    "document_id": document_id,
                    "page_id": page_id,
                    "page_number": c["page_number"],
                    "clause_type": c["clause_type"],
                    "clause_title": c["clause_title"],
                    "clause_text": c["clause_text"],
                    "structured_data": c["structured_data"],
                    "extraction_method": c["extraction_method"],
                    "extraction_confidence": c["extraction_confidence"],
                })
            counters.clauses_written = self.clauses.replace_by_contract(contract_id=contract_id, rows=clause_rows)
        else:
            warnings.append("NO_CONTRACT_ID_CLAUSES_SKIPPED")
        self.events.append(job_id=job_id, document_id=document_id, event_type="CLAUSES_WRITTEN", payload={"count": counters.clauses_written, "warnings": warnings})

        # STEP 4: price items (baseline deterministic)
        self.events.append(job_id=job_id, document_id=document_id, event_type="PRICE_EXTRACT_STARTED")
        price_rows, rejected = extract_price_rows_from_pages(pages)
        if rejected:
            warnings.append("PRICE_ITEMS_PARTIALLY_REJECTED")
            self.events.append(job_id=job_id, document_id=document_id, event_type="PRICE_ITEMS_REJECTED", payload={"count": len(rejected), "sample": rejected[:20]})
        price_db_rows = []
        if contract_id:
            for r in price_rows:
                page_id = self.pages.resolve_page_id(document_id=document_id, page_number=r.page_number)
                price_db_rows.append({
                    "contract_id": contract_id,
                    "document_id": document_id,
                    "page_id": page_id,
                    "page_number": r.page_number,
                    "sku": r.sku,
                    "item_name": r.name,
                    "unit_price": r.unit_price,
                    "currency": r.currency,
                    "uom": r.uom,
                    "snippet": r.snippet,
                    "confidence_score": r.confidence,
                    "highlight_text": r.highlight_text,
                })
            counters.price_items_written = self.prices.replace_by_contract(contract_id=contract_id, rows=price_db_rows)
        else:
            warnings.append("NO_CONTRACT_ID_PRICE_ITEMS_SKIPPED")
        self.events.append(job_id=job_id, document_id=document_id, event_type="PRICE_ITEMS_WRITTEN", payload={"count": counters.price_items_written})

        # STEP 5: chunks
        self.events.append(job_id=job_id, document_id=document_id, event_type="CHUNKS_BUILD_STARTED")
        chunk_rows_raw = chunk_pages(pages)
        chunk_rows = []
        for ch in chunk_rows_raw:
            page_id = self.pages.resolve_page_id(document_id=document_id,page_number=ch["page_number"])
            chunk_rows.append({
                "document_id": document_id,
                "page_id": page_id,
                  #"chunk_text": ch["text"],
                "chunk_type": "NARRATIVE",
                "page_number": ch["page_number"],
                "content": ch["text"],
                "metadata": {"entity_id": entity_id, "entity_type": entity_type,"contract_id": contract_id},
            })
        inserted_chunks = self.chunks.replace_by_document(document_id=document_id, rows=chunk_rows)
        counters.chunks_written = len(inserted_chunks)
        
        self.events.append(job_id=job_id, document_id=document_id, event_type="CHUNKS_WRITTEN", payload={"count": counters.chunks_written})

        # STEP 6: embeddings (logging only; persist after schema confirms returning ids)
        try:
            self.events.append(job_id=job_id, document_id=document_id, event_type="EMBED_STARTED")
            self.embedder = self.embedder or Embedder()
            self.embed = self.embed or EmbeddingService()
            # texts = [r["chunk_text"] for r in chunk_rows][:64]
            # vecs = self.embedder.embed_texts(texts) if texts else []
            # self.events.append(job_id=job_id, document_id=document_id, event_type="EMBED_OK", payload={"count": len(vecs), "note": "Persist vectors with chunk_id batch update once schema is confirmed"})
            for ch in inserted_chunks:
                text = ch["content"]
                # vec = self.embedder.embed_texts([text])[0]
                vec = self.embed.embed(text)
                self.chunks.update_embedding(
                    chunk_id=ch["chunk_id"],
                    embedding=vec
                )
            self.events.append(job_id=job_id, document_id=document_id, event_type="EMBED_OK", payload={"count": len(inserted_chunks), "note": "Persist vectors with chunk_id batch update once schema is confirmed"})
        except Exception as e:
            print("Embedded Error")
            print(str(e))
            warnings.append("EMBEDDING_STALE")
            self.events.append(job_id=job_id, document_id=document_id, event_type="EMBED_FAILED", payload={"error": str(e)})

        # finalize
        ctr = counters.__dict__
        self.jobs.mark_done(job_id, counters=ctr, warnings=warnings)
        self.events.append(job_id=job_id, document_id=document_id, event_type="JOB_DONE", payload={"counters": ctr, "warnings": warnings})
        return ctr, warnings
