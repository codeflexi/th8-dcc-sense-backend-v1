from __future__ import annotations
from dataclasses import dataclass
import datetime

from app.repositories.storage_repo import StorageRepository
from app.repositories.document_repo import DocumentRepository
from app.repositories.page_repo import PageRepository
from app.repositories.clause_repo import ClauseRepository
from app.repositories.price_repo import PriceItemRepository
from app.repositories.chunk_repo import ChunkRepository
from app.repositories.ingestion_repo import (
    IngestionEventRepository,
    IngestionJobRepository,
)
from app.repositories.document_header_repo import DocumentHeaderRepository

from app.services.parsing.page_reader import read_pages_with_llamaparse
from app.services.extraction.clause_extractor_llm import ClauseExtractor
from app.services.extraction.price_table_extractor import extract_price_rows_from_pages
from app.services.chunking.chunker import chunk_pages
from app.services.embedding.embedder import Embedder
from app.services.extraction.header_extractor_llm import HeaderExtractor
from app.services.extraction.header_deterministic_enricher import HeaderDeterministicEnricher
from app.services.semantic.semantic_extractor import SemanticExtractor
from app.services.semantic.semantic_validator import SemanticValidator
from app.services.semantic.semantic_to_header_mapper import SemanticToHeaderMapper
from app.services.embedding.embedding_service import EmbeddingService



from app.services.ingestion.supersession_resolver import SupersessionResolver 
from app.services.ingestion.document_meta_rules import (
    normalize_doc_type,
    infer_document_role,
    build_signal_flags,
    build_classification_trace,
    build_extraction_summary,
)
from app.services.extraction.extracted_fields_validator import validate_extracted_fields


from typing import Any

def _json_safe(obj: Any) -> Any:
    """
    Recursively convert non-JSON-serializable objects (date/datetime)
    into ISO-8601 strings.
    """
    if obj is None:
        return None

    if isinstance(obj, (datetime.date, datetime.datetime)):
        return obj.isoformat()

    if isinstance(obj, dict):
        return {k: _json_safe(v) for k, v in obj.items()}

    if isinstance(obj, (list, tuple)):
        return [_json_safe(v) for v in obj]

    return obj


@dataclass
class IngestionCounters:
    pages_written: int = 0
    clauses_written: int = 0
    price_items_written: int = 0
    chunks_written: int = 0


class IngestionPipeline:
    """
    IngestionPipeline (enterprise-grade wiring)

    RULES:
    - One Supabase client (sb) per pipeline execution
    - All repositories share the same sb
    - No Repository() without sb
    """

    def __init__(self, sb):
        self.sb = sb

        # -------------------------------------------------
        # Repositories (ALL MUST SHARE sb)
        # -------------------------------------------------
        self.storage = StorageRepository(sb)
        self.docs = DocumentRepository(sb)
        self.pages = PageRepository(sb)
        self.clauses = ClauseRepository(sb)
        self.prices = PriceItemRepository(sb)
        self.chunks = ChunkRepository(sb)
        self.jobs = IngestionJobRepository(sb)
        self.events = IngestionEventRepository(sb)
        self.document_headers = DocumentHeaderRepository(sb)

        # -------------------------------------------------
        # Services (stateless / safe)
        # -------------------------------------------------
        self.embedder = None
        self.embed = EmbeddingService()

        self.clause_extractor = ClauseExtractor()
        self.document_header_extractor = HeaderExtractor()
        self.enricher = HeaderDeterministicEnricher()
        self.semantic_extractor = SemanticExtractor()
        self.semantic_validator = SemanticValidator()
        self.semantic_mapper = SemanticToHeaderMapper()
        
           # -------------------------------------------------
        # New (enterprise doc governance)
        # -------------------------------------------------
        self.supersession = SupersessionResolver(self.docs)

    # -------------------------------------------------
    # Helpers
    # -------------------------------------------------
    @staticmethod
    def _merge_non_null(base: dict, overlay: dict) -> dict:
        out = dict(base)
        for k, v in overlay.items():
            if v is not None:
                out[k] = v
        return out


  

    # -------------------------------------------------
    # Main pipeline
    # -------------------------------------------------
    async def run(
        self,
        *,
        job: dict,
        entity_id: str,
       
        contract_id: str | None,
        filename: str,
        content_type: str,
        data: bytes,
    ):
        job_id = job["job_id"]
        document_id = job["document_id"]

        warnings: list[str] = []
        counters = IngestionCounters()

        # =================================================
        # STEP 0: Upload
        # =================================================
        storage_key = f"{entity_id}/{document_id}/{filename}"

        self.events.append(
            job_id=job_id,
            document_id=document_id,
            event_type="DOC_UPLOAD_STARTED",
        )

        self.storage.upload_bytes(
            storage_key=storage_key,
            data=data,
            content_type=content_type,
        )

        self.docs.update_storage_key(document_id, storage_key)

        self.events.append(
            job_id=job_id,
            document_id=document_id,
            event_type="DOC_UPLOAD_OK",
            payload={"storage_key": storage_key},
        )

        # =================================================
        # STEP 1: Parse pages (LlamaParse)
        # =================================================
        self.events.append(
            job_id=job_id,
            document_id=document_id,
            event_type="DOC_PARSE_STARTED",
        )

        pages = await read_pages_with_llamaparse(data, filename=filename)

        self.events.append(
            job_id=job_id,
            document_id=document_id,
            event_type="DOC_PARSE_OK",
            payload={"page_count": len(pages)},
        )

        # =================================================
        # STEP 2: Persist pages (citation anchor)
        # =================================================
        page_rows = [
            {
                "document_id": document_id,
                "page_number": p["page_number"],
                "page_text": p.get("text", ""),
            }
            for p in pages
        ]

        self.events.append(
            job_id=job_id,
            document_id=document_id,
            event_type="PAGES_WRITE_STARTED",
        )

        # PRE-CLEAN (order matters)
        self.clauses.delete_by_document(document_id=document_id)
        self.prices.delete_by_document(document_id=document_id)
        self.chunks.delete_by_document(document_id=document_id)
        # self.document_headers.delete_by_document(document_id=document_id)

        counters.pages_written = self.pages.replace_pages(
            document_id=document_id,
            pages=page_rows,
        )

        self.events.append(
            job_id=job_id,
            document_id=document_id,
            event_type="PAGES_WRITTEN",
            payload={"count": counters.pages_written},
        )
        
       
       
        # =================================================
        # STEP 2.5: Header extraction + deterministic enrich + meta update
        # =================================================

        header = {}
        doc_type = "OTHER"
        doc_role = None
        eff_from = None
        eff_to = None

        self.events.append(
            job_id=job_id,
            document_id=document_id,
            event_type="DOC_HEADER_EXTRACT_STARTED",
            payload={},
        )

        try:
            # 1️⃣ LLM extract
            hdr_res = self.document_header_extractor.extract_document_header(pages)
            warnings.extend(hdr_res.warnings or [])
            header = hdr_res.header or {}

            # 2️⃣ Deterministic enrich
            header = self.enricher.enrich(pages=pages, header=header)

            # 3️⃣ Normalize
            doc_type = normalize_doc_type(header.get("doc_type"))
            doc_role = infer_document_role(doc_type, contract_id)

            eff_from = header.get("effective_from")  # date or None
            eff_to = header.get("effective_to")

            confidence = float(header.get("confidence") or hdr_res.confidence or 0.0)

            # 4️⃣ Signals
            signals = build_signal_flags(pages)
            # Build small, safe snippets (truncate + strip)
            snippets = []
            for p in pages[:2]:
                txt = (p.get("text", "") or "").strip()
                if txt:
                    snippets.append(txt[:300])   # limit to 300 chars

            # 5️⃣ Classification (JSON-safe by design)
            class_trace = build_classification_trace(
                method="LLM_HEADER+RULE_ENRICH",
                final_type=doc_type,
                final_role=doc_role,
                confidence=confidence,
                signals=signals,
                evidence={
                    "page_numbers": [p.get("page_number") for p in pages[:2]],
                    "snippets": snippets,
                },
            )

            # 6️⃣ Extraction summary (convert date → string here)
            extraction_summary = build_extraction_summary(
                effective_from=eff_from,
                effective_to=eff_to,
                extraction_method="LLM_HEADER",
                confidence=confidence,
                page_number=(pages[0].get("page_number") if pages else None),
                raw_from=None,
                raw_to=None,
            )

            # 7️⃣ Upsert header table (DB expects date → pass date)
            try:
                self.document_headers.upsert(
                    document_id=document_id,
                    header={
                        "doc_type": doc_type,
                        "doc_title": header.get("doc_title"),
                        "doc_number": header.get("doc_number"),
                        "effective_from": eff_from,
                        "effective_to": eff_to,
                        "parties": header.get("parties") or {},
                        "extracted_fields": header.get("extracted_fields") or {},
                        "extraction_method": "LLM_HEADER",
                        "confidence": confidence,
                    },
                )

                self.events.append(
                    job_id=job_id,
                    document_id=document_id,
                    event_type="DOC_HEADER_ROW_UPSERT_OK",
                    payload={
                        "doc_type": doc_type,
                        "confidence": confidence,
                    },
                )

            except Exception as e:
                warnings.append("DOC_HEADER_ROW_UPSERT_FAILED")
                self.events.append(
                    job_id=job_id,
                    document_id=document_id,
                    event_type="DOC_HEADER_ROW_UPSERT_FAILED",
                    payload={"error": str(e), "doc_type": doc_type},
                )

           
            # 8️⃣ Update canonical meta (date columns → date, JSON → safe)
            
            meta_payload = {
                "document_type": doc_type,
                "document_role": doc_role,
                "effective_from": eff_from.isoformat() if eff_from else None,
                "effective_to": eff_to.isoformat() if eff_to else None,
                "source_system": "USER_UPLOAD",
                "classification": class_trace,
                "extraction_summary": extraction_summary,
            }

            meta_payload = _json_safe(meta_payload)

            self.docs.update_meta(
                document_id=document_id,
                **meta_payload,
            )

            self.events.append(
                job_id=job_id,
                document_id=document_id,
                event_type="DOC_META_UPDATED",
                payload=_json_safe({
                    "document_type": doc_type,
                    "document_role": doc_role,
                    "effective_from": eff_from,
                    "effective_to": eff_to,
                    "confidence": confidence,
                    "signals": signals,
                })
                )

        except Exception as e:
            warnings.append("DOC_HEADER_META_FAILED")
            self.events.append(
                job_id=job_id,
                document_id=document_id,
                event_type="DOC_HEADER_META_FAILED",
                payload={"error": str(e)},
            )


        # =================================================
        # STEP 2.6: SupersessionResolver (version detection)
        # =================================================
        try:
            doc = self.docs.get(document_id) or {}

            doc_type2 = (doc.get("document_type") or "OTHER").upper()
            eff_from_raw = doc.get("effective_from")

            # Parse ISO string to date if needed
            new_eff_date = None
            if isinstance(eff_from_raw, str):
                try:
                    from datetime import date
                    new_eff_date = date.fromisoformat(eff_from_raw)
                except Exception:
                    new_eff_date = None
            elif eff_from_raw:
                new_eff_date = eff_from_raw

            self.events.append(
                job_id=job_id,
                document_id=document_id,
                event_type="DOC_SUPERSESSION_STARTED",
                payload=_json_safe({
                    "document_type": doc_type2,
                    "effective_from": eff_from_raw,
                }),
            )

            sup = self.supersession.resolve(
                new_document_id=document_id,
                entity_id=entity_id,
                contract_id=contract_id,
                document_type=doc_type2,
                new_effective_from=new_eff_date,
            )

            if sup.warnings:
                warnings.extend(sup.warnings)

            self.events.append(
                job_id=job_id,
                document_id=document_id,
                event_type="DOC_SUPERSESSION_RESOLVED",
                payload={
                    "applied": sup.applied,
                    "superseded_document_ids": sup.superseded_document_ids,
                    "summary": sup.summary,
                    "warnings": sup.warnings,
                },
            )

        except Exception as e:
            warnings.append("DOC_SUPERSESSION_FAILED")
            self.events.append(
                job_id=job_id,
                document_id=document_id,
                event_type="DOC_SUPERSESSION_FAILED",
                payload={"error": str(e)},
            )


        
       
        # =================================================
        # STEP 3: Clause extraction (LLM)
        # =================================================
        self.events.append(
            job_id=job_id,
            document_id=document_id,
            event_type="CLAUSE_EXTRACT_STARTED",
        )

        clause_res = self.clause_extractor.extract_from_pages(pages)
        warnings.extend(clause_res.warnings)

        clause_rows = []

        if contract_id:
            for c in clause_res.clauses:
                page_id = self.pages.resolve_page_id(
                    document_id=document_id,
                    page_number=c["page_number"],
                )
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

            counters.clauses_written = self.clauses.replace_by_contract(
                contract_id=contract_id,
                rows=clause_rows,
            )
        else:
            warnings.append("NO_CONTRACT_ID_CLAUSES_SKIPPED")

        self.events.append(
            job_id=job_id,
            document_id=document_id,
            event_type="CLAUSES_WRITTEN",
            payload={
                "count": counters.clauses_written,
                "warnings": warnings,
            },
        )

        # =================================================
        # STEP 4: Price items (deterministic)
        # =================================================
        self.events.append(
            job_id=job_id,
            document_id=document_id,
            event_type="PRICE_EXTRACT_STARTED",
        )

        price_rows, rejected = extract_price_rows_from_pages(pages)

        if rejected:
            warnings.append("PRICE_ITEMS_PARTIALLY_REJECTED")
            self.events.append(
                job_id=job_id,
                document_id=document_id,
                event_type="PRICE_ITEMS_REJECTED",
                payload={
                    "count": len(rejected),
                    "sample": rejected[:20],
                },
            )

        price_db_rows = []

        if contract_id:
            for r in price_rows:
                page_id = self.pages.resolve_page_id(
                    document_id=document_id,
                    page_number=r.page_number,
                )
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

            counters.price_items_written = self.prices.replace_by_contract(
                contract_id=contract_id,
                rows=price_db_rows,
            )
        else:
            warnings.append("NO_CONTRACT_ID_PRICE_ITEMS_SKIPPED")

        self.events.append(
            job_id=job_id,
            document_id=document_id,
            event_type="PRICE_ITEMS_WRITTEN",
            payload={"count": counters.price_items_written},
        )

        # =================================================
        # STEP 5: Chunking
        # =================================================
        self.events.append(
            job_id=job_id,
            document_id=document_id,
            event_type="CHUNKS_BUILD_STARTED",
        )

        chunk_rows_raw = chunk_pages(pages)
        chunk_rows = []

        for ch in chunk_rows_raw:
            page_id = self.pages.resolve_page_id(
                document_id=document_id,
                page_number=ch["page_number"],
            )
            chunk_rows.append({
                "document_id": document_id,
                "page_id": page_id,
                "chunk_type": "NARRATIVE",
                "page_number": ch["page_number"],
                "content": ch["text"],
                "metadata": {
                    "entity_id": entity_id,
                    
                    "contract_id": contract_id,
                },
            })

        inserted_chunks = self.chunks.replace_by_document(
            document_id=document_id,
            rows=chunk_rows,
        )

        counters.chunks_written = len(inserted_chunks)

        self.events.append(
            job_id=job_id,
            document_id=document_id,
            event_type="CHUNKS_WRITTEN",
            payload={"count": counters.chunks_written},
        )

        # =================================================
        # STEP 6: Embeddings
        # =================================================
        try:
            self.events.append(
                job_id=job_id,
                document_id=document_id,
                event_type="EMBED_STARTED",
            )

            self.embedder = self.embedder or Embedder()

            for ch in inserted_chunks:
                vec = self.embed.embed(ch["content"])
                self.chunks.update_embedding(
                    chunk_id=ch["chunk_id"],
                    embedding=vec,
                )

            self.events.append(
                job_id=job_id,
                document_id=document_id,
                event_type="EMBED_OK",
                payload={
                    "count": len(inserted_chunks),
                    "note": "Embeddings persisted per chunk",
                },
            )

        except Exception as e:
            warnings.append("EMBEDDING_STALE")
            self.events.append(
                job_id=job_id,
                document_id=document_id,
                event_type="EMBED_FAILED",
                payload={"error": str(e)},
            )

        # =================================================
        # FINALIZE
        # =================================================
        ctr = counters.__dict__

        self.jobs.mark_done(
            job_id,
            counters=ctr,
            warnings=warnings,
        )

        self.events.append(
            job_id=job_id,
            document_id=document_id,
            event_type="JOB_DONE",
            payload={
                "counters": ctr,
                "warnings": warnings,
            },
        )

        return ctr, warnings
