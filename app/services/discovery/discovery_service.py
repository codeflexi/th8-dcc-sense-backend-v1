# app/services/discovery/discovery_service.py
from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Any, Dict, List, Optional

from app.repositories.audit_repo import AuditRepository
from app.repositories.case_repo import CaseRepository
from app.repositories.case_line_item_repo import CaseLineItemRepository
from app.repositories.case_document_link_repo import CaseDocumentLinkRepository
from app.repositories.document_repo import DocumentRepository
from app.repositories.document_header_repo import DocumentHeaderRepository
from app.repositories.price_repo import PriceItemRepository
from app.repositories.vector_discovery_repo import VectorDiscoveryRepository

from app.services.embedding.embedding_service import EmbeddingService
from app.services.signal.signal_extraction_service import SignalExtractionService
from app.services.discovery.resolve_contract_per_sku_service import ResolveContractPerSKUService


# =========================================================
# Deterministic text utils (NO ML)
# =========================================================
_SKU_CLEAN_RE = re.compile(r"[^A-Z0-9]+")
_WORD_RE = re.compile(r"[A-Z0-9]+")


def _norm_sku(x: Optional[str]) -> Optional[str]:
    if not x:
        return None
    x = str(x).strip().upper()
    x = _SKU_CLEAN_RE.sub("", x)
    return x or None


def _tokenize(x: Optional[str]) -> List[str]:
    if not x:
        return []
    return _WORD_RE.findall(str(x).upper())


def _jaccard(a: List[str], b: List[str]) -> float:
    sa, sb = set(a), set(b)
    if not sa or not sb:
        return 0.0
    return len(sa & sb) / float(len(sa | sb))


def _to_utc(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _parse_dt(x: Any) -> Optional[datetime]:
    """
    Always return timezone-aware UTC datetime (or None).
    Handles:
    - datetime (naive/aware)
    - date
    - strings like "2026-02-12 12:49:25.604705+00" / ISO / "Z"
    """
    if not x:
        return None
    if isinstance(x, datetime):
        return _to_utc(x)
    if isinstance(x, date):
        return _to_utc(datetime(x.year, x.month, x.day))
    try:
        s = str(x).strip()
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        return _to_utc(dt)
    except Exception:
        return None


def _date_in_range(ref: datetime, start: Optional[datetime], end: Optional[datetime]) -> Optional[bool]:
    """
    True/False if range known; None if range unknown.
    Assumes ref/start/end are timezone-aware UTC.
    """
    if not start and not end:
        return None
    if start and ref < start:
        return False
    if end and ref > end:
        return False
    return True


@dataclass
class DiscoveryPolicy:
    # relational
    min_relational_docs: int = 3
    max_relational_docs: int = 50

    # vector (must match VectorDiscoveryRepository.discover_documents signature)
    enable_vector_backfill: bool = True
    top_k_chunks: int = 50
    top_k_docs: int = 15
    top_chunks_per_doc: int = 3
    vector_min_similarity: float = 0.35

    # scoring weights
    w_valid: float = 0.35
    w_prod: float = 0.45
    w_head: float = 0.15
    w_vec: float = 0.05

    # cutoff
    keep_top_n: int = 20
    min_final_score: float = 0.20


class DiscoveryService:
    """
    Production-safe Discovery:
    - RELATIONAL first (must use entity_id)
    - VECTOR backfill only when relational is insufficient
    - scoring uses validity + product coverage + header confidence (+ vector bonus only in vector stage)
    - writes ONLY dcc_case_document_links (no override)
    - emits audit events (start/rel/vector/done + failures)
    - optionally runs ResolveContractPerSKUService (event-only) after discovery
    """

    def __init__(self, sb):
        self.sb = sb
        self.audit_repo = AuditRepository(sb)
        self.case_repo = CaseRepository(sb)
        self.line_repo = CaseLineItemRepository(sb)
        self.link_repo = CaseDocumentLinkRepository(sb)

        self.doc_repo = DocumentRepository(sb)
        self.header_repo = DocumentHeaderRepository(sb)
        self.price_repo = PriceItemRepository(sb)
        self.vector_repo = VectorDiscoveryRepository(sb)

        # keep compatibility if EmbeddingService signature differs
        try:
            self.embedder = EmbeddingService(sb)
        except TypeError:
            self.embedder = EmbeddingService()

        self.resolve_service = ResolveContractPerSKUService(sb)

    def discover(
        self,
        case_id: str,
        actor_id: str = "SYSTEM",
        *,
        run_resolve_contract_per_sku: bool = True,
    ) -> Dict[str, Any]:
        policy = DiscoveryPolicy()
        run_id = f"disc:{datetime.now(timezone.utc).isoformat()}"

        self.audit_repo.emit(
            case_id=case_id,
            event_type="DISCOVERY_STARTED",
            actor=actor_id,
            payload={"run_id": run_id},
        )

        case = self.case_repo.get(case_id)
        if not case:
            self.audit_repo.emit(
                case_id=case_id,
                event_type="DISCOVERY_FAILED",
                actor=actor_id,
                payload={"run_id": run_id, "reason": "CASE_NOT_FOUND"},
            )
            raise ValueError(f"Case not found: {case_id}")

        line_items = self.line_repo.list_by_case(case_id) or []

        # -------------------------
        # Signals (pydantic model)
        # -------------------------
        signals = SignalExtractionService.extract(case=case, line_items=line_items)
        counterparty_id = None
        counterparty_name = None

        try:
            if signals and signals.counterparty:
                counterparty_id = signals.counterparty.counterparty_id
                counterparty_name = signals.counterparty.counterparty_name
        except Exception:
            counterparty_id = None
            counterparty_name = None

        # fallback: case schema has entity_id
        if not counterparty_id:
            counterparty_id = case.get("entity_id")

        if not counterparty_id:
            self.audit_repo.emit(
                case_id=case_id,
                event_type="DISCOVERY_FAILED",
                actor=actor_id,
                payload={"run_id": run_id, "reason": "MISSING_COUNTERPARTY_ID"},
            )
            raise ValueError("Signals missing counterparty_id")

        # contract number (optional): locked to dcc_document_headers.doc_number compare
        requested_contract_number = case.get("contract_id") or None

        # reference date for contract validity
        ref_dt = _parse_dt(case.get("document_date") or case.get("created_at")) or datetime.now(timezone.utc)
        ref_dt = _to_utc(ref_dt)

        po_sig = self._build_po_signature(line_items)

        # IMPORTANT: use BASIC list to avoid schema/join drift breaking discovery
        existing_links = self.link_repo.list_by_case(case_id) or []
        existing_by_doc = {l.get("document_id"): l for l in existing_links if l.get("document_id")}

        # =====================================================
        # 1) RELATIONAL candidates
        # =====================================================
        raw_rel = self._fetch_relational_docs(counterparty_id, limit=policy.max_relational_docs)

        rel_candidates = self._score_and_filter(
            docs=raw_rel,
            inferred_by="RELATIONAL",
            run_id=run_id,
            ref_dt=ref_dt,
            requested_contract_number=requested_contract_number,
            po_sig=po_sig,
            policy=policy,
        )

        rel_inserted = self._insert_links_no_override(
            case_id=case_id,
            candidates=rel_candidates,
            existing_by_doc=existing_by_doc,
        )

        self.audit_repo.emit(
            case_id=case_id,
            event_type="DISCOVERY_RELATIONAL_DONE",
            actor=actor_id,
            payload={
                "run_id": run_id,
                "counterparty_id": counterparty_id,
                "counterparty_name": counterparty_name,
                "requested_contract_number": requested_contract_number,
                "ref_dt": ref_dt.isoformat(),
                "raw_candidates": len(raw_rel or []),
                "scored_candidates": len(rel_candidates or []),
                "inserted": rel_inserted,
                "top": (rel_candidates or [])[:5],
            },
        )

        # refresh existing after relational
        if rel_inserted > 0:
            existing_links = self.link_repo.list_by_case(case_id) or []
            existing_by_doc = {l.get("document_id"): l for l in existing_links if l.get("document_id")}

        # =====================================================
        # 2) VECTOR backfill only if not enough relational links
        # =====================================================
        vec_inserted = 0
        vec_candidates: List[Dict[str, Any]] = []

        if policy.enable_vector_backfill and len(existing_by_doc) < policy.min_relational_docs:
            query_text = self._build_vector_query_text(
                counterparty_name=(counterparty_name or counterparty_id),
                line_items=line_items,
            )

            query_embedding = self._embed_text(query_text)

            # must match your repo signature:
            # discover_documents(query_embedding, top_k_chunks, top_k_docs, min_similarity, top_chunks_per_doc)
            hits = self.vector_repo.discover_documents(
                query_embedding=query_embedding,
                top_k_chunks=policy.top_k_chunks,
                top_k_docs=policy.top_k_docs,
                min_similarity=policy.vector_min_similarity,
                top_chunks_per_doc=policy.top_chunks_per_doc,
            )

            hit_ids = [h.get("document_id") for h in (hits or []) if h.get("document_id")]
            vec_docs = self.doc_repo.list_by_ids(hit_ids) if hit_ids else []

            by_id = {d.get("document_id"): d for d in (vec_docs or []) if d.get("document_id")}

            augmented: List[Dict[str, Any]] = []
            for h in hits or []:
                did = h.get("document_id")
                if not did or did not in by_id:
                    continue
                d = dict(by_id[did])
                d["_vector"] = {"similarity": h.get("similarity"), "top_chunks": h.get("top_chunks")}
                augmented.append(d)

            vec_candidates = self._score_and_filter(
                docs=augmented,
                inferred_by="VECTOR",
                run_id=run_id,
                ref_dt=ref_dt,
                requested_contract_number=requested_contract_number,
                po_sig=po_sig,
                policy=policy,
            )

            vec_inserted = self._insert_links_no_override(
                case_id=case_id,
                candidates=vec_candidates,
                existing_by_doc=existing_by_doc,
            )

            self.audit_repo.emit(
                case_id=case_id,
                event_type="DISCOVERY_VECTOR_DONE",
                actor=actor_id,
                payload={
                    "run_id": run_id,
                    "query_text": query_text,
                    "min_similarity": policy.vector_min_similarity,
                    "hits": len(hits or []),
                    "scored_candidates": len(vec_candidates or []),
                    "inserted": vec_inserted,
                    "top": (vec_candidates or [])[:5],
                },
            )

        # =====================================================
        # 3) ResolveContractPerSKU (event-only)
        # =====================================================
        resolve_payload = None
        if run_resolve_contract_per_sku:
            resolve_payload = self.resolve_service.resolve(case_id=case_id, actor_id=actor_id)

        links = self.link_repo.list_by_case(case_id) or []

        self.audit_repo.emit(
            case_id=case_id,
            event_type="DISCOVERY_DONE",
            actor=actor_id,
            payload={
                "run_id": run_id,
                "inserted": {"relational": rel_inserted, "vector": vec_inserted},
                "total_links": len(links),
                "resolve_contract_per_sku_emitted": bool(resolve_payload is not None),
            },
        )

        return {
            "case_id": case_id,
            "run_id": run_id,
            "ref_dt": ref_dt.isoformat(),
            "counterparty_id": counterparty_id,
            "requested_contract_number": requested_contract_number,
            "inserted": {"relational": rel_inserted, "vector": vec_inserted},
            "resolve_contract_per_sku": resolve_payload,
            "links": links,
        }

    # =====================================================
    # Compatibility helpers
    # =====================================================
    def _embed_text(self, text: str) -> List[float]:
        # Support either embed_text(text) or embed(text) naming
        if hasattr(self.embedder, "embed_text"):
            return self.embedder.embed_text(text)
        if hasattr(self.embedder, "embed"):
            return self.embedder.embed(text)
        raise AttributeError("EmbeddingService missing embed_text/embed")

    def _fetch_relational_docs(self, entity_id: str, limit: int) -> List[Dict[str, Any]]:
        """
        Keep compatibility with DocumentRepository signature drift.
        Must be entity_id-based (RELATIONAL stage).
        """
        try:
            return self.doc_repo.find_relational_candidates(
                entity_id=entity_id,
                contract_id=None,
                allow_vendor_fallback=True,
                limit=limit,
            )
        except TypeError:
            try:
                return self.doc_repo.find_relational_candidates(entity_id=entity_id, limit=limit)
            except Exception:
                try:
                    return self.doc_repo.list_by_entity_id(entity_id, limit=limit)
                except Exception:
                    return []

    # =====================================================
    # Scoring
    # =====================================================
    def _build_po_signature(self, lines: List[Dict[str, Any]]) -> Dict[str, Any]:
        sku_set = set()
        name_token_sets: List[List[str]] = []
        for l in lines or []:
            sku = _norm_sku(l.get("sku") or l.get("item_code"))
            if sku:
                sku_set.add(sku)
            nm = l.get("item_name") or l.get("description") or l.get("name")
            if nm:
                name_token_sets.append(_tokenize(nm))
        return {"skus": sku_set, "name_tokens": name_token_sets}

    def _build_vector_query_text(self, *, counterparty_name: str, line_items: List[Dict[str, Any]]) -> str:
        hints: List[str] = []
        for l in line_items or []:
            sku = _norm_sku(l.get("sku") or l.get("item_code"))
            if sku:
                hints.append(sku)
            nm = l.get("item_name") or l.get("description") or l.get("name")
            if nm:
                hints.append(str(nm)[:80])
        return f"contract pricing document for vendor {counterparty_name}. items: {'; '.join(hints[:10])}"

    def _score_and_filter(
        self,
        *,
        docs: List[Dict[str, Any]],
        inferred_by: str,
        run_id: str,
        ref_dt: datetime,
        requested_contract_number: Optional[str],
        po_sig: Dict[str, Any],
        policy: DiscoveryPolicy,
    ) -> List[Dict[str, Any]]:
        if not docs:
            return []

        doc_ids = [d.get("document_id") for d in docs if d.get("document_id")]
        headers = self.header_repo.list_by_document_ids(doc_ids) or []
        prices = self.price_repo.list_by_document_ids(doc_ids) or []

        # best header per doc (max confidence)
        header_by_doc: Dict[str, Dict[str, Any]] = {}
        for h in headers:
            did = h.get("document_id")
            if not did:
                continue
            if (did not in header_by_doc) or (
                float(h.get("confidence", 0) or 0) > float(header_by_doc[did].get("confidence", 0) or 0)
            ):
                header_by_doc[did] = h

        price_by_doc: Dict[str, List[Dict[str, Any]]] = {}
        for p in prices:
            did = p.get("document_id")
            if did:
                price_by_doc.setdefault(did, []).append(p)

        sku_set = po_sig.get("skus") or set()
        po_name_tokens = po_sig.get("name_tokens") or []

        out: List[Dict[str, Any]] = []

        for d in docs:
            did = d.get("document_id")
            if not did:
                continue

            h = header_by_doc.get(did)
            pitems = price_by_doc.get(did, []) or []

            explain: Dict[str, Any] = {
                "technique": "DISCOVERY_MATCH_V3",
                "run_id": run_id,
                "inferred_by": inferred_by,
            }

            # ---- optional contract number gate (header.doc_number) ----
            doc_number = None
            if h:
                doc_number = h.get("doc_number") or ((h.get("extracted_fields") or {}).get("document_number"))
            if requested_contract_number:
                if doc_number and str(doc_number).strip() != str(requested_contract_number).strip():
                    continue
                explain["requested_contract_number"] = requested_contract_number
                explain["doc_number"] = doc_number

            # ---- validity gate ----
            eff_from = _parse_dt((h or {}).get("effective_from") or d.get("effective_from"))
            eff_to = _parse_dt((h or {}).get("effective_to") or d.get("effective_to"))
            in_range = _date_in_range(ref_dt, eff_from, eff_to)
            if in_range is False:
                continue

            explain["effective_from"] = eff_from.date().isoformat() if eff_from else None
            explain["effective_to"] = eff_to.date().isoformat() if eff_to else None
            explain["valid_on_ref_date"] = in_range

            validity_score = 1.0 if in_range is True else 0.6  # unknown -> partial

            # ---- product coverage (SKU exact + name fuzzy) ----
            exact_sku_hits = 0
            best_name_sim = 0.0

            for pi in pitems:
                pi_sku = _norm_sku(pi.get("sku") or pi.get("item_code"))
                if pi_sku and pi_sku in sku_set:
                    exact_sku_hits += 1

                pi_name_tokens = _tokenize(pi.get("item_name") or pi.get("description") or "")
                for t in po_name_tokens:
                    best_name_sim = max(best_name_sim, _jaccard(pi_name_tokens, t))

            sku_coverage = (exact_sku_hits / float(max(1, len(sku_set)))) if sku_set else 0.0
            product_score = max(sku_coverage, best_name_sim)

            explain["sku_set_size"] = len(sku_set)
            explain["exact_sku_hits"] = exact_sku_hits
            explain["sku_coverage"] = round(sku_coverage, 6)
            explain["best_name_sim"] = round(best_name_sim, 6)

            # ---- header confidence ----
            header_conf = float((h or {}).get("confidence", 0) or 0)
            explain["header_confidence"] = header_conf

            # ---- vector bonus only in VECTOR stage ----
            vec_sim = 0.0
            if inferred_by == "VECTOR":
                vec = d.get("_vector") or {}
                vec_sim = float(vec.get("similarity", 0) or 0)
                explain["vector_similarity"] = vec_sim
                explain["top_chunks"] = vec.get("top_chunks")

            score = (
                policy.w_valid * validity_score
                + policy.w_prod * product_score
                + policy.w_head * min(1.0, header_conf)
                + (policy.w_vec * min(1.0, vec_sim) if inferred_by == "VECTOR" else 0.0)
            )
            score = max(0.0, min(1.0, score))
            explain["final_score"] = round(score, 6)

            if score < policy.min_final_score:
                continue

            out.append({"document_id": did, "match_score": round(score, 6), "match_explain_json": explain})

        out.sort(key=lambda x: float(x.get("match_score", 0) or 0), reverse=True)
        if policy.keep_top_n:
            out = out[: policy.keep_top_n]
        return out

    # =====================================================
    # Insert policy (NO OVERRIDE)
    # =====================================================
    def _insert_links_no_override(
        self,
        *,
        case_id: str,
        candidates: List[Dict[str, Any]],
        existing_by_doc: Dict[str, Dict[str, Any]],
    ) -> int:
        """
        Rule:
        - never override existing (case_id, document_id)
        - insert as INFERRED only
        - provenance is in match_explain_json.inferred_by and run_id
        """
        n = 0
        for c in candidates or []:
            did = c.get("document_id")
            if not did:
                continue
            if did in existing_by_doc:
                continue

            explain = c.get("match_explain_json") or {}
            inferred_by = explain.get("inferred_by") or "RELATIONAL"
            score = float(c.get("match_score", 0) or 0)

            self.link_repo.insert_inferred(
                case_id=case_id,
                document_id=did,
                inferred_by=inferred_by,
                match_score=score,
                match_explain_json=explain,
            )
            n += 1
            existing_by_doc[did] = {"document_id": did}  # prevent duplicates in same run
        return n
