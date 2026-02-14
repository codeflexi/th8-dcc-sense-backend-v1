# app/services/discovery/resolve_contract_per_sku_service.py
from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from app.repositories.audit_repo import AuditRepository
from app.repositories.case_repo import CaseRepository
from app.repositories.case_line_item_repo import CaseLineItemRepository
from app.repositories.case_document_link_repo import CaseDocumentLinkRepository
from app.repositories.document_header_repo import DocumentHeaderRepository
from app.repositories.price_repo import PriceItemRepository


_SKU_CLEAN_RE = re.compile(r"[^A-Z0-9]+")
_WORD_RE = re.compile(r"[A-Z0-9]+")


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


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


def _parse_dt(x: Any) -> Optional[datetime]:
    if not x:
        return None
    if isinstance(x, datetime):
        # if naive, treat as UTC to avoid random local conversion
        return x if x.tzinfo else x.replace(tzinfo=timezone.utc)
    try:
        s = str(x).strip()
        # handle trailing Z
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        dt = datetime.fromisoformat(s)
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    except Exception:
        return None


def _date_in_range(ref: datetime, start: Optional[datetime], end: Optional[datetime]) -> Optional[bool]:
    """
    None means "no window available"
    True/False means evaluated
    """
    if not start and not end:
        return None
    if start and ref < start:
        return False
    if end and ref > end:
        return False
    return True


def _as_float(x: Any, default: float = 0.0) -> float:
    try:
        if x is None:
            return default
        return float(x)
    except Exception:
        return default


def _safe_iso(dt: Optional[datetime]) -> Optional[str]:
    if not dt:
        return None
    if not isinstance(dt, datetime):
        dt = _parse_dt(dt)
    return dt.isoformat() if dt else None


def _coerce_json_safe(obj: Any) -> Any:
    """
    Ensure payload is JSON-serializable (Supabase/PostgREST safety).
    """
    if obj is None:
        return None
    if isinstance(obj, (str, int, float, bool)):
        return obj
    if isinstance(obj, datetime):
        return obj.isoformat()
    if isinstance(obj, dict):
        return {str(k): _coerce_json_safe(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple, set)):
        return [_coerce_json_safe(v) for v in obj]
    # fallback
    return str(obj)


class ResolveContractPerSKUService:
    """
    After Discovery:
    - No new tables
    - Emit audit events only
    - Deterministic mapping contract-per-SKU (per PO_ITEM anchor)
    """

    # Audit event types (enterprise-grade)
    EVT_STARTED = "CONTRACT_PER_SKU_RESOLVE_STARTED"
    EVT_CANDIDATES = "CONTRACT_CANDIDATES_FILTERED"
    EVT_FALLBACK = "CONTRACT_VENDOR_FALLBACK_SELECTED"
    EVT_COMPLETED = "CONTRACT_PER_SKU_RESOLVE_COMPLETED"
    EVT_EMPTY = "CONTRACT_PER_SKU_RESOLVE_EMPTY"
    EVT_FAILED = "CONTRACT_PER_SKU_RESOLVE_FAILED"

    def __init__(self, sb):
        self.sb = sb
        self.audit_repo = AuditRepository(sb)
        self.case_repo = CaseRepository(sb)
        self.line_repo = CaseLineItemRepository(sb)
        self.link_repo = CaseDocumentLinkRepository(sb)
        self.header_repo = DocumentHeaderRepository(sb)
        self.price_repo = PriceItemRepository(sb)

    def resolve(self, case_id: str, actor_id: str = "SYSTEM") -> Dict[str, Any]:
        started_at = _utcnow()

        try:
            case = self.case_repo.get(case_id)
            if not case:
                raise ValueError(f"Case not found: {case_id}")

            # reference datetime for validity window: prefer document_date (if present) then created_at
            ref_dt = _parse_dt(case.get("document_date") or case.get("created_at")) or _utcnow()

            lines = self.line_repo.list_by_case(case_id) or []
            links = self.link_repo.list_by_case(case_id) or []

            doc_ids = [
                l.get("document_id")
                for l in links
                if l.get("link_status") != "REMOVED" and l.get("document_id")
            ]

            # 1) START event
            self.audit_repo.emit(
                case_id=case_id,
                event_type=self.EVT_STARTED,
                actor=actor_id,
                payload=_coerce_json_safe(
                    {
                        "case_id": case_id,
                        "ref_dt": ref_dt.isoformat(),
                        "line_count": len(lines),
                        "document_link_count": len(doc_ids),
                        "started_at": started_at.isoformat(),
                    }
                ),
            )

            # Fast exit if no docs
            if not doc_ids:
                payload = {
                    "ref_dt": ref_dt.isoformat(),
                    "mapping_count": 0,
                    "mappings": [],
                    "vendor_fallback": None,
                    "notes": ["NO_DISCOVERED_DOCUMENTS"],
                    "elapsed_ms": int((_utcnow() - started_at).total_seconds() * 1000),
                }
                self.audit_repo.emit(
                    case_id=case_id,
                    event_type=self.EVT_EMPTY,
                    actor=actor_id,
                    payload=_coerce_json_safe(payload),
                )
                return payload

            headers = self.header_repo.list_by_document_ids(doc_ids) or []
            prices = self.price_repo.list_by_document_ids(doc_ids) or []

            # pick best header per document_id (highest confidence)
            header_by_doc: Dict[str, Dict[str, Any]] = {}
            for h in headers:
                did = h.get("document_id")
                if not did:
                    continue
                prev = header_by_doc.get(did)
                if not prev or _as_float(h.get("confidence"), 0.0) > _as_float(prev.get("confidence"), 0.0):
                    header_by_doc[did] = h

            # filter eligible contract docs by:
            # - doc_type == CONTRACT
            # - validity window contains ref_dt (if window exists)
            eligible_docs: List[Tuple[str, Dict[str, Any]]] = []
            for did, h in header_by_doc.items():
                if (h.get("doc_type") or "").upper() != "CONTRACT":
                    continue

                eff_from = _parse_dt(h.get("effective_from"))
                eff_to = _parse_dt(h.get("effective_to"))
                ok = _date_in_range(ref_dt, eff_from, eff_to)
                if ok is False:
                    continue

                eligible_docs.append((did, h))

            # 2) CANDIDATES event (post filter)
            self.audit_repo.emit(
                case_id=case_id,
                event_type=self.EVT_CANDIDATES,
                actor=actor_id,
                payload=_coerce_json_safe(
                    {
                        "ref_dt": ref_dt.isoformat(),
                        "eligible_contract_count": len(eligible_docs),
                        "eligible_document_ids": [d[0] for d in eligible_docs],
                        "total_headers_considered": len(header_by_doc),
                        "total_prices_loaded": len(prices),
                        "filter_rules": {
                            "doc_type": "CONTRACT",
                            "validity_window": True,
                        },
                    }
                ),
            )

            # index prices per document
            price_by_doc: Dict[str, List[Dict[str, Any]]] = {}
            for p in prices:
                did = p.get("document_id")
                if did:
                    price_by_doc.setdefault(did, []).append(p)

            # vendor-level fallback: choose ONE best contract doc by header confidence
            vendor_fallback: Optional[Dict[str, Any]] = None
            if eligible_docs:
                best_did, best_h = max(eligible_docs, key=lambda t: _as_float(t[1].get("confidence"), 0.0))
                vendor_fallback = {
                    "document_id": best_did,
                    "contract_doc_number": best_h.get("doc_number")
                    or ((best_h.get("extracted_fields") or {}).get("document_number")),
                    "header_confidence": _as_float(best_h.get("confidence"), 0.0),
                    "effective_from": _safe_iso(best_h.get("effective_from")),
                    "effective_to": _safe_iso(best_h.get("effective_to")),
                    "method": "VENDOR_FALLBACK",
                    "match_score": 0.3,
                }

                # 3) FALLBACK event (selection only, not applied per line yet)
                self.audit_repo.emit(
                    case_id=case_id,
                    event_type=self.EVT_FALLBACK,
                    actor=actor_id,
                    payload=_coerce_json_safe(
                        {
                            "ref_dt": ref_dt.isoformat(),
                            "fallback": vendor_fallback,
                            "reason": "DEFAULT_BEST_CONTRACT_BY_HEADER_CONFIDENCE",
                        }
                    ),
                )

            mappings: List[Dict[str, Any]] = []

            # deterministic per line (PO_ITEM anchor)
            for line in lines:
                item_id = line.get("item_id")
                if not item_id:
                    # not valid anchor; skip deterministically
                    continue

                po_sku = _norm_sku(line.get("sku") or line.get("item_code"))
                po_name = line.get("item_name") or line.get("description") or line.get("name")
                po_name_tokens = _tokenize(po_name)

                best_match: Optional[Dict[str, Any]] = None

                # If no eligible contract docs, only fallback is possible (vendor_fallback may be None)
                for did, h in eligible_docs:
                    candidates = price_by_doc.get(did, []) or []
                    if not candidates:
                        continue

                    best_in_doc: Optional[Dict[str, Any]] = None

                    for pi in candidates:
                        pi_sku = _norm_sku(pi.get("sku") or pi.get("item_code"))
                        pi_name_tokens = _tokenize(pi.get("item_name") or pi.get("description"))

                        sku_score = 1.0 if (po_sku and pi_sku and po_sku == pi_sku) else 0.0
                        name_score = _jaccard(po_name_tokens, pi_name_tokens)
                        base = sku_score if sku_score > 0 else name_score

                        if not best_in_doc or base > _as_float(best_in_doc.get("_base"), 0.0):
                            best_in_doc = {
                                "_base": base,
                                "price_item": pi,
                                "match_method": "SKU_EXACT" if sku_score > 0 else "NAME_FUZZY",
                                "header_conf": _as_float(h.get("confidence"), 0.0),
                                "doc_number": h.get("doc_number")
                                or ((h.get("extracted_fields") or {}).get("document_number")),
                                "effective_from": _safe_iso(h.get("effective_from")),
                                "effective_to": _safe_iso(h.get("effective_to")),
                            }

                    if best_in_doc and _as_float(best_in_doc["_base"], 0.0) > 0.0:
                        # scoring: base similarity (70%) + header confidence (30%)
                        final = 0.70 * _as_float(best_in_doc["_base"], 0.0) + 0.30 * min(
                            1.0, _as_float(best_in_doc["header_conf"], 0.0)
                        )

                        if (best_match is None) or (final > _as_float(best_match.get("match_score"), 0.0)):
                            pi = best_in_doc["price_item"]
                            best_match = {
                                "anchor_type": "PO_ITEM",
                                "anchor_id": item_id,
                                "po_sku": po_sku,
                                "po_name": po_name,
                                "document_id": did,
                                "contract_doc_number": best_in_doc["doc_number"],
                                "price_item_id": pi.get("price_item_id"),
                                "matched_sku": pi.get("sku") or pi.get("item_code"),
                                "matched_name": pi.get("item_name") or pi.get("description"),
                                "unit_price": pi.get("unit_price") or pi.get("price"),
                                "currency": pi.get("currency"),
                                "match_method": best_in_doc["match_method"],
                                "match_score": round(final, 6),
                                "explain": {
                                    "base_similarity": round(_as_float(best_in_doc["_base"], 0.0), 6),
                                    "header_confidence": best_in_doc["header_conf"],
                                    "effective_from": best_in_doc.get("effective_from"),
                                    "effective_to": best_in_doc.get("effective_to"),
                                },
                            }

                # apply vendor-level fallback if no item-level match
                if not best_match and vendor_fallback:
                    best_match = {
                        "anchor_type": "PO_ITEM",
                        "anchor_id": item_id,
                        "po_sku": po_sku,
                        "po_name": po_name,
                        **vendor_fallback,
                        "explain": {"reason": "NO_PRICE_ITEM_MATCH"},
                    }

                if best_match:
                    mappings.append(best_match)

            payload = {
                "ref_dt": ref_dt.isoformat(),
                "mapping_count": len(mappings),
                "mappings": mappings,
                "vendor_fallback": vendor_fallback,
                "elapsed_ms": int((_utcnow() - started_at).total_seconds() * 1000),
            }

            # 4) COMPLETED vs EMPTY event
            event_type = self.EVT_COMPLETED if mappings else self.EVT_EMPTY
            self.audit_repo.emit(
                case_id=case_id,
                event_type=event_type,
                actor=actor_id,
                payload=_coerce_json_safe(payload),
            )

            return payload

        except Exception as e:
            # 5) FAILED event (must be JSON safe)
            self.audit_repo.emit(
                case_id=case_id,
                event_type=self.EVT_FAILED,
                actor=actor_id,
                payload=_coerce_json_safe(
                    {
                        "error": str(e),
                        "error_type": e.__class__.__name__,
                        "elapsed_ms": int((_utcnow() - started_at).total_seconds() * 1000),
                    }
                ),
            )
            raise
