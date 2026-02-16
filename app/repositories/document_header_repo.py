from __future__ import annotations

from typing import Dict, Any, Optional,List
from datetime import date, datetime
import json

from app.repositories.base import BaseRepository


_LANG_MAP = {
    "english": "EN",
    "en": "EN",
    "thai": "TH",
    "th": "TH",
}


def _json_safe(obj: Any) -> Any:
    """
    Make payload JSON-serializable (for jsonb columns).
    - date/datetime -> ISO string
    - dict/list -> recursively json-safe
    - fallback -> string
    """
    if obj is None:
        return None
    if isinstance(obj, (date, datetime)):
        # datetime -> ISO datetime, date -> ISO date
        return obj.isoformat()
    if isinstance(obj, dict):
        return {str(k): _json_safe(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_json_safe(v) for v in obj]
    if isinstance(obj, (str, int, float, bool)):
        return obj
    # last resort
    return str(obj)


class DocumentHeaderRepository(BaseRepository):
    TABLE = "dcc_document_headers"
    TABLE_CONTRACT = "dcc_contract_headers"

    def __init__(self, sb):
        super().__init__(sb)

    def _normalize_language(self, value: Optional[str]) -> Optional[str]:
        if not value:
            return None
        key = value.strip().lower()
        # if unknown, keep original short code upper (e.g., "EN", "TH") if provided
        if key in _LANG_MAP:
            return _LANG_MAP[key]
        if len(key) <= 5:
            return key.upper()
        return None

    def _normalize_date_to_iso(self, value: Any) -> Optional[str]:
        """
        Normalize to ISO date string for DB 'date' columns.
        Using string avoids accidental JSON serialization issues upstream.
        """
        if value is None:
            return None
        if isinstance(value, datetime):
            return value.date().isoformat()
        if isinstance(value, date):
            return value.isoformat()
        if isinstance(value, str):
            # trust upstream ISO-ish strings (HeaderExtractor returns date objects typically,
            # but deterministic enrich may also create date objects)
            v = value.strip()
            return v if v else None
        return None

    def upsert(self, *, document_id: str, header: Dict[str, Any]):
        """
        Deterministic normalization BEFORE DB write.

        IMPORTANT (supabase-py v2):
        - APIResponse has no `.error`
        - errors are raised as exceptions
        - use `.data` for result
        """

        parties = header.get("parties")
        extracted_fields = header.get("extracted_fields")

        row: Dict[str, Any] = {
            "document_id": document_id,
            "doc_type": header.get("doc_type"),
            "doc_title": header.get("doc_title"),
            "doc_number": header.get("doc_number"),
            "language": self._normalize_language(header.get("language")),
            "effective_from": self._normalize_date_to_iso(header.get("effective_from")),
            "effective_to": self._normalize_date_to_iso(header.get("effective_to")),
            # jsonb columns: ensure dict and JSON-safe
            "parties": _json_safe(parties) if parties is not None else {},
            "extracted_fields": _json_safe(extracted_fields) if extracted_fields is not None else {},
            "extraction_method": header.get("extraction_method"),
            "confidence": header.get("confidence"),
        }

        # Remove None only (keep {} for jsonb)
        clean_row = {k: v for k, v in row.items() if v is not None}

        # Defensive: ensure jsonb payloads are actually JSON-serializable
        # (will raise TypeError early with a clear error)
        try:
            json.dumps(clean_row.get("parties", {}))
            json.dumps(clean_row.get("extracted_fields", {}))
        except TypeError as e:
            raise RuntimeError(f"[dcc_document_headers.upsert] jsonb not serializable: {e}")

        try:
            res = (
                self.sb
                .table(self.TABLE)
                .upsert(clean_row, on_conflict="document_id")
                .execute()
            )
        except Exception as e:
            # supabase-py v2 raises exceptions for API errors
            raise RuntimeError(f"[dcc_document_headers.upsert] failed: {e}")

        # res is APIResponse; success path -> res.data
        return res.data

   
   
    def upsert_contract_header(self, *, document_id: str, header: Dict[str, Any]):
        """
        Deterministic normalization BEFORE DB write.

        IMPORTANT (supabase-py v2):
        - APIResponse has no `.error`
        - errors are raised as exceptions
        - use `.data` for result
        """

        parties = header.get("parties")
        extracted_fields = header.get("extracted_fields")

        row: Dict[str, Any] = {
            "primary_document_id": document_id,
            "vendor_entity_id": header.get("entity_id"),
            "contract_code": header.get("doc_number"),
            "vendor_name": _json_safe(parties) if parties is not None else {},
            "buyer_name": _json_safe(parties) if parties is not None else {},
            "effective_from": self._normalize_date_to_iso(header.get("effective_from")),
            "effective_to": self._normalize_date_to_iso(header.get("effective_to")),
            "status": header.get("status"),
            # jsonb columns: ensure dict and JSON-safe
           
            "metadata": _json_safe(extracted_fields) if extracted_fields is not None else {},
        }

        # Remove None only (keep {} for jsonb)
        clean_row = {k: v for k, v in row.items() if v is not None}

        # Defensive: ensure jsonb payloads are actually JSON-serializable
        # (will raise TypeError early with a clear error)
        try:
            json.dumps(clean_row.get("parties", {}))
            json.dumps(clean_row.get("extracted_fields", {}))
        except TypeError as e:
            raise RuntimeError(f"[dcc_document_headers.upsert] jsonb not serializable: {e}")

        try:
            res = (
                self.sb
                .table(self.TABLE_CONTRACT)
                .upsert(clean_row, on_conflict="document_id")
                .execute()
            )
        except Exception as e:
            # supabase-py v2 raises exceptions for API errors
            raise RuntimeError(f"[dcc_document_headers.upsert] failed: {e}")

        # res is APIResponse; success path -> res.data
        return res.data

   
   
        
    def get_by_document(self, document_id: str) -> Optional[Dict[str, Any]]:
        """
        IMPORTANT (supabase-py v2):
        - no `.error`
        - errors raise exceptions
        """
        try:
            res = (
                self.sb
                .table(self.TABLE)
                .select("*")
                .eq("document_id", document_id)
                .maybe_single()
                .execute()
            )
        except Exception as e:
            raise RuntimeError(f"[dcc_document_headers.get_by_document] failed: {e}")

        return res.data

    def get_by_document_id(self, document_id: str) -> Optional[Dict[str, Any]]:
        r = (
            self.sb.table(self.TABLE)
            .select("*")
            .eq("document_id", document_id)
            .limit(1)
            .execute()
        )
        data = getattr(r, "data", None) or []
        return data[0] if data else None

    def list_by_document_ids(self, document_ids: List[str]) -> List[Dict[str, Any]]:
        if not document_ids:
            return []
        r = (
            self.sb.table(self.TABLE)
            .select("*")
            .in_("document_id", document_ids)
            .execute()
        )
        return getattr(r, "data", None) or []
    
    # -----------------------------
    # REQUIRED for locked design:
    # doc_type lives here
    # -----------------------------
    def list_header_by_document_ids(self, document_ids: List[str]) -> List[Dict[str, Any]]:
        ids = [str(x) for x in (document_ids or []) if x]
        if not ids:
            return []

        # If multiple headers exist per document_id, we want the latest.
        # Supabase can't "distinct on" easily from client; so we fetch ordered and pick first per doc_id.
        res = (
            self.sb.table(self.TABLE)
            .select("header_id,document_id,doc_type,doc_title,doc_number,language,effective_from,effective_to,parties,extraction_method,confidence,created_at")
            .in_("document_id", ids)
            .order("created_at", desc=True)
            .execute()
        )
        rows = res.data or []
        latest: Dict[str, Dict[str, Any]] = {}
        for r in rows:
            did = str(r.get("document_id") or "")
            if did and did not in latest:
                latest[did] = r
        return list(latest.values())