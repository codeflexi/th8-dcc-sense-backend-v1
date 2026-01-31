from typing import Dict, Any, Optional
from datetime import date, datetime
from app.repositories.base import BaseRepository


_LANG_MAP = {
    "english": "EN",
    "en": "EN",
    "thai": "TH",
    "th": "TH",
}


class DocumentHeaderRepository(BaseRepository):
    TABLE = "dcc_document_headers"

    def _normalize_language(self, value: Optional[str]) -> Optional[str]:
        if not value:
            return None
        return _LANG_MAP.get(value.strip().lower())

    def _normalize_date(self, value):
        if value is None:
            return None
        if isinstance(value, datetime):
            return value.date()
        if isinstance(value, date):
            return value
        return None

    def upsert(self, *, document_id: str, header: Dict[str, Any]):
        """
        Deterministic normalization BEFORE DB write.
        Fail-loud on constraint errors.
        """

        row = {
            "document_id": document_id,
            "doc_type": header.get("doc_type"),
            "doc_title": header.get("doc_title"),
            "doc_number": header.get("doc_number"),
            "language": self._normalize_language(header.get("language")),
            "effective_from": self._normalize_date(header.get("effective_from")),
            "effective_to": self._normalize_date(header.get("effective_to")),
            # DB = jsonb NOT NULL → force {}
            "parties": header.get("parties") or {},
            "extracted_fields": header.get("extracted_fields") or {},
            "extraction_method": header.get("extraction_method"),
            "confidence": header.get("confidence"),
        }

        # remove None only ({} ต้องอยู่)
        clean_row = {k: v for k, v in row.items() if v is not None}

        res = (
            self.sb
            .table(self.TABLE)
            .upsert(clean_row, on_conflict="document_id")
            .execute()
        )

        if res.error:
            raise RuntimeError(
                f"[dcc_document_headers.upsert] failed: {res.error}"
            )

        return res.data

    def get_by_document(self, document_id: str) -> Optional[Dict[str, Any]]:
        res = (
            self.sb
            .table(self.TABLE)
            .select("*")
            .eq("document_id", document_id)
            .maybe_single()
            .execute()
        )

        if res.error:
            raise RuntimeError(
                f"[dcc_document_headers.get_by_document] failed: {res.error}"
            )

        return res.data
