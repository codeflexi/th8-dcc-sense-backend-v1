# app/repositories/copilot_repo.py
import os
import httpx
from typing import List, Optional, Dict, Any
from dotenv import load_dotenv

from app.repositories.base import BaseRepository

load_dotenv()


class CopilotRepositoryAgent(BaseRepository):
    """
    ENTERPRISE COPILOT REPO (FINAL)

    - ใช้ internal API เท่านั้น (ไม่เดา schema DB)
    - case scoped
    - multi-domain
    - ดึง evidence ผ่าน /groups/{group_id}/evidence
    - รองรับ structure จริงของ backend
    """

    def __init__(self):
        self.api_base_url = os.getenv("API_BASE_URL", "http://127.0.0.1:8000/api/v1")
        print(f"[CopilotRepositoryAgent] API_BASE_URL={self.api_base_url}")

    # -------------------------------------------------------
    # INTERNAL GET (with optional query params)
    # -------------------------------------------------------
    async def _get(
        self,
        path: str,
        *,
        params: Optional[Dict[str, Any]] = None,
        timeout: float = 30.0,
    ) -> Optional[Any]:
        url = f"{self.api_base_url}{path}"

        try:
            async with httpx.AsyncClient(timeout=timeout) as client:
                r = await client.get(url, params=params)

            if r.status_code == 200:
                return r.json()

            print(f"[CopilotRepo] GET {path} -> {r.status_code}")
            return None

        except Exception as e:
            print(f"[CopilotRepo] ERROR GET {path}: {e}")
            return None

    # -------------------------------------------------------
    # INTERNAL POST (for future tool use)
    # -------------------------------------------------------
    async def _post(
        self,
        path: str,
        *,
        json_body: Optional[Dict[str, Any]] = None,
        params: Optional[Dict[str, Any]] = None,
        timeout: float = 30.0,
    ) -> Optional[Any]:
        url = f"{self.api_base_url}{path}"

        try:
            async with httpx.AsyncClient(timeout=timeout) as client:
                r = await client.post(url, json=json_body, params=params)

            if r.status_code == 200:
                return r.json()

            print(f"[CopilotRepo] POST {path} -> {r.status_code}")
            return None

        except Exception as e:
            print(f"[CopilotRepo] ERROR POST {path}: {e}")
            return None

    # -------------------------------------------------------
    # CASE
    # -------------------------------------------------------
    async def get_case_detail(self, case_id: str) -> Optional[dict]:
        return await self._get(f"/cases/{case_id}", timeout=20.0)

    async def get_case_decision_summary(self, case_id: str) -> Optional[dict]:
        return await self._get(f"/cases/{case_id}/decision-summary", timeout=20.0)

    # -------------------------------------------------------
    # GROUPS
    # -------------------------------------------------------
    async def get_case_groups(self, case_id: str) -> List[dict]:
        data = await self._get(f"/cases/{case_id}/groups", timeout=25.0)

        if not data:
            return []

        # รองรับทั้ง {groups:[]} และ list ตรง
        if isinstance(data, dict) and isinstance(data.get("groups"), list):
            return data["groups"]

        if isinstance(data, list):
            return data

        return []

    async def get_group_rules(self, group_id: str) -> Optional[dict]:
        """
        ใช้ endpoint จริง:
        GET /api/v1/groups/{group_id}/rules
        """
        return await self._get(f"/groups/{group_id}/rules", timeout=25.0)

    async def get_group_evidence(self, group_id: str) -> Optional[dict]:
        """
        ENTERPRISE FIX:
        ใช้ endpoint กลาง
        GET /api/v1/groups/{group_id}/evidence
        """
        return await self._get(f"/groups/{group_id}/evidence", timeout=40.0)

    # -------------------------------------------------------
    # DOCUMENT VIEW
    # -------------------------------------------------------
    async def open_document_page(self, document_id: str, page: int) -> Optional[dict]:
        """
        NOTE:
        Router ของคุณเป็น /documents/{document_id}/pages-no/{page_no}
        (ไม่ใช่ /pages/{page})
        """
        return await self._get(f"/documents/{document_id}/pages-no/{int(page)}", timeout=30.0)

    async def get_document_page_context(
        self,
        *,
        document_id: str,
        page: int,
        case_id: Optional[str] = None,
        group_id: Optional[str] = None,
    ) -> Optional[dict]:
        """
        ใช้ endpoint:
        GET /documents/{document_id}/page-context/{page_no}?case_id=&group_id=
        """
        params: Dict[str, Any] = {}
        if case_id:
            params["case_id"] = case_id
        if group_id:
            params["group_id"] = group_id

        return await self._get(
            f"/documents/{document_id}/page-context/{int(page)}",
            params=params or None,
            timeout=40.0,
        )

    # -------------------------------------------------------
    # NEW TOOL: BUILD DOCUMENT CONTEXT (COPILOT)
    # -------------------------------------------------------
    async def build_document_context(
        self,
        *,
        document_id: str,
        pages: List[int],
        case_id: Optional[str] = None,
        group_id: Optional[str] = None,
        max_chunks_per_page: int = 4,
        max_total_chunks: int = 12,
        max_chunk_chars: int = 900,
        max_clause_chars: int = 900,
    ) -> Dict[str, Any]:
        """
        Build compact doc context for Copilot from your existing page-context endpoint.

        Output schema (stable):
        {
          "document_id": "...",
          "doc_facts": {...},
          "key_clauses": [{clause_type, page, chunk_id, text}],
          "pages": [{page, chunks:[{chunk_id, chunk_type, content}]}],
        }
        """
        pages = self._dedupe_ints(pages)
        if not pages:
            pages = [1]

        page_payloads: List[dict] = []
        for p in pages:
            payload = await self.get_document_page_context(
                document_id=document_id,
                page=int(p),
                case_id=case_id,
                group_id=group_id,
            )
            if isinstance(payload, dict):
                page_payloads.append(payload)

        if not page_payloads:
            return {"document_id": document_id, "doc_facts": {}, "key_clauses": [], "pages": []}

        primary = page_payloads[0]
        doc = primary.get("document") or {}
        header = primary.get("document_header") or {}

        doc_facts = self._extract_doc_facts(doc=doc, header=header)

        # Build slim pages + extract key clauses
        all_chunks_for_clause: List[Dict[str, Any]] = []
        pages_out: List[Dict[str, Any]] = []

        total_chunks = 0
        for payload in page_payloads:
            page_obj = payload.get("page") or {}
            page_no = int(page_obj.get("page_number") or 0) or None
            if not page_no:
                continue

            chunks = ((payload.get("content") or {}).get("chunks") or [])
            chunks = [c for c in chunks if isinstance(c, dict)]

            # slim chunks for UI / LLM budget
            slim_chunks: List[Dict[str, Any]] = []
            for c in chunks:
                if len(slim_chunks) >= max_chunks_per_page:
                    break
                if total_chunks >= max_total_chunks:
                    break

                content = (c.get("content") or "").strip()
                if not content:
                    continue

                slim_chunks.append(
                    {
                        "chunk_id": c.get("chunk_id"),
                        "chunk_type": c.get("chunk_type"),
                        "page_number": c.get("page_number"),
                        "content": content[:max_chunk_chars],
                    }
                )
                total_chunks += 1

            pages_out.append({"page": page_no, "chunks": slim_chunks})

            # for clause extraction (use original chunks but still bounded)
            all_chunks_for_clause.extend(
                [
                    {
                        "chunk_id": c.get("chunk_id"),
                        "page_number": c.get("page_number") or page_no,
                        "content": (c.get("content") or "")[:max_clause_chars],
                    }
                    for c in chunks[: max_chunks_per_page * 2]
                    if isinstance(c, dict)
                ]
            )

        key_clauses = self._extract_key_clauses(all_chunks_for_clause)

        return {
            "document_id": document_id,
            "doc_facts": doc_facts,
            "key_clauses": key_clauses,
            "pages": pages_out,
        }

    # -------------------------------------------------------
    # OPTIONAL VECTOR SEARCH (ยังใช้ของเดิมได้)
    # -------------------------------------------------------
    def search_evidence(self, query_embedding: List[float], match_count: int = 3) -> List[dict]:
        try:
            params = {
                "query_embedding": query_embedding,
                "match_count": match_count,
                "filter_policy_id": None,
            }
            res = self.sb.rpc("match_evidence", params).execute()
            return res.data or []
        except Exception as e:
            print(f"[CopilotRepo] vector search error: {e}")
            return []

    # =======================================================
    # INTERNAL HELPERS (deterministic)
    # =======================================================
    def _dedupe_ints(self, xs: List[int]) -> List[int]:
        seen = set()
        out: List[int] = []
        for x in xs or []:
            try:
                xi = int(x)
            except Exception:
                continue
            if xi in seen:
                continue
            seen.add(xi)
            out.append(xi)
        return out

    def _extract_doc_facts(self, *, doc: Dict[str, Any], header: Dict[str, Any]) -> Dict[str, Any]:
        extraction = doc.get("extraction_summary") or {}
        parties = header.get("parties") or {}
        extracted_fields = header.get("extracted_fields") or {}

        vendor = None
        if isinstance(parties, dict):
            vendor = parties.get("vendor") or parties.get("supplier")
        if not vendor and isinstance(extracted_fields, dict):
            p2 = extracted_fields.get("parties") or {}
            if isinstance(p2, dict):
                vendor = p2.get("vendor") or p2.get("supplier")

        effective_from = (
            header.get("effective_from")
            or (extraction.get("effective_from") or {}).get("value")
            or doc.get("effective_from")
        )
        effective_to = (
            header.get("effective_to")
            or (extraction.get("effective_to") or {}).get("value")
            or doc.get("effective_to")
        )

        return {
            "doc_type": header.get("doc_type") or doc.get("document_type"),
            "doc_title": header.get("doc_title") or extracted_fields.get("document_title") or None,
            "doc_number": header.get("doc_number") or extracted_fields.get("document_number") or None,
            "vendor": vendor,
            "status": doc.get("status"),
            "effective_from": effective_from,
            "effective_to": effective_to,
            "confidence": header.get("confidence") or (doc.get("classification") or {}).get("confidence"),
        }

    def _extract_key_clauses(self, chunks: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Deterministic keyword-based clause picks (good enough for production v1).
        """
        rules = [
            ("PAYMENT", ["payment", "invoice", "within", "days", "net"]),
            ("PRICING", ["price", "pricing", "fixed", "rate", "fee"]),
            ("TERMINATION", ["terminate", "termination", "notice", "breach"]),
            ("GOVERNING_LAW", ["governing law", "laws of", "jurisdiction", "kingdom of thailand"]),
            ("LIABILITY", ["liability", "indirect", "consequential", "damages", "negligence"]),
            ("CONFIDENTIALITY", ["confidential", "non-public", "disclose"]),
            ("WARRANTY", ["warranty", "warrants", "defects", "fit for"]),
            ("SCOPE", ["scope", "agreement governs", "services", "procurement"]),
        ]

        best_by_type: Dict[str, Dict[str, Any]] = {}

        for c in chunks or []:
            text = (c.get("content") or "").strip()
            if not text:
                continue

            text_lc = text.lower()
            page_no = int(c.get("page_number") or 0) or None
            chunk_id = c.get("chunk_id")

            for ctype, kws in rules:
                score = sum(1 for kw in kws if kw in text_lc)
                if score < 2:
                    continue

                cur = best_by_type.get(ctype)
                if (not cur) or (score > int(cur.get("_score", 0))):
                    best_by_type[ctype] = {
                        "clause_type": ctype,
                        "page": page_no,
                        "chunk_id": chunk_id,
                        "text": text,
                        "_score": score,
                    }

        # return in stable order
        order = ["PAYMENT", "PRICING", "TERMINATION", "GOVERNING_LAW", "LIABILITY", "CONFIDENTIALITY", "WARRANTY", "SCOPE"]
        out: List[Dict[str, Any]] = []
        for k in order:
            v = best_by_type.get(k)
            if v:
                v.pop("_score", None)
                out.append(v)

        return out
