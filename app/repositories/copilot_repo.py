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
    # INTERNAL GET
    # -------------------------------------------------------
    async def _get(self, path: str, timeout: float = 30.0) -> Optional[Any]:
        url = f"{self.api_base_url}{path}"

        try:
            async with httpx.AsyncClient(timeout=timeout) as client:
                r = await client.get(url)

            if r.status_code == 200:
                return r.json()

            print(f"[CopilotRepo] {path} -> {r.status_code}")
            return None

        except Exception as e:
            print(f"[CopilotRepo] ERROR {path}: {e}")
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
        return await self._get(f"/documents/{document_id}/pages/{int(page)}", timeout=30.0)

    # -------------------------------------------------------
    # OPTIONAL VECTOR SEARCH (ยังใช้ของเดิมได้)
    # -------------------------------------------------------
    def search_evidence(self, query_embedding: List[float], match_count: int = 3) -> List[dict]:
        try:
            params = {
                "query_embedding": query_embedding,
                "match_count": match_count,
                "filter_policy_id": None
            }
            res = self.sb.rpc("match_evidence", params).execute()
            return res.data or []
        except Exception as e:
            print(f"[CopilotRepo] vector search error: {e}")
            return []
