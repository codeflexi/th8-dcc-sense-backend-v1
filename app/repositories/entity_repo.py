# app/repositories/entity_repo.py
from __future__ import annotations
from typing import Any, Dict, Optional


class EntityRepository:
    TABLE = "dcc_entities"

    def __init__(self, sb):
        self.sb = sb

    def get(self, entity_id: str) -> Optional[Dict[str, Any]]:
        res = (
            self.sb.table(self.TABLE)
            .select("*")
            .eq("entity_id", entity_id)
            .limit(1)
            .execute()
        )
        data = (res.data or [])
        return data[0] if data else None
