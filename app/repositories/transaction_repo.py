from __future__ import annotations
from typing import Any, Dict, Optional


class TransactionRepository:
    TABLE = "dcc_transactions"

    def __init__(self, sb):
        self.sb = sb

    def get_by_aggregate(self, *, aggregate_type: str, aggregate_key: str) -> Optional[Dict[str, Any]]:
        res = (
            self.sb.table(self.TABLE)
            .select("*")
            .eq("aggregate_type", aggregate_type)
            .eq("aggregate_key", aggregate_key)
            .limit(1)
            .execute()
        )
        data = (res.data or [])
        return data[0] if data else None

    def create(
        self,
        *,
        aggregate_type: str,
        aggregate_key: str,
        entity_id: str,
        entity_type: str,
        currency: str | None,
        amount_total: float | None,
        lifecycle_status: str = "OPEN",
        metadata_json: Dict[str, Any] | None = None,
        created_by: str = "SYSTEM",
    ) -> Dict[str, Any]:
        payload = {
            "aggregate_type": aggregate_type,
            "aggregate_key": aggregate_key,
            "entity_id": entity_id,
            "entity_type": entity_type,
            "currency": currency,
            "amount_total": amount_total,
            "lifecycle_status": lifecycle_status,
            "metadata_json": metadata_json or {},
            "created_by": created_by,
        }
        res = self.sb.table(self.TABLE).insert(payload).execute()
        return (res.data or [payload])[0]
