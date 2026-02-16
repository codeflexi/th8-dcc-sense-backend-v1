# app/repositories/transaction_line_item_repo.py
from __future__ import annotations
from typing import Any, Dict, List, Optional


class TransactionLineItemRepository:
    TABLE = "dcc_transaction_line_items"

    def __init__(self, sb):
        self.sb = sb

    def insert_many(self, rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        if not rows:
            return []
        res = self.sb.table(self.TABLE).insert(rows).execute()
        return res.data or []

    def exists_doc_for_entity(
        self,
        *,
        transaction_id: str,
        source_type: str,
        source_ref_id: str,
        entity_id: str,
    ) -> bool:
        # “มีแถวใด ๆ ของเอกสารนี้แล้วไหม” (doc-level existence)
        res = (
            self.sb.table(self.TABLE)
            .select("txn_item_id")
            .eq("transaction_id", transaction_id)
            .eq("source_type", source_type)
            .eq("source_ref_id", source_ref_id)
            .eq("entity_id", entity_id)
            .limit(1)
            .execute()
        )
        return bool(res.data)

    def sum_qty_by_sku(
        self,
        *,
        transaction_id: str,
        source_type: str,
        sku: str,
    ) -> float:
        # ใช้ในอนาคต (เช่น status WAITING_ARTIFACTS) — ไม่ critical ตอนนี้
        res = (
            self.sb.table(self.TABLE)
            .select("quantity")
            .eq("transaction_id", transaction_id)
            .eq("source_type", source_type)
            .eq("sku", sku)
            .execute()
        )
        total = 0.0
        for r in (res.data or []):
            try:
                total += float(r.get("quantity") or 0)
            except Exception:
                continue
        return total
