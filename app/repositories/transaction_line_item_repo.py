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

    def list_by_transaction(self, *, transaction_id: str, entity_id: Optional[str] = None) -> List[Dict[str, Any]]:
        q = self.sb.table(self.TABLE).select("*").eq("transaction_id", transaction_id)
        if entity_id:
            q = q.eq("entity_id", entity_id)
        res = q.execute()
        return res.data or []

    def list_by_transaction_and_source(
        self,
        *,
        transaction_id: str,
        source_type: str,
        entity_id: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        q = (
            self.sb.table(self.TABLE)
            .select("*")
            .eq("transaction_id", transaction_id)
            .eq("source_type", source_type)
        )
        if entity_id:
            q = q.eq("entity_id", entity_id)
        res = q.execute()
        return res.data or []

    def list_by_transaction_and_sources(
        self,
        *,
        transaction_id: str,
        source_types: List[str],
        entity_id: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        # Supabase .in_() exists; keep defensive fallback
        try:
            q = self.sb.table(self.TABLE).select("*").eq("transaction_id", transaction_id).in_("source_type", source_types)
            if entity_id:
                q = q.eq("entity_id", entity_id)
            res = q.execute()
            return res.data or []
        except Exception:
            out: List[Dict[str, Any]] = []
            for t in source_types or []:
                out.extend(self.list_by_transaction_and_source(transaction_id=transaction_id, source_type=t, entity_id=entity_id))
            return out
