from app.repositories.base import BaseRepository
from typing import List


class CaseLineItemRepository(BaseRepository):
    """
    Immutable PO Line Item Snapshot Repository

    - Used in Phase A: Case Ingestion
    - Used in Phase C3.5+: Selection / Decision (READ ONLY)
    - MUST NOT mutate data after ingestion
    """

    TABLE = "dcc_case_line_items"
    
    # =====================================================
    # Constructor
    # =====================================================
    def __init__(self, sb):
        super().__init__(sb)

    # =====================================================
    # Write (Ingestion only)
    # =====================================================
    def bulk_insert(self, items: List[dict]):
        """
        Insert PO line items as immutable snapshot.
        Called ONLY during case ingestion.
        """
        if not items:
            return

        self.sb.table(self.TABLE).insert(items).execute()

    # =====================================================
    # Read
    # =====================================================
    def list_by_case(self, case_id: str) -> List[dict]:
        """
        Return immutable PO snapshot line items for a case.

        Output shape is CANONICAL for downstream services.
        **item_id is REQUIRED for C3.5 anchor**
        """
        res = (
            self.sb
            .table(self.TABLE)
            .select("*")
            .eq("case_id", case_id)
            .order("created_at")
            .execute()
        )

        return [
            {
                # ---------- identity (CRITICAL) ----------
                "item_id": row.get("item_id"),   # ✅ MUST EXIST
                "sku": row.get("sku"),
                "name": row.get("item_name"),
                "description": row.get("description"),
                "item_name": row.get("item_name"),
                "created_at": row.get("created_at"),

                # ---------- quantity ----------
                "quantity": row.get("quantity"),
                "uom": row.get("uom"),

                # ---------- pricing ----------
                "unit_price": {
                    "value": row.get("unit_price"),
                    "currency": row.get("currency"),
                },
                "total_price": {
                    "value": row.get("total_price"),
                    "currency": row.get("currency"),
                },

                # ---------- trace ----------
                "source_line_ref": row.get("source_line_ref"),
            }
            for row in (res.data or [])
        ]

    # =====================================================
    # Read
    # =====================================================
    def list_by_id(self, item_id: str) -> List[dict]:
        """
        Return immutable PO snapshot line items for a case.

        Output shape is CANONICAL for downstream services.
        **item_id is REQUIRED for C3.5 anchor**
        """
        res = (
            self.sb
            .table(self.TABLE)
            .select("*")
            .eq("item_id", item_id)
            .order("created_at")
            .execute()
        )

        return [
            {
                # ---------- identity (CRITICAL) ----------
                "item_id": row.get("item_id"),   # ✅ MUST EXIST
                "sku": row.get("sku"),
                "name": row.get("item_name"),
                "description": row.get("description"),
                "item_name": row.get("item_name"),
                "created_at": row.get("created_at"),

                # ---------- quantity ----------
                "quantity": row.get("quantity"),
                "uom": row.get("uom"),

                # ---------- pricing ----------
                "unit_price": {
                    "value": row.get("unit_price"),
                    "currency": row.get("currency"),
                },
                "total_price": {
                    "value": row.get("total_price"),
                    "currency": row.get("currency"),
                },

                # ---------- trace ----------
                "source_line_ref": row.get("source_line_ref"),
            }
            for row in (res.data or [])
        ]
