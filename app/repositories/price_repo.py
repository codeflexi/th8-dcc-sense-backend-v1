from app.repositories.base import BaseRepository
from typing import List, Dict, Any, Optional

class PriceItemRepository(BaseRepository):
    TABLE = "dcc_contract_price_items"
    
    # =====================================================
    # Constructor (REQUIRED)
    # =====================================================
    def __init__(self, sb):
        super().__init__(sb)



    def replace_by_contract(self, *, contract_id: str, rows: list[dict]) -> int:
        self.sb.table(self.TABLE).delete().eq("contract_id", contract_id).execute()
        if not rows:
            return 0
        res = self.sb.table(self.TABLE).insert(rows).execute()
        return len(res.data or [])

    def delete_by_document(self, *, document_id: str):
        self.sb.table(self.TABLE).delete().eq("document_id", document_id).execute()
        
    def list_by_document(self, document_id: str):
        return (
            self.sb
            .table(self.TABLE)
            .select("*")
            .eq("document_id", document_id)
            .execute()
        ).data

    def list_by_document_page(
        self,
        *,
        document_id: str,
        page_number: int,
    ) -> List[Dict[str, Any]]:
        res = (
            self.sb
            .table(self.TABLE)
            .select("""
                price_item_id,
                contract_id,
                document_id,
                page_id,
                page_number,
                sku,
                item_name,
                unit_price,
                currency,
                uom,
                effective_from,
                effective_to,
                snippet,
                confidence_score,
                highlight_text,
                created_at
            """)
            .eq("document_id", document_id)
            .eq("page_number", page_number)
            .order("created_at", desc=False)
            .execute()
        )

        return res.data or []
    
    # =====================================================
    # Read — anchor-based (สำคัญสำหรับ Evidence Group)
    # =====================================================
    def list_by_anchor(
        self,
        *,
        
        anchor_id: str,
        document_id: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """
        Used by EvidenceGroupingService

        anchor_type: e.g. 'PO_ITEM'
        anchor_id  : item_id from dcc_case_line_items
        """

        q = (
            self.sb
            .table(self.TABLE)
            .select("""
                price_item_id,
                contract_id,
                document_id,
                page_id,
                page_number,
                sku,
                item_name,
                unit_price,
                currency,
                uom,
                effective_from,
                effective_to,
                snippet,
                confidence_score,
                highlight_text,
                created_at
            """)
            .eq("price_item_id", anchor_id)
            
        )

        if document_id:
            q = q.eq("document_id", document_id)

        res = q.order("created_at", desc=False).execute()
        return res.data or []