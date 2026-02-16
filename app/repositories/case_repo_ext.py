# app/repositories/case_repo_ext.py
from __future__ import annotations
from typing import Any, Dict, Optional


class CaseRepositoryExt:
    TABLE = "dcc_cases"

    def __init__(self, sb):
        self.sb = sb

    def find_finance_ap_case(
        self,
        *,
        transaction_id: str,
        invoice_number: str,
    ) -> Optional[Dict[str, Any]]:
        res = (
            self.sb.table(self.TABLE)
            .select("*")
            .eq("domain", "FINANCE_AP")
            .eq("reference_type", "ERP_INVOICE")
            .eq("reference_id", invoice_number)
            .eq("transaction_id", transaction_id)
            .limit(1)
            .execute()
        )
        data = (res.data or [])
        return data[0] if data else None

    def find_procurement_case_for_transaction(self, *, transaction_id: str) -> Optional[Dict[str, Any]]:
        res = (
            self.sb.table(self.TABLE)
            .select("*")
            .eq("domain", "PROCUREMENT")
            .eq("transaction_id", transaction_id)
            .limit(1)
            .execute()
        )
        data = (res.data or [])
        return data[0] if data else None

    def create_finance_ap_case(
        self,
        *,
        transaction_id: str,
        entity_id: str,
        entity_type: str,
        invoice_number: str,
        currency: str | None,
        created_by: str,
        case_detail: Dict[str, Any] | None = None,
    ) -> Dict[str, Any]:
        payload = {
            "transaction_id": transaction_id,
            "entity_id": entity_id,
            "entity_type": entity_type,
            "domain": "FINANCE_AP",
            "reference_type": "ERP_INVOICE",
            "reference_id": invoice_number,
            "currency": currency,
            "status": "OPEN",
            "case_detail": case_detail or {},
            "created_by": created_by,
        }
        res = self.sb.table(self.TABLE).insert(payload).execute()
        return (res.data or [payload])[0]
    
   


    def patch_case_detail(self, *, case_id: str, patch: Dict[str, Any]) -> None:
        # merge แบบง่าย: overwrite keys ที่ส่งมา
        # (ถ้าคุณมี merge strategy กลางในระบบค่อยเปลี่ยนได้)
        res = (
            self.sb.table(self.TABLE)
            .update({"case_detail": patch})
            .eq("case_id", case_id)
            .execute()
        )
        _ = res
