# app/services/orchestrators/ledger_orchestrator.py
from __future__ import annotations

import hashlib
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, List, Tuple


def _s(x: Any) -> str:
    return "" if x is None else str(x).strip()


def _d(x: Any) -> Decimal:
    if x is None:
        return Decimal("0")
    if isinstance(x, Decimal):
        return x
    try:
        return Decimal(str(x))
    except (InvalidOperation, ValueError, TypeError):
        return Decimal("0")


def _sha256_hex(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


class LedgerOrchestrator:
    """
    Finance AP 3-Way Matching orchestrator (enterprise-grade, deterministic)

    Responsibilities:
    - Resolve transaction_id from case
    - Aggregate PO / GRN / INVOICE lines
    - Build C3.5-compatible selection payload
    - Ensure dcc_case_evidence_groups rows exist (FK-safe)
    """

    domain = "finance_ap"

    def __init__(self, sb: Any):
        self.sb = sb

    # =====================================================
    # Load helpers
    # =====================================================

    def _get_case(self, case_id: str) -> Dict[str, Any]:
        res = (
            self.sb.table("dcc_cases")
            .select("*")
            .eq("case_id", case_id)
            .limit(1)
            .execute()
        )
        data = getattr(res, "data", None) or []
        if not data:
            raise ValueError(f"Case not found: {case_id}")
        return data[0]

    def _get_case_line_items(self, case_id: str) -> List[Dict[str, Any]]:
        res = (
            self.sb.table("dcc_case_line_items")
            .select("*")
            .eq("case_id", case_id)
            .execute()
        )
        return getattr(res, "data", None) or []

    def _resolve_transaction_id(self, case: Dict[str, Any]) -> str:
        tx = case.get("transaction_id") or case.get("source_transaction_id")
        if tx:
            return str(tx)

        detail = case.get("case_detail") or {}
        if isinstance(detail, dict):
            tx = detail.get("transaction_id") or detail.get("source_transaction_id")
            if tx:
                return str(tx)

        raise ValueError("finance_ap case must reference transaction_id")

    def _list_tx_lines(self, transaction_id: str) -> List[Dict[str, Any]]:
        res = (
            self.sb.table("dcc_transaction_line_items")
            .select("*")
            .eq("transaction_id", transaction_id)
            .execute()
        )
        return getattr(res, "data", None) or []

    def _split_by_source(
        self, lines: List[Dict[str, Any]]
    ) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]]]:
        po, gr, inv = [], [], []

        for ln in lines or []:
            t = _s(
                ln.get("source_type")
                or ln.get("doc_type")
                or ln.get("line_type")
                or ln.get("transaction_type")
            ).upper()

            if t in ("PO", "PURCHASE_ORDER", "PROCUREMENT_PO"):
                po.append(ln)
            elif t in ("GRN", "GR", "GOODS_RECEIPT"):
                gr.append(ln)
            elif t in ("INVOICE", "INV", "AP_INVOICE"):
                inv.append(ln)

        return po, gr, inv

    def _agg_by_sku(self, lines: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
        out: Dict[str, Dict[str, Any]] = {}

        for ln in lines or []:
            sku = _s(ln.get("sku") or ln.get("item_sku") or ln.get("product_sku"))
            if not sku:
                continue

            qty = _d(ln.get("quantity") or ln.get("qty"))
            unit_price = (
                ln.get("unit_price")
                or ln.get("price_per_unit")
                or ln.get("unit_cost")
            )

            if sku not in out:
                out[sku] = {
                    "sku": sku,
                    "qty": Decimal("0"),
                    "unit_price": unit_price,
                }

            out[sku]["qty"] += qty

            if out[sku]["unit_price"] is None and unit_price is not None:
                out[sku]["unit_price"] = unit_price

        return out

    # =====================================================
    # Evidence group (FK-safe)
    # =====================================================

    def _ensure_evidence_groups(
        self, *, case_id: str, case_items: List[Dict[str, Any]], created_by: str
    ) -> None:

        rows: List[Dict[str, Any]] = []

        for it in case_items or []:
            item_id = it.get("item_id") or it.get("case_line_item_id")
            if not item_id:
                continue

            sku = _s(it.get("sku"))
            group_key = sku or "UNKNOWN_SKU"
            semantic_key = f"sku:{group_key}"

            rows.append(
                {
                    "group_id": str(item_id),
                    "case_id": case_id,
                    "group_type": "LEDGER",
                    "claim_type": "3WAY",
                    "group_key": group_key,
                    "semantic_key": semantic_key,
                    "anchor_type": "PO_ITEM",
                    "anchor_id": str(item_id),
                    "evidence_ids": [],
                    "created_by": created_by,
                }
            )

        if not rows:
            return

        # ✅ correct table name
        try:
            self.sb.table("dcc_case_evidence_groups").upsert(rows).execute()
        except Exception:
            self.sb.table("dcc_case_evidence_groups").insert(rows).execute()

    # =====================================================
    # Duplicate invoice detection
    # =====================================================

    def _dup_invoice_flag_best_effort(
        self,
        *,
        entity_id: str,
        vendor_id: str,
        invoice_number: str,
        current_txn_id: str,
    ) -> int:

        if not vendor_id or not invoice_number:
            return 0

        try:
            q = (
                self.sb.table("dcc_transactions")
                .select("transaction_id")
                .eq("entity_id", entity_id)
                .eq("vendor_id", vendor_id)
                .eq("invoice_number", invoice_number)
                .limit(2)
                .execute()
            )

            rows = getattr(q, "data", None) or []
            for r in rows:
                if str(r.get("transaction_id")) != str(current_txn_id):
                    return 1
            return 0

        except Exception:
            return 0

    # =====================================================
    # Public API
    # =====================================================

    def prepare_context(
        self, *, case_id: str, actor_id: str = "SYSTEM", force_prepare: bool = False
    ):
        from app.services.orchestrators.base_orchestrator import (
            OrchestratorOutput,
        )

        case = self._get_case(case_id)
        entity_id = _s(case.get("entity_id"))
        tx_id = self._resolve_transaction_id(case)

        case_items = self._get_case_line_items(case_id)

        # Ensure FK-safe evidence groups
        self._ensure_evidence_groups(
            case_id=case_id,
            case_items=case_items,
            created_by=actor_id,
        )

        tx_lines = self._list_tx_lines(tx_id)
        po_lines, gr_lines, inv_lines = self._split_by_source(tx_lines)

        po_ag = self._agg_by_sku(po_lines)
        gr_ag = self._agg_by_sku(gr_lines)
        inv_ag = self._agg_by_sku(inv_lines)

        vendor_id = _s(case.get("vendor_id"))
        invoice_number = _s(case.get("invoice_number"))
        invoice_fp = _sha256_hex(f"{vendor_id}::{invoice_number}".upper())

        dup_flag = self._dup_invoice_flag_best_effort(
            entity_id=entity_id,
            vendor_id=vendor_id,
            invoice_number=invoice_number,
            current_txn_id=tx_id,
        )

        groups: List[Dict[str, Any]] = []

        for it in case_items or []:
            sku = _s(it.get("sku"))
            item_id = it.get("item_id")

            if not sku or not item_id:
                continue

            qty_po = _d((po_ag.get(sku) or {}).get("qty"))
            qty_gr = _d((gr_ag.get(sku) or {}).get("qty"))
            qty_inv = _d((inv_ag.get(sku) or {}).get("qty"))

            over_gr_qty = max(qty_gr - qty_po, Decimal("0"))
            over_inv_qty = max(qty_inv - qty_gr, Decimal("0"))

            inv_without_gr_flag = Decimal("1") if (qty_gr == 0 and qty_inv > 0) else Decimal("0")

            ap_context = {
                "sku": sku,
                "qty_po": float(qty_po),
                "qty_gr": float(qty_gr),
                "qty_inv": float(qty_inv),
                "over_gr_qty": float(over_gr_qty),
                "over_inv_qty": float(over_inv_qty),
                "inv_without_gr_flag": float(inv_without_gr_flag),
                "po_unit_price": (po_ag.get(sku) or {}).get("unit_price"),
                "inv_unit_price": (inv_ag.get(sku) or {}).get("unit_price"),
                "vendor_id": vendor_id,
                "invoice_number": invoice_number,
                "invoice_fp": invoice_fp,
                "dup_flag": int(dup_flag),
            }

            groups.append(
                {
                    "group_id": str(item_id),
                    "group_key": {"sku": sku},
                    "selected_technique": "T_LEDGER_3WAY",
                    "baseline": None,
                    "baseline_source": None,
                    "selection_trace": [],
                    "readiness_flags": {"baseline_available": True},
                    "ap_context": ap_context,
                }
            )

        selection_override = {
            "case_id": case_id,
            "domain": "finance_ap",
            "groups": groups,
        }

        notes = {
            "transaction_id": tx_id,
            "group_count": len(groups),
            "vendor_id": vendor_id,
            "invoice_number": invoice_number,
            "entity_id": entity_id,
        }

        return OrchestratorOutput(
            domain="finance_ap",
            selection_override=selection_override,
            notes=notes,
        )
