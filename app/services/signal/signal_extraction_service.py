# app/services/signal/signal_extraction_service.py

from __future__ import annotations

from datetime import datetime
from typing import Optional, List

from app.services.signal.signal_models import (
    CaseSignal,
    CounterpartySignal,
    ItemSignal,
    TimeWindowSignal,
    QueryContextSignal,
)


# ---------------------------------------------------------
# Internal deterministic helpers
# ---------------------------------------------------------

def _norm_str(x: Optional[str]) -> Optional[str]:
    if not x:
        return None
    s = str(x).strip()
    return s or None


def _norm_sku(x: Optional[str]) -> Optional[str]:
    if not x:
        return None
    return str(x).strip().upper()


def _safe_date(x) -> Optional[str]:
    """
    Return ISO date string if possible (no timezone conversion here).
    Used only as signal reference.
    """
    if not x:
        return None
    try:
        if isinstance(x, datetime):
            return x.date().isoformat()
        return str(x)
    except Exception:
        return None


# =========================================================
# Signal Extraction (Enterprise Deterministic)
# =========================================================

class SignalExtractionService:

    @staticmethod
    def extract(case: dict, line_items: List[dict]) -> CaseSignal:
        """
        Deterministic, cheap, recomputable.
        No DB call.
        No ML.
        No hidden logic.
        """

        if not case:
            raise ValueError("case payload required")

        case_id = case.get("case_id")
        if not case_id:
            raise ValueError("case_id missing in case")

        # -------------------------------------------------
        # Counterparty (Vendor / Customer / etc.)
        # -------------------------------------------------
        counterparty_id = case.get("entity_id")
        counterparty_type = case.get("entity_type")

        counterparty = CounterpartySignal(
            counterparty_id=_norm_str(counterparty_id),
            counterparty_type=_norm_str(counterparty_type),
            confidence=1.0 if counterparty_id else 0.0,
            source="CASE_CONTEXT"
        )

        # -------------------------------------------------
        # Items
        # -------------------------------------------------
        items: List[ItemSignal] = []
        keywords: List[str] = []
        text_parts: List[str] = []

        for li in (line_items or []):
            if not isinstance(li, dict):
                continue

            # ---- canonical item name (schema tolerant) ----
            name = (
                li.get("item_name")
                or li.get("name")
                or li.get("description")
            )
            name = _norm_str(name)

            # ---- SKU normalize ----
            sku = _norm_sku(li.get("sku") or li.get("item_code"))

            # ---- quantity ----
            qty = li.get("quantity") or li.get("qty")

            # ---- UOM ----
            uom = li.get("uom")

            # ---- unit_price normalize (dict or scalar) ----
            up = li.get("unit_price")

            if isinstance(up, dict):
                unit_price_value = up.get("value")
                unit_price_ccy = up.get("currency")
            else:
                unit_price_value = up
                unit_price_ccy = li.get("currency")

            item = ItemSignal(
                sku=sku,
                item_name=name,
                quantity=qty,
                uom=uom,
                unit_price=unit_price_value,
                currency=unit_price_ccy,
            )
            items.append(item)

            # ---- query context build ----
            if name:
                keywords.append(name)
                text_parts.append(name)

            if sku:
                keywords.append(sku)

        # -------------------------------------------------
        # Query Context
        # -------------------------------------------------
        query_context = QueryContextSignal(
            text=" ".join(text_parts).strip(),
            keywords=sorted(set([k for k in keywords if k])),
        )

        # -------------------------------------------------
        # Time Window (Discovery & Analytics)
        # -------------------------------------------------
        # Prefer document_date → created_at → now
        ref_date = (
            case.get("document_date")
            or case.get("created_at")
        )

        ref_date_str = _safe_date(ref_date)

        time_window = TimeWindowSignal(
            lookback_months=12,
            reference_date=ref_date_str,
        )

        # -------------------------------------------------
        # Return CaseSignal
        # -------------------------------------------------
        return CaseSignal(
            case_id=case_id,
            counterparty=counterparty,
            items=items,
            time_window=time_window,
            query_context=query_context,
        )
