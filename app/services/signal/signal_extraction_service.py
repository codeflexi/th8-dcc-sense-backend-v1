from app.services.signal.signal_models import (
    CaseSignal,
    CounterpartySignal,
    ItemSignal,
    TimeWindowSignal,
    QueryContextSignal,
)


class SignalExtractionService:

    @staticmethod
    def extract(case: dict, line_items: list[dict]) -> CaseSignal:
        """
        Cheap, deterministic, recomputable
        """

        counterparty = CounterpartySignal(
            counterparty_id=case.get("entity_id"),
            counterparty_type=case.get("entity_type"),
            confidence=1.0,
            source="CASE_CONTEXT"
        )

        items = []
        keywords = []
        text_parts = []

        for li in (line_items or []):
            # ---- canonical name key (new) with fallback (old) ----
            name = li.get("name") or li.get("item_name")

            # ---- unit_price normalize: support dict(new) or number(old) ----
            up = li.get("unit_price")
            if isinstance(up, dict):
                unit_price_value = up.get("value")
                unit_price_ccy = up.get("currency")
            else:
                unit_price_value = up
                unit_price_ccy = li.get("currency")

            item = ItemSignal(
                sku=li.get("sku"),
                item_name=name,
                quantity=li.get("quantity"),
                uom=li.get("uom"),
                unit_price=unit_price_value,
                currency=unit_price_ccy,
            )
            items.append(item)

            # ---- query context ----
            if name:
                keywords.append(name)
                text_parts.append(name)
            if li.get("sku"):
                keywords.append(li["sku"])

        query_context = QueryContextSignal(
            text=" ".join(text_parts).strip(),
            keywords=sorted(set([k for k in keywords if k])),
        )

        return CaseSignal(
            case_id=case["case_id"],
            counterparty=counterparty,
            items=items,
            time_window=TimeWindowSignal(lookback_months=12),
            query_context=query_context,
        )
