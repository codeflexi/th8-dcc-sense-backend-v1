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

        # --- Counterparty ---
        counterparty = CounterpartySignal(
            counterparty_id=case.get("entity_id"),
            counterparty_type=case.get("entity_type"),
            confidence=1.0,
            source="CASE_CONTEXT"
        )

        # --- Items ---
        items = []
        keywords = []
        text_parts = []

        for li in line_items:
            item = ItemSignal(
                sku=li.get("sku"),
                item_name=li.get("item_name"),
                quantity=li.get("quantity"),
                uom=li.get("uom"),
                unit_price=li.get("unit_price"),
                currency=li.get("currency"),
            )
            items.append(item)

            if li.get("item_name"):
                keywords.append(li["item_name"])
                text_parts.append(li["item_name"])

            if li.get("sku"):
                keywords.append(li["sku"])

        # --- Query context for vector search ---
        query_context = QueryContextSignal(
            text=" ".join(text_parts),
            keywords=list(set(keywords))
        )

        return CaseSignal(
            case_id=case["case_id"],
            counterparty=counterparty,
            items=items,
            time_window=TimeWindowSignal(lookback_months=12),
            query_context=query_context,
        )
