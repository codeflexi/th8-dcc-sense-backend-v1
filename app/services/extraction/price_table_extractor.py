from __future__ import annotations

import re
from dataclasses import dataclass
from typing import List, Tuple, Dict, Any


# -----------------------------
# Data model (internal)
# -----------------------------
@dataclass
class PriceRow:
    page_number: int
    sku: str
    name: str
    unit_price: float
    currency: str
    uom: str | None
    snippet: str
    confidence: float
    highlight_text: str | None = None


# -----------------------------
# Helpers
# -----------------------------
def _clean_price(raw: str) -> tuple[float, str]:
    """
    '1,500.00 THB' -> (1500.0, 'THB')
    '45.00'        -> (45.0, 'THB')
    """
    if not raw:
        return 0.0, "THB"

    raw = raw.strip().upper()
    currency = "THB"

    if "THB" in raw:
        currency = "THB"
        raw = raw.replace("THB", "").strip()

    raw = re.sub(r"[^\d\.\-]", "", raw)
    try:
        return float(raw), currency
    except Exception:
        return 0.0, currency


def _normalize(text: str) -> str:
    return re.sub(r"\s+", " ", text.upper().strip())


# -----------------------------
# Main extractor
# -----------------------------
def extract_price_rows_from_pages(
    pages: List[Dict[str, Any]],
) -> Tuple[List[PriceRow], List[Dict[str, Any]]]:
    """
    Extract markdown price tables from page_text.
    Supports headers:
      - Item Code
      - Item Description
      - Net Unit Price
    """

    results: List[PriceRow] = []
    rejected: List[Dict[str, Any]] = []

    HEADER_MAP = {
        "sku": ["ITEM CODE", "SKU", "CODE"],
        "name": ["ITEM DESCRIPTION", "DESCRIPTION", "ITEM"],
        "price": ["NET UNIT PRICE", "UNIT PRICE", "PRICE"],
    }

    for page in pages:
        page_number = page.get("page_number")
        text = page.get("text") or ""
        lines = [l.strip() for l in text.splitlines() if "|" in l]

        if len(lines) < 2:
            continue

        header_line = _normalize(lines[0])
        headers = [h.strip() for h in lines[0].split("|") if h.strip()]

        col_index: dict[str, int] = {}

        for idx, h in enumerate(headers):
            h_norm = _normalize(h)
            for key, variants in HEADER_MAP.items():
                if any(v in h_norm for v in variants):
                    col_index[key] = idx

        # ต้องมีครบ 3 column
        if not {"sku", "name", "price"}.issubset(col_index.keys()):
            continue

        for row_line in lines[2:]:
            cols = [c.strip() for c in row_line.split("|") if c.strip()]
            if len(cols) < len(headers):
                continue

            try:
                sku = cols[col_index["sku"]]
                name = cols[col_index["name"]]
                price_raw = cols[col_index["price"]]

                unit_price, currency = _clean_price(price_raw)

                if not sku or unit_price <= 0:
                    raise ValueError("Invalid price row")

                results.append(
                    PriceRow(
                        page_number=page_number,
                        sku=sku,
                        name=name,
                        unit_price=unit_price,
                        currency=currency,
                        uom=None,
                        snippet=row_line,
                        confidence=0.95,
                        highlight_text=None,
                    )
                )

            except Exception as e:
                rejected.append(
                    {
                        "page_number": page_number,
                        "row": row_line,
                        "error": str(e),
                    }
                )

    return results, rejected
