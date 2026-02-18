# app/services/decision/calculation_service.py
from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from typing import Any, Dict, List, Optional, Tuple
import hashlib


# =========================================================
# Result model (audit-ready)
# =========================================================

@dataclass
class CalcResult:
    values: Dict[str, Any]
    trace: List[Dict[str, Any]]


# =========================================================
# Calculation Service (Enterprise v1)
# - Deterministic
# - No external calls
# - Traceable outputs
# =========================================================

class CalculationService:
    def compute_all(
        self,
        *,
        calcs: List[Dict[str, Any]],
        ctx: Dict[str, Any],
        rounding: Optional[Dict[str, Any]] = None,
    ) -> CalcResult:
        rounding = rounding or {}
        values: Dict[str, Any] = {}
        trace: List[Dict[str, Any]] = []

        for c in calcs or []:
            if not isinstance(c, dict):
                continue

            formula_id = str(c.get("formula_id") or "").strip().upper()
            inputs = c.get("inputs") or {}
            guards = c.get("guards") or []
            out_cfg = c.get("output") or {}
            out_field = str(out_cfg.get("field") or "").strip()

            step = {
                "formula_id": formula_id,
                "output_field": out_field,
                "inputs": {},
                "guards": guards,
                "status": "SKIPPED",
                "error": None,
            }

            if not out_field:
                step["status"] = "ERROR"
                step["error"] = "missing output.field"
                trace.append(step)
                continue

            # Resolve inputs from ctx
            resolved_inputs: Dict[str, Any] = {}
            for k, v in inputs.items():
                resolved_inputs[k] = self._resolve(v, ctx)
            step["inputs"] = self._json_safe(resolved_inputs)

            # Guards check
            ok, reason = self._guards_ok(guards, resolved_inputs)
            if not ok:
                step["status"] = "GUARD_BLOCKED"
                step["error"] = reason
                trace.append(step)
                continue

            try:
                # Execute formula
                out_val = self._execute_formula(formula_id, resolved_inputs, rounding=rounding, cfg=c)

                # Save value
                values[out_field] = self._json_safe(out_val)

                step["status"] = "OK"
                step["result"] = self._json_safe(out_val)
            except Exception as e:
                step["status"] = "ERROR"
                step["error"] = str(e)

            trace.append(step)

        return CalcResult(values=values, trace=trace)

    # =====================================================
    # Formula dispatcher
    # =====================================================

    def _execute_formula(
        self,
        formula_id: str,
        inputs: Dict[str, Any],
        *,
        rounding: Dict[str, Any],
        cfg: Dict[str, Any],
    ) -> Any:
        if formula_id == "PCT_DIFF":
            # returns percent number (e.g. 6.25 means 6.25%)
            a = self._dec(inputs.get("po_unit_price_value"))
            b = self._dec(inputs.get("baseline_price_value"))
            if a is None or b is None or b == 0:
                raise ValueError("PCT_DIFF invalid inputs")
            pct = (a - b) / b * Decimal("100")
            return self._round(pct, int(rounding.get("pct_decimals", 2)))

        if formula_id == "GT":
            left = self._dec(inputs.get("left_value"))
            right = self._dec(inputs.get("right_value"))
            if left is None or right is None:
                raise ValueError("GT invalid inputs")
            return bool(left > right)

        if formula_id == "ABS_DIFF_LTE":
            left = self._dec(inputs.get("left_value"))
            right = self._dec(inputs.get("right_value"))
            expected = self._dec(inputs.get("expected_value"))
            if left is None or right is None or expected is None:
                raise ValueError("ABS_DIFF_LTE invalid inputs")
            diff = left - right
            if diff < 0:
                diff = diff * Decimal("-1")
            return bool(diff <= expected)

        if formula_id == "THREE_WAY_MATCH":
            # Deterministic 3-way match (PO vs GRN vs Invoice)
            # Inputs expected:
            #  - po_lines: list[dict]
            #  - gr_lines: list[dict]
            #  - inv_lines: list[dict]
            # Optional:
            #  - qty_abs_tolerance (default 0)
            #  - qty_pct_tolerance (default 0)
            #  - price_abs_tolerance (default 0)
            #  - price_pct_tolerance (default 0)
            po_lines = self._as_list(inputs.get("po_lines"))
            gr_lines = self._as_list(inputs.get("gr_lines"))
            inv_lines = self._as_list(inputs.get("inv_lines"))

            params = cfg.get("params") or {}
            qty_abs_tol = self._dec(params.get("qty_abs_tolerance")) or Decimal("0")
            qty_pct_tol = self._dec(params.get("qty_pct_tolerance")) or Decimal("0")
            price_abs_tol = self._dec(params.get("price_abs_tolerance")) or Decimal("0")
            price_pct_tol = self._dec(params.get("price_pct_tolerance")) or Decimal("0")

            result = three_way_match(
                po_lines=po_lines,
                gr_lines=gr_lines,
                inv_lines=inv_lines,
                qty_abs_tolerance=qty_abs_tol,
                qty_pct_tolerance=qty_pct_tol,
                price_abs_tolerance=price_abs_tol,
                price_pct_tolerance=price_pct_tol,
                money_decimals=int(rounding.get("money_decimals", 2)),
            )
            return result

        if formula_id == "DUP_INVOICE":
            # Deterministic: hash(vendor_id + invoice_number)
            inv = str(inputs.get("invoice_number") or "").strip().upper()
            ven = str(inputs.get("vendor_id") or "").strip().upper()
            if not inv or not ven:
                raise ValueError("DUP_INVOICE missing invoice_number/vendor_id")
            key = f"{ven}::{inv}".encode("utf-8")
            # returning a stable fingerprint; caller/rule can use lookup elsewhere later
            return {"fingerprint": hashlib.sha256(key).hexdigest(), "flag": True}

        raise ValueError(f"unknown formula_id={formula_id}")

    # =====================================================
    # Guards
    # =====================================================

    def _guards_ok(self, guards: List[Any], inputs: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
        for g in guards or []:
            if not isinstance(g, dict):
                continue

            if "not_null" in g:
                fields = g.get("not_null") or []
                for f in fields:
                    if inputs.get(f) is None:
                        return False, f"not_null failed: {f}"

            if "non_zero" in g:
                fields = g.get("non_zero") or []
                for f in fields:
                    v = self._dec(inputs.get(f))
                    if v is None or v == 0:
                        return False, f"non_zero failed: {f}"

        return True, None

    # =====================================================
    # Context resolver: supports "$a.b.c"
    # =====================================================

    def _resolve(self, expr: Any, ctx: Dict[str, Any]) -> Any:
        if not isinstance(expr, str):
            return expr
        s = expr.strip()
        if not s.startswith("$"):
            return expr

        path = s[1:].split(".")
        cur: Any = ctx
        for p in path:
            if cur is None:
                return None
            if isinstance(cur, dict):
                cur = cur.get(p)
            else:
                return None
        return cur

    # =====================================================
    # Utils
    # =====================================================

    def _dec(self, v: Any) -> Optional[Decimal]:
        if v is None:
            return None
        try:
            return Decimal(str(v))
        except (InvalidOperation, ValueError, TypeError):
            return None

    def _round(self, v: Decimal, n: int) -> float:
        q = Decimal("1") / (Decimal("10") ** Decimal(n))
        return float(v.quantize(q, rounding=ROUND_HALF_UP))

    def _as_list(self, v: Any) -> List[Dict[str, Any]]:
        if v is None:
            return []
        if isinstance(v, list):
            return [x for x in v if isinstance(x, dict)]
        return []

    def _json_safe(self, v: Any) -> Any:
        if isinstance(v, Decimal):
            return str(v)
        if isinstance(v, dict):
            return {str(k): self._json_safe(x) for k, x in v.items()}
        if isinstance(v, list):
            return [self._json_safe(x) for x in v]
        return v


# =========================================================
# 3-Way Matching (Enterprise deterministic)
# - Works with partial receipts & partial invoices (cumulative)
# - Matches by sku (preferred) else item_code else item_name hash
# =========================================================

def three_way_match(
    *,
    po_lines: List[Dict[str, Any]],
    gr_lines: List[Dict[str, Any]],
    inv_lines: List[Dict[str, Any]],
    qty_abs_tolerance: Decimal,
    qty_pct_tolerance: Decimal,
    price_abs_tolerance: Decimal,
    price_pct_tolerance: Decimal,
    money_decimals: int,
) -> Dict[str, Any]:
    # Normalize & index
    po_idx = _index_lines(po_lines, kind="PO")
    gr_idx = _index_lines(gr_lines, kind="GR")
    inv_idx = _index_lines(inv_lines, kind="INV")

    keys = sorted(set(po_idx.keys()) | set(gr_idx.keys()) | set(inv_idx.keys()))
    mismatches: List[Dict[str, Any]] = []

    total_items = 0
    ok_items = 0

    for k in keys:
        po = po_idx.get(k) or _empty_bucket()
        gr = gr_idx.get(k) or _empty_bucket()
        inv = inv_idx.get(k) or _empty_bucket()

        # If item not in PO at all → deterministic mismatch (unexpected GR/INV)
        if po["qty"] == 0 and (gr["qty"] > 0 or inv["qty"] > 0):
            mismatches.append({
                "key": k,
                "type": "UNEXPECTED_ITEM",
                "po_qty": _d(po["qty"]),
                "gr_qty": _d(gr["qty"]),
                "inv_qty": _d(inv["qty"]),
            })
            total_items += 1
            continue

        # If item exists in PO
        if po["qty"] > 0:
            total_items += 1

            # Qty checks (cumulative)
            # 1) GR cannot exceed PO beyond tolerance
            if not _within_upper_bound(
                actual=gr["qty"],
                allowed=po["qty"],
                abs_tol=qty_abs_tolerance,
                pct_tol=qty_pct_tolerance,
            ):
                mismatches.append({
                    "key": k,
                    "type": "QTY_GR_EXCEEDS_PO",
                    "po_qty": _d(po["qty"]),
                    "gr_qty": _d(gr["qty"]),
                    "tolerance": _tol_payload(qty_abs_tolerance, qty_pct_tolerance),
                })
                continue

            # 2) Invoice cannot exceed GR beyond tolerance (if there is GR)
            # If no GR yet, still allow INV? enterprise rule: FAIL (needs GR)
            if gr["qty"] <= 0 and inv["qty"] > 0:
                mismatches.append({
                    "key": k,
                    "type": "INVOICE_WITHOUT_GR",
                    "po_qty": _d(po["qty"]),
                    "gr_qty": _d(gr["qty"]),
                    "inv_qty": _d(inv["qty"]),
                })
                continue

            if inv["qty"] > 0 and not _within_upper_bound(
                actual=inv["qty"],
                allowed=gr["qty"],
                abs_tol=qty_abs_tolerance,
                pct_tol=qty_pct_tolerance,
            ):
                mismatches.append({
                    "key": k,
                    "type": "QTY_INV_EXCEEDS_GR",
                    "gr_qty": _d(gr["qty"]),
                    "inv_qty": _d(inv["qty"]),
                    "tolerance": _tol_payload(qty_abs_tolerance, qty_pct_tolerance),
                })
                continue

            # Price checks
            # invoice unit_price compared to PO unit_price (weighted avg for invoice)
            po_price = po["unit_price"]
            inv_price = inv["unit_price"]

            # If invoice exists but missing price → mismatch
            if inv["qty"] > 0 and inv_price is None:
                mismatches.append({
                    "key": k,
                    "type": "MISSING_INVOICE_PRICE",
                    "po_unit_price": _d(po_price) if po_price is not None else None,
                })
                continue

            # If invoice exists, require price within tolerance
            if inv["qty"] > 0 and po_price is not None and inv_price is not None:
                if not _within_price_tolerance(
                    actual=inv_price,
                    expected=po_price,
                    abs_tol=price_abs_tolerance,
                    pct_tol=price_pct_tolerance,
                ):
                    mismatches.append({
                        "key": k,
                        "type": "PRICE_INV_DIFFERS_FROM_PO",
                        "po_unit_price": _d(po_price),
                        "inv_unit_price": _d(inv_price),
                        "tolerance": _tol_payload(price_abs_tolerance, price_pct_tolerance),
                    })
                    continue

            ok_items += 1

    ok = (len(mismatches) == 0)
    return {
        "ok": ok,
        "items_total": total_items,
        "items_ok": ok_items,
        "mismatches": mismatches,
        "inputs_summary": {
            "po_lines": len(po_lines or []),
            "gr_lines": len(gr_lines or []),
            "inv_lines": len(inv_lines or []),
        },
    }


# =========================================================
# Internal line normalization helpers
# =========================================================

def _index_lines(lines: List[Dict[str, Any]], *, kind: str) -> Dict[str, Dict[str, Any]]:
    out: Dict[str, Dict[str, Any]] = {}
    for l in lines or []:
        key = _match_key(l)
        if not key:
            continue

        qty = _dec_any(l.get("quantity") or l.get("qty") or l.get("qty_received") or l.get("qty_invoiced"))
        if qty is None:
            qty = Decimal("0")

        unit_price = _dec_any(l.get("unit_price") or l.get("price"))
        line_total = _dec_any(l.get("total_price") or l.get("line_total"))

        b = out.setdefault(key, _empty_bucket())
        # qty cumulative sum
        b["qty"] += qty

        # unit_price: use weighted avg if possible (by qty)
        if unit_price is not None:
            if b["unit_price"] is None:
                b["unit_price"] = unit_price
                b["_weighted_qty"] = qty
            else:
                # weighted average
                wq = b.get("_weighted_qty") or Decimal("0")
                if (wq + qty) > 0:
                    b["unit_price"] = ((b["unit_price"] * wq) + (unit_price * qty)) / (wq + qty)
                    b["_weighted_qty"] = (wq + qty)

        # keep some labels
        b["kind_seen"].add(kind)
    return out


def _empty_bucket() -> Dict[str, Any]:
    return {"qty": Decimal("0"), "unit_price": None, "_weighted_qty": Decimal("0"), "kind_seen": set()}


def _match_key(l: Dict[str, Any]) -> Optional[str]:
    sku = (l.get("sku") or l.get("item_code") or l.get("product_code") or "")
    sku = str(sku).strip().upper()
    if sku:
        return f"SKU:{sku}"

    name = (l.get("item_name") or l.get("description") or "")
    name = str(name).strip().upper()
    if name:
        # deterministic fallback key
        h = hashlib.sha1(name.encode("utf-8")).hexdigest()[:12]
        return f"NAME:{h}"
    return None


def _dec_any(v: Any) -> Optional[Decimal]:
    if v is None:
        return None
    try:
        return Decimal(str(v))
    except Exception:
        return None


def _within_upper_bound(*, actual: Decimal, allowed: Decimal, abs_tol: Decimal, pct_tol: Decimal) -> bool:
    # actual <= allowed + max(abs_tol, allowed*pct_tol)
    pct = (allowed * pct_tol / Decimal("100")) if pct_tol > 0 else Decimal("0")
    tol = abs_tol if abs_tol >= pct else pct
    return actual <= (allowed + tol)


def _within_price_tolerance(*, actual: Decimal, expected: Decimal, abs_tol: Decimal, pct_tol: Decimal) -> bool:
    # |actual-expected| <= max(abs_tol, expected*pct_tol)
    diff = actual - expected
    if diff < 0:
        diff = -diff
    pct = (expected * pct_tol / Decimal("100")) if pct_tol > 0 else Decimal("0")
    tol = abs_tol if abs_tol >= pct else pct
    return diff <= tol


def _tol_payload(abs_tol: Decimal, pct_tol: Decimal) -> Dict[str, Any]:
    return {"abs": str(abs_tol), "pct": str(pct_tol)}


def _d(x: Any) -> Any:
    if isinstance(x, Decimal):
        return str(x)
    return x
