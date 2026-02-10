# app/services/calculation_service.py

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from typing import Any, Dict, List, Optional, Tuple


@dataclass(frozen=True)
class CalculationResult:
    values: Dict[str, Any]          # computed output fields (e.g. {"variance_pct": 6.25})
    trace: List[Dict[str, Any]]     # audit trace per calculation


class CalculationService:
    """
    Stateless, deterministic calculation engine driven by policy.domains.<domain>.calculations
    Supported formula_id (extend safely later):
      - PCT_DIFF : percent difference between a and b => ((a-b)/b)*100
      - GT       : boolean a > b
      - LT       : boolean a < b
      - EQ       : boolean a == b
    """

    # -------------------------
    # Public API
    # -------------------------
    def compute_all(
        self,
        *,
        calcs: Dict[str, dict],
        ctx: Dict[str, Any],
        rounding: Dict[str, int],
    ) -> CalculationResult:
        values: Dict[str, Any] = {}
        trace: List[Dict[str, Any]] = []

        for calc_key, calc_def in (calcs or {}).items():
            out_field = ((calc_def or {}).get("output") or {}).get("field") or calc_key

            step = {
                "calc_key": calc_key,
                "formula_id": (calc_def or {}).get("formula_id"),
                "unit": (calc_def or {}).get("unit"),
                "inputs": {},
                "guards": (calc_def or {}).get("guards") or [],
                "output_field": out_field,
                "status": "SKIPPED",
                "note": None,
            }

            # 1) resolve inputs
            resolved_inputs, input_trace = self._resolve_inputs(calc_def.get("inputs") or {}, ctx)
            step["inputs"] = input_trace

            # 2) guards
            ok, guard_note = self._check_guards(calc_def.get("guards") or [], resolved_inputs)
            if not ok:
                step["status"] = "SKIPPED"
                step["note"] = guard_note
                trace.append(step)
                continue

            # 3) compute
            try:
                result_value = self._compute(calc_def, resolved_inputs, rounding)
                values[str(out_field)] = result_value
                step["status"] = "OK"
                step["note"] = None
            except Exception as e:
                step["status"] = "ERROR"
                step["note"] = f"{type(e).__name__}: {e}"

            trace.append(step)

        return CalculationResult(values=values, trace=trace)

    # -------------------------
    # Core compute
    # -------------------------
    def _compute(self, calc_def: dict, inputs: Dict[str, Any], rounding: Dict[str, int]) -> Any:
        formula_id = str(calc_def.get("formula_id") or "").upper()
        unit = str(calc_def.get("unit") or "").upper()

        if formula_id == "PCT_DIFF":
            # expects: po_unit_price_value, baseline_price_value (names are policy-defined)
            a = self._dec_any(self._first(inputs, ["po_unit_price_value", "left_value", "a_value", "a"]))
            b = self._dec_any(self._first(inputs, ["baseline_price_value", "right_value", "b_value", "b"]))
            if a is None or b is None:
                raise ValueError("PCT_DIFF missing inputs")
            if b == 0:
                raise ValueError("PCT_DIFF baseline is zero")

            pct = ((a - b) / b) * Decimal("100")

            # rounding key from output.rounding (e.g., "pct_decimals")
            out_rounding_key = ((calc_def.get("output") or {}).get("rounding") or "pct_decimals")
            decimals = int(rounding.get(out_rounding_key, 2))

            return self._round_decimal(pct, decimals)

        if formula_id in ("GT", "LT", "EQ"):
            left = self._dec_any(self._first(inputs, ["left_value", "po_unit_price_value", "a_value", "a"]))
            right = self._dec_any(self._first(inputs, ["right_value", "baseline_price_value", "b_value", "b"]))
            if left is None or right is None:
                raise ValueError(f"{formula_id} missing inputs")

            if formula_id == "GT":
                return bool(left > right)
            if formula_id == "LT":
                return bool(left < right)
            return bool(left == right)

        raise ValueError(f"Unsupported formula_id: {formula_id} (unit={unit})")

    # -------------------------
    # Input resolution
    # -------------------------
    def _resolve_inputs(self, inputs_spec: Dict[str, Any], ctx: Dict[str, Any]) -> Tuple[Dict[str, Any], Dict[str, Any]]:
        resolved: Dict[str, Any] = {}
        trace: Dict[str, Any] = {}

        for name, ref in (inputs_spec or {}).items():
            val = None
            if isinstance(ref, str) and ref.startswith("$"):
                val = self._resolve_path(ctx, ref)
                trace[name] = {"ref": ref, "value": val}
            else:
                # literal
                val = ref
                trace[name] = {"ref": None, "value": val}

            resolved[str(name)] = val

        return resolved, trace

    def _resolve_path(self, ctx: Dict[str, Any], ref: str) -> Any:
        """
        Supports simple JSON pointer-ish path:
          "$po.unit_price.value"
          "$selection.baseline.value"
        """
        path = ref[1:]  # remove $
        if path.startswith("."):
            path = path[1:]
        if not path:
            return ctx

        cur: Any = ctx
        for part in path.split("."):
            if cur is None:
                return None
            if isinstance(cur, dict):
                cur = cur.get(part)
            else:
                # do not traverse non-dicts
                return None
        return cur

    # -------------------------
    # Guards
    # -------------------------
    def _check_guards(self, guards: List[dict], inputs: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
        for g in guards or []:
            if not isinstance(g, dict):
                continue

            if "not_null" in g:
                fields = g.get("not_null") or []
                for f in fields:
                    if inputs.get(str(f)) is None:
                        return False, f"GUARD_NOT_NULL_FAILED:{f}"

            if "non_zero" in g:
                fields = g.get("non_zero") or []
                for f in fields:
                    v = self._dec_any(inputs.get(str(f)))
                    if v is None:
                        return False, f"GUARD_NON_ZERO_INPUT_MISSING:{f}"
                    if v == 0:
                        return False, f"GUARD_NON_ZERO_FAILED:{f}"

        return True, None

    # -------------------------
    # Utils
    # -------------------------
    def _first(self, d: Dict[str, Any], keys: List[str]) -> Any:
        for k in keys:
            if k in d:
                return d.get(k)
        return None

    def _dec_any(self, v: Any) -> Optional[Decimal]:
        if v is None:
            return None
        try:
            return Decimal(str(v))
        except (InvalidOperation, ValueError, TypeError):
            return None

    def _round_decimal(self, v: Decimal, decimals: int) -> float:
        q = Decimal("1").scaleb(-decimals)  # 10^-decimals
        return float(v.quantize(q, rounding=ROUND_HALF_UP))
