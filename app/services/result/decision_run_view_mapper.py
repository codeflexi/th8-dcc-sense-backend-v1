from __future__ import annotations

from typing import Any, Dict, List, Optional
from datetime import datetime

from app.schemas.decision_run_view_model import (
    ArtifactFlags,
    DecisionRunItemView,
    DecisionRunViewContext,
    DriverInfo,
    ExposureInfo,
    ItemInfo,
    PolicyInfo,
    PriceInfo,
    QuantityFlags,
    QuantityInfo,
    RuleCalc,
    RuleView,
    RunSummary,
    StatusInfo,
    TopReasonCode,
)
from app.services.result.policy_registry import PolicyRegistry

# ViewModel constraints (pydantic):
# - StatusInfo.decision: Literal[APPROVE, REVIEW, ESCALATE, REJECT]
# - RuleCalc.field: str (required) BUT RuleView.calculation is Optional
# - PriceInfo.context: Literal[BASELINE, 3WAY_MATCH, UNKNOWN] (required)


DECISION_ORDER = {"REJECT": 4, "ESCALATE": 3, "REVIEW": 2, "APPROVE": 1}
RISK_ORDER = {"CRITICAL": 4, "HIGH": 3, "MED": 2, "LOW": 1}


def _upper(x: Any, default: str = "") -> str:
    return str(x or default).strip().upper()


def _to_float(x: Any, default: float = 0.0) -> float:
    try:
        if x is None:
            return default
        return float(x)
    except Exception:
        return default


def _to_str(x: Any, default: str = "") -> str:
    if x is None:
        return default
    s = str(x)
    return s if s is not None else default


def _parse_dt(x: Any) -> Optional[datetime]:
    if not x:
        return None
    try:
        return datetime.fromisoformat(str(x).replace("Z", "+00:00"))
    except Exception:
        return None


def _normalize_decision(x: Any) -> str:
    """
    Map raw engine statuses -> ViewModel DecisionStatus
    - PASS -> APPROVE
    - FAIL -> REVIEW
    """
    v = _upper(x, "REVIEW")
    if v in ("APPROVE", "REVIEW", "ESCALATE", "REJECT"):
        return v
    if v == "PASS":
        return "APPROVE"
    if v == "FAIL":
        return "REVIEW"
    return "REVIEW"


def _normalize_risk(x: Any) -> str:
    v = _upper(x, "LOW")
    if v in ("LOW", "MED", "HIGH", "CRITICAL"):
        return v
    return "LOW"


# -------------------------------------------------------
# DOMAIN DETECTION
# -------------------------------------------------------
def _detect_domain(trace: Dict[str, Any]) -> str:
    rules = trace.get("rules") or []
    for r in rules:
        d = r.get("domain")
        if d:
            return str(d)
    return "unknown"


# -------------------------------------------------------
# PRICE NORMALIZATION -> PriceInfo fields
# -------------------------------------------------------
def _normalize_price(domain: str, trace: Dict[str, Any]) -> Dict[str, Any]:
    """
    Return dict compatible with PriceInfo constructor:
    - context
    - po_unit, inv_unit, baseline_unit
    - variance_pct, variance_abs
    - tolerance_abs, currency
    - within_tolerance, has_baseline
    """
    explain = trace.get("explainability") or {}
    price_explain = explain.get("price") or {}
    qty_explain = explain.get("qty") or {}

    po_item = (trace.get("inputs") or {}).get("po_item") or {}
    po_unit_from_po = _to_float(((po_item.get("unit_price") or {}).get("value")), 0.0)
    currency = ((po_item.get("unit_price") or {}).get("currency")) or "THB"
    po_qty = _to_float(po_item.get("quantity"), 0.0)

    calc_values = ((trace.get("calculations") or {}).get("values") or {})

    if domain == "finance_ap":
        po_unit = _to_float(price_explain.get("po_unit_price"), po_unit_from_po)
        inv_unit = _to_float(price_explain.get("inv_unit_price"), 0.0)

        # diff_abs in explainability.price is "unit diff" in your raw trace
        unit_diff_abs = _to_float(price_explain.get("diff_abs"), abs(inv_unit - po_unit))
        variance_pct = _to_float(price_explain.get("diff_pct"), 0.0)

        tol_abs = _to_float(price_explain.get("tolerance_abs"), 0.0)
        inv_qty = _to_float(qty_explain.get("inv"), po_qty)

        variance_abs = unit_diff_abs * inv_qty

        within = bool(calc_values.get("price_within_tolerance", True))

        return dict(
            context="3WAY_MATCH",
            po_unit=po_unit,
            inv_unit=inv_unit,
            baseline_unit=None,
            variance_pct=variance_pct,
            variance_abs=variance_abs,
            tolerance_abs=tol_abs,
            currency=currency,
            within_tolerance=within,
            has_baseline=False,
        )

    # procurement
    if domain == "procurement":
        selection = trace.get("selection") or {}
        baseline_value = _to_float(((selection.get("baseline") or {}).get("value")), 0.0)
        has_baseline = bool(baseline_value and baseline_value > 0.0)

        # variance_pct typically produced by calculation layer; if baseline missing -> keep None
        variance_pct = None
        if has_baseline:
            variance_pct = _to_float((((trace.get("calculations") or {}).get("values") or {}).get("variance_pct")), 0.0)

        # exposure = (po_unit - baseline) * qty (matches your procurement summary 3600)
        variance_abs = (po_unit_from_po - baseline_value) * po_qty if has_baseline else 0.0

        return dict(
            context="BASELINE" if has_baseline else "UNKNOWN",
            po_unit=po_unit_from_po,
            inv_unit=0.0,  # UI-friendly (optional in schema, but keep numeric)
            baseline_unit=(baseline_value if has_baseline else None),
            variance_pct=variance_pct,
            variance_abs=variance_abs,
            tolerance_abs=0.0,
            currency=currency,
            within_tolerance=bool((variance_pct or 0.0) <= 0.0),
            has_baseline=has_baseline,
        )

    # unknown domain safe defaults
    return dict(
        context="UNKNOWN",
        po_unit=po_unit_from_po,
        inv_unit=0.0,
        baseline_unit=None,
        variance_pct=None,
        variance_abs=0.0,
        tolerance_abs=0.0,
        currency=currency,
        within_tolerance=True,
        has_baseline=False,
    )


def _quantity_flags_from_calc_values(calc_values: Dict[str, Any]) -> QuantityFlags:
    return QuantityFlags(
        gr_exceeds_po=bool(calc_values.get("gr_exceeds_po") or False),
        inv_exceeds_gr=bool(calc_values.get("inv_exceeds_gr") or False),
        inv_without_gr=bool(calc_values.get("inv_without_gr") or False),
    )


def _artifact_flags_from_trace(trace: Dict[str, Any]) -> ArtifactFlags:
    present = (trace.get("inputs") or {}).get("artifacts_present") or []
    present_set = {str(x).upper() for x in present}
    return ArtifactFlags(
        po=("PO" in present_set),
        grn=("GRN" in present_set),
        invoice=("INVOICE" in present_set),
    )


# -------------------------------------------------------
# CORE MAPPER
# -------------------------------------------------------
def to_decision_run_view_context(
    raw: Dict[str, Any],
    policy_registry: PolicyRegistry,
) -> DecisionRunViewContext:
    results = raw.get("results") or []
    

    
    items: List[DecisionRunItemView] = []

    decision_list: List[str] = []
    risk_list: List[str] = []
    confidence_list: List[float] = []
    variance_abs_sum = 0.0
    reason_counter: Dict[str, int] = {}

    first_trace: Optional[Dict[str, Any]] = None

    for res in results:
        decision = _normalize_decision(res.get("decision_status"))
        risk = _normalize_risk(res.get("risk_level"))
        confidence = _to_float(res.get("confidence"), 0.0)

        decision_list.append(decision)
        risk_list.append(risk)
        confidence_list.append(confidence)

        trace = res.get("trace") or {}
        if first_trace is None:
            first_trace = trace

        domain = _detect_domain(trace)

        calc_values = ((trace.get("calculations") or {}).get("values") or {})
        qty_flags = _quantity_flags_from_calc_values(calc_values)

        # ----------------------------
        # PRICE (Enterprise unified)
        # ----------------------------
        price_norm = _normalize_price(domain, trace)
        variance_abs_sum += _to_float(price_norm.get("variance_abs"), 0.0)

        # ----------------------------
        # RULE VIEW (calculation is Optional)
        # ----------------------------
        rule_views: List[RuleView] = []
        for r in (trace.get("rules") or []):
            rule_id = r.get("rule_id")
            meta = policy_registry.get_rule_meta(rule_id) or {}

            calc = r.get("calculation") or {}
            field = calc.get("field", None)

            calc_obj: Optional[RuleCalc] = None
            # IMPORTANT: only create RuleCalc when field exists and is non-empty
            if field is not None and str(field).strip() != "":
                calc_obj = RuleCalc(
                    field=_to_str(field, ""),
                    actual=calc.get("actual"),
                    expected=calc.get("expected"),
                    operator=calc.get("operator"),
                )

            explain = r.get("explanation") or {}

            exec_msg_raw = explain.get("exec")
            audit_msg_raw = explain.get("audit")

            result_raw = _upper(r.get("result"), "PASS")

            # enterprise message logic
            if result_raw == "FAIL":
                exec_msg = exec_msg_raw or audit_msg_raw
            else:
                # PASS → use audit wording (neutral/positive)
                exec_msg = audit_msg_raw or "ผ่านเงื่อนไข"

            rule_views.append(
                RuleView(
                    rule_id=_to_str(rule_id, ""),
                    group=_upper(r.get("group"), "OTHER"),
                    domain=_to_str(r.get("domain"), ""),
                    result=result_raw,
                    severity=_to_str(meta.get("severity") or r.get("severity"), "HIGH"),

                    exec_message=exec_msg,
                    audit_message=audit_msg_raw,

                    calculation=calc_obj,
                    fail_actions=r.get("fail_actions") or [],
                )
            )

        # ----------------------------
        # DRIVERS
        # ----------------------------
        drivers: List[DriverInfo] = []
        for code in (res.get("reason_codes") or []):
            meta = policy_registry.get_rule_meta(code) or {}
            drivers.append(
                DriverInfo(
                    rule_id=_to_str(code, ""),
                    label=_to_str(meta.get("label") or code, ""),
                    severity=_to_str(meta.get("severity") or "HIGH", "HIGH"),
                )
            )
            reason_counter[str(code)] = reason_counter.get(str(code), 0) + 1

        # ----------------------------
        # ITEM / QTY
        # ----------------------------
        po_item = (trace.get("inputs") or {}).get("po_item") or {}
        explain = trace.get("explainability") or {}
        qty_explain = explain.get("qty") or {}

        po_qty_fallback = _to_float(po_item.get("quantity"), 0.0)

        items.append(
            DecisionRunItemView(
                group_id=_to_str(res.get("group_id"), ""),
                status=StatusInfo(
                    decision=decision,
                    risk=risk,
                    confidence=confidence,
                ),
                item=ItemInfo(
                    sku=_to_str(po_item.get("sku"), ""),
                    name=_to_str(po_item.get("item_name"), ""),
                    uom=_to_str(po_item.get("uom"), ""),
                ),
                quantity=QuantityInfo(
                    po=_to_float(qty_explain.get("po"), po_qty_fallback),
                    gr=_to_float(qty_explain.get("gr"), 0.0),
                    inv=_to_float(qty_explain.get("inv"), 0.0),
                    over_gr_qty=_to_float(qty_explain.get("over_gr_qty"), 0.0),
                    over_inv_qty=_to_float(qty_explain.get("over_inv_qty"), 0.0),
                    flags=qty_flags,
                ),
                price=PriceInfo(
                    context=price_norm["context"],
                    po_unit=_to_float(price_norm.get("po_unit"), 0.0),
                    inv_unit=(None if price_norm.get("inv_unit") is None else _to_float(price_norm.get("inv_unit"), 0.0)),
                    baseline_unit=(None if price_norm.get("baseline_unit") is None else _to_float(price_norm.get("baseline_unit"), 0.0)),
                    variance_pct=(None if price_norm.get("variance_pct") is None else _to_float(price_norm.get("variance_pct"), 0.0)),
                    variance_abs=_to_float(price_norm.get("variance_abs"), 0.0),
                    tolerance_abs=_to_float(price_norm.get("tolerance_abs"), 0.0),
                    currency=_to_str(price_norm.get("currency"), "THB"),
                    within_tolerance=bool(price_norm.get("within_tolerance", True)),
                    has_baseline=bool(price_norm.get("has_baseline", False)),
                ),
                drivers=drivers,
                next_action=((res.get("fail_actions") or [{}])[0].get("type")),
                rules=rule_views,
                artifacts=_artifact_flags_from_trace(trace),
                created_at=_parse_dt(res.get("created_at")),
            )
        )

    # ----------------------------
    # SUMMARY
    # ----------------------------
    overall_decision = (
        max(decision_list, key=lambda x: DECISION_ORDER.get(x, 0)) if decision_list else "REVIEW"
    )
    overall_risk = max(risk_list, key=lambda x: RISK_ORDER.get(x, 0)) if risk_list else "LOW"

    summary = RunSummary(
        overall_decision=overall_decision,  # already normalized to allowed DecisionStatus
        risk_level=overall_risk,
        confidence_avg=(sum(confidence_list) / len(confidence_list)) if confidence_list else 0.0,
        item_count=len(items),
        exposure=ExposureInfo(currency="THB", unit_variance_sum=variance_abs_sum),
        top_reason_codes=[
            TopReasonCode(code=k, count=v)
            for k, v in sorted(reason_counter.items(), key=lambda i: -i[1])
        ],
    )

    first_trace = first_trace or {}
    policy_block = first_trace.get("policy") or {}
    selection_block = first_trace.get("selection") or {}

    created_at = None
    created_candidates = [it.created_at for it in items if it.created_at is not None]
    if created_candidates:
        created_at = min(created_candidates)

    return DecisionRunViewContext(
        case_id=_to_str(raw.get("case_id"), ""),
        run_id=_to_str(raw.get("run_id"), ""),
        policy=PolicyInfo(
            policy_id=_to_str(policy_block.get("policy_id", "TH8-Sense"), "TH8-Sense"),
            policy_version=_to_str(policy_block.get("policy_version", "v1.0"), "v1.0"),
        ),
        technique=_to_str(selection_block.get("selected_technique"), ""),
        created_at=created_at,
        summary=summary,
        items=items,
    )