# app/services/decision_run_service.py
"""
C4 — Decision Run Service (CONSUMES C3.5 TECHNICAL SELECTION)
------------------------------------------------------------
LOCKED constraints respected:
- Continue forward only (no redesign / no reset)
- Deterministic, evidence-first, audit-grade
- Anchors are group_id-based
- C4 MUST consume baseline/technique from C3.5 selection output (NO baseline re-derivation)

Inputs:
- case_id
- domain_code (typically "procurement")
- selection payload from C3.5 (SelectionService.select_for_case output)

Persists:
- dcc_decision_runs (run header) via DecisionRunRepository
- dcc_case_decision_results (per group) via CaseDecisionResultRepository
"""

from __future__ import annotations

import hashlib
import json
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, List, Optional

import yaml

# ---- Optional imports (do NOT break existing runtime if paths differ) ----
try:
    # preferred (as you referenced)
    from app.services.policy.calculation_requirements import required_calculations  # type: ignore
except Exception:
    try:
        # common location
        from app.services.policy.calculation_requirements import required_calculations  # type: ignore
    except Exception:
        required_calculations = None  # type: ignore

try:
    from app.services.decision.calculation_service import CalculationService  # type: ignore
except Exception:
    try:
        from app.services.decision.calculation_service import CalculationService  # type: ignore
    except Exception:
        CalculationService = None  # type: ignore

try:
    from uuid import UUID
except Exception:
    UUID = None  # type: ignore


SEVERITY_ORDER = {"LOW": 1, "MED": 2, "HIGH": 3, "CRITICAL": 4}


class DecisionRunService:
    def __init__(
        self,
        *,
        run_repo,
        result_repo,
        group_repo,
        case_line_repo,
        doc_link_repo=None,  # optional; if provided, used for document_presence artifacts
        audit_repo=None, 
        policy_path: str,
    ):
        self.run_repo = run_repo
        self.result_repo = result_repo
        self.group_repo = group_repo
        self.case_line_repo = case_line_repo
        self.doc_link_repo = doc_link_repo
        self.audit_repo = audit_repo
        self.policy = self._load_policy(policy_path)

    # =====================================================
    # Public API
    # =====================================================
    def run_case(
        self,
        *,
        case_id: str,
        domain_code: str,
        selection: Dict[str, Any],
        created_by: str = "SYSTEM",
    ) -> Dict[str, Any]:
        meta = self.policy.get("meta") or {}
        # keep backward compatible keys
        policy_id = str(meta.get("policy_name") or meta.get("policy_id") or "UNKNOWN_POLICY")
        policy_version = str(meta.get("version") or "UNKNOWN_VERSION")

        # determinism guard: hash case + policy + selection summary
        input_hash = self._compute_input_hash(case_id, policy_id, policy_version, selection)

        inputs_snapshot = {
            "case_id": case_id,
            "domain": domain_code,
            "policy": {"policy_id": policy_id, "policy_version": policy_version},
            "selection_summary": self._selection_summary(selection),
            "started_at": datetime.now(timezone.utc).isoformat(),
        }
        inputs_snapshot = self._json_safe(inputs_snapshot)

        run = self.run_repo.create_run(
            case_id=case_id,
            policy_id=policy_id,
            policy_version=policy_version,
            input_hash=input_hash,
            created_by=created_by,
            inputs_snapshot=inputs_snapshot,
        )
        run_id = run["run_id"]
        
        # =====================================================
        # AUDIT: Decision run started
        # =====================================================
        if self.audit_repo:
            self.audit_repo.emit(
                case_id=case_id,
                event_type="DECISION_RUN_STARTED",
                actor=created_by,
                run_id=run_id,
                payload={
                    "run_id": run_id,
                    "domain": domain_code,
                    "policy_id": policy_id,
                    "policy_version": policy_version,
                    "input_hash": input_hash,
                },
            )
        

        try:
            selection_by_group = self._index_selection_by_group(selection, case_id, domain_code)

            po_lines = self.case_line_repo.list_by_case(case_id)
            po_by_item_id = {str(l.get("item_id")): l for l in (po_lines or []) if l.get("item_id")}

            artifacts_present = self._detect_artifacts_present(case_id)
            groups = self.group_repo.list_by_case(case_id)

            group_results: List[Dict[str, Any]] = []
            for g in groups:
                group_results.append(
                    self._evaluate_group(
                        run_id=run_id,
                        case_id=case_id,
                        domain_code=domain_code,
                        group=g,
                        selection_by_group=selection_by_group,
                        po_by_item_id=po_by_item_id,
                        artifacts_present=artifacts_present,
                        created_by=created_by,
                    )
                )

            agg = self._aggregate_case(group_results)
            summary = self._json_safe(agg["summary"])

            risk_level = self._normalize_risk_level(agg["risk_level"])
            
            agg = self._aggregate_case(group_results)
            summary = self._json_safe(agg["summary"])

            risk_level = self._normalize_risk_level(agg["risk_level"])

            # =====================================================
            # RUN-LEVEL DECISION MAPPING (DB CONSTRAINT SAFE)
            # DB allows: APPROVE / REVIEW / ESCALATE / REJECT
            # =====================================================
            group_decision = agg["decision"]  # PASS / REVIEW / REJECT

            if group_decision == "PASS":
                run_decision = "APPROVE"

            elif group_decision == "REVIEW":
                # escalate automatically if high risk
                if risk_level in ("HIGH", "CRITICAL"):
                    run_decision = "ESCALATE"
                else:
                    run_decision = "REVIEW"

            elif group_decision == "REJECT":
                run_decision = "REJECT"

            else:
                # deterministic fallback
                run_decision = "REVIEW"

            self.run_repo.complete_run(
                run_id=run_id,
                decision=run_decision,
                risk_level=risk_level,
                confidence=agg["confidence"],
                summary=summary,
            )

            
            response = {
                "run_id": run_id,
                "case_id": case_id,
                "domain": domain_code,
                "decision": agg["decision"],
                "risk_level": agg["risk_level"],
                "confidence": agg["confidence"],
                "groups": group_results,
            }
            
            # =====================================================   
            
            
            return self._json_safe(response)

        except Exception as e:
            self.run_repo.fail_run(run_id=run_id, error=str(e))
            raise

    # =====================================================
    # Group evaluation
    # =====================================================
    def _evaluate_group(
        self,
        *,
        run_id: str,
        case_id: str,
        domain_code: str,
        group: Dict[str, Any],
        selection_by_group: Dict[str, Dict[str, Any]],
        po_by_item_id: Dict[str, Dict[str, Any]],
        artifacts_present: set[str],
        created_by: str,
    ) -> Dict[str, Any]:

        group_id = str(group.get("group_id"))
        anchor_type = group.get("anchor_type")
        anchor_id = group.get("anchor_id")

        # =====================================================
        # AUDIT: Group evaluation started
        # =====================================================
        if self.audit_repo:
            self.audit_repo.emit(
                case_id=case_id,
                event_type="GROUP_EVAL_STARTED",
                actor="SYSTEM",
                run_id=run_id,
                payload={
                    "run_id": run_id,
                    "group_id": group_id,
                    "anchor_type": anchor_type,
                    "anchor_id": anchor_id,
                },
            )
        
        
        
        # =====================================================
        # PO context
        # =====================================================
        po_line = None
        if anchor_type == "PO_ITEM" and anchor_id:
            po_line = po_by_item_id.get(str(anchor_id))

        # =====================================================
        # PO missing → deterministic REVIEW (KEEP AS-IS)
        # =====================================================
        if not po_line:
            decision_status = "REVIEW"
            risk_level = "HIGH"
            confidence = 0.20

            trace = self._json_safe({
                "policy": {
                    "policy_id": str((self.policy.get("meta") or {}).get("policy_id") or ""),
                    "policy_version": str((self.policy.get("meta") or {}).get("version") or ""),
                },
                "inputs": {
                    "group_id": group_id,
                    "anchor_type": anchor_type,
                    "anchor_id": str(anchor_id) if anchor_id else None,
                    "po_line_found": False,
                    "artifacts_present": sorted(list(artifacts_present)),
                },
                "selection": None,
                "calculations": {},
                "rules": [],
                "notes": ["PO_LINE_MISSING_FOR_GROUP"],
            })
            
         
            self.result_repo.upsert_result(
                run_id=run_id,
                group_id=group_id,
                decision_status="REVIEW",
                risk_level="HIGH",
                confidence=confidence,
                reason_codes=["PO_LINE_MISSING_FOR_GROUP"],
                fail_actions=[{"type": "REVIEW"}],
                trace=trace,
                evidence_refs={"fact_ids": [], "evidence_ids": []},
                created_by=created_by,
            )

            return {
                "group_id": group_id,
                "decision": "REVIEW",
                "risk_level": "HIGH",
                "confidence": confidence,
            }

        # =====================================================
        # Selection (C3.5)
        # =====================================================
        sel = selection_by_group.get(group_id)
        baseline_ctx = self._baseline_from_selection(sel)
        readiness = (sel or {}).get("readiness_flags") or {}


        # =====================================================
        # AUDIT: Baseline selected
        # =====================================================
        if self.audit_repo and baseline_ctx.get("baseline_available"):
            self.audit_repo.emit(
                case_id=case_id,
                event_type="BASELINE_SELECTED",
                actor="SYSTEM",
                run_id=run_id,
                payload={
                    "run_id": run_id,
                    "group_id": group_id,
                    "baseline": baseline_ctx["baseline"],
                    "baseline_source": baseline_ctx.get("baseline_source"),
                    "technique": baseline_ctx.get("selected_technique"),
                },
            )


        # =====================================================
        # Calculations (KEEP BEHAVIOR)
        # =====================================================
        calculated = {}
        calc_trace = []
        calc_error = None

        try:
            if required_calculations and CalculationService:
                calc_defs = required_calculations(self.policy, domain=domain_code)

                meta = self.policy.get("meta") or {}
                defaults = meta.get("defaults") or {}
                rounding = defaults.get("rounding") or {}

                calc_context = {
                    "po": {
                        "unit_price": self._normalize_money(
                            po_line.get("unit_price"),
                            fallback_currency=defaults.get("currency"),
                        ),
                    },
                    "selection": {
                        "baseline": baseline_ctx.get("baseline"),
                    },
                }

                calc_engine = CalculationService()
                calc_result = calc_engine.compute_all(
                    calcs=calc_defs,
                    ctx=calc_context,
                    rounding=rounding,
                )

                calculated = self._json_safe(calc_result.values)
                calc_trace = calc_result.trace

        except Exception as e:
            calc_error = str(e)
            calculated = {}

        # =====================================================
        # Rule evaluation (KEEP)
        # =====================================================
        rule_traces: List[Dict[str, Any]] = []
        fail_actions: List[Dict[str, Any]] = []

        for rule in self._iter_rules(domain_code):
            rt = self._eval_rule(
                rule=rule,
                po_line=po_line,
                baseline_ctx=baseline_ctx,
                artifacts_present=artifacts_present,
                readiness=readiness,
                calculated=calculated,
            )
            if rt:
                rule_traces.append(rt)
                if rt["result"] == "FAIL":
                    fail_actions.extend(rt.get("fail_actions") or [])

        # =====================================================
        # ✅ FINAL Decision Aggregation (SINGLE SOURCE OF TRUTH)
        # =====================================================
        failed_rules = [
            r for r in rule_traces
            if r.get("result") == "FAIL"
        ]

        reason_codes = [
            r["rule_id"]
            for r in failed_rules
            if r.get("rule_id")
        ]

        max_severity = None
        for r in failed_rules:
            max_severity = self._max_severity(max_severity, r.get("severity"))

        if not failed_rules:
            decision_status = "PASS"
            risk_level = "LOW"
        else:
            if max_severity == "CRITICAL":
                decision_status = "REJECT"
                risk_level = "CRITICAL"
            else:
                decision_status = "REVIEW"
                risk_level = max_severity or "MED"

        confidence = self._confidence(baseline_ctx, rule_traces)

        # =====================================================
        # Persist
        # =====================================================
        trace = self._json_safe({
            "policy": {
                "policy_id": str((self.policy.get("meta") or {}).get("policy_id") or ""),
                "policy_version": str((self.policy.get("meta") or {}).get("version") or ""),
            },
            "inputs": {
                "group_id": group_id,
                "anchor_type": anchor_type,
                "anchor_id": str(anchor_id) if anchor_id else None,
                "po_item": self._safe_po(po_line),
                "artifacts_present": sorted(list(artifacts_present)),
            },
            "selection": {
                "selected_technique": (sel or {}).get("selected_technique"),
                "baseline": baseline_ctx.get("baseline"),
                "baseline_source": baseline_ctx.get("baseline_source"),
                "readiness_flags": readiness,
                "selection_refs": self._refs_from_selection(sel),
            },
            "calculations": {
                "values": calculated,
                "trace": calc_trace,
                "error": calc_error,
            },
            "rules": rule_traces,
        })

        self.result_repo.upsert_result(
            run_id=run_id,
            group_id=group_id,
            decision_status=decision_status,   # PASS / REVIEW / REJECT ONLY
            risk_level=risk_level,
            confidence=confidence,
            reason_codes=reason_codes,
            fail_actions=self._dedup_actions(fail_actions),
            trace=trace,
            evidence_refs=self._json_safe(self._refs_from_selection(sel)),
            created_by=created_by,
        )
        
        # =====================================================
        # AUDIT: Group decision finalized
        # =====================================================
        if self.audit_repo:
            self.audit_repo.emit(
                case_id=case_id,
                event_type="GROUP_DECISION_FINALIZED",
                actor="SYSTEM",
                run_id=run_id,
                payload={
                    "run_id": run_id,
                    "group_id": group_id,
                    "decision": decision_status,
                    "risk_level": risk_level,
                    "confidence": confidence,
                    "reason_codes": reason_codes,
                },
            )

        return {
            "group_id": group_id,
            "decision": decision_status,
            "risk_level": risk_level,
            "confidence": confidence,
        }

    @staticmethod
    def _normalize_risk_level(risk_level: str) -> str:
        m = (risk_level or "").upper().strip()
        mapping = {
            "LOW": "LOW",
            "L": "LOW",

            "MED": "MEDIUM",
            "MID": "MEDIUM",
            "M": "MEDIUM",
            "MEDIUM": "MEDIUM",

            "HIGH": "HIGH",
            "H": "HIGH",
        }
        return mapping.get(m, m)
    
    # =====================================================
    # Rule Evaluation (Policy YAML)
    # =====================================================
    def _eval_rule(
        self,
        *,
        rule: Dict[str, Any],
        po_line: Dict[str, Any],
        baseline_ctx: Dict[str, Any],
        artifacts_present: set[str],
        readiness: Dict[str, Any],
        calculated: Dict[str, Any],
    ) -> Optional[Dict[str, Any]]:
        if not self._preconditions_ok(rule.get("preconditions") or {}, baseline_ctx, artifacts_present, readiness):
            return None

        logic = rule.get("logic") or {}
        logic_type = logic.get("type")

        po_price = self._dec(self._extract_money_value(po_line.get("unit_price"), fallback=po_line.get("unit_price")))
        baseline = baseline_ctx.get("baseline")
        baseline_price = self._dec((baseline or {}).get("value"))

        # -------------------------------------------------
        # Enterprise v1: compare (uses calculated fields)
        # -------------------------------------------------
        if logic_type == "compare":
            field = str(logic.get("field") or "")
            op = str(logic.get("operator") or "").strip()
            expected = logic.get("value")

            actual = calculated.get(field)

            # deterministic if missing required calculated value
            if actual is None:
                return self._trace(
                    rule,
                    "FAIL",
                    {"note": f"missing calculated field: {field}"},
                    self._normalize_fail_actions(rule.get("fail_actions") or [{"type": "REVIEW"}]),
                    {"missing_field": field},
                )

            is_fail = self._compare(actual, op, expected)
            calculation = {
                "field": field,
                "actual": actual,
                "operator": op,
                "expected": expected,
            }
            return self._trace(
                rule,
                "FAIL" if is_fail else "PASS",
                calculation,
                self._normalize_fail_actions(rule.get("fail_actions") or []) if is_fail else [],
)

        # -------------------------------------------------
        # Legacy: variance_pct (works without calc layer)
        # -------------------------------------------------
        if logic_type == "variance_pct":
            if po_price is None or baseline_price is None or baseline_price == 0:
                return self._trace(rule, "FAIL", {"note": "missing inputs for variance_pct"}, [{"type": "REVIEW"}])

            var = (po_price - baseline_price) / baseline_price
            max_pct = Decimal(str((rule.get("thresholds") or {}).get("variance_pct_max", 0)))

            ok = (var <= max_pct)
            calculation = {
                "po_price": float(po_price),
                "baseline_price": float(baseline_price),
                "variance_pct": float(var),
                "threshold": float(max_pct),
            }
            return self._trace(
                rule,
                "PASS" if ok else "FAIL",
                calculation,
                [] if ok else self._normalize_fail_actions(rule.get("fail_actions") or []),
                {},
            )

        # -------------------------------------------------
        # Legacy: greater_than
        # -------------------------------------------------
        if logic_type == "greater_than":
            if po_price is None or baseline_price is None:
                return self._trace(rule, "FAIL", {"note": "missing inputs for greater_than"}, [{"type": "REVIEW"}])

            ok = not (po_price > baseline_price)
            calculation = {
                "po_price": float(po_price),
                "baseline_price": float(baseline_price),
                "comparison": "po_price <= baseline_price",
            }
            return self._trace(
                rule,
                "PASS" if ok else "FAIL",
                calculation,
                [] if ok else self._normalize_fail_actions(rule.get("fail_actions") or []),
                {},
            )

        # -------------------------------------------------
        # document_presence
        # -------------------------------------------------
        if logic_type == "document_presence":
            required_docs = [str(x).upper() for x in (logic.get("required_docs") or [])]
            has_any_doc = ("DOCUMENT" in artifacts_present)

            missing = []
            if not has_any_doc:
                missing = required_docs[:]

            ok = (len(missing) == 0)
            calculation = {"required_docs": required_docs, "has_any_document": has_any_doc}
            extra = {"missing_docs": missing}
            return self._trace(
                rule,
                "PASS" if ok else "FAIL",
                calculation,
                [] if ok else self._normalize_fail_actions(rule.get("fail_actions") or []),
                extra,
            )

        # -------------------------------------------------
        # placeholders
        # -------------------------------------------------
        if logic_type in ("three_way_match", "two_way_match", "duplicate_pattern"):
            calculation = {"note": "MVP placeholder – insufficient artifacts/data"}
            return self._trace(
                rule,
                "FAIL",
                calculation,
                self._normalize_fail_actions(rule.get("fail_actions") or []),
                {"reason": "placeholder"},
            )

        return self._trace(
            rule,
            "FAIL",
            {"note": f"unknown logic_type={logic_type}"},
            [{"type": "REVIEW"}],
            {"reason": "unknown_logic"},
        )

    # =====================================================
    # Consume C3.5 Selection
    # =====================================================
    def _index_selection_by_group(self, selection: Dict[str, Any], case_id: str, domain_code: str) -> Dict[str, Dict[str, Any]]:
        if not selection:
            raise ValueError("C4 requires C3.5 selection payload (selection is missing)")

        if str(selection.get("case_id")) != str(case_id):
            inner = selection.get("selection")
            if inner and str(inner.get("case_id")) == str(case_id):
                selection = inner
            else:
                raise ValueError("selection.case_id mismatch")

        if str(selection.get("domain")) != str(domain_code):
            raise ValueError(f"selection.domain mismatch expected={domain_code} got={selection.get('domain')}")

        groups = selection.get("groups") or []
        idx: Dict[str, Dict[str, Any]] = {}
        for g in groups:
            gid = str(g.get("group_id"))
            if gid:
                idx[gid] = g
        return idx

    def _baseline_from_selection(self, sel: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        if not sel:
            return {"baseline_available": False, "baseline": None, "baseline_source": None}

        baseline = sel.get("baseline")
        baseline_source = sel.get("baseline_source")
        if baseline and baseline.get("value") is not None:
            return {
                "baseline_available": True,
                "baseline": {"value": baseline.get("value"), "currency": baseline.get("currency")},
                "baseline_source": baseline_source,
                "selected_technique": sel.get("selected_technique"),
            }
        return {
            "baseline_available": False,
            "baseline": None,
            "baseline_source": baseline_source,
            "selected_technique": sel.get("selected_technique"),
        }

    def _refs_from_selection(self, sel: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        out = {"fact_ids": [], "evidence_ids": []}
        if not sel:
            return out

        trace = sel.get("selection_trace") or []
        for step in trace:
            if step.get("passed") is True:
                refs = (step.get("references") or {})
                out["fact_ids"] = refs.get("fact_ids") or []
                out["evidence_ids"] = refs.get("evidence_ids") or []
                return out

        return out

    def _selection_summary(self, selection: Dict[str, Any]) -> Dict[str, Any]:
        inner = selection.get("selection") if selection.get("selection") else selection
        groups = inner.get("groups") or []
        return {
            "case_id": inner.get("case_id"),
            "domain": inner.get("domain"),
            "group_count": len(groups),
            "technique_counts": self._count([g.get("selected_technique") for g in groups]),
        }

    # =====================================================
    # Preconditions / Aggregation
    # =====================================================
    def _preconditions_ok(
        self,
        pre: Dict[str, Any],
        baseline_ctx: Dict[str, Any],
        artifacts_present: set[str],
        readiness: Dict[str, Any],
    ) -> bool:
        if pre.get("baseline_available") is True:
            if not baseline_ctx.get("baseline_available"):
                return False

        if "baseline_source" in pre:
            expected = str(pre.get("baseline_source") or "")
            actual = str((baseline_ctx.get("baseline_source") or {}).get("fact_type") or "")
            if expected and expected != actual:
                return False

        if "artifacts_present" in pre:
            needed = {str(x).upper() for x in (pre.get("artifacts_present") or [])}
            if not needed.issubset(artifacts_present):
                return False

        if "artifact_missing" in pre:
            missing = str(pre.get("artifact_missing") or "").upper()
            if missing and (missing in artifacts_present):
                return False

        if pre.get("baseline_available") is True and readiness.get("baseline_available") is False:
            return False

        return True

    def _aggregate_decision(self, max_severity: Optional[str], rule_traces: List[Dict[str, Any]]) -> str:
        failed = [r for r in (rule_traces or []) if r.get("result") == "FAIL"]
        if not failed:
            return "PASS"

        if max_severity in ("HIGH", "CRITICAL"):
            return "REVIEW"
        if max_severity == "MED":
            return "REVIEW"
        return "PASS"

    def _aggregate_case(self, group_results: List[Dict[str, Any]]) -> Dict[str, Any]:
        worst = None
        for g in group_results or []:
            worst = self._max_severity(worst, g.get("risk_level"))

        decision = "PASS"
        if any((g.get("decision") == "REVIEW") for g in (group_results or [])):
            decision = "REVIEW"

        risk_level = worst or "LOW"
        return {
            "decision": decision,
            "risk_level": risk_level,
            "confidence": self._avg([g.get("confidence") for g in (group_results or [])]),
            "summary": {
                "groups": len(group_results or []),
                "review_count": sum(1 for g in (group_results or []) if g.get("decision") == "REVIEW"),
            },
        }

    def _confidence(self, baseline_ctx: Dict[str, Any], rule_traces: List[Dict[str, Any]]) -> float:
        if not baseline_ctx.get("baseline_available"):
            return 0.40
        return 0.85

    # =====================================================
    # Artifacts
    # =====================================================
    def _detect_artifacts_present(self, case_id: str) -> set[str]:
        present = {"PO"}
        if not self.doc_link_repo:
            return present

        links = self.doc_link_repo.list_by_case(case_id)
        for l in links or []:
            if str(l.get("link_status") or "").upper() == "CONFIRMED":
                present.add("DOCUMENT")
                break
        return present

    # =====================================================
    # Trace helpers
    # =====================================================
    def _trace(
        self,
        rule: Dict[str, Any],
        result: str,
        calculation: Dict[str, Any],
        fail_actions: List[Dict[str, Any]],
        extra: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        out = {
            "rule_id": rule.get("rule_id"),
            "domain": rule.get("domain"),
            "group": rule.get("group"),
            "severity": rule.get("severity"),
            "result": result,
            "calculation": calculation,
            "fail_actions": fail_actions,
            "explanation": rule.get("explanation") or {},
        }
        if extra:
            out["extra"] = extra
        return out

    # =====================================================
    # Policy shape helpers (DO NOT break old policy)
    # =====================================================
    def _iter_rules(self, domain_code: str) -> List[Dict[str, Any]]:
        """
        Supports BOTH:
          - legacy: policy["rules"] with rule["domain"]
          - enterprise v1: policy["domains"][domain]["rules"] with no rule.domain
        """
        # enterprise v1 shape
        domains = self.policy.get("domains") or {}
        d = domains.get(domain_code)
        if isinstance(d, dict) and isinstance(d.get("rules"), list):
            out = []
            for r in d.get("rules") or []:
                if isinstance(r, dict):
                    rr = dict(r)
                    # ensure domain exists for trace consistency
                    rr.setdefault("domain", domain_code)
                    out.append(rr)
            return out

        # legacy shape
        out = []
        for r in self.policy.get("rules") or []:
            if not isinstance(r, dict):
                continue
            if (r.get("domain") or "").strip() != domain_code:
                continue
            out.append(r)
        return out

    # =====================================================
    # Generic helpers
    # =====================================================
    def _compute_input_hash(self, case_id: str, policy_id: str, policy_version: str, selection: Dict[str, Any]) -> str:
        payload = {
            "case_id": case_id,
            "policy_id": policy_id,
            "policy_version": policy_version,
            "selection_summary": self._selection_summary(selection),
        }
        raw = json.dumps(payload, sort_keys=True, ensure_ascii=False, default=str)
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()

    def _safe_po(self, po_line: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "item_id": po_line.get("item_id"),
            "sku": po_line.get("sku"),
            "item_name": po_line.get("item_name"),
            "quantity": po_line.get("quantity"),
            "unit_price": po_line.get("unit_price"),
            "currency": po_line.get("currency"),
            "total_price": po_line.get("total_price"),
            "uom": po_line.get("uom"),
            "source_line_ref": po_line.get("source_line_ref"),
            "created_at": po_line.get("created_at"),
        }

    def _dec(self, v: Any) -> Optional[Decimal]:
        if v is None:
            return None
        try:
            return Decimal(str(v))
        except (InvalidOperation, ValueError, TypeError):
            return None

    def _avg(self, xs: List[Any]) -> float:
        vals = [float(x) for x in xs if x is not None]
        if not vals:
            return 0.0
        return sum(vals) / len(vals)

    def _max_severity(self, a: Optional[str], b: Optional[str]) -> Optional[str]:
        if not b:
            return a
        if not a:
            return b
        return a if SEVERITY_ORDER.get(a, 0) >= SEVERITY_ORDER.get(b, 0) else b

    def _normalize_fail_actions(self, fail_actions: List[Any]) -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []
        for a in fail_actions or []:
            if isinstance(a, str):
                out.append({"type": a})
            elif isinstance(a, dict):
                for k, v in a.items():
                    out.append({"type": k, "value": v})
            else:
                out.append({"type": "unknown_action", "raw": str(a)})
        return out

    def _dedup_actions(self, actions: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        seen = set()
        out = []
        for a in actions or []:
            safe = self._json_safe(a)
            s = json.dumps(safe, sort_keys=True, ensure_ascii=False, default=str)
            if s in seen:
                continue
            seen.add(s)
            out.append(safe)
        return out

    def _count(self, xs: List[Any]) -> Dict[str, int]:
        out: Dict[str, int] = {}
        for x in xs or []:
            k = str(x) if x is not None else "null"
            out[k] = out.get(k, 0) + 1
        return out

    def _load_policy(self, path: str) -> Dict[str, Any]:
        with open(path, "r", encoding="utf-8") as f:
            return yaml.safe_load(f) or {}

    # =====================================================
    # Value extraction / compare helpers (enterprise grade)
    # =====================================================
    def _compare(self, actual: Any, operator: str, expected: Any) -> bool:
        op = operator.strip()
        if op in (">", ">=", "<", "<="):
            a = self._dec(actual)
            e = self._dec(expected)
            if a is None or e is None:
                return False
            if op == ">":
                return a > e
            if op == ">=":
                return a >= e
            if op == "<":
                return a < e
            if op == "<=":
                return a <= e
        if op in ("==", "!="):
            if op == "==":
                return actual == expected
            return actual != expected
        # unknown operator => deterministic FAIL
        return False

    def _extract_money_value(self, v: Any, fallback: Any = None) -> Any:
        """
        Supports unit_price stored as:
          - number
          - {"value": number, "currency": "THB"}
        """
        if isinstance(v, dict) and "value" in v:
            return v.get("value")
        return v if v is not None else fallback

    def _normalize_money(self, v: Any, fallback_currency: Optional[str] = None) -> Dict[str, Any]:
        if isinstance(v, dict) and "value" in v:
            return {"value": v.get("value"), "currency": v.get("currency") or fallback_currency}
        if v is None:
            return {"value": None, "currency": fallback_currency}
        return {"value": v, "currency": fallback_currency}

    def _json_safe(self, v: Any) -> Any:
        if isinstance(v, datetime):
            return v.isoformat()
        if isinstance(v, date):
            return v.isoformat()

        if isinstance(v, Decimal):
            return str(v)

        if UUID is not None and isinstance(v, UUID):
            return str(v)

        if isinstance(v, set):
            return [self._json_safe(x) for x in sorted(list(v), key=lambda z: str(z))]
        if isinstance(v, tuple):
            return [self._json_safe(x) for x in v]

        if isinstance(v, dict):
            return {str(k): self._json_safe(x) for k, x in v.items()}
        if isinstance(v, list):
            return [self._json_safe(x) for x in v]

        return v
