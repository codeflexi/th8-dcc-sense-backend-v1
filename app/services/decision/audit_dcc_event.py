# app/services/decision_run_service.py
"""
C4 — Decision Run Service (CONSUMES C3.5 TECHNICAL SELECTION)
------------------------------------------------------------
LOCKED constraints respected:
- Continue forward only (no redesign / no reset)
- Deterministic, evidence-first, audit-grade
- Anchors are group_id-based
- C4 MUST consume baseline/technique from C3.5 selection output (NO baseline re-derivation)
"""

from __future__ import annotations

import hashlib
import json
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, List, Optional

import yaml

# ---- Optional imports ----
try:
    from app.services.policy.calculation_requirements import required_calculations
except Exception:
    required_calculations = None

try:
    from app.services.decision.calculation_service import CalculationService
except Exception:
    CalculationService = None

try:
    from uuid import UUID
except Exception:
    UUID = None


SEVERITY_ORDER = {"LOW": 1, "MED": 2, "HIGH": 3, "CRITICAL": 4}


class AuditDCCEventService:
    def __init__(
        self,
        *,
        run_repo,
        result_repo,
        group_repo,
        case_line_repo,
        doc_link_repo=None,
        audit_repo=None,               # ✅ AUDIT: inject audit repository
        policy_path: str,
    ):
        self.run_repo = run_repo
        self.result_repo = result_repo
        self.group_repo = group_repo
        self.case_line_repo = case_line_repo
        self.doc_link_repo = doc_link_repo
        self.audit_repo = audit_repo   # ✅ AUDIT
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
        policy_id = str(meta.get("policy_id") or "UNKNOWN_POLICY")
        policy_version = str(meta.get("version") or "UNKNOWN_VERSION")

        input_hash = self._compute_input_hash(case_id, policy_id, policy_version, selection)

        run = self.run_repo.create_run(
            case_id=case_id,
            policy_id=policy_id,
            policy_version=policy_version,
            input_hash=input_hash,
            created_by=created_by,
            inputs_snapshot=self._json_safe({
                "case_id": case_id,
                "domain": domain_code,
                "policy": {"policy_id": policy_id, "policy_version": policy_version},
                "selection_summary": self._selection_summary(selection),
                "started_at": datetime.now(timezone.utc).isoformat(),
            }),
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
            po_by_item_id = {str(l["item_id"]): l for l in po_lines or [] if l.get("item_id")}
            artifacts_present = self._detect_artifacts_present(case_id)
            groups = self.group_repo.list_by_case(case_id)

            group_results = []
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

            self.run_repo.complete_run(
                run_id=run_id,
                decision=agg["decision"],
                risk_level=agg["risk_level"],
                confidence=agg["confidence"],
                summary=self._json_safe(agg["summary"]),
            )

            # =====================================================
            # AUDIT: Case decision finalized
            # =====================================================
            if self.audit_repo:
                self.audit_repo.emit(
                    case_id=case_id,
                    event_type="CASE_DECISION_FINALIZED",
                    actor="SYSTEM",
                    payload={
                        "run_id": run_id,
                        "decision": agg["decision"],
                        "risk_level": agg["risk_level"],
                        "confidence": agg["confidence"],
                        "summary": agg["summary"],
                    },
                )

            return self._json_safe({
                "run_id": run_id,
                "case_id": case_id,
                "domain": domain_code,
                "decision": agg["decision"],
                "risk_level": agg["risk_level"],
                "confidence": agg["confidence"],
                "groups": group_results,
            })

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
                payload={
                    "run_id": run_id,
                    "group_id": group_id,
                    "anchor_type": anchor_type,
                    "anchor_id": anchor_id,
                },
            )

        po_line = None
        if anchor_type == "PO_ITEM" and anchor_id:
            po_line = po_by_item_id.get(str(anchor_id))

        # =====================================================
        # PO missing → forced REVIEW
        # =====================================================
        if not po_line:
            if self.audit_repo:
                self.audit_repo.emit(
                    case_id=case_id,
                    event_type="GROUP_REVIEW_REQUIRED",
                    actor="SYSTEM",
                    payload={
                        "run_id": run_id,
                        "group_id": group_id,
                        "reason": "PO_LINE_MISSING",
                        "risk_level": "HIGH",
                    },
                )

            self.result_repo.upsert_result(
                run_id=run_id,
                group_id=group_id,
                decision_status="REVIEW",
                risk_level="HIGH",
                confidence=0.2,
                reason_codes=["PO_LINE_MISSING_FOR_GROUP"],
                fail_actions=[{"type": "REVIEW"}],
                trace={"note": "PO missing"},
                evidence_refs={"fact_ids": [], "evidence_ids": []},
                created_by=created_by,
            )
            return {
                "group_id": group_id,
                "decision": "REVIEW",
                "risk_level": "HIGH",
                "confidence": 0.2,
            }

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
                payload={
                    "run_id": run_id,
                    "group_id": group_id,
                    "baseline": baseline_ctx["baseline"],
                    "baseline_source": baseline_ctx.get("baseline_source"),
                    "technique": baseline_ctx.get("selected_technique"),
                },
            )

        # =====================================================
        # Calculations
        # =====================================================
        calculated = {}
        if required_calculations and CalculationService:
            calc_defs = required_calculations(self.policy, domain=domain_code)
            calc_engine = CalculationService()
            calc_result = calc_engine.compute_all(
                calcs=calc_defs,
                ctx={
                    "po": {"unit_price": po_line.get("unit_price")},
                    "selection": {"baseline": baseline_ctx.get("baseline")},
                },
            )
            calculated = self._json_safe(calc_result.values)

        # =====================================================
        # Rule evaluation
        # =====================================================
        rule_traces = []
        fail_actions = []

        for rule in self._iter_rules(domain_code):
            rt = self._eval_rule(
                rule=rule,
                po_line=po_line,
                baseline_ctx=baseline_ctx,
                artifacts_present=artifacts_present,
                readiness=readiness,
                calculated=calculated,
            )
            if not rt:
                continue

            rule_traces.append(rt)

            if rt["result"] == "FAIL":
                fail_actions.extend(rt.get("fail_actions") or [])

                # =================================================
                # AUDIT: Rule failed
                # =================================================
                if self.audit_repo:
                    self.audit_repo.emit(
                        case_id=case_id,
                        event_type="RULE_FAILED",
                        actor="SYSTEM",
                        payload={
                            "run_id": run_id,
                            "group_id": group_id,
                            "rule_id": rt.get("rule_id"),
                            "severity": rt.get("severity"),
                            "calculation": rt.get("calculation"),
                            "explanation": rt.get("explanation"),
                        },
                    )

        failed = [r for r in rule_traces if r["result"] == "FAIL"]
        reason_codes = [r["rule_id"] for r in failed if r.get("rule_id")]

        max_sev = None
        for r in failed:
            max_sev = self._max_severity(max_sev, r.get("severity"))

        if not failed:
            decision_status = "PASS"
            risk_level = "LOW"
        elif max_sev == "CRITICAL":
            decision_status = "REJECT"
            risk_level = "CRITICAL"
        else:
            decision_status = "REVIEW"
            risk_level = max_sev or "MED"

        confidence = self._confidence(baseline_ctx, rule_traces)

        self.result_repo.upsert_result(
            run_id=run_id,
            group_id=group_id,
            decision_status=decision_status,
            risk_level=risk_level,
            confidence=confidence,
            reason_codes=reason_codes,
            fail_actions=self._dedup_actions(fail_actions),
            trace={"rules": rule_traces},
            evidence_refs=self._refs_from_selection(sel),
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

    # =====================================================
    # Helpers (unchanged logic)
    # =====================================================
    # ... (helper methods same as original: _eval_rule, _baseline_from_selection,
    # _iter_rules, _aggregate_case, _json_safe, etc.)
