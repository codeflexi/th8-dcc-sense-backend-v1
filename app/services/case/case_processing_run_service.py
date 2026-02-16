# app/services/pipeline/case_processing_service.py
from __future__ import annotations

from datetime import datetime, timezone
from typing import Dict, Any, Optional

from app.repositories.audit_repo import AuditRepository
from app.repositories.case_repo import CaseRepository

from app.services.discovery.discovery_service import DiscoveryService
from app.services.evidence.evidence_extraction_service import EvidenceExtractionService
from app.services.evidence.evidence_grouping_service import EvidenceGroupingService
from app.services.fact.fact_derivation_service import FactDerivationService
from app.services.decision.selection_service import SelectionService
from app.services.decision.decision_run_service import DecisionRunService

from app.repositories.decision_run_repo import DecisionRunRepository
from app.repositories.case_decision_result_repo import CaseDecisionResultRepository
from app.repositories.case_evidence_group_repo import CaseEvidenceGroupRepository
from app.repositories.case_line_item_repo import CaseLineItemRepository
from app.repositories.case_document_link_repo import CaseDocumentLinkRepository
from app.repositories.document_header_repo import DocumentHeaderRepository


class CaseProcessingRunService:
    """
    Enterprise Orchestrator

    Domains:
        - procurement  → document-driven
        - finance_ap   → ledger-driven (no discovery)

    Principles:
        - Evidence preparation runs once (or when forced)
        - Decision evaluation can re-run safely
        - Audit trace is append-only
        - Deterministic per domain
    """

    def __init__(self, sb):
        self.sb = sb
        self.audit_repo = AuditRepository(sb)
        self.case_repo = CaseRepository(sb)

        self.discovery_service = DiscoveryService(sb)
        self.extract_service = EvidenceExtractionService(sb=sb)
        self.group_service = EvidenceGroupingService(sb=sb)
        self.fact_service = FactDerivationService(sb=sb)
        self.selection_service = SelectionService(sb=sb)

        self.decision_service = DecisionRunService(
            run_repo=DecisionRunRepository(sb),
            result_repo=CaseDecisionResultRepository(sb),
            group_repo=CaseEvidenceGroupRepository(sb),
            case_line_repo=CaseLineItemRepository(sb),
            doc_link_repo=CaseDocumentLinkRepository(sb),
            audit_repo=self.audit_repo,
            policy_path="app/policies/sense_policy_mvp_v1.yaml",
        )

        self.header_repo = DocumentHeaderRepository(sb)
        self.link_repo = CaseDocumentLinkRepository(sb)

    # =====================================================
    # PUBLIC ENTRYPOINT
    # =====================================================

    def run(
        self,
        case_id: str,
        *,
        domain: str,
        actor_id: str = "SYSTEM",
        force_prepare: bool = False
    ) -> Dict[str, Any]:

        pipeline_run_id = f"pipeline:{datetime.now(timezone.utc).isoformat()}"

        self.audit_repo.emit(
            case_id=case_id,
            event_type="PIPELINE_STARTED",
            actor=actor_id,
            payload={
                "run_id": pipeline_run_id,
                "domain": domain
            },
        )

        try:

            # -------------------------------------------------
            # DOMAIN ROUTING
            # -------------------------------------------------

            if domain.lower() == "procurement":
                prepare_result = self._prepare_case_if_needed(
                    case_id=case_id,
                    actor_id=actor_id,
                    force=force_prepare,
                )
                decision_result = self._evaluate_decision(
                    case_id=case_id,
                    domain=domain,
                    actor_id=actor_id
                )

                response = {
                    "prepare": prepare_result,
                    "decision": decision_result,
                }

            elif domain.lower() == "finance_ap":
                # finance_ap is ledger-driven
                decision_result = self._evaluate_decision(
                    case_id=case_id,
                    domain=domain,
                    actor_id=actor_id
                )

                response = {
                    "prepare": None,
                    "decision": decision_result,
                }

            else:
                raise ValueError(f"Unsupported domain: {domain}")

            self.audit_repo.emit(
                case_id=case_id,
                event_type="PIPELINE_COMPLETED",
                actor=actor_id,
                payload={"run_id": pipeline_run_id},
            )

            return {
                "case_id": case_id,
                "domain": domain,
                "pipeline_run_id": pipeline_run_id,
                **response,
            }

        except Exception as e:
            self.audit_repo.emit(
                case_id=case_id,
                event_type="PIPELINE_FAILED",
                actor=actor_id,
                payload={"run_id": pipeline_run_id, "error": str(e)},
            )
            raise

    # =====================================================
    # PROCUREMENT: PREPARATION PHASE
    # =====================================================

    def _prepare_case_if_needed(
        self,
        *,
        case_id: str,
        actor_id: str,
        force: bool = False,
    ) -> Dict[str, Any]:

        case = self.case_repo.get_case(case_id)
        if not case:
            raise ValueError("Case not found")

        case_detail = case.get("case_detail") or {}
        already_prepared = case_detail.get("evidence_prepared", False)

        if already_prepared and not force:
            return {
                "status": "SKIPPED",
                "reason": "evidence already prepared",
            }

        # 1) DISCOVERY
        print("1.DISCOVERY Run")
        discovery_result = self.discovery_service.discover(
            case_id=case_id,
            actor_id=actor_id,
        )

        print("2.EVIDENT EXTRACT Run")
        # 2) EXTRACT
        extract_result = self.extract_service.extract(
            case_id=case_id,
            actor_id=actor_id,
        )

        print("3.EVIDENT GROUP Run")
        # 3) GROUP
        group_result = self.group_service.group_case(case_id=case_id)

        print("4.DERIVE Run")
        # 4) DERIVE
        fact_result = self.fact_service.derive(
            case_id=case_id,
            actor_id=actor_id,
        )

        # Mark case as prepared
        case_detail["evidence_prepared"] = True
        case_detail["last_prepared_at"] = datetime.now(timezone.utc).isoformat()

        self.case_repo.merge_case_detail(case_id, case_detail)

        return {
            "status": "PREPARED",
            "discovery": discovery_result,
            "extract": extract_result,
            "group": group_result,
            "facts": fact_result,
        }

    # =====================================================
    # DECISION PHASE (ALL DOMAINS)
    # =====================================================

    def _evaluate_decision(
        self,
        *,
        case_id: str,
        domain: str,
        actor_id: str,
       
    ) -> Dict[str, Any]:

        # 1) SELECTION (only meaningful for procurement)
        selection_result = self.selection_service.select_for_case(
            case_id=case_id,
            domain_code=domain,
        )

        # 2) DECISION RUN
        decision_result = self.decision_service.run_case(
            case_id=case_id,
            domain_code=domain,
            selection=selection_result,
            created_by=actor_id
        )

        # # 3) OFFICIAL MODE → update case header decision
        # if mode.upper() == "OFFICIAL":
        #     self.case_repo.update_decision(
        #         case_id=case_id,
        #         decision=decision_result.get("decision"),
        #         risk_level=decision_result.get("risk_level"),
        #         confidence_score=decision_result.get("confidence_score"),
        #     )

        return decision_result
