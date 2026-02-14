# app/services/pipeline/case_processing_service.py
from __future__ import annotations

from datetime import datetime, timezone
from typing import Dict, Any

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
from app.services.decision.decision_run_service import DecisionRunService


class CaseProcessingService:
    """
    Enterprise deterministic pipeline orchestrator.

    Executes full decision pipeline in strict order:
    DISCOVER → EXTRACT → GROUP → DERIVE → SELECT → DECISION_RUN

    Guarantees:
    - Ordered execution
    - Audit trail
    - Failure isolation
    - Deterministic output
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
        # ---------- decision (manual injection required) ----------
        self.decision_service = DecisionRunService(
            run_repo=DecisionRunRepository(sb),
            result_repo=CaseDecisionResultRepository(sb),
            group_repo=CaseEvidenceGroupRepository(sb),
            case_line_repo=CaseLineItemRepository(sb),
            doc_link_repo=CaseDocumentLinkRepository(sb),
            audit_repo=self.audit_repo,
            policy_path="app/policies/sense_policy_mvp_v1.yaml",  # <-- adjust
        )

    def _safe_init(self, cls, sb):
        try:
            return cls(sb)
        except TypeError:
            return cls()
        
    def _safe_init(self, cls, sb):
        """
        Supports both constructor styles:
        - __init__(self)
        - __init__(self, sb)
        """
        try:
            return cls(sb)
        except TypeError:
            return cls()

    def run(
        self,
        case_id: str,
        *,
        domain: str,
        actor_id: str = "SYSTEM",
    ) -> Dict[str, Any]:

        run_id = f"pipeline:{datetime.now(timezone.utc).isoformat()}"

        self.audit_repo.emit(
            case_id=case_id,
            event_type="PIPELINE_STARTED",
            actor=actor_id,
            payload={"run_id": run_id, "domain": domain},
        )

        try:
            # ----------------------------
            # 1) DISCOVERY
            # ----------------------------
             #self._update_status(case_id, "DISCOVERING")

            discovery_result = self.discovery_service.discover(
                case_id=case_id,
                actor_id=actor_id,
            )

             #self._update_status(case_id, "DISCOVERED")

            # ----------------------------
            # 2) EVIDENCE EXTRACT
            # ----------------------------
             #self._update_status(case_id, "EVIDENCE_EXTRACTING")

            extract_result = self.extract_service.extract(
                case_id=case_id,
                actor_id=actor_id,
            )

             #self._update_status(case_id, "EVIDENCE_EXTRACTED")

            # ----------------------------
            # 3) EVIDENCE GROUP
            # ----------------------------
             #self._update_status(case_id, "EVIDENCE_GROUPING")

            group_result = self.group_service.group_case(
                case_id=case_id
            )

             #self._update_status(case_id, "EVIDENCE_GROUPED")

            # ----------------------------
            # 4) FACT DERIVE
            # ----------------------------
             #self._update_status(case_id, "FACTS_DERIVING")

            fact_result = self.fact_service.derive(
                case_id=case_id,actor_id=actor_id
            )

             #self._update_status(case_id, "FACTS_DERIVED")

            # ----------------------------
            # 5) DECISION SELECTION
            # ----------------------------
             #self._update_status(case_id, "DECISION_SELECTING")

            selection_result = self.selection_service.select_for_case(
                case_id=case_id,
                domain_code=domain
            )

             #self._update_status(case_id, "DECISION_SELECTED")

            # ----------------------------
            # 6) DECISION RUN
            # ----------------------------
             #self._update_status(case_id, "DECISION_RUNNING")
             
    

            decision_result = self.decision_service.run_case(
                case_id=case_id,
                domain_code=domain,
                selection=selection_result,
                created_by=actor_id
            )
                
    
             #self._update_status(case_id, "DECISION_COMPLETED")

            self.audit_repo.emit(
                case_id=case_id,
                event_type="PIPELINE_COMPLETED",
                actor=actor_id,
                payload={"run_id": run_id},
            )

            return {
                "case_id": case_id,
                "run_id": run_id,
                "domain": domain,
                "steps": {
                    "discovery": discovery_result,
                    "extract": extract_result,
                    "group": group_result,
                    "facts": fact_result,
                    "selection": selection_result,
                    "decision": decision_result,
                },
                "status": "SUCCESS",
            }

        except Exception as e:

             #self._update_status(case_id, "FAILED")

            self.audit_repo.emit(
                case_id=case_id,
                event_type="PIPELINE_FAILED",
                actor=actor_id,
                payload={
                    "run_id": run_id,
                    "error": str(e),
                },
            )

            raise

    def _update_status(self, case_id: str, status: str):
        self.case_repo.update_status(case_id, status)
