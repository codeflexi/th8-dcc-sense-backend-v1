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
from app.repositories.document_header_repo import DocumentHeaderRepository


class CaseProcessingService:
    """
    Enterprise deterministic pipeline orchestrator.
    Strict order:
      DISCOVER → EXTRACT → GROUP → DERIVE → SELECT → DECISION_RUN
    """

    def __init__(self, sb):
        self.sb = sb
        self.audit_repo = AuditRepository(sb)
        self.case_repo = CaseRepository(sb)

        self.discovery_service = DiscoveryService(sb)

        self.extract_service = self._safe_construct(EvidenceExtractionService, sb)
        self.group_service = self._safe_construct(EvidenceGroupingService, sb)
        self.fact_service = self._safe_construct(FactDerivationService, sb)
        self.selection_service = self._safe_construct(SelectionService, sb)

        # decision-run (repo-injected; locked)
        self.decision_service = DecisionRunService(
            run_repo=DecisionRunRepository(sb),
            result_repo=CaseDecisionResultRepository(sb),
            group_repo=CaseEvidenceGroupRepository(sb),
            case_line_repo=CaseLineItemRepository(sb),
            doc_link_repo=CaseDocumentLinkRepository(sb),
            audit_repo=self.audit_repo,
            policy_path="app/policies/sense_policy_mvp_v1.yaml",
            # NOTE: for doc_type presence later, inject header_repo via your DecisionRunService enhancement
        )

        self.header_repo = DocumentHeaderRepository(sb)
        self.link_repo = CaseDocumentLinkRepository(sb)

    def _safe_construct(self, cls, sb):
        # Supports:
        # - __init__(self, sb)
        # - __init__(self, *, sb)
        # - __init__(self)
        try:
            return cls(sb)
        except TypeError:
            try:
                return cls(sb=sb)
            except TypeError:
                return cls()

    def run(
        self,
        case_id: str,
        *,
        domain: str,
        actor_id: str = "SYSTEM",
    ) -> Dict[str, Any]:
        pipeline_run_id = f"pipeline:{datetime.now(timezone.utc).isoformat()}"

        self.audit_repo.emit(
            case_id=case_id,
            event_type="PIPELINE_STARTED",
            actor=actor_id,
            payload={"run_id": pipeline_run_id, "domain": domain},
        )

        try:
            # ----------------------------
            # 1) DISCOVERY
            # ----------------------------
            print("1.DISCOVERY Started")
            discovery_result = self.discovery_service.discover(case_id=case_id, actor_id=actor_id)
            
            # ----------------------------
            # 2) EVIDENCE EXTRACT
            # ----------------------------
            print("2.EVIDENCE EXTRACT Started")
            extract_result = self.extract_service.extract(case_id=case_id, actor_id=actor_id)
            
            # ----------------------------
            # 3) EVIDENCE GROUP
            # ----------------------------
            print("3.EVIDENCE GROUP Started")
            group_result = self.group_service.group_case(case_id=case_id)
            
            # ----------------------------
            # 4) FACT DERIVE
            # ----------------------------
            print("4.FACT DERIVE Started")
            fact_result = self.fact_service.derive(case_id=case_id, actor_id=actor_id)
            
            # ----------------------------
            # 5) DECISION SELECTION
            # ----------------------------
            print("5.Technical Selection Started")
            selection_result = self.selection_service.select_for_case(case_id=case_id, domain_code=domain)
            
            # ----------------------------
            # 6) DECISION RUN
            # ----------------------------
            print("6.Decision Run Started")
            decision_result = self.decision_service.run_case(
                case_id=case_id,
                domain_code=domain,
                selection=selection_result,
                created_by=actor_id,
            )

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
                "discovery": discovery_result,
                "extract": extract_result,
                "group": group_result,
                "facts": fact_result,
                "selection": selection_result,
                "decision": decision_result,
            }

        except Exception as e:
            self.audit_repo.emit(
                case_id=case_id,
                event_type="PIPELINE_FAILED",
                actor=actor_id,
                payload={"run_id": pipeline_run_id, "error": str(e)},
            )
            raise
