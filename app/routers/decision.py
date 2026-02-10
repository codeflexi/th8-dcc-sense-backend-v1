from fastapi import APIRouter, Depends, HTTPException, Query , Path, status , Request
from typing import Dict, Any


from app.services.decision.selection_service import SelectionService

from app.services.decision.decision_run_service import DecisionRunService
from app.services.decision.selection_service import SelectionService

from app.repositories.decision_run_repo import DecisionRunRepository
from app.repositories.case_decision_result_repo import CaseDecisionResultRepository
from app.repositories.case_evidence_group_repo import CaseEvidenceGroupRepository
from app.repositories.case_line_item_repo import CaseLineItemRepository
from app.repositories.case_document_link_repo import CaseDocumentLinkRepository
from app.repositories.audit_repo import AuditRepository

router = APIRouter(
)


@router.post("/{case_id}/selection")
def run_selection_for_case(
    request: Request,
    case_id: str,
    domain: str = Query(
        ...,
        description="Decision domain (e.g. procurement, finance_ap)"
    ),
    
) -> Dict[str, Any]:
    """
    C.3.5 Technical Selection (Preview)

    - Deterministically select baseline technique per evidence group
    - NO decision logic
    - NO persistence
    """
    sb = request.state.sb
    service = SelectionService(sb = sb)

    try:
        result = service.select_for_case(
            case_id=case_id,
            domain_code=domain,
        )
    except Exception as e:
        # keep error explicit for debugging MVP
        raise HTTPException(
            status_code=500,
            detail=str(e)
        )

    if not result["groups"]:
        raise HTTPException(
            status_code=404,
            detail="No evidence groups found for case"
        )

    return {
        "status": "OK",
        "case_id": case_id,
        "domain": domain,
        "selection": result,
    }


# =========================================================
# POST /cases/{case_id}/decision-run
# =========================================================
@router.post(
    "/{case_id}/decision-run",
    status_code=status.HTTP_200_OK,
    summary="Run TH8 SENSE Decision Engine (C4)",
    description="""
Execute deterministic, evidence-first decision run (C4).

Flow:
1) Run Technical Selection (C3.5)
2) Consume selection → evaluate rules (C4)
3) Persist audit-grade results
"""
)
def run_decision_for_case(
    request: Request,
    case_id: str = Path(..., description="Case ID"),
    domain_code: str = "procurement"
) -> Dict[str, Any]:
    """
    Orchestration layer only:
    - compose repositories
    - run C3.5 selection
    - run C4 decision
    """
    sb = request.state.sb
    try:
        # =================================================
        # Compose repositories
        # =================================================
        run_repo = DecisionRunRepository(sb)
        result_repo = CaseDecisionResultRepository(sb)
        group_repo = CaseEvidenceGroupRepository(sb)
        case_line_repo = CaseLineItemRepository(sb)
        doc_link_repo = CaseDocumentLinkRepository(sb)
        audit_repo = AuditRepository(sb)  # TODO

        # =================================================
        # Step 1: Technical Selection (C3.5)
        # =================================================
        selection_service = SelectionService(sb = sb)
        

        selection = selection_service.select_for_case(
            case_id=case_id,
            domain_code=domain_code,
        )

        # =================================================
        # Step 2: Decision Run (C4)
        # =================================================
        decision_service = DecisionRunService(
            run_repo=run_repo,
            result_repo=result_repo,
            group_repo=group_repo,
            case_line_repo=case_line_repo,
            doc_link_repo=doc_link_repo,
            audit_repo =audit_repo,
            policy_path="app/policies/sense_policy_mvp_v1.yaml",
        )

        result = decision_service.run_case(
            case_id=case_id,
            domain_code=domain_code,
            selection=selection,
        )

        return {
            "status": "OK",
            "case_id": case_id,
            "domain": domain_code,
            "run_id": result["run_id"],
            "decision": result["decision"],
            "risk_level": result["risk_level"],
            "confidence": result["confidence"],
            "groups": result["groups"],
        }

    except ValueError as ve:
        # deterministic / validation error
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(ve),
        )

    except Exception as e:
        # unexpected system error
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Decision run failed: {str(e)}",
        )
