from fastapi import APIRouter, HTTPException, Query , Request
from app.services.evidence.evidence_extraction_service import EvidenceExtractionService
from app.services.evidence.evidence_grouping_service import EvidenceGroupingService
from app.services.document.document_service import DocumentPageService

router = APIRouter()

@router.post("/{case_id}/evidence/extract")
def extract_case_evidence(request: Request, case_id: str):
    evidence_extraction_service = EvidenceExtractionService(sb=request.state.sb)
    return evidence_extraction_service.extract(case_id)

@router.post("/{case_id}/evidence/group")
def group_case_evidence(request: Request, case_id: str, actor_id: str = Query(default="SYSTEM")):
    svc = EvidenceGroupingService(sb=request.state.sb)
    return svc.group_case(case_id)

@router.get(
    "/cases/{case_id}/groups/{group_id}/evidence",
    summary="Evidence drill-down (document → page → highlight)",
)
def get_group_evidence(request: Request, case_id: str, group_id: str):
    try:
        service = EvidenceGroupingService()
        return service.get_group_evidence(
            case_id=case_id,
            group_id=group_id,
        )
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=str(e),
        )


     
       
        