from fastapi import APIRouter, Header, HTTPException

from app.services.case.case_service import CaseService
from app.services.case.case_models import CreateCaseFromPORequest,CaseResponse
from app.services.signal.signal_extraction_service import SignalExtractionService
from app.repositories.case_repo import CaseRepository
from app.repositories.case_line_item_repo import CaseLineItemRepository
from app.repositories.case_document_link_repo import CaseDocumentLinkRepository
from app.services.evidence.evidence_grouping_service import EvidenceGroupingService


router = APIRouter()


@router.post("", response_model=CaseResponse)
def create_case_from_po(
    payload: CreateCaseFromPORequest,
    x_actor_id: str = Header(default="SYSTEM")
):
    case = CaseService.create_case_from_po(
        payload.model_dump(),
        actor_id=x_actor_id
    )

    if not case:
        raise HTTPException(status_code=500, detail="Failed to create case")

    return {
        "case_id": case["case_id"],
        "reference_type": case["reference_type"],
        "reference_id": case["reference_id"],
        "status": case["status"]
    }

@router.get("/{case_id}/signals")
def debug_case_signals(case_id: str):
    """
    DEBUG endpoint
    - Extract signals from case + PO snapshot
    - No DB write
    - Deterministic, recomputable
    """

    case_repo = CaseRepository()
    line_item_repo = CaseLineItemRepository()

    # 1. Load case
    case = case_repo.get(case_id)
    if not case:
        raise HTTPException(status_code=404, detail="Case not found")

    # 2. Load immutable PO snapshot
    line_items = line_item_repo.list_by_case(case_id)
    if not line_items:
        raise HTTPException(
            status_code=400,
            detail="No line items found for case"
        )

    # 3. Extract signals
    signals = SignalExtractionService.extract(case, line_items)

    # 4. Return as JSON (Pydantic -> dict)
    return signals.model_dump()

@router.get("/cases/{case_id}/documents")
def list_case_documents(case_id: str):
    repo = CaseDocumentLinkRepository()
    return {
        "case_id": case_id,
        "documents": repo.list_by_case(case_id)
    }

@router.post("/case-document-links/{link_id}/confirm")
def confirm_document(link_id: str, body: dict):
    actor_id = body.get("actor_id")
    if not actor_id:
        raise HTTPException(400, "actor_id required")

    repo = CaseDocumentLinkRepository()
    repo.confirm(link_id, actor_id)

    return {"status": "confirmed", "link_id": link_id}


@router.post("/case-document-links/{link_id}/remove")
def remove_document(link_id: str, body: dict):
    actor_id = body.get("actor_id")
    if not actor_id:
        raise HTTPException(400, "actor_id required")

    repo = CaseDocumentLinkRepository()
    repo.remove(link_id, actor_id)

    return {"status": "removed", "link_id": link_id}

