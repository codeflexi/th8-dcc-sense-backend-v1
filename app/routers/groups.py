# app/routers/groups.py
from fastapi import APIRouter, HTTPException ,Request
from app.services.evidence.evidence_grouping_service import EvidenceGroupingService
from app.services.case.case_group_service import CaseGroupService

router = APIRouter()

@router.get("/{group_id}/evidence")
def get_group_evidence(req: Request, group_id: str):
    try:
        service = EvidenceGroupingService(sb=req.state.sb)
        return service.get_group_only_evidence(group_id=group_id)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/{group_id}/rules")
def get_group_rules(req: Request, group_id: str):
    try:
        service = CaseGroupService(sb=req.state.sb)
        return service.get_group_rules(group_id=group_id)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))