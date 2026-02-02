from fastapi import APIRouter, HTTPException
from app.services.evidence.evidence_extraction_service import EvidenceExtractionService
from app.services.evidence.evidence_grouping_service import EvidenceGroupingService

router = APIRouter()

@router.post("/{case_id}/evidence/extract")
def extract_case_evidence(case_id: str):
    return EvidenceExtractionService.extract(case_id)

@router.post("/{case_id}/evidence/group")
def group_case_evidence(case_id: str):
    return EvidenceGroupingService.group(case_id)