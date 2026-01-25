from fastapi import APIRouter, HTTPException
from app.services.evidence.evidence_builder import EvidenceBuilder

router = APIRouter()

@router.get("/cases/{case_id}/evidence")
def get_case_evidence(case_id: str):
    try:
        return EvidenceBuilder().build_for_case(case_id).model_dump()
    except ValueError as e:
        if str(e) == "CASE_NOT_FOUND":
            raise HTTPException(status_code=404, detail="Case not found")
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
