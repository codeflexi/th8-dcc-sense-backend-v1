from fastapi import APIRouter, Depends, HTTPException, Query
from typing import Dict, Any

from app.infra.supabase_client import get_supabase
from app.services.decision.selection_service import SelectionService

router = APIRouter(
)


@router.post("/{case_id}/selection")
def run_selection_for_case(
    case_id: str,
    domain: str = Query(
        ...,
        description="Decision domain (e.g. procurement, finance_ap)"
    ),
    sb = Depends(get_supabase),
) -> Dict[str, Any]:
    """
    C.3.5 Technical Selection (Preview)

    - Deterministically select baseline technique per evidence group
    - NO decision logic
    - NO persistence
    """

    service = SelectionService()

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
