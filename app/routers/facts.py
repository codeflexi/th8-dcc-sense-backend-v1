from fastapi import APIRouter, Query , Request
from app.services.fact.fact_derivation_service import FactDerivationService
from app.repositories.case_fact_repo import CaseFactRepository

router = APIRouter()


@router.post("/{case_id}/facts/derive")
def derive_case_facts(request: Request, case_id: str, actor_id: str = Query(default="SYSTEM")):
    fact_derivation_service = FactDerivationService(sb=request.state.sb)
    return fact_derivation_service.derive(case_id=case_id, actor_id=actor_id)
   


@router.get("/{case_id}/facts")
def list_case_facts(request: Request, case_id: str):
    case_fact_repo = CaseFactRepository(sb=request.state.sb)
    return {
        "case_id": case_id,
        "facts": case_fact_repo.list_by_case(case_id)
    }
