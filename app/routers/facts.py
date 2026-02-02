from fastapi import APIRouter, Query
from app.services.fact.fact_derivation_service import FactDerivationService
from app.repositories.case_fact_repo import CaseFactRepository

router = APIRouter()


@router.post("/{case_id}/facts/derive")
def derive_case_facts(case_id: str, actor_id: str = Query(default="SYSTEM")):
    return FactDerivationService.derive(case_id=case_id, actor_id=actor_id)


@router.get("/{case_id}/facts")
def list_case_facts(case_id: str):
    return {
        "case_id": case_id,
        "facts": CaseFactRepository().list_by_case(case_id)
    }
