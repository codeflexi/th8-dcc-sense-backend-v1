from fastapi import APIRouter
from app.services.discovery.discovery_service import DiscoveryService

# router = APIRouter(prefix="/cases", tags=["discovery"])
router = APIRouter()

@router.post("/{case_id}/discover/relational")
def discover_relational(case_id: str):
    result = DiscoveryService.discover_relational(case_id)
    return {
        "status": "ok",
        **result
    }
