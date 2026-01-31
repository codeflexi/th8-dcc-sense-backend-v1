from fastapi import APIRouter, Header, HTTPException , Query
from app.services.discovery.discovery_service import DiscoveryService
from app.services.discovery.discovery_query_service import DiscoveryQueryService


# router = APIRouter(
#     prefix="/cases",
#     tags=["discovery"]
# )

router = APIRouter()


@router.post("/{case_id}/discover")
def discover_case(
    case_id: str,
    actor_id: str = Header(default="SYSTEM", alias="X-Actor-Id")
):
    """
    Orchestrated discovery:
    - Signal extraction
    - Relational discovery
    - Vector discovery
    - Append-only inferred links
    """

    try:
        result = DiscoveryService.discover(
            case_id=case_id,
            actor_id=actor_id
        )
        return {
            "status": "ok",
            **result
        }
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))

@router.get("/{case_id}/discovery")
def list_case_discovery(
    case_id: str,
    status: str | None = Query(None),
    source: str | None = Query(None)
):
    return DiscoveryQueryService.list_discovery_results(
        case_id=case_id,
        status=status,
        source=source
    )