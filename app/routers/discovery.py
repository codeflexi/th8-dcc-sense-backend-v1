from fastapi import APIRouter, Header, HTTPException , Query ,Request
from app.services.discovery.discovery_service import DiscoveryService
from app.services.discovery.discovery_query_service import DiscoveryQueryService


# router = APIRouter(
#     prefix="/cases",
#     tags=["discovery"]
# )

router = APIRouter()


@router.post("/{case_id}/discover")
def discover_case(
    request: Request,
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
    discover_service = DiscoveryService(request.state.sb)

    try:
        result = discover_service.discover(
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
    request: Request,
    case_id: str,
    status: str | None = Query(None),
    source: str | None = Query(None)
):
    discover_query_service = DiscoveryQueryService(request.state.sb)
    return discover_query_service.list_discovery_results(
        case_id=case_id,
        status=status,
        source=source
    )