from fastapi import FastAPI
from app.routers.health import router as health_router
from app.routers.ingestion import router as ingestion_router
from app.routers.evidence import router as evidence_router
from app.routers.documents import router as documents_router
from app.routers.viewer import router as viewer_router
from app.routers.cases import router as cases_router
from app.routers.discovery import router as discovery_router    
from app.routers.debug import router as debug_router

def create_app() -> FastAPI:
    app = FastAPI(title="TH8 Sense DCC Backend")
    app.include_router(health_router, prefix="/api/v1/health", tags=["health"])
    app.include_router(ingestion_router, prefix="/api/v1/ingestion", tags=["ingestion"])
    app.include_router(evidence_router, prefix="/api/v1", tags=["evidence"])
    app.include_router(documents_router, prefix="/api/v1", tags=["documents"])
    app.include_router(cases_router, prefix="/api/v1", tags=["cases"])
    app.include_router(discovery_router, prefix="/api/v1", tags=["discovery"])
    app.include_router(debug_router, prefix="/api/v1", tags=["debug"])
    app.include_router(viewer_router, tags=["viewer"])
    return app

app = create_app()
