from fastapi import FastAPI

# Routers
from app.routers.health import router as health_router
from app.routers.ingestion import router as ingestion_router
from app.routers.evidence import router as evidence_router
from app.routers.documents import router as documents_router
from app.routers.viewer import router as viewer_router
from app.routers.cases import router as cases_router
from app.routers.discovery import router as discovery_router
from app.routers.debug import router as debug_router
from app.routers.facts import router as facts_router
from app.routers.decision import router as decision_router

# ✅ Policy bootstrap (สำคัญ)
from app.services.policy.loader import load_policy_from_file
from app.services.policy.registry import PolicyRegistry


def create_app() -> FastAPI:
    app = FastAPI(title="TH8 Sense DCC Backend")

    # -----------------------------
    # Startup: Load Policy YAML
    # -----------------------------
    @app.on_event("startup")
    def load_policy_on_startup():
        policy = load_policy_from_file(
            "app/policies/sense_policy_mvp.yaml"
        )
        PolicyRegistry.load(policy)

        # debug ชั่วคราว (เอาออกได้)
        print(f"[BOOT] Policy loaded: {policy.meta.policy_name} ({policy.meta.version})")

    # -----------------------------
    # Routers
    # -----------------------------
    app.include_router(health_router, prefix="/api/v1/health", tags=["health"])
    app.include_router(ingestion_router, prefix="/api/v1/ingestion", tags=["ingestion"])
    app.include_router(evidence_router, prefix="/api/v1", tags=["evidence"])
    app.include_router(documents_router, prefix="/api/v1", tags=["documents"])
    app.include_router(cases_router, prefix="/api/v1", tags=["cases"])
    app.include_router(discovery_router, prefix="/api/v1", tags=["discovery"])
    app.include_router(debug_router, prefix="/api/v1", tags=["debug"])
    app.include_router(facts_router, prefix="/api/v1", tags=["facts"])
    app.include_router(decision_router, prefix="/api/v1/decision", tags=["decision"])
    app.include_router(viewer_router, tags=["viewer"])

    return app


app = create_app()
