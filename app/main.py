from fastapi import FastAPI , Request
from fastapi.middleware.cors import CORSMiddleware


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
from app.routers.groups import router as groups_router
from app.routers.copilot import router as copilot_router
from app.routers.transactions import router as transactions_router

# Policy bootstrap
from app.services.policy.loader import load_policy_from_file
from app.services.policy.registry import PolicyRegistry

# Supabase (singleton)
from app.infra.supabase_client import get_supabase


def create_app() -> FastAPI:
    app = FastAPI(title="TH8 Sense DCC Backend")

    print(">>> LOADING app.main <<<")

    # -------------------------------------------------
    # CORS
    # -------------------------------------------------
    app.add_middleware(
        CORSMiddleware,
        allow_origins=[
            "http://localhost:5173",
            "http://localhost:5174",
            "http://localhost:5175",
        ],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    @app.middleware("http")
    async def inject_request_context(request: Request, call_next):
        if not hasattr(request.app.state, "sb"):
            raise RuntimeError("Supabase client (app.state.sb) is not initialized")
        request.state.sb = request.app.state.sb
        return await call_next(request)


    # -------------------------------------------------
    # Startup
    # -------------------------------------------------
    @app.on_event("startup")
    def startup():
        # 1) Load policy
        policy = load_policy_from_file(
            "app/policies/sense_policy_mvp_v1.yaml"
        )
        PolicyRegistry.load(policy)

        print(
            f"[BOOT] Policy loaded: "
            f"{policy.meta.policy_id} ({policy.meta.version})"

        )

        # 2) Initialize Supabase singleton (fail fast)
        app.state.sb = get_supabase()
        print("[BOOT] Supabase client initialized")

    # -------------------------------------------------
    # Routers
    # -------------------------------------------------
    app.include_router(health_router, prefix="/api/v1/health", tags=["health"])
    app.include_router(ingestion_router, prefix="/api/v1/ingestion", tags=["ingestion"])
    app.include_router(evidence_router, prefix="/api/v1", tags=["evidence"])
    app.include_router(documents_router, prefix="/api/v1", tags=["documents"])
    app.include_router(cases_router, prefix="/api/v1", tags=["cases"])
    app.include_router(discovery_router, prefix="/api/v1", tags=["discovery"])
    app.include_router(debug_router, prefix="/api/v1", tags=["debug"])
    app.include_router(facts_router, prefix="/api/v1", tags=["facts"])
    app.include_router(groups_router, prefix="/api/v1/groups", tags=["groups"])
    app.include_router(copilot_router, prefix="/api/v1/copilot", tags=["copilot"])
    app.include_router(decision_router, prefix="/api/v1/decision", tags=["decision"])
    app.include_router(transactions_router, prefix="/api/v1/transactions", tags=["transactions"])
    app.include_router(viewer_router, tags=["viewer"])

    return app


app = create_app()
