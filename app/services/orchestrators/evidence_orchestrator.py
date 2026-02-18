# app/services/orchestrators/evidence_orchestrator.py
from __future__ import annotations

from typing import Any, Dict

from app.services.orchestrators.base_orchestrator import BaseOrchestrator, OrchestratorOutput


class EvidenceOrchestrator(BaseOrchestrator):
    """
    Procurement orchestrator.
    IMPORTANT: no behavior change. This orchestrator does NOT re-implement
    discovery/extraction/grouping/derivation. CaseProcessingRunService keeps
    the exact preparation pipeline as-is.

    This orchestrator exists only to enforce orchestrator architecture separation.
    """
    domain = "procurement"

    def prepare_context(self, *, case_id: str, actor_id: str = "SYSTEM", force_prepare: bool = False) -> OrchestratorOutput:
        return OrchestratorOutput(
            domain=self.domain,
            selection_override=None,
            notes={"mode": "pass_through", "case_id": case_id},
        )
