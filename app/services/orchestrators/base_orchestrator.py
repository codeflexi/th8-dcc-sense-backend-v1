# app/services/orchestrators/base_orchestrator.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional


@dataclass(frozen=True)
class OrchestratorOutput:
    """
    Output returned by orchestrators.
    - selection_override: payload compatible with DecisionRunService selection (C3.5 shape)
    - notes: debug/audit hints (JSON-serializable)
    """
    domain: str
    selection_override: Optional[Dict[str, Any]] = None
    notes: Optional[Dict[str, Any]] = None


class BaseOrchestrator:
    domain: str = "base"

    def __init__(self, sb: Any):
        self.sb = sb

    def prepare_context(
        self,
        *,
        case_id: str,
        actor_id: str = "SYSTEM",
        force_prepare: bool = False,
    ) -> OrchestratorOutput:
        raise NotImplementedError
