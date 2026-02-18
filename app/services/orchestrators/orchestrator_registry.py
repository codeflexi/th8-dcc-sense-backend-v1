# app/services/orchestrators/orchestrator_registry.py
from __future__ import annotations

from typing import Any, Dict, Type

from app.services.orchestrators.base_orchestrator import BaseOrchestrator
from app.services.orchestrators.evidence_orchestrator import EvidenceOrchestrator
from app.services.orchestrators.ledger_orchestrator import LedgerOrchestrator


class OrchestratorRegistry:
    """
    Domain → orchestrator mapping.
    Add new domains here without touching DecisionRunService.
    """
    def __init__(self, sb: Any):
        self.sb = sb
        self._map: Dict[str, Type[BaseOrchestrator]] = {
            "procurement": EvidenceOrchestrator,
            "finance_ap": LedgerOrchestrator,
        }

    def get(self, domain: str) -> BaseOrchestrator:
        key = (domain or "").strip().lower()
        if key not in self._map:
            raise ValueError(f"Unsupported domain: {domain}")
        return self._map[key](self.sb)
