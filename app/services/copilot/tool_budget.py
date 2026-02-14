# app/services/copilot/tool_budget.py
from dataclasses import dataclass
from typing import Any, Dict, Optional


DEFAULT_TOOL_COST = {
    "get_case_detail": 150,
    "get_case_decision_summary": 120,
    "get_case_groups": 200,
    "get_group_rules": 220,
    "get_group_evidence": 450,
    "open_document_page": 500,
    "get_contract_summary": 250,
    "get_contract_clauses": 350,
    "evaluate_contract_risk": 300,
}


@dataclass
class BudgetState:
    max_tokens: int
    used_tokens: int = 0

    def remaining(self) -> int:
        return max(0, self.max_tokens - self.used_tokens)

    def can_spend(self, cost: int) -> bool:
        return self.used_tokens + cost <= self.max_tokens

    def spend(self, cost: int) -> None:
        self.used_tokens += cost


class ToolBudget:
    """
    Enterprise tool budget (token-cost style).
    Purpose:
    - bound tool calling
    - avoid over-fetching
    - deterministic guardrail in orchestrator
    """

    def __init__(self, max_tokens: int, tool_cost_map: Optional[Dict[str, int]] = None):
        self.state = BudgetState(max_tokens=max_tokens)
        self.cost_map = tool_cost_map or dict(DEFAULT_TOOL_COST)

    def estimate_cost(self, tool_name: str, args: Dict[str, Any], predicted_bytes: int = 0) -> int:
        base = int(self.cost_map.get(tool_name, 300))

        long_text_chars = 0
        for v in (args or {}).values():
            if isinstance(v, str):
                long_text_chars += len(v)

        # Rough token approximation (4 chars ≈ 1 token), dampened
        extra = int((long_text_chars + predicted_bytes) / 4 / 3)
        return base + max(0, extra)

    def allow(self, tool_name: str, args: Dict[str, Any], predicted_bytes: int = 0) -> bool:
        cost = self.estimate_cost(tool_name, args, predicted_bytes)
        return self.state.can_spend(cost)

    def charge(self, tool_name: str, args: Dict[str, Any], predicted_bytes: int = 0) -> int:
        cost = self.estimate_cost(tool_name, args, predicted_bytes)
        self.state.spend(cost)
        return cost
