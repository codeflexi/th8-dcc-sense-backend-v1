from typing import Any, Dict, List, Optional, Tuple

from app.services.copilot.tool_budget import ToolBudget
from app.services.copilot.tool_planner import ToolPlanner


TOOL_PARAM_SCHEMA: Dict[str, Dict[str, Any]] = {
    # NOTE: keep in schema for validation if needed elsewhere,
    # but orchestrator will NOT plan/execute this tool by design.
    "get_case_detail": {"required": ["case_id"]},

    "get_case_decision_summary": {"required": ["case_id"]},
    "get_case_groups": {"required": ["case_id"]},
    "get_group_rules": {"required": ["group_id"]},
    "get_group_evidence": {"required": ["group_id"]},
    "open_document_page": {"required": ["document_id", "page"]},

    "get_contract_summary": {"required": ["document_id"]},
    "get_contract_clauses": {"required": ["document_id"]},
    "evaluate_contract_risk": {"required": ["document_id"]},
}

# Orchestrator tools (exclude get_case_detail for performance)
ORCH_ALLOWED_TOOLS = [k for k in TOOL_PARAM_SCHEMA.keys() if k != "get_case_detail"]


class CopilotOrchestrator:
    """
    Enterprise Orchestrator (production):
    - does NOT preload case_detail (agent handles it)
    - LLM proposes tool plan (bounded)
    - validate plan (allowlist + schema + case-scope)
    - tool budget guardrail
    - audit persistence
    """

    def __init__(
        self,
        *,
        tool_fn,
        audit_repo,
        planner_model: str,
        max_budget_tokens: int = 1200,
        max_calls_per_round: int = 3,
        max_rounds: int = 1,
        enforce_case_scope: bool = True,
    ):
        self._tool = tool_fn
        self.audit = audit_repo

        self.budget = ToolBudget(max_tokens=max_budget_tokens)
        self.planner = ToolPlanner(model_name=planner_model)

        self.max_calls_per_round = max_calls_per_round
        self.max_rounds = max_rounds
        self.enforce_case_scope = enforce_case_scope

    def _validate_tool_call(
        self,
        tool: str,
        args: Dict[str, Any],
        case_id: str,
    ) -> Tuple[bool, str, Dict[str, Any]]:
        if tool not in ORCH_ALLOWED_TOOLS:
            return False, "tool_not_allowed", args

        schema = TOOL_PARAM_SCHEMA.get(tool, {})
        required = schema.get("required", [])

        if self.enforce_case_scope and "case_id" in required:
            args["case_id"] = case_id

        missing = [k for k in required if k not in args]
        if missing:
            return False, f"missing_required:{missing}", args

        return True, "ok", args

    async def _exec_plan(self, plan: List[Dict[str, Any]], case_id: str) -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []

        for step in plan[: self.max_calls_per_round]:
            tool = step.get("tool")
            args = step.get("args") or {}
            why = step.get("why")

            ok, reason, args2 = self._validate_tool_call(tool, args, case_id)
            if not ok:
                out.append({"tool": tool, "skipped": True, "reason": reason, "args": args})
                continue

            if not self.budget.allow(tool, args2):
                out.append({"tool": tool, "skipped": True, "reason": "budget_exceeded", "args": args2})
                continue

            cost = self.budget.charge(tool, args2)

            await self.audit.log_tool_call(
                case_id=case_id,
                tool_name=tool,
                tool_args=args2,
                meta={"why": why, "budget_cost": cost, "budget_remaining": self.budget.state.remaining()},
            )

            try:
                res = await self._tool(tool, **args2)
                await self.audit.log_tool_result(
                    case_id=case_id,
                    tool_name=tool,
                    result=res,
                    meta={"budget_cost": cost},
                )
                out.append({"tool": tool, "args": args2, "ok": True, "budget_cost": cost, "result": res})
            except Exception as e:
                await self.audit.log_tool_result(
                    case_id=case_id,
                    tool_name=tool,
                    result={"error": str(e)},
                    meta={"budget_cost": cost, "failed": True},
                )
                out.append({"tool": tool, "args": args2, "ok": False, "budget_cost": cost, "error": str(e)})

        return out

    def _assemble(self, executed: List[Dict[str, Any]]) -> Dict[str, Any]:
        ctx: Dict[str, Any] = {
            "tool_results": [],
            "budget": {
                "max": self.budget.state.max_tokens,
                "used": self.budget.state.used_tokens,
                "remaining": self.budget.state.remaining(),
            },
        }

        for r in executed[-20:]:
            keep = {k: r.get(k) for k in ("tool", "ok", "skipped", "reason", "args", "budget_cost")}
            res = r.get("result")
            if isinstance(res, dict):
                keep["result_keys"] = list(res.keys())[:30]
            ctx["tool_results"].append(keep)

        return ctx

    async def run(
        self,
        *,
        user_query: str,
        case_id: str,
        context_hint: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:

        await self.audit.log_trace(case_id=case_id, step="ORCH_PLAN", status="active", detail={"remaining_budget": self.budget.state.remaining()})

        executed_all: List[Dict[str, Any]] = []

        for round_idx in range(1, self.max_rounds + 1):
            plan_obj = self.planner.propose_plan(
                user_query=user_query,
                allowed_tools=ORCH_ALLOWED_TOOLS,
                tool_param_schema=TOOL_PARAM_SCHEMA,
                max_calls=self.max_calls_per_round,
                context_hint={"case_id": case_id, **(context_hint or {})},
            )
            plan = plan_obj.get("plan", []) or []

            await self.audit.log_trace(case_id=case_id, step="ORCH_PLAN", status="completed", detail={"plan": plan_obj})

            await self.audit.log_trace(case_id=case_id, step="ORCH_EXEC", status="active", detail={"count": len(plan)})
            executed = await self._exec_plan(plan, case_id)
            executed_all.extend(executed)
            await self.audit.log_trace(case_id=case_id, step="ORCH_EXEC", status="completed", detail={"executed_count": len(executed)})

            break  # bounded (production safe default)

        assembled = self._assemble(executed=executed_all)
        await self.audit.log_trace(case_id=case_id, step="ORCH_DONE", status="completed", detail={"budget": assembled.get("budget")})
        return assembled
