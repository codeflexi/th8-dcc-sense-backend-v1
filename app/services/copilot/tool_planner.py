import json
from typing import Any, Dict, List, Optional

from openai import OpenAI


class ToolPlanner:
    """
    Production tool planner:
    - LLM proposes a JSON plan (no execution)
    - Orchestrator validates & executes
    - Strict JSON-only contract
    - IMPORTANT: planner must NOT call get_case_detail (base is already loaded)
    """

    def __init__(self, model_name: str):
        self.client = OpenAI()
        self.model_name = model_name

    def _safe_json(self, text: str) -> Dict[str, Any]:
        try:
            return json.loads(text)
        except Exception:
            return {"plan": [], "note": "invalid_json"}

    def propose_plan(
        self,
        *,
        user_query: str,
        allowed_tools: List[str],
        tool_param_schema: Dict[str, Dict[str, Any]],
        max_calls: int,
        context_hint: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        # Hard guard: never allow get_case_detail in planner stage
        allowed = [t for t in (allowed_tools or []) if t != "get_case_detail"]

        hint = json.dumps(context_hint or {}, ensure_ascii=False)

        prompt = f"""
You are an enterprise tool planner for a case-scoped copilot.
Return STRICT JSON ONLY (no markdown, no comments).

Rules:
- Use ONLY tools in allowed_tools.
- NEVER include "get_case_detail" (base case is already loaded).
- Max {max_calls} tool calls.
- Provide minimal args required by schema.
- Do NOT fabricate ids.
- If not needed, return empty plan.

allowed_tools:
{json.dumps(allowed, ensure_ascii=False)}

tool_param_schema:
{json.dumps(tool_param_schema, ensure_ascii=False)}

context_hint (partial):
{hint}

Return format:
{{
  "plan": [
    {{"tool":"tool_name","args":{{...}},"why":"short reason"}}
  ],
  "note":"optional"
}}

User query:
\"\"\"{user_query}\"\"\"
""".strip()

        resp = self.client.chat.completions.create(
            model=self.model_name,
            messages=[{"role": "system", "content": prompt}],
            temperature=0.0,
        )
        content = (resp.choices[0].message.content or "").strip()
        data = self._safe_json(content)

        if not isinstance(data, dict):
            return {"plan": [], "note": "not_dict"}

        plan = data.get("plan")
        if not isinstance(plan, list):
            data["plan"] = []

        # Final sanitize: strip forbidden tool
        data["plan"] = [x for x in data["plan"] if isinstance(x, dict) and x.get("tool") != "get_case_detail"]

        return data
