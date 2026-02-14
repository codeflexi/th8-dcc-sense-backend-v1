# app/repositories/copilot_audit_repo.py
from typing import Any, Dict


class CopilotAuditRepository:
    """
    Persist copilot trace/tool calls into dcc_copilot_audit_events.

    Table columns expected:
    - case_id (text or uuid)
    - event_type (text)
    - step (text nullable)
    - tool_name (text nullable)
    - payload (jsonb)
    - meta (jsonb)
    """

    def __init__(self, sb):
        self.sb = sb

    def _insert(self, row: Dict[str, Any]):
        try:
            self.sb.table("dcc_copilot_audit_events").insert(row).execute()
        except Exception:
            # Do not break copilot if audit insert fails
            pass

    async def log_trace(self, *, case_id: str, step: str, status: str, detail: Dict[str, Any]):
        self._insert({
            "case_id": case_id,
            "event_type": "TRACE",
            "step": step,
            "payload": {"status": status, "detail": detail},
            "meta": {},
        })

    async def log_tool_call(self, *, case_id: str, tool_name: str, tool_args: Dict[str, Any], meta: Dict[str, Any]):
        self._insert({
            "case_id": case_id,
            "event_type": "TOOL_CALL",
            "tool_name": tool_name,
            "payload": {"args": tool_args},
            "meta": meta or {},
        })

    async def log_tool_result(self, *, case_id: str, tool_name: str, result: Any, meta: Dict[str, Any]):
        payload = result
        if isinstance(result, dict) and len(str(result)) > 12000:
            payload = {"note": "result_truncated", "keys": list(result.keys())[:100]}

        self._insert({
            "case_id": case_id,
            "event_type": "TOOL_RESULT",
            "tool_name": tool_name,
            "payload": {"result": payload},
            "meta": meta or {},
        })
