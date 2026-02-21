from typing import List, Dict, Any, Optional
from datetime import datetime


class AuditTimelineBuilder:
    """
    Standalone builder.
    No dependency on Pydantic or other modules.
    Returns pure dict for frontend.
    """

    @staticmethod
    def build(case_id: str, raw_events: List[Dict[str, Any]]) -> Dict[str, Any]:

        events = []

        for e in raw_events:
            payload = e.get("payload") or {}
            event_type = e.get("event_type")

            title = AuditTimelineBuilder._map_title(event_type, payload)
            severity = AuditTimelineBuilder._map_severity(event_type)

            events.append({
                "id": str(e.get("audit_id")),
                "timestamp": e.get("created_at"),
                "type": event_type,
                "actor": e.get("actor"),
                "title": title,
                "severity": severity,
                "run_id": e.get("run_id"),
                "group_id": payload.get("group_id"),
                "meta": payload,
            })

        return {
            "case_id": case_id,
            "events": events,
        }

    @staticmethod
    def _map_title(event_type: Optional[str], payload: Dict[str, Any]) -> str:

        if event_type == "DECISION_RUN_STARTED":
            return "Decision run started"

        if event_type == "DECISION_RUN_DONE":
            decision = payload.get("decision")
            return f"Decision completed: {decision}"

        if event_type == "GROUP_EVAL_STARTED":
            return f"Evaluating group {payload.get('group_id')}"

        if event_type == "GROUP_DECISION_FINALIZED":
            decision = payload.get("decision")
            return f"Group decision finalized: {decision}"

        if event_type == "DECISION_RUN_FAILED":
            return "Decision run failed"

        return event_type or "UNKNOWN_EVENT"

    @staticmethod
    def _map_severity(event_type: Optional[str]) -> str:

        if event_type == "DECISION_RUN_FAILED":
            return "ERROR"

        return "INFO"