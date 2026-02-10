from app.repositories.base import BaseRepository
from typing import List, Optional , Dict, Any
from datetime import datetime , timezone
from fastapi.encoders import jsonable_encoder

class AuditRepository(BaseRepository):
    
    TABLE = "dcc_audit_events"
    
    def __init__(self, sb):
        super().__init__(sb)

    def latest_decision_run(self, case_id: str) -> dict | None:
        res = (
            self.sb.table(self.TABLE)
            .select("*")
            .eq("case_id", case_id)
            .eq("event_type", "DECISION_RUN")
            .order("created_at", desc=True)
            .limit(1)
            .execute()
        )
        
        return res.data[0] if res.data else None
        # -------------------------
    # Write Audit Event
    # -------------------------
    def emit(
        self,
        case_id: Optional[str],
        event_type: str,
        actor: str,
        payload: dict,
        run_id: Optional[str] = None,
    ) -> None:
        res = self.sb.table(self.TABLE).insert(
            {
                "case_id": case_id,
                "event_type": event_type,
                "actor": actor,
                "payload": jsonable_encoder(payload or {}),
                "run_id": run_id,
                "created_at": datetime.now(timezone.utc).isoformat(),
            }
        ).execute()
        
        return res.data[0] if res.data else None
        

    # -------------------------
    # REQUIRED by AuditRepository
    # -------------------------
    def list_events(self, case_id: str) -> List[dict]:
        """
        Default timeline reader (required by interface)
        """
        return self.list_events_by_case(case_id)

    # -------------------------
    # Read – timeline by case
    # -------------------------
    def list_events_by_case(self, case_id: str) -> List[dict]:
        res = (
            self.sb
            .table(self.TABLE)
            .select("*")
            .eq("case_id", case_id)
            .order("created_at", desc=False)
            .execute()
        )
        return res.data or []

    # -------------------------
    # Read – poll new events (Phase 6)
    # -------------------------
    def list_events_since(self, since_ts: str) -> List[dict]:
        """
        Return all events created AFTER since_ts
        Used by EmbeddedOrchestrator poll loop
        """
        res = (
            self.sb
            .table(self.TABLE)
            .select("*")
            .gt("created_at", since_ts)
            .order("created_at", desc=False)
            .execute()
        )
        return res.data or []

    # -------------------------
    # Idempotency helper (Phase 6)
    # -------------------------
    def has_action_success(
        self,
        case_id: str,
        action_type: str,
        idempotency_key: str,
    ) -> bool:
        """
        Check if ACTION_SUCCEEDED already exists
        for (case_id, action_type, idempotency_key)

        This is the ONLY idempotency source of truth.
        """
        res = (
            self.sb
            .table(self.TABLE)
            .select("id")
            .eq("case_id", case_id)
            .eq("event_type", "ACTION_SUCCEEDED")
            .eq("payload->>action_type", action_type)
            .eq("payload->>idempotency_key", idempotency_key)
            .limit(1)
            .execute()
        )
        return bool(res.data)
    
   