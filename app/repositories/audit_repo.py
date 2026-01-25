from app.repositories.base import BaseRepository

class AuditRepository(BaseRepository):
    TABLE = "dcc_audit_events"

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
