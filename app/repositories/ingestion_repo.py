from app.repositories.base import BaseRepository

class IngestionJobRepository(BaseRepository):
    TABLE = "dcc_ingestion_jobs"
    def __init__(self, sb):
        super().__init__(sb)

    def create_job(self, *, document_id: str) -> dict:
        payload = {"document_id": document_id, "status": "PENDING"}
        res = self.sb.table(self.TABLE).insert(payload).execute()
        return res.data[0]

    def fetch_next_pending(self) -> dict | None:
        res = (
            self.sb.table(self.TABLE)
            .select("*")
            .eq("status", "PENDING")
            .order("created_at", desc=False)
            .limit(1)
            .execute()
        )
        return res.data[0] if res.data else None

    def mark_running(self, job_id: str):
        self.sb.table(self.TABLE).update({"status": "RUNNING"}).eq("job_id", job_id).execute()

    def mark_done(self, job_id: str, counters: dict, warnings: list[str]):
        self.sb.table(self.TABLE).update({"status": "DONE" if not warnings else "DONE_WITH_WARNINGS", "counters": counters, "warnings": warnings}).eq("job_id", job_id).execute()

    def mark_failed(self, job_id: str, error: str, retryable: bool = True):
        self.sb.table(self.TABLE).update({"status": "NEEDS_RETRY" if retryable else "FAILED", "error_message": error}).eq("job_id", job_id).execute()


class IngestionEventRepository(BaseRepository):
    TABLE = "dcc_ingestion_events"
    
    def __init__(self, sb):
        super().__init__(sb)

    def append(self, *, job_id: str, document_id: str, event_type: str, payload: dict | None = None):
        self.sb.table(self.TABLE).insert({"job_id": job_id, "document_id": document_id, "event_type": event_type, "payload": payload or {}}).execute()
