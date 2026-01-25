import time
import asyncio
from app.core.config import settings
from app.repositories.ingestion_repo import IngestionJobRepository, IngestionEventRepository
from app.repositories.document_repo import DocumentRepository
from app.repositories.storage_repo import StorageRepository
from app.services.ingestion.pipeline import IngestionPipeline

async def loop():
    jobs = IngestionJobRepository()
    docs = DocumentRepository()
    events = IngestionEventRepository()
    storage = StorageRepository()
    pipeline = IngestionPipeline()

    while True:
        job = jobs.fetch_next_pending()
        if not job:
            await asyncio.sleep(settings.INGESTION_POLL_SECONDS)
            continue

        job_id = job["job_id"]
        document_id = job["document_id"]
        try:
            jobs.mark_running(job_id)
            events.append(job_id=job_id, document_id=document_id, event_type="JOB_RUNNING")
            doc = docs.get(document_id)
            if not doc:
                raise RuntimeError("Document not found")

            storage_key = doc["storage_key"]
            data = storage.download_bytes(storage_key=storage_key)

            await pipeline.run(
                job=job,
                entity_id=doc["entity_id"],
                entity_type=doc["entity_type"],
                contract_id=doc.get("contract_id"),
                filename=doc.get("filename") or "document.pdf",
                content_type=doc.get("content_type") or "application/pdf",
                data=data,
            )
        except Exception as e:
            jobs.mark_failed(job_id, error=str(e), retryable=True)
            events.append(job_id=job_id, document_id=document_id, event_type="JOB_FAILED", payload={"error": str(e)})
        finally:
            await asyncio.sleep(0.2)

def main():
    asyncio.run(loop())

if __name__ == "__main__":
    main()
