from fastapi import APIRouter, UploadFile, File
from app.repositories.document_repo import DocumentRepository
from app.repositories.ingestion_repo import IngestionJobRepository, IngestionEventRepository
from app.core.hashing import sha256_bytes
from app.services.ingestion.pipeline import IngestionPipeline

router = APIRouter()

@router.post("/documents")
async def ingest_document(
    entity_id: str,
    entity_type: str,
    contract_id: str | None = None,
    process_inline: bool = False,
    file: UploadFile = File(...),
):
    data = await file.read()
    file_hash = sha256_bytes(data)

    docs = DocumentRepository()
    jobs = IngestionJobRepository()
    events = IngestionEventRepository()

    # storage_key updated by pipeline after DB document_id exists
    storage_key = f"{entity_id}/pending/{file.filename}"
    doc = docs.upsert_by_hash(
        entity_id=entity_id,
        entity_type=entity_type,
        contract_id=contract_id,
        file_hash=file_hash,
        filename=file.filename or "uploaded.pdf",
        content_type=file.content_type or "application/pdf",
        storage_key=storage_key,
    )

    job = jobs.create_job(document_id=doc.get("document_id"))
    events.append(job_id=job["job_id"], document_id=job["document_id"], event_type="DOC_REGISTERED", payload={
        "entity_id": entity_id,
        "entity_type": entity_type,
        "contract_id": contract_id,
        "filename": file.filename,
        "file_hash": file_hash
    })

    if not process_inline:
        return {"job_id": job["job_id"], "document_id": job["document_id"], "status": "PENDING"}

    pipeline = IngestionPipeline()
    ctr, warnings = await pipeline.run(
        job=job,
        entity_id=entity_id,
        entity_type=entity_type,
        contract_id=contract_id,
        filename=file.filename or "uploaded.pdf",
        content_type=file.content_type or "application/pdf",
        data=data,
    )
    return {"job_id": job["job_id"], "document_id": job["document_id"], "status": "DONE", "counters": ctr, "warnings": warnings}
