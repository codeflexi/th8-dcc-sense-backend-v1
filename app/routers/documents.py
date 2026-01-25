from fastapi import APIRouter, HTTPException
from app.repositories.document_repo import DocumentRepository
from app.repositories.document_open_repo import DocumentOpenRepository

router = APIRouter()

@router.get("/documents/{document_id}/open_url")
def get_document_open_url(document_id: str, expires_in: int = 3600):
    doc = DocumentRepository().get(document_id)
    if not doc:
        raise HTTPException(status_code=404, detail="Document not found")
    storage_key = doc.get("storage_key")
    if not storage_key:
        raise HTTPException(status_code=400, detail="Document has no storage_key")
    signed = DocumentOpenRepository().create_signed_url(storage_key=storage_key, expires_in=expires_in)
    if not signed:
        raise HTTPException(status_code=500, detail="Failed to create signed url")
    return {"document_id": document_id, "signed_url": signed, "expires_in": expires_in}
