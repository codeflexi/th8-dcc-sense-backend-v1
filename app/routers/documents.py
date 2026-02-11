from fastapi import APIRouter, HTTPException,Request
from app.repositories.document_repo import DocumentRepository
from app.repositories.document_open_repo import DocumentOpenRepository
from app.services.document.document_service import DocumentPageService
from typing import Optional, Dict, Any


router = APIRouter()

@router.get("/documents/{document_id}/open_url")
def get_document_open_url(request: Request, document_id: str, expires_in: int = 3600):
    sb = request.state.sb
    doc_repo = DocumentRepository(sb)
    doc_open_repo = DocumentOpenRepository(sb)
    
    doc = doc_repo.get(document_id)
    if not doc:
        raise HTTPException(status_code=404, detail="Document not found")
    storage_key = doc.get("storage_key")
    if not storage_key:
        raise HTTPException(status_code=400, detail="Document has no storage_key")
    signed = doc_open_repo.create_signed_url(storage_key=storage_key, expires_in=expires_in)
    if not signed:
        raise HTTPException(status_code=500, detail="Failed to create signed url")
    return {"document_id": document_id, "signed_url": signed, "expires_in": expires_in}

@router.get("/documents/{document_id}/pages-no/{page_no}")
def open_file(request: Request, document_id: str, page_no: int):
    sb = request.state.sb
    doc_repo = DocumentRepository(sb)
    open_repo = DocumentOpenRepository(sb)
    doc = doc_repo.get(document_id)
    
    
    if not doc:
        raise HTTPException(status_code=404, detail="Document not found")

    storage_key = doc.get("storage_key")
    if not storage_key:
        raise HTTPException(status_code=400, detail="Document has no storage_key")

    signed = open_repo.create_signed_url(
        storage_key=storage_key,
        expires_in=3600
    )

    if not signed:
        raise HTTPException(status_code=500, detail="Failed to create signed url")

    return {
        "document_id": document_id,
        "page": page_no,
        "pdf_url": signed,        # ⭐ สำคัญ
        "page_text": "",          # optional future
        "text_blocks": []
    }

@router.get("/documents/{document_id}/pages/{page_no}")
def get_document_page(request: Request, document_id: str, page_no: int):
    sb = request.state.sb
    doc_repo = DocumentRepository(sb)
    try:
        return doc_repo.get_page(document_id, page_no)
    except ValueError as e:
        raise HTTPException(404, str(e))
    
# =========================================================
# NEW: FULL PAGE CONTEXT (HEADER + CHUNKS + PRICE + EVIDENCE)
# =========================================================
@router.get("/documents/{document_id}/page-context/{page_no}")
def get_document_page_context(
    request: Request,
    document_id: str,
    page_no: int,
    case_id: Optional[str] = None,
    group_id: Optional[str] = None,
):
    """
    Enterprise page context endpoint

    Used by:
    - Evidence modal
    - Copilot context builder
    - PDF viewer right panel
    """

    sb = request.state.sb
    service = DocumentPageService(sb)

    try:
        data = service.get_page(
            document_id=document_id,
            page_number=page_no,
            case_id=case_id,
            group_id=group_id,
        )
        return data

    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
