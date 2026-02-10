# app/routers/copilot.py
from typing import Optional, AsyncGenerator
from fastapi import APIRouter
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

from app.services.copilot.copilot_agent import CopilotAgent

router = APIRouter()


# ===============================
# Request Schema
# ===============================
class ChatRequest(BaseModel):
    query: str = Field(..., min_length=1)
    case_id: str = Field(..., min_length=1)

    # optional
    group_id: Optional[str] = None
    locale: str = "th-TH"
    max_evidence_refs: int = 6


# ===============================
# STREAM CHAT (NDJSON)
# ===============================
@router.post("/chat/stream")
async def chat_stream(req: ChatRequest):
    """
    Enterprise Copilot stream endpoint

    - case scoped only
    - multi-domain by case
    - evidence-first
    - NDJSON stream
    """

    agent = CopilotAgent()

    generator: AsyncGenerator[str, None] = agent.run_workflow(
        user_query=req.query,
        case_id=req.case_id,
        group_id=req.group_id,
        locale=req.locale,
        max_evidence_refs=req.max_evidence_refs,
    )

    return StreamingResponse(
        generator,
        media_type="application/x-ndjson"
    )
