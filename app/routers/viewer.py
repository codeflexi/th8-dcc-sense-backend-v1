from fastapi import APIRouter
from fastapi.responses import HTMLResponse
from pathlib import Path

router = APIRouter()

@router.get("/viewer", response_class=HTMLResponse)
def viewer():
    html = Path(__file__).resolve().parent.parent / "static" / "viewer.html"
    return HTMLResponse(html.read_text(encoding="utf-8"))
