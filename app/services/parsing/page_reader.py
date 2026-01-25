"""Adapter: bytes -> temp file -> LlamaParse -> page dicts.

This keeps the pipeline stable even if reader changes later.
"""

import os
import tempfile
from typing import List, Dict, Any
from app.services.parsing.parser import parse_pdf_with_metadata

async def read_pages_with_llamaparse(data: bytes, filename: str = "document.pdf") -> List[Dict[str, Any]]:
    suffix = ".pdf" if not filename.lower().endswith(".pdf") else ""
    with tempfile.NamedTemporaryFile(delete=False, suffix=suffix or ".pdf") as tmp:
        tmp.write(data)
        tmp_path = tmp.name

    try:
        docs = await parse_pdf_with_metadata(tmp_path)
        pages = []
        for d in docs:
            # LlamaParse metadata page_label can be str; normalize to int if possible.
            page_label = d.metadata.get("page_label") or d.metadata.get("page") or d.metadata.get("page_number")
            try:
                page_no = int(str(page_label).strip())
            except Exception:
                page_no = len(pages) + 1
            pages.append({"page_number": page_no, "text": d.text, "meta": d.metadata})
        # stable sort by page_number
        pages.sort(key=lambda x: x["page_number"])
        return pages
    finally:
        try:
            os.remove(tmp_path)
        except Exception:
            pass
