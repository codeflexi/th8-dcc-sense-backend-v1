def chunk_pages(pages: list[dict], max_chars: int = 1200, overlap: int = 150) -> list[dict]:
    rows = []
    for p in pages:
        page_no = p["page_number"]
        text = p.get("text") or ""
        if not text.strip():
            continue
        i = 0
        while i < len(text):
            chunk = text[i:i+max_chars]
            rows.append({"page_number": page_no, "text": chunk})
            i += max_chars - overlap
    return rows
