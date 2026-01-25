# TH8 Sense DCC Backend (Ingestion + Evidence)

This package implements the **ingestion side** aligned with your locked `dcc_*` schema (entity-based).  
Decision Engine + Audit are assumed DONE and are not coupled here.

## Install with uv
```bash
uv venv
source .venv/bin/activate
uv pip install -r requirements.txt
# or
uv pip install -e .
```

## Configure
Copy `.env.example` to `.env` and set:
- `SUPABASE_URL`, `SUPABASE_SERVICE_ROLE_KEY`, `SUPABASE_STORAGE_BUCKET`
- `OPENAI_API_KEY` (embeddings + LLM clause extraction)
- `LLAMA_CLOUD_API_KEY` (LlamaParse)

## Run API
```bash
uvicorn app.main:app --reload --port 8000
```

## Run worker
```bash
python -m app.workers.ingestion_worker
```

## Ingestion behavior (deterministic, retry-safe)
- `dcc_documents`: upsert by `(entity_id, file_hash)`
- `dcc_ingestion_jobs`: queue + retries
- `dcc_ingestion_events`: append-only step events
- `dcc_document_pages`: stable page anchors (citation)
- `dcc_contract_clauses`: LLM structured extraction + validation (fail-closed for structured fields)
- `dcc_contract_price_items`: deterministic extraction (conservative baseline)
- `dcc_document_chunks`: retrieval chunks

## Evidence API + Viewer
- `GET /api/v1/cases/{case_id}/evidence`
- `GET /api/v1/documents/{document_id}/open_url`
- Viewer: `/viewer?document_id=...&page=...&case_id=...&rule_id=...&snippet=...`
