# TH8 Backend — Project Context

## 1. Project Overview
- Project name: TH8 Decision Intelligence (Procurement MVP)
- Goal:
  Build an end-to-end, production-grade decision loop
  from ERP PO → evidence-based decision → human approval → audit.

- Current status:
  Document ingestion pipeline is COMPLETED and WORKING.
  Decision / case / policy layers are NOT implemented yet.

---

## 2. Tech Stack (Current)
- Language: Python
- API framework: FastAPI
- DB: Supabase (Postgres)
- Vector DB: pgvector (Supabase)
- ORM / DB access:
  - [ ] SQLAlchemy
  - [ ] SQLModel
  - [ ] asyncpg
  - [/] Supabase Python client
  (please tick / explain)

- LLM / AI:
  - LangChain
  - LangGraph
  - LLM provider: OpenAI 

---

## 3. Project Structure (Authoritative)

> This structure is already in use.
> New decision-related code MUST follow the same conventions.

- Root folder: `backend/`
- Key patterns:
  - API routes live in: `app/api/...`
  - Business logic in: `app/services/...`
  - DB access in: `app/repositories/...`
  - Background workers in: `app/workers/...`

(Actual tree pasted below or referenced)

---

## 4. Ingestion Pipeline (DO NOT CHANGE)

### Ingestion is FINAL and MUST NOT be redesigned.

Guaranteed outputs from ingestion:
- document_id (UUID)
- document_type (contract, quotation, etc.)
- vendor_id (if extractable)
- item price tables (if present)
- clauses / price items extracted
- embeddings available
- readiness_level: L0–L6
- ingestion job + audit events already implemented

Key tables already exist:
- dcc_documents
- dcc_document_chunks
- dcc_contract_clauses
- dcc_contract_price_items
- dcc_ingestion_jobs
- dcc_audit_events
(please add/remove if needed)

---

## 5. Vector Search Details
- Vector DB: pgvector (Supabase)
- Embedding dimension: __1536____
- Similarity metric:
  - [/] cosine
  - [ ] inner product
  - [ ] L2
- Vector tables:
  - dcc_document_chunks
 

---

## 6. Decision MVP Scope (What to Build Next)

### Phase A — Case & Discovery
- Case ingestion from ERP PO (header + lines only)
- Case signals extraction
- Document discovery (relational + vector)
- Case ↔ document linking with states:
  - INFERRED
  - CONFIRMED
  - REMOVED

### Phase B — Decision Run
- Technique selection (policy-driven)
- Fact derivation (median + fallback + confidence)
- Deterministic rule evaluation
- Evidence Pack generation (PRIMARY vs SUPPORTING)

### Phase C — Human-in-loop
- Approve / Escalate / Override
- Mandatory rationale
- Immutable audit trail

### Phase D — Observability
- KPIs:
  - Coverage
  - Confidence
  - Override rate
  - Cycle time

---

## 7. Non-Negotiable Design Principles
- Evidence > Prediction
- Discovery ≠ Decision
- Only CONFIRMED documents can be used for decisions
- Policy-driven orchestration (DB-driven)
- Deterministic logic over black-box AI
- Human is accountable
- Audit-by-design (append-only events)

---

## 8. Open Questions / Constraints
- Auth / security model (JWT, role-based, etc.): _____Auth_____
- Multi-tenant considerations: ___multi tenant_______
- Any deadlines / demo dates: ____08-02-2026______

## 9.Database 
- select * from dcc_documents where document_id = '6976d155-d5ce-4203-80ea-f831b68e3008'
- select * from dcc_document_pages where document_id  = '6976d155-d5ce-4203-80ea-f831b68e3008'
- select * from dcc_document_chunks where document_id = '6976d155-d5ce-4203-80ea-f831b68e3008'
- select * from dcc_contract_price_items where document_id = '6976d155-d5ce-4203-80ea-f831b68e3008'
- select * from dcc_contract_clauses where document_id = '6976d155-d5ce-4203-80ea-f831b68e3008'
- select * from dcc_ingestion_events where document_id = '6976d155-d5ce-4203-80ea-f831b68e3008'
- select * from dcc_ingestion_jobs where document_id = '6976d155-d5ce-4203-80ea-f831b68e3008'
- key document_id , page_id ,chunk_id , price_item_id+contract_id , clause_id , event_id , job_id