# app/services/transactions/transaction_ingestion_service.py
from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple
from uuid import uuid4

from app.repositories.audit_repo import AuditRepository
from app.repositories.entity_repo import EntityRepository
from app.repositories.transaction_repo import TransactionRepository
from app.repositories.transaction_line_item_repo import TransactionLineItemRepository
from app.repositories.case_repo_ext import CaseRepositoryExt
from app.repositories.case_line_item_repo import CaseLineItemRepository


class TransactionIngestionService:
    def __init__(self, sb):
        self.sb = sb
        self.audit_repo = AuditRepository(sb)
        self.entity_repo = EntityRepository(sb)
        self.txn_repo = TransactionRepository(sb)
        self.ledger_repo = TransactionLineItemRepository(sb)
        self.case_repo = CaseRepositoryExt(sb)
        self.case_line_repo = CaseLineItemRepository(sb)

    # ----------------------------
    # GRN ingestion (PO-led only)
    # ----------------------------
    def ingest_grn(
        self,
        *,
        actor_id: str,
        entity_id: str,
        po_number: str,
        grn_number: str,
        currency: str,
        lines: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        entity = self._require_entity(entity_id)
        txn = self._require_po_transaction(po_number)

        # currency guard
        self._ensure_currency_match(txn, currency)

        # insert ledger rows
        rows = self._build_ledger_rows(
            transaction_id=txn["transaction_id"],
            source_type="GRN",
            source_ref_id=grn_number,
            entity_id=entity_id,
            currency=currency,
            lines=lines,
            source_system="ERP",
            trust_level="HIGH",
            created_by=actor_id,
        )

        # idempotency at doc-level (fast path)
        if self.ledger_repo.exists_doc_for_entity(
            transaction_id=txn["transaction_id"],
            source_type="GRN",
            source_ref_id=grn_number,
            entity_id=entity_id,
        ):
            # return OK (idempotent)
            proc_case = self.case_repo.find_procurement_case_for_transaction(transaction_id=txn["transaction_id"])
            self._emit_audit_safe(
                case_id=(proc_case or {}).get("case_id"),
                event_type="GRN_ALREADY_EXISTS",
                actor=actor_id,
                payload={"transaction_id": txn["transaction_id"], "grn_number": grn_number, "entity_id": entity_id},
            )
            return {
                "status": "ALREADY_EXISTS",
                "transaction_id": txn["transaction_id"],
                "grn_number": grn_number,
                "entity_id": entity_id,
            }

        inserted = self._insert_ledger_rows_idempotent(rows)

        proc_case = self.case_repo.find_procurement_case_for_transaction(transaction_id=txn["transaction_id"])
        self._emit_audit_safe(
            case_id=(proc_case or {}).get("case_id"),
            event_type="GRN_RECEIVED",
            actor=actor_id,
            payload={
                "transaction_id": txn["transaction_id"],
                "po_number": po_number,
                "grn_number": grn_number,
                "entity_id": entity_id,
                "line_count": len(inserted),
            },
        )

        return {
            "status": "OK",
            "transaction_id": txn["transaction_id"],
            "grn_number": grn_number,
            "entity_id": entity_id,
            "inserted_line_count": len(inserted),
        }

    # ----------------------------
    # Invoice ingestion (PO-led or invoice-led)
    # ----------------------------
    def ingest_invoice(
        self,
        *,
        actor_id: str,
        entity_id: str,
        invoice_number: str,
        currency: str,
        lines: List[Dict[str, Any]],
        po_number: Optional[str] = None,
    ) -> Dict[str, Any]:
        entity = self._require_entity(entity_id)

        if po_number:
            txn = self._require_po_transaction(po_number)
            self._ensure_currency_match(txn, currency)
            mismatch = (str(txn.get("entity_id") or "") != str(entity_id))
        else:
            # invoice-led
            agg_key = f"{entity_id}:{invoice_number}"
            txn = self.txn_repo.get_by_aggregate(aggregate_type="FINANCE_FLOW", aggregate_key=agg_key)
            if not txn:
                txn = self.txn_repo.create(
                    aggregate_type="FINANCE_FLOW",
                    aggregate_key=agg_key,
                    entity_id=entity_id,
                    entity_type=entity.get("entity_type") or "unknown",
                    currency=currency,
                    amount_total=None,
                    lifecycle_status="OPEN",
                    metadata_json={"invoice_led": True},
                    created_by=actor_id,
                )
            mismatch = False

        # If invoice already has finance_ap case: return existing (idempotent)
        existing = self.case_repo.find_finance_ap_case(
            transaction_id=txn["transaction_id"],
            invoice_number=invoice_number,
        )
        if existing:
            self._emit_audit_safe(
                case_id=existing.get("case_id"),
                event_type="INVOICE_ALREADY_EXISTS",
                actor=actor_id,
                payload={
                    "transaction_id": txn["transaction_id"],
                    "invoice_number": invoice_number,
                    "entity_id": entity_id,
                },
            )
            return {
                "status": "ALREADY_EXISTS",
                "transaction_id": txn["transaction_id"],
                "case_id": existing.get("case_id"),
                "invoice_number": invoice_number,
                "entity_id": entity_id,
            }

        # doc-level ledger idempotency fast path
        if self.ledger_repo.exists_doc_for_entity(
            transaction_id=txn["transaction_id"],
            source_type="INVOICE",
            source_ref_id=invoice_number,
            entity_id=entity_id,
        ):
            # ledger exists but case missing => create case anyway (repair path)
            base_case_detail = self._build_case_detail(entity_id=entity_id, txn=txn, mismatch=mismatch)
            case_detail = self._merge_case_detail(
                base_case_detail,
                {
                    "vendor_id": entity_id,
                    "invoice_number": invoice_number,
                    "po_number": po_number,
                    "transaction_id": txn["transaction_id"],
                },
            )

            created_case = self.case_repo.create_finance_ap_case(
                transaction_id=txn["transaction_id"],
                entity_id=entity_id,
                entity_type=entity.get("entity_type") or "unknown",
                invoice_number=invoice_number,
                currency=currency,
                created_by=actor_id,
                case_detail=case_detail,
            )

            # 🔥 Ensure dcc_case_line_items exists for finance_ap (repair path)
            self._ensure_finance_case_line_items(
                case_id=created_case.get("case_id"),
                transaction_id=txn["transaction_id"],
                invoice_number=invoice_number,
                currency=currency,
                lines=lines,  # may be empty; will fallback to ledger read
            )

            self._emit_audit_safe(
                case_id=created_case.get("case_id"),
                event_type="FINANCE_CASE_CREATED_FROM_EXISTING_LEDGER",
                actor=actor_id,
                payload={"transaction_id": txn["transaction_id"], "invoice_number": invoice_number, "entity_id": entity_id},
            )
            return {
                "status": "OK",
                "transaction_id": txn["transaction_id"],
                "case_id": created_case.get("case_id"),
                "invoice_number": invoice_number,
                "entity_id": entity_id,
                "inserted_line_count": 0,
            }

        # Insert invoice ledger
        rows = self._build_ledger_rows(
            transaction_id=txn["transaction_id"],
            source_type="INVOICE",
            source_ref_id=invoice_number,
            entity_id=entity_id,
            currency=currency,
            lines=lines,
            source_system="ERP",
            trust_level="HIGH",
            created_by=actor_id,
        )
        inserted = self._insert_ledger_rows_idempotent(rows)

        # Create finance_ap case
        base_case_detail = self._build_case_detail(entity_id=entity_id, txn=txn, mismatch=mismatch)
        case_detail = self._merge_case_detail(
            base_case_detail,
            {
                "vendor_id": entity_id,
                "invoice_number": invoice_number,
                "po_number": po_number,
                "transaction_id": txn["transaction_id"],
            },
        )

        created_case = self.case_repo.create_finance_ap_case(
            transaction_id=txn["transaction_id"],
            entity_id=entity_id,
            entity_type=entity.get("entity_type") or "unknown",
            invoice_number=invoice_number,
            currency=currency,
            created_by=actor_id,
            case_detail=case_detail,
        )

        # 🔥 CRITICAL FIX: seed dcc_case_line_items for finance_ap
        self._ensure_finance_case_line_items(
            case_id=created_case.get("case_id"),
            transaction_id=txn["transaction_id"],
            invoice_number=invoice_number,
            currency=currency,
            lines=lines,
        )

        self._emit_audit_safe(
            case_id=created_case.get("case_id"),
            event_type="INVOICE_RECEIVED",
            actor=actor_id,
            payload={
                "transaction_id": txn["transaction_id"],
                "invoice_number": invoice_number,
                "entity_id": entity_id,
                "po_number": po_number,
                "ledger_lines": len(inserted),
                "review_required": bool(case_detail.get("review_required")),
            },
        )

        return {
            "status": "OK",
            "transaction_id": txn["transaction_id"],
            "case_id": created_case.get("case_id"),
            "invoice_number": invoice_number,
            "entity_id": entity_id,
            "inserted_line_count": len(inserted),
        }

    # ======================================================
    # Internals
    # ======================================================
    def _require_entity(self, entity_id: str) -> Dict[str, Any]:
        ent = self.entity_repo.get(entity_id)
        if not ent:
            raise ValueError(f"Unknown entity_id: {entity_id}")
        return ent

    def _require_po_transaction(self, po_number: str) -> Dict[str, Any]:
        txn = self.txn_repo.get_by_aggregate(aggregate_type="PROCUREMENT_FLOW", aggregate_key=po_number)
        if not txn:
            raise ValueError(f"PO transaction not found for po_number: {po_number}")
        return txn

    def _ensure_currency_match(self, txn: Dict[str, Any], currency: str) -> None:
        txn_ccy = (txn.get("currency") or "").strip()
        if txn_ccy and currency and txn_ccy != currency:
            raise ValueError(f"Currency mismatch: txn={txn_ccy} payload={currency}")

    def _build_case_detail(self, *, entity_id: str, txn: Dict[str, Any], mismatch: bool) -> Dict[str, Any]:
        if not mismatch:
            return {}
        return {
            "review_required": True,
            "review_reasons": [
                {
                    "code": "ENTITY_MISMATCH",
                    "expected_entity_id": txn.get("entity_id"),
                    "payload_entity_id": entity_id,
                    "severity": "HIGH",
                }
            ],
        }

    def _merge_case_detail(self, base: Dict[str, Any], patch: Dict[str, Any]) -> Dict[str, Any]:
        out = dict(base or {})
        for k, v in (patch or {}).items():
            if v is not None:
                out[k] = v
        return out

    def _case_has_line_items(self, case_id: str) -> bool:
        try:
            res = (
                self.sb.table("dcc_case_line_items")
                .select("item_id")
                .eq("case_id", case_id)
                .limit(1)
                .execute()
            )
            data = getattr(res, "data", None) or []
            return bool(data)
        except Exception:
            return False

    def _fetch_invoice_ledger_lines_best_effort(
        self,
        *,
        transaction_id: str,
        invoice_number: str,
    ) -> List[Dict[str, Any]]:
        # fallback when caller didn't supply lines
        try:
            res = (
                self.sb.table("dcc_transaction_line_items")
                .select("*")
                .eq("transaction_id", transaction_id)
                .eq("source_type", "INVOICE")
                .eq("source_ref_id", invoice_number)
                .execute()
            )
            return getattr(res, "data", None) or []
        except Exception:
            return []

    def _ensure_finance_case_line_items(
        self,
        *,
        case_id: Optional[str],
        transaction_id: str,
        invoice_number: str,
        currency: str,
        lines: List[Dict[str, Any]],
    ) -> None:
        if not case_id:
            return

        # idempotent: don't insert twice
        if self._case_has_line_items(case_id):
            return

        src_lines = lines or []
        if not src_lines:
            src_lines = self._fetch_invoice_ledger_lines_best_effort(
                transaction_id=transaction_id,
                invoice_number=invoice_number,
            )

        items: List[Dict[str, Any]] = []
        for i, ln in enumerate(src_lines or []):
            sku = ln.get("sku")
            if not sku:
                continue

            qty = ln.get("quantity")
            unit_price = ln.get("unit_price")

            # total_price uses existing amount if present; else compute
            total_price = ln.get("amount")
            if total_price is None:
                try:
                    total_price = float(qty or 0) * float(unit_price or 0)
                except Exception:
                    total_price = None

            items.append(
                {
                    "item_id": str(uuid4()),
                    "case_id": case_id,
                    "sku": sku,
                    "item_name": ln.get("item_name"),
                    "description": ln.get("description"),
                    "quantity": qty,
                    "unit_price": unit_price,
                    "currency": (ln.get("currency") or currency),
                    "total_price": total_price,
                    "uom": ln.get("uom"),
                    "source_line_ref": ln.get("source_line_ref") or ln.get("line_ref") or str(i + 1),
                }
            )

        if not items:
            return

        try:
            self.case_line_repo.bulk_insert(items)
        except Exception:
            # best-effort: do not fail ingestion
            return

    def _build_ledger_rows(
        self,
        *,
        transaction_id: str,
        source_type: str,
        source_ref_id: str,
        entity_id: str,
        currency: str,
        lines: List[Dict[str, Any]],
        source_system: str,
        trust_level: str,
        created_by: str,
    ) -> List[Dict[str, Any]]:
        rows: List[Dict[str, Any]] = []
        for i, ln in enumerate(lines or []):
            # source_line_ref is mandatory for unique key; if missing, generate stable index-based ref
            source_line_ref = str(ln.get("source_line_ref") or ln.get("line_ref") or (i + 1))

            qty = ln.get("quantity")
            unit_price = ln.get("unit_price")

            amount = ln.get("amount")
            if amount is None:
                try:
                    amount = float(qty or 0) * float(unit_price or 0)
                except Exception:
                    amount = None

            rows.append(
                {
                    "transaction_id": transaction_id,
                    "source_type": source_type,
                    "source_ref_id": source_ref_id,
                    "source_line_ref": source_line_ref,
                    "entity_id": entity_id,
                    "sku": ln.get("sku"),
                    "item_name": ln.get("item_name"),
                    "description": ln.get("description"),
                    "uom": ln.get("uom"),
                    "quantity": qty,
                    "unit_price": unit_price,
                    "currency": ln.get("currency") or currency,
                    "amount": amount,
                    "source_system": source_system,
                    "trust_level": trust_level,
                    "document_id": ln.get("document_id"),
                    "metadata_json": ln.get("metadata_json") or {},
                    "created_by": created_by,
                }
            )
        return rows

    def _insert_ledger_rows_idempotent(self, rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        if not rows:
            return []
        try:
            return self.ledger_repo.insert_many(rows)
        except Exception:
            # ถ้าชน unique (ยิงซ้ำบางบรรทัด) ให้ fallback เป็น “best effort”:
            inserted: List[Dict[str, Any]] = []
            for r in rows:
                try:
                    _ = self.ledger_repo.insert_many([r])
                    inserted.append(r)
                except Exception:
                    continue
            return inserted

    def _emit_audit_safe(self, *, case_id: Optional[str], event_type: str, actor: str, payload: Dict[str, Any]) -> None:
        if not case_id:
            return
        try:
            self.audit_repo.emit(
                case_id=case_id,
                event_type=event_type,
                actor=actor,
                run_id=None,
                payload=payload,
            )
        except Exception:
            return
