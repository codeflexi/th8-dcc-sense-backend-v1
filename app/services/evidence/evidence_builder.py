from __future__ import annotations
from typing import Dict, List
import uuid
from urllib.parse import quote

from app.repositories.case_repo import CaseRepository
from app.repositories.audit_repo import AuditRepository
from app.repositories.price_repo import PriceItemRepository
from app.repositories.clause_repo import ClauseRepository
from app.services.evidence.evidence_models import EvidenceResponse, EvidenceRef

class EvidenceBuilder:
    def __init__(self):
        self.cases = CaseRepository()
        self.audit = AuditRepository()
        self.prices = PriceItemRepository()
        self.clauses = ClauseRepository()

    def build_for_case(self, case_id: str) -> EvidenceResponse:
        c = self.cases.get(case_id)
        if not c:
            raise ValueError("CASE_NOT_FOUND")

        contract_id = c.get("contract_id")
        latest_run = self.audit.latest_decision_run(case_id)
        rule_hits = []
        if latest_run and isinstance(latest_run.get("payload"), dict):
            rule_hits = latest_run["payload"].get("rule_hits") or latest_run["payload"].get("rule_results") or []

        groups: Dict[str, List[EvidenceRef]] = {
            "PRICE_MISMATCH": [],
            "CONTRACT_VALIDITY": [],
            "SUPPORTING_CLAUSES": [],
            "CASE_FACTS": [],
            "OTHER": [],
        }

        groups["CASE_FACTS"].append(EvidenceRef(
            evidence_id=str(uuid.uuid4()),
            case_id=case_id,
            source_type="CASE_FACT",
            snippet=f"Case {case_id} amount_total={c.get('amount_total')} currency={c.get('currency')} status={c.get('status')}",
            confidence_score=1.0,
            meta={"case": {"entity_id": c.get("entity_id"), "entity_type": c.get("entity_type")}}
        ))

        if not contract_id:
            groups["OTHER"].append(EvidenceRef(
                evidence_id=str(uuid.uuid4()),
                case_id=case_id,
                source_type="CASE_FACT",
                snippet="No contract_id bound to this case; contract-based evidence unavailable.",
                confidence_score=1.0,
            ))
            return EvidenceResponse(case_id=case_id, entity_id=c.get("entity_id"), entity_type=c.get("entity_type"),
                                   contract_id=None, evidence_groups={k:v for k,v in groups.items() if v},
                                   generated_from="db+audit" if latest_run else "db")

        price_items = self.prices.sb.table(self.prices.TABLE).select("*").eq("contract_id", contract_id).limit(200).execute().data or []
        clauses = self.clauses.sb.table(self.clauses.TABLE).select("*").eq("contract_id", contract_id).limit(200).execute().data or []

        price_by_sku = { (r.get("sku") or "").strip(): r for r in price_items if (r.get("sku") or "").strip() }
        clause_by_type: Dict[str, list] = {}
        for cl in clauses:
            clause_by_type.setdefault((cl.get("clause_type") or "").upper(), []).append(cl)

        for hit in rule_hits:
            rule_id = hit.get("rule_id") or hit.get("id") or "UNKNOWN_RULE"
            sku = hit.get("sku") or hit.get("item_code") or hit.get("sku_code")

            if rule_id in ("PRICE_MISMATCH","CONTRACT_PRICE_VARIANCE","UNIT_PRICE_EXCEEDS_CONTRACT") and sku and sku in price_by_sku:
                groups["PRICE_MISMATCH"].append(self._price_evidence(case_id, rule_id, price_by_sku[sku]))
                continue

            if rule_id in ("CONTRACT_EXPIRED","CONTRACT_INVALID","CONTRACT_NOT_EFFECTIVE"):
                candidates = clause_by_type.get("TERMINATION") or clause_by_type.get("OTHER") or (clauses[:1] if clauses else [])
                if candidates:
                    groups["CONTRACT_VALIDITY"].append(self._clause_evidence(case_id, rule_id, candidates[0]))
                else:
                    groups["CONTRACT_VALIDITY"].append(EvidenceRef(
                        evidence_id=str(uuid.uuid4()),
                        case_id=case_id,
                        rule_id=rule_id,
                        source_type="CASE_FACT",
                        snippet="Contract validity issue detected, but no clause extracted yet for this contract.",
                        confidence_score=0.8,
                    ))
                continue

            if rule_id in ("PAYMENT_TERM","SLA_RISK","PENALTY_APPLICABLE","REBATE_MISSED"):
                ct_map = {"PAYMENT_TERM":"PAYMENT_TERM","SLA_RISK":"SLA","PENALTY_APPLICABLE":"PENALTY","REBATE_MISSED":"REBATE"}
                ct = ct_map.get(rule_id)
                candidates = clause_by_type.get(ct, [])
                if candidates:
                    groups["SUPPORTING_CLAUSES"].append(self._clause_evidence(case_id, rule_id, candidates[0]))
                continue

            groups["OTHER"].append(EvidenceRef(
                evidence_id=str(uuid.uuid4()),
                case_id=case_id,
                rule_id=rule_id,
                source_type="CASE_FACT",
                snippet=f"Rule hit: {rule_id} (no evidence mapping yet)",
                confidence_score=0.7,
                meta={"hit": hit},
            ))

        return EvidenceResponse(
            case_id=case_id,
            entity_id=c.get("entity_id"),
            entity_type=c.get("entity_type"),
            contract_id=contract_id,
            evidence_groups={k:v for k,v in groups.items() if v},
            generated_from="db+audit" if latest_run else "db",
        )

    def _viewer_url(self, document_id: str | None, page_number: int, case_id: str, rule_id: str, snippet: str) -> str | None:
        if not document_id:
            return None
        return f"/viewer?document_id={document_id}&page={page_number}&case_id={quote(case_id)}&rule_id={quote(rule_id)}&snippet={quote(snippet[:180])}"

    def _price_evidence(self, case_id: str, rule_id: str, r: dict) -> EvidenceRef:
        document_id = r.get("document_id")
        page_number = int(r.get("page_number") or 1)
        snippet = (r.get("snippet") or f"Contract unit price: {r.get('unit_price')} {r.get('currency')}").strip()
        return EvidenceRef(
            evidence_id=str(uuid.uuid4()),
            case_id=case_id,
            rule_id=rule_id,
            source_type="PRICE_ITEM",
            source_ref_id=r.get("price_item_id") or r.get("id"),
            document_id=document_id,
            page_number=page_number,
            snippet=snippet,
            confidence_score=float(r.get("confidence_score") or 0.9),
            open_url=f"/api/v1/documents/{document_id}/open_url" if document_id else None,
            viewer_url=self._viewer_url(document_id, page_number, case_id, rule_id, snippet),
            meta={"sku": r.get("sku"), "unit_price": r.get("unit_price"), "currency": r.get("currency"), "uom": r.get("uom")},
        )

    def _clause_evidence(self, case_id: str, rule_id: str, cl: dict) -> EvidenceRef:
        document_id = cl.get("document_id")
        page_number = int(cl.get("page_number") or 1)
        snippet = (cl.get("clause_text") or "")[:180].replace("\n"," ").strip() or (cl.get("clause_title") or "Clause evidence")
        return EvidenceRef(
            evidence_id=str(uuid.uuid4()),
            case_id=case_id,
            rule_id=rule_id,
            source_type="CLAUSE",
            source_ref_id=cl.get("clause_id"),
            document_id=document_id,
            page_number=page_number,
            snippet=snippet,
            confidence_score=float(cl.get("extraction_confidence") or 0.85),
            open_url=f"/api/v1/documents/{document_id}/open_url" if document_id else None,
            viewer_url=self._viewer_url(document_id, page_number, case_id, rule_id, snippet),
            meta={"clause_type": cl.get("clause_type"), "clause_title": cl.get("clause_title"), "structured_data": cl.get("structured_data") or {}},
        )
