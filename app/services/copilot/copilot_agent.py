# app/services/copilot/copilot_agent.py
import json
import os
from typing import AsyncGenerator, Optional, Dict, Any, List, Tuple

from openai import OpenAI

from app.repositories.copilot_repo import CopilotRepositoryAgent


ALLOWED_TOOLS = {
    "get_case_detail",
    "get_case_decision_summary",
    "get_case_groups",
    "get_group_rules",
    "get_group_evidence",
    "open_document_page",
}


class CopilotAgent:
    """
    Enterprise Copilot (case scoped)
    - Case-scoped only
    - Deterministic tool allowlist
    - Multi-domain via case.domain
    - Evidence-first (reveal evidence refs)
    - Produces decision-ready answer (approve/reject/review + rationale + next steps)
    """

    def __init__(self):
        self.repo = CopilotRepositoryAgent()
        self.client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
        self.model_name = os.getenv("COPILOT_MODEL", "gpt-4o-mini")

    # -------------------------
    # NDJSON event helper
    # -------------------------
    def _evt(self, t: str, data: Dict[str, Any]) -> str:
        return json.dumps({"type": t, "data": data}, ensure_ascii=False) + "\n"

    # -------------------------
    # Tool wrapper
    # -------------------------
    async def _tool(self, name: str, **kwargs):
        if name not in ALLOWED_TOOLS:
            raise RuntimeError(f"Tool not allowed: {name}")
        fn = getattr(self.repo, name, None)
        if not fn:
            raise RuntimeError(f"Tool not implemented: {name}")
        return await fn(**kwargs)

    # -------------------------
    # Normalization helpers
    # -------------------------
    def _extract_case_payload(self, case_detail: Dict[str, Any]) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
        """
        รองรับทั้ง:
        A) { "case": {...}, "line_items":[...] }
        B) { ...case fields at root..., "line_items":[...] }
        """
        if not isinstance(case_detail, dict):
            return {}, []

        case_obj = case_detail.get("case")
        if isinstance(case_obj, dict):
            line_items = case_detail.get("line_items") or []
            return case_obj, line_items if isinstance(line_items, list) else []

        # fallback: assume root is case
        line_items = case_detail.get("line_items") or []
        return case_detail, line_items if isinstance(line_items, list) else []

    def _normalize_entity(self, case_obj: Dict[str, Any]) -> Dict[str, Any]:
        """
        ทำให้ AI เข้าใจ entity แบบ enterprise:
        - entity_type: VENDOR/CUSTOMER/EMPLOYEE/...
        - entity_id / entity_name (ถ้ามี)
        """
        entity_type = (case_obj.get("entity_type") or "").upper().strip()
        if entity_type in ("VENDOR", "SUPPLIER"):
            entity_class = "VENDOR"
        elif entity_type in ("CUSTOMER", "CLIENT"):
            entity_class = "CUSTOMER"
        elif entity_type:
            entity_class = entity_type
        else:
            entity_class = "UNKNOWN"

        return {
            "entity_class": entity_class,
            "entity_type": entity_type or None,
            "entity_id": case_obj.get("entity_id"),
            "vendor_name": case_obj.get("entity_name"),
        }

    def _risk_score(self, x: Any) -> int:
        t = str(x or "").upper()
        return 4 if t == "CRITICAL" else 3 if t == "HIGH" else 2 if t == "MEDIUM" else 1 if t == "LOW" else 0

    def _pick_group(self, groups: List[Dict[str, Any]], requested_group_id: Optional[str]) -> Optional[str]:
        if requested_group_id:
            return requested_group_id
        if not groups:
            return None
        groups_sorted = sorted(groups, key=lambda g: self._risk_score(g.get("risk_level")), reverse=True)
        return groups_sorted[0].get("group_id")

    def _slim_groups(self, groups: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []
        for g in (groups or [])[:50]:
            sku = g.get("sku")
            # sku ในระบบคุณเป็น object (มี sku/name/price ฯลฯ)
            sku_obj = sku if isinstance(sku, dict) else {}
            out.append({
                "group_id": g.get("group_id"),
                "decision": g.get("decision"),
                "risk_level": g.get("risk_level"),
                "confidence": g.get("confidence"),
                "reasons": g.get("reasons") if isinstance(g.get("reasons"), list) else [],
                "baseline": g.get("baseline"),
                "sku": {
                    "sku": sku_obj.get("sku"),
                    "name": sku_obj.get("name") or sku_obj.get("item_name"),
                    "quantity": sku_obj.get("quantity"),
                    "uom": sku_obj.get("uom"),
                    "unit_price": sku_obj.get("unit_price"),
                    "total_price": sku_obj.get("total_price"),
                    "source_line_ref": sku_obj.get("source_line_ref"),
                } if sku_obj else None,
                "evidence_refs": g.get("evidence_refs"),
            })
        return out

    def _extract_actionable_evidence_refs(self, evid_pack: Dict[str, Any], case_id: str, group_id: str, max_refs: int) -> List[Dict[str, Any]]:
        """
        Normalize evidence for both:
        - evidence.source_page/source_snippet
        - evidence.price_items[*].page_number/snippet
        """
        if not isinstance(evid_pack, dict):
            return []

        documents = evid_pack.get("documents") or []
        evidences = evid_pack.get("evidences") or []

        doc_name_by_id = {}
        if isinstance(documents, list):
            for d in documents:
                if isinstance(d, dict) and d.get("document_id"):
                    doc_name_by_id[d["document_id"]] = d.get("file_name") or d["document_id"]

        refs: List[Dict[str, Any]] = []
        for ev in evidences:
            if len(refs) >= max_refs:
                break
            if not isinstance(ev, dict):
                continue

            document_id = ev.get("document_id")
            if not document_id:
                continue

            page = ev.get("source_page")
            snippet = ev.get("source_snippet")

            # fallback: use price_items
            if (page is None or snippet is None) and isinstance(ev.get("price_items"), list) and ev["price_items"]:
                pi0 = ev["price_items"][0] if isinstance(ev["price_items"][0], dict) else {}
                page = page or pi0.get("page_number")
                snippet = snippet or pi0.get("snippet")

            # final fallback
            page = int(page) if page else None
            snippet = (snippet or "").strip()

            if page:
                refs.append({
                    "document_id": document_id,
                    "file_name": doc_name_by_id.get(document_id) or document_id,
                    "page": page,
                    "highlight_text": snippet[:400],
                    "score": ev.get("confidence") or 0,
                    "source": {"case_id": case_id, "group_id": group_id, "evidence_id": ev.get("evidence_id")},
                })

        return refs
    
    def _build_decision_brief(
        self,
        case_detail: dict,
        decision_summary: dict,
        groups: list,
        evid_pack: dict | None,
    ) -> dict:

        case_root = case_detail.get("case", case_detail)

        # -------- PARTY / VENDOR ----------
        vendor = (
            case_root.get("entity_name")
            or case_root.get("vendor_name")
            or case_root.get("entity_id")
        )

        # -------- ITEMS ----------
        items = case_detail.get("line_items") or []

        items_slim = []
        for it in items[:20]:
            items_slim.append({
                "sku": it.get("sku"),
                "name": it.get("item_name") or it.get("name"),
                "qty": it.get("quantity"),
                "unit_price": (it.get("unit_price") or {}).get("value"),
                "total": (it.get("total_price") or {}).get("value"),
            })

        # -------- GROUP RISKS ----------
        risks = []
        for g in groups:
            if g.get("risk_level") in ("HIGH", "CRITICAL") or g.get("decision") == "REVIEW":
                sku = g.get("sku") or {}
                risks.append({
                    "group_id": g.get("group_id"),
                    "sku": sku.get("sku"),
                    "item": sku.get("item_name"),
                    "decision": g.get("decision"),
                    "risk": g.get("risk_level"),
                    "baseline": (g.get("baseline") or {}).get("value"),
                    "price": (sku.get("unit_price") or {}).get("value"),
                    "reasons": [r.get("exec") for r in g.get("reasons", [])]
                })

        # -------- EVIDENCE ----------
        evidence_refs = []
        if evid_pack:
            docs = {d["document_id"]: d["file_name"] for d in evid_pack.get("documents", [])}
            for ev in evid_pack.get("evidences", [])[:6]:
                doc_id = ev.get("document_id")
                page = ev.get("source_page") or (ev.get("price_items") or [{}])[0].get("page_number")
                evidence_refs.append({
                    "doc": docs.get(doc_id),
                    "page": page,
                    "snippet": ev.get("source_snippet") or (ev.get("price_items") or [{}])[0].get("snippet"),
                })

        return {
            "case": {
                "case_id": case_root.get("case_id"),
                "po": case_root.get("reference_id"),
                "vendor": vendor,
                "amount": case_root.get("amount_total"),
                "currency": case_root.get("currency"),
                "status": case_root.get("status"),
            },
            "decision": decision_summary,
            "items": items_slim,
            "risk_items": risks,
            "evidence": evidence_refs,
        }


    # -------------------------
    # Context pack
    # -------------------------
    def _build_context_pack(
        self,
        *,
        locale: str,
        case_obj: Dict[str, Any],
        line_items: List[Dict[str, Any]],
        decision_summary: Optional[Dict[str, Any]],
        groups: List[Dict[str, Any]],
        picked_group_id: Optional[str],
        picked_group_rules: Optional[Dict[str, Any]],
        picked_group_evidence: Optional[Dict[str, Any]]
        
    ) -> Dict[str, Any]:
        entity = self._normalize_entity(case_obj)

        header = {
            "case_id": case_obj.get("case_id") or case_obj.get("id"),
            "domain": case_obj.get("domain"),
            "reference_type": case_obj.get("reference_type"),
            "reference_id": case_obj.get("reference_id"),
            "status": case_obj.get("status"),
            "amount_total": case_obj.get("amount_total"),
            "currency": case_obj.get("currency"),
            "created_by": case_obj.get("created_by"),
            "created_at": case_obj.get("created_at"),
            "updated_at": case_obj.get("updated_at"),
            **entity,
        }

        # line_items slim
        items_slim: List[Dict[str, Any]] = []
        for it in (line_items or [])[:50]:
            if not isinstance(it, dict):
                continue
            up = it.get("unit_price") if isinstance(it.get("unit_price"), dict) else {}
            tp = it.get("total_price") if isinstance(it.get("total_price"), dict) else {}
            items_slim.append({
                "item_id": it.get("item_id"),
                "source_line_ref": it.get("source_line_ref"),
                "sku": it.get("sku"),
                "name": it.get("item_name") or it.get("name"),
                "description": it.get("description"),
                "quantity": it.get("quantity"),
                "uom": it.get("uom"),
                "unit_price": {"value": up.get("value"), "currency": up.get("currency")},
                "total_price": {"value": tp.get("value"), "currency": tp.get("currency")},
            })

        pack: Dict[str, Any] = {
            "locale": locale,
            "case": header,
            "decision_summary": decision_summary or {},
            "line_items": items_slim,
            "groups": self._slim_groups(groups),
            "picked": {
                "group_id": picked_group_id,
                "rules": picked_group_rules or {},
                "evidence": picked_group_evidence or {},
            },
            
        }
        return pack

    # -------------------------
    # System prompt (Enterprise)
    # -------------------------
    def _system_prompt(self, ctx: Dict[str, Any]) -> str:
        """
        ให้ AI ตอบแบบ “พร้อมตัดสินใจ” และ “ไม่เดา”
        """
        return f"""
คุณคือ TH8 Enterprise Case Copilot (Decision Support).
เป้าหมาย: ช่วยผู้อนุมัติ/ผู้ตรวจสอบ “ตัดสินใจเคสนี้” จากข้อมูลใน CONTEXT เท่านั้น

กติกา (บังคับ):
1) ห้ามเดา ห้ามเติมข้อมูลนอก CONTEXT
2) ถ้าข้อมูลไม่พอ ให้ระบุชัดเจนว่า “ขาดอะไร” และเสนอวิธีเอาข้อมูลนั้น
3) ทุกคำตอบต้องมี "หลักฐานอ้างอิง" อย่างน้อย 1 รายการ ถ้าใน CONTEXT มี rules/evidence
4) ตอบเป็นภาษาไทย แบบมืออาชีพ กระชับ แต่ครบประเด็น

รูปแบบคำตอบ (ต้องทำตาม):
A) Executive Summary (1-3 บรรทัด): Decision / Risk / Why
B) What failed & impact: สรุป rule ที่ FAIL + ผลกระทบเชิงธุรกิจ/ควบคุม
C) Evidence: อ้างอิงเอกสาร/หน้า/ข้อความที่เกี่ยวข้อง (ถ้ามี)
D) Recommendation: ทำอะไรต่อ (Approve/Reject/Request info) + เงื่อนไข
E) Missing info (ถ้ามี): ต้องการข้อมูลอะไรเพิ่ม และถามคำถามเจาะจง 3-6 ข้อ

CONTEXT (JSON):
{json.dumps(ctx, ensure_ascii=False)}
        """.strip() 
        



    # -------------------------
    # Main workflow
    # -------------------------
    async def run_workflow(
        self,
        user_query: str,
        case_id: str,
        group_id: Optional[str] = None,
        locale: str = "th-TH",
        max_evidence_refs: int = 6,
    ) -> AsyncGenerator[str, None]:

        # Step 1: load case
        yield self._evt("trace", {"step_id": 1, "title": "Load case", "status": "active", "desc": f"case_id={case_id}"})
        case_detail = await self._tool("get_case_detail", case_id=case_id)
        if not case_detail:
            yield self._evt("error", {"message": "Case not found"})
            yield self._evt("message_chunk", {"text": "ไม่พบข้อมูลเคสนี้ในระบบ"})
            return

        case_obj, line_items = self._extract_case_payload(case_detail)

        # Step 2: decision summary (optional)
        decision_summary = None
        try:
            yield self._evt("trace", {"step_id": 2, "title": "Load decision summary", "status": "active", "desc": "decision-summary"})
            decision_summary = await self._tool("get_case_decision_summary", case_id=case_id)
        except Exception as e:
            decision_summary = None
            yield self._evt("trace", {"step_id": 2, "title": "Load decision summary", "status": "failed", "desc": str(e)})

        # Step 3: groups
        yield self._evt("trace", {"step_id": 3, "title": "Load groups", "status": "active", "desc": "case groups"})
        groups_res = await self._tool("get_case_groups", case_id=case_id)
        groups = groups_res or []

        picked_group_id = self._pick_group(groups, group_id)

        # Step 4: rules + evidence for picked group
        picked_rules = None
        picked_evidence = None

        if picked_group_id:
            # rules
            try:
                yield self._evt("trace", {"step_id": 4, "title": "Load rules", "status": "active", "desc": f"group_id={picked_group_id}"})
                picked_rules = await self._tool("get_group_rules", group_id=picked_group_id)
                yield self._evt("trace", {"step_id": 4, "title": "Load rules", "status": "completed", "desc": "rules loaded"})
            except Exception as e:
                picked_rules = None
                yield self._evt("trace", {"step_id": 4, "title": "Load rules", "status": "failed", "desc": str(e)})

            # evidence
            try:
                yield self._evt("trace", {"step_id": 5, "title": "Load evidence", "status": "active", "desc": f"group_id={picked_group_id}"})
                picked_evidence = await self._tool("get_group_evidence", group_id=picked_group_id)
                yield self._evt("trace", {"step_id": 5, "title": "Load evidence", "status": "completed", "desc": "evidence loaded"})
            except Exception as e:
                picked_evidence = None
                yield self._evt("trace", {"step_id": 5, "title": "Load evidence", "status": "failed", "desc": str(e)})

        # Step 6: reveal evidence refs to UI (for transparency)
        if picked_group_id and isinstance(picked_evidence, dict):
            refs = self._extract_actionable_evidence_refs(picked_evidence, case_id, picked_group_id, max_evidence_refs)
            for r in refs:
                yield self._evt("evidence_reveal", r)
                
       

        # Build context
        ctx_pack = self._build_context_pack(
            locale=locale,
            case_obj=case_obj,
            line_items=line_items,
            decision_summary=decision_summary,
            groups=groups,
            picked_group_id=picked_group_id,
            picked_group_rules=picked_rules,
            picked_group_evidence=picked_evidence
            
        )
        
        

       
        yield self._evt("trace", {"step_id": 6, "title": "Context ready", "status": "completed", "desc": "context pack built"})
        yield self._evt("trace", {"step_id": 7, "title": "Answer", "status": "active", "desc": "LLM streaming"})


        brief = self._build_decision_brief(
            case_detail,
            decision_summary,
            groups,
            picked_evidence
        )
        system_prompt = self._system_prompt(brief)
        # system_prompt = self._system_prompt(ctx_pack)

        try:
            stream = self.client.chat.completions.create(
                model=self.model_name,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_query},
                ],
                temperature=0.2,
                stream=True,
            )

            for chunk in stream:
                delta = chunk.choices[0].delta
                if delta and delta.content:
                    yield self._evt("message_chunk", {"text": delta.content})

        except Exception as e:
            yield self._evt("error", {"message": f"LLM failed: {e}"})
            yield self._evt("message_chunk", {"text": "\n[ระบบขัดข้อง] ไม่สามารถสร้างคำตอบได้ในขณะนี้"})

        yield self._evt("trace", {"step_id": 7, "title": "Answer", "status": "completed", "desc": "done"})
