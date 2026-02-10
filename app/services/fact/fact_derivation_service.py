from typing import Dict
from statistics import median

from app.repositories.case_evidence_group_repo import CaseEvidenceGroupRepository
from app.repositories.case_evidence_repo import CaseEvidenceRepository
from app.repositories.case_fact_repo import CaseFactRepository


class FactDerivationService:
    """
    C3.5 — Fact Derivation (FINAL / LOCKED)

    RULES:
    - Fact OWNED by group_id
    - Derive only from evidences attached to group
    - PRICE evidence only

    ENTERPRISE CONSTRAINT (ADDED):
    - Repositories MUST be constructed with sb (single lifecycle)
    - No Repo() without sb
    """

    # ------------------------------------------------------------------
    # CHANGED: switch from @staticmethod to instance method with sb
    # WHY:
    # - staticmethod prevents dependency injection
    # - causes Repo() without sb → inconsistent DB client
    # ------------------------------------------------------------------
    def __init__(self, *, sb):
        self.sb = sb

        # CHANGED: inject sb into repositories
        # WHY: enforce single Supabase client lifecycle
        self.group_repo = CaseEvidenceGroupRepository(sb)
        self.evidence_repo = CaseEvidenceRepository(sb)
        self.fact_repo = CaseFactRepository(sb)

    # ------------------------------------------------------------------
    # CHANGED: remove @staticmethod
    # WHY:
    # - needs access to injected repositories
    # - preserves deterministic behavior
    # ------------------------------------------------------------------
    def derive(self, case_id: str, actor_id: str = "SYSTEM") -> Dict:
        # NOTE:
        # logic below is intentionally kept 1:1 with original
        # only repository access pattern has changed

        groups = self.group_repo.list_by_case(case_id)

        if not groups:
            return {
                "case_id": case_id,
                "status": "no_groups",
                "facts_created": 0,
            }

        facts_created = 0

        for group in groups:
            group_id = group["group_id"]

            # ✅ DB requires NOT NULL
            fact_key = group.get("group_key") or f"GROUP:{group_id}"

            # ------------------------------------------------------------------
            # CHANGED:
            #   evidences = evidence_repo.list_by_group(group_id)
            # → evidences = self.evidence_repo.list_by_group_id(group_id)
            #
            # WHY:
            # - Contract: Evidence OWNED by group_id only
            # - Avoid legacy overloaded list_by_group signatures
            # - Deterministic + audit-safe
            # ------------------------------------------------------------------
            evidences = self.evidence_repo.list_by_group_id(group_id)
            if not evidences:
                continue

            contract_prices = []
            historical_prices = []
            evidence_ids = []

            for ev in evidences:
                if ev.get("evidence_type") != "PRICE":
                    continue

                payload = ev.get("evidence_payload") or {}
                price = payload.get("unit_price")
                currency = payload.get("currency")

                if price is None:
                    continue

                evidence_ids.append(ev["evidence_id"])

                if ev.get("source") == "CONTRACT":
                    contract_prices.append((float(price), currency))
                else:
                    historical_prices.append((float(price), currency))

            # ----------------------------
            # FACT 1: CONTRACT_PRICE (MIN)
            # ----------------------------
            if contract_prices:
                value, currency = min(contract_prices, key=lambda x: x[0])

                self.fact_repo.upsert_fact({
                    "case_id": case_id,
                    "group_id": group_id,
                    "fact_type": "CONTRACT_PRICE",
                    "fact_key": fact_key,
                    "value": value,
                    "currency": currency,
                    "value_json": {
                        "price": value,
                        "currency": currency,
                        "method": "MIN_CONTRACT_PRICE",
                    },
                    "confidence": 0.95,
                    "derivation_method": "MIN_CONTRACT_PRICE",
                    "source_evidence_ids": evidence_ids,
                    "created_by": actor_id,
                })
                facts_created += 1
                continue

            # ----------------------------
            # FACT 2: MEDIAN_12M
            # ----------------------------
            if len(historical_prices) >= 3:
                prices = [p for p, _ in historical_prices]
                value = float(median(prices))
                currency = next((c for _, c in historical_prices if c), None)

                self.fact_repo.upsert_fact({
                    "case_id": case_id,
                    "group_id": group_id,
                    "fact_type": "MEDIAN_12M",
                    "fact_key": fact_key,
                    "value": value,
                    "currency": currency,
                    "value_json": {
                        "price": value,
                        "currency": currency,
                        "method": "MEDIAN_12M",
                    },
                    "confidence": 0.7,
                    "derivation_method": "MEDIAN_12M",
                    "source_evidence_ids": evidence_ids,
                    "created_by": actor_id,
                })
                facts_created += 1
                continue

            # ----------------------------
            # FACT 3: LAST_OBSERVED_PRICE
            # ----------------------------
            if historical_prices:
                value, currency = historical_prices[-1]

                self.fact_repo.upsert_fact({
                    "case_id": case_id,
                    "group_id": group_id,
                    "fact_type": "LAST_OBSERVED_PRICE",
                    "fact_key": fact_key,
                    "value": value,
                    "currency": currency,
                    "value_json": {
                        "price": value,
                        "currency": currency,
                        "method": "LAST_OBSERVED",
                    },
                    "confidence": 0.4,
                    "derivation_method": "LAST_OBSERVED",
                    "source_evidence_ids": evidence_ids,
                    "created_by": actor_id,
                })
                facts_created += 1

        return {
            "case_id": case_id,
            "status": "facts_derived",
            "facts_created": facts_created,
        }
