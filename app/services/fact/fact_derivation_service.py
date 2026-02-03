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
    """

    @staticmethod
    def derive(case_id: str, actor_id: str = "SYSTEM") -> Dict:
        group_repo = CaseEvidenceGroupRepository()
        evidence_repo = CaseEvidenceRepository()
        fact_repo = CaseFactRepository()

        groups = group_repo.list_by_case(case_id)

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

            evidences = evidence_repo.list_by_group(group_id)
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

                fact_repo.upsert_fact({
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

                fact_repo.upsert_fact({
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

                fact_repo.upsert_fact({
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
