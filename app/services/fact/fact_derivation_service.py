from typing import Dict
from statistics import median

from app.repositories.case_evidence_group_repo import CaseEvidenceGroupRepository
from app.repositories.case_evidence_repo import CaseEvidenceRepository
from app.repositories.case_fact_repo import CaseFactRepository


class FactDerivationService:
    """
    C.3 Fact Derivation (FINAL)

    - Input: Evidence Groups (C.2)
    - Output: Facts (persisted)
    - Deterministic
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
                "status": "no_evidence_groups",
                "facts_created": 0
            }

        facts_created = 0

        for group in groups:
            group_id = group["group_id"]
            group_key = group["group_key"]

            evidences = evidence_repo.list_by_group(group_id)
            if not evidences:
                continue

            contract_prices = []
            historical_prices = []
            evidence_ids = []

            for ev in evidences:
                payload = ev.get("evidence_payload") or {}
                price = payload.get("unit_price")

                if price is None:
                    continue

                evidence_ids.append(ev["evidence_id"])

                if ev.get("source") == "CONTRACT":
                    contract_prices.append(price)
                else:
                    historical_prices.append(price)

            # ---------------------------------
            # FACT 1: CONTRACT_PRICE
            # ---------------------------------
            if contract_prices:
                value = min(contract_prices)

                fact_repo.upsert_fact({
                    "case_id": case_id,
                    "fact_key": group_key,
                    "fact_type": "CONTRACT_PRICE",
                    "value": value,
                    "value_json": {
                        "price": value,
                        "currency": "THB",
                        "method": "MIN_CONTRACT_PRICE"
                    },
                    "confidence": 0.95,
                    "derivation_method": "MIN_CONTRACT_PRICE",
                    "source_evidence_ids": evidence_ids,
                    "created_by": actor_id
                })
                facts_created += 1
                continue

            # ---------------------------------
            # FACT 2: MEDIAN_12M
            # ---------------------------------
            if len(historical_prices) >= 3:
                value = median(historical_prices)

                fact_repo.upsert_fact({
                    "case_id": case_id,
                    "fact_key": group_key,
                    "fact_type": "MEDIAN_12M",
                    "value": value,
                    "value_json": {
                        "price": value,
                        "currency": "THB",
                        "method": "MEDIAN_12M"
                    },
                    "confidence": 0.7,
                    "derivation_method": "MEDIAN",
                    "source_evidence_ids": evidence_ids,
                    "created_by": actor_id
                })
                facts_created += 1
                continue

            # ---------------------------------
            # FACT 3: FALLBACK_LAST_PRICE
            # ---------------------------------
            if historical_prices:
                value = historical_prices[-1]

                fact_repo.upsert_fact({
                    "case_id": case_id,
                    "fact_key": group_key,
                    "fact_type": "LAST_OBSERVED_PRICE",
                    "value": value,
                    "value_json": {
                        "price": value,
                        "currency": "THB",
                        "method": "LAST_OBSERVED"
                    },
                    "confidence": 0.4,
                    "derivation_method": "LAST_OBSERVED",
                    "source_evidence_ids": evidence_ids,
                    "created_by": actor_id
                })
                facts_created += 1

        return {
            "case_id": case_id,
            "status": "facts_derived",
            "facts_created": facts_created
        }
