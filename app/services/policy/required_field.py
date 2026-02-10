# app/services/policy/required_fields.py

from typing import Set, Dict, Any


def required_fields_from_policy(policy: Dict[str, Any], domain_code: str) -> Set[str]:
    fields: Set[str] = set()

    for rule in policy.get("rules", []):
        if (rule.get("domain") or "").strip() != domain_code:
            continue

        logic = rule.get("logic") or {}
        field = logic.get("field")

        if field:
            fields.add(field)

    return fields
