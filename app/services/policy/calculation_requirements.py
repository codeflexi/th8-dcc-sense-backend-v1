# app/policy/calculation_requirements.py

from __future__ import annotations

from typing import Any, Dict, Set


def required_calculations(policy: dict, *, domain: str) -> Dict[str, dict]:
    """
    Enterprise v1 policy shape:
      policy["domains"][domain]["calculations"] : dict
      policy["domains"][domain]["rules"]        : list with rule.uses: [calc_key, ...]

    Returns a dict of calculation_key -> calculation_def (as defined in YAML)
    Example:
      {
        "variance_pct": {...},
        "contract_breach": {...}
      }
    """
    if not isinstance(policy, dict):
        raise ValueError("policy must be a dict")

    domains = policy.get("domains") or {}
    d = domains.get(domain) or {}
    if not d:
        raise ValueError(f"domain not found in policy: {domain}")

    calc_defs = d.get("calculations") or {}
    if not isinstance(calc_defs, dict):
        raise ValueError(f"policy.domains.{domain}.calculations must be a dict")

    rules = d.get("rules") or []
    if not isinstance(rules, list):
        raise ValueError(f"policy.domains.{domain}.rules must be a list")

    needed: Set[str] = set()
    for r in rules:
        if not isinstance(r, dict):
            continue
        uses = r.get("uses") or []
        if isinstance(uses, list):
            for u in uses:
                if u:
                    needed.add(str(u))

    out: Dict[str, dict] = {}
    for key in sorted(needed):
        if key not in calc_defs:
            raise ValueError(
                f"Rule pack references calculation '{key}' but it is missing under policy.domains.{domain}.calculations"
            )
        out[key] = calc_defs[key]

    return out


def required_output_fields(policy: dict, *, domain: str) -> Set[str]:
    """
    Convenience: returns set of output fields produced by required calculations
    Example: {"variance_pct", "contract_breach"}
    """
    calcs = required_calculations(policy, domain=domain)
    fields: Set[str] = set()
    for _, c in calcs.items():
        out = (c or {}).get("output") or {}
        f = out.get("field")
        if f:
            fields.add(str(f))
    return fields
