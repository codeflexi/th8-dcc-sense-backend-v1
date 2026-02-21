from typing import Dict, List
from app.services.policy.schema import PolicyBundle, DomainSpec, RuleSpec


class ResolvedDomainPolicy:
    """
    Lean resolved policy for a single domain
    Compatible with SelectionService (C3.5)
    """

    def __init__(
        self,
        domain_code: str,
        domain: DomainSpec,
    ):
        self.domain_code = domain_code

        # profile must behave like object or dict
        self.profile = domain.profile or {}

        # 🔥 FIX: inject techniques
        self.techniques: Dict[str, Dict] = domain.techniques or {}

        self.calculations = domain.calculations or {}
        self.rules: List[RuleSpec] = domain.rules or []


def resolve_domain_policy(policy: PolicyBundle, domain_code: str) -> ResolvedDomainPolicy:
    """
    Resolve domain policy from enterprise YAML
    structure:
        policy.domains.{procurement|finance_ap}
    """

    if not policy.domains or domain_code not in policy.domains:
        raise ValueError(f"Domain not found in policy: {domain_code}")

    domain = policy.domains[domain_code]

    return ResolvedDomainPolicy(
        domain_code=domain_code,
        domain=domain,
    )
