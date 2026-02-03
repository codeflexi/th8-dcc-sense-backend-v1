from typing import Dict, List
from app.services.policy.schema import PolicyBundle, DomainProfile, TechniqueSpec, RuleSpec


class ResolvedDomainPolicy:
    def __init__(
        self,
        domain_code: str,
        profile: DomainProfile,
        techniques: Dict[str, TechniqueSpec],
        rules: List[RuleSpec],
    ):
        self.domain_code = domain_code
        self.profile = profile
        self.techniques = techniques          # id -> spec
        self.rules = rules


def resolve_domain_policy(policy: PolicyBundle, domain_code: str) -> ResolvedDomainPolicy:
    if domain_code not in policy.domain_profiles:
        raise ValueError(f"Domain not found in policy: {domain_code}")

    profile = policy.domain_profiles[domain_code]

    # filter techniques
    tech_map = {}
    for t in policy.techniques:
        if t.domain is None or t.domain == domain_code:
            tech_map[t.id] = t

    # filter rules
    rules = [r for r in policy.rules if r.domain == domain_code]

    return ResolvedDomainPolicy(
        domain_code=domain_code,
        profile=profile,
        techniques=tech_map,
        rules=rules,
    )
