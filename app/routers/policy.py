from fastapi import APIRouter, HTTPException
from app.services.policy.registry import PolicyRegistry
from app.services.policy.resolver import resolve_domain_policy

router = APIRouter()


@router.get("/meta")
def get_policy_meta():
    policy = PolicyRegistry.get()
    return policy.meta.dict()


@router.get("/domains")
def list_domains():
    policy = PolicyRegistry.get()
    return list(policy.domain_profiles.keys())


@router.get("/domains/{domain_code}")
def get_domain_policy(domain_code: str):
    policy = PolicyRegistry.get()
    try:
        resolved = resolve_domain_policy(policy, domain_code)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))

    return {
        "domain": resolved.domain_code,
        "baseline_priority": resolved.profile.baseline_priority,
        "techniques": list(resolved.techniques.keys()),
        "rules": [r.rule_id for r in resolved.rules],
    }
