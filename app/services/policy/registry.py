from typing import Optional
from app.services.policy.schema import PolicyBundle


class PolicyRegistry:
    """
    In-memory policy registry.
    In MVP: load once at startup.
    Later: can support reload / multi-version.
    """
    _policy: Optional[PolicyBundle] = None

    @classmethod
    def load(cls, policy: PolicyBundle):
        cls._policy = policy

    @classmethod
    def get(cls) -> PolicyBundle:
        if cls._policy is None:
            raise RuntimeError("Policy not loaded")
        return cls._policy
