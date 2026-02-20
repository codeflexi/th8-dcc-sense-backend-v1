from typing import Optional, Dict, Any
from app.services.policy.schema import PolicyBundle


class PolicyRegistry:
    """
    Lean in-memory policy registry
    - load() called once at startup
    - get() returns registry (for rule meta lookup)
    - get_bundle() returns raw PolicyBundle (if needed elsewhere)
    """

    _bundle: Optional[PolicyBundle] = None
    _rule_index: Optional[Dict[str, Dict[str, Any]]] = None

    # ---------- LOAD ON STARTUP ----------

    @classmethod
    def load(cls, bundle: PolicyBundle) -> None:
        cls._bundle = bundle

        index: Dict[str, Dict[str, Any]] = {}

        if bundle.domains:
            for domain_name, domain in bundle.domains.items():
                if not domain.rules:
                    continue

                for rule in domain.rules:
                    index[rule.rule_id] = {
                        "label": (
                            rule.explanation.exec
                            if rule.explanation and rule.explanation.exec
                            else rule.rule_id
                        ),
                        "severity": rule.severity,
                        "domain": domain_name,
                    }

        cls._rule_index = index

    # ---------- GET REGISTRY (FOR MAPPER) ----------

    @classmethod
    def get(cls):
        """
        Return registry itself (so mapper can call get_rule_meta)
        """
        if cls._rule_index is None:
            raise RuntimeError("Policy not loaded")
        return cls

    # ---------- OPTIONAL: GET RAW POLICY ----------

    @classmethod
    def get_bundle(cls) -> PolicyBundle:
        if cls._bundle is None:
            raise RuntimeError("Policy not loaded")
        return cls._bundle

    # ---------- RULE LOOKUP ----------

    @classmethod
    def get_rule_meta(cls, rule_id: str) -> Optional[Dict[str, Any]]:
        if cls._rule_index is None:
            raise RuntimeError("Policy not loaded")
        return cls._rule_index.get(rule_id)
