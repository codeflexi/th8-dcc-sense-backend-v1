from __future__ import annotations
from typing import Optional, Dict, Any


class PolicyRegistry:
    """
    Abstraction layer for rule metadata lookup.
    This must be wired to your YAML policy loader.
    """

    def __init__(self, rule_index: Dict[str, Dict[str, Any]]):
        self._rule_index = rule_index or {}

    def get_rule_meta(self, rule_id: str) -> Optional[Dict[str, Any]]:
        return self._rule_index.get(rule_id)

    def get_rule_label(self, rule_id: str) -> Optional[str]:
        meta = self.get_rule_meta(rule_id)
        if not meta:
            return None
        return meta.get("label")

    def get_rule_severity(self, rule_id: str) -> Optional[str]:
        meta = self.get_rule_meta(rule_id)
        if not meta:
            return None
        return meta.get("severity")
