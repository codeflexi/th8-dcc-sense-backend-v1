from __future__ import annotations
from typing import Dict, Any
from app.services.semantic.semantic_proposal_models import SemanticProposal


class SemanticToHeaderMapper:
    """
    Convert SemanticProposal → deterministic header patch
    """

    @staticmethod
    def map(semantic: SemanticProposal) -> Dict[str, Any]:
        out: Dict[str, Any] = {}

        if semantic.language and semantic.language.value:
            out["language"] = semantic.language.value

        if semantic.effective_period:
            out["effective_from"] = semantic.effective_period.value_from
            out["effective_to"] = semantic.effective_period.value_to

        parties = {}
        if semantic.vendor and semantic.vendor.value:
            parties["vendor"] = semantic.vendor.value
        if semantic.buyer and semantic.buyer.value:
            parties["buyer"] = semantic.buyer.value

        if parties:
            out["parties"] = parties

        if out:
            out["extracted_fields"] = {
                "semantic_keys": list(out.keys())
            }

        return out
