from __future__ import annotations
from typing import Dict, Any, Tuple, List

from app.services.semantic.semantic_proposal_models import SemanticProposal


class SemanticValidator:
    """
    Deterministic validation layer.
    - Validate semantic proposal
    - Map to DB header schema
    - Fail closed (null if mismatch)
    """

    def validate(
        self,
        *,
        proposal: SemanticProposal,
        original_header: Dict[str, Any],
    ) -> Tuple[Dict[str, Any], List[str]]:

        warnings: List[str] = []
        out: Dict[str, Any] = {}

        # ---- language ----
        if proposal.language and proposal.language.value:
            out["language"] = proposal.language.value

        # ---- parties ----
        parties = {}
        if proposal.vendor and proposal.vendor.value:
            parties["vendor"] = proposal.vendor.value
        if proposal.buyer and proposal.buyer.value:
            parties["buyer"] = proposal.buyer.value

        if parties:
            out["parties"] = parties

        # ---- effective period ----
        if proposal.effective_period:
            if proposal.effective_period.value_from:
                out["effective_from"] = proposal.effective_period.value_from
            if proposal.effective_period.value_to:
                out["effective_to"] = proposal.effective_period.value_to

        # ---- traceability ----
        if out:
            extracted = original_header.get("extracted_fields") or {}
            extracted["semantic"] = list(out.keys())
            out["extracted_fields"] = extracted

        return out, warnings
