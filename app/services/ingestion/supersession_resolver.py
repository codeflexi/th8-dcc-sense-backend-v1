from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Optional, Dict, Any, List


@dataclass
class SupersessionResult:
    applied: bool
    superseded_document_ids: List[str]
    warnings: List[str]
    summary: Dict[str, Any]


class SupersessionResolver:
    """
    Enterprise-grade, deterministic supersession resolver.
    """

    def __init__(self, docs_repo):
        self.docs = docs_repo

    def resolve(
        self,
        *,
        new_document_id: str,
        entity_id: str,
        contract_id: Optional[str],
        document_type: str,
        new_effective_from: Optional[date],
    ) -> SupersessionResult:

        warnings: List[str] = []
        superseded: List[str] = []

        if document_type not in {"CONTRACT", "AMENDMENT"}:
            return SupersessionResult(
                applied=False,
                superseded_document_ids=[],
                warnings=["INFO_SUPERSESSION_SKIPPED_NON_CONTRACT_TYPE"],
                summary={"document_type": document_type},
            )

        if not new_effective_from:
            return SupersessionResult(
                applied=False,
                superseded_document_ids=[],
                warnings=["INFO_SUPERSESSION_SKIPPED_NO_EFFECTIVE_FROM"],
                summary={"document_type": document_type},
            )

        candidates = self.docs.list_active_docs_for_supersession(
            entity_id=entity_id,
            contract_id=contract_id,
            document_type=document_type,
            exclude_document_id=new_document_id,
        )

        if not candidates:
            return SupersessionResult(
                applied=False,
                superseded_document_ids=[],
                # warnings=["NO_SUPERSESSION_CANDIDATES"],
                warnings=[],
                summary={"document_type": document_type},
            )

        for d in candidates:
            old_id = d["document_id"]
            old_eff = d.get("effective_from")

            if not old_eff:
                warnings.append(f"SUPERSESSION_CANDIDATE_NO_EFFECTIVE_FROM:{old_id}")
                continue

            if old_eff < new_effective_from:
                self.docs.update_meta(
                    document_id=old_id,
                    superseded_by=new_document_id,
                    extraction_summary={
                        "supersession": {
                            "superseded_by": new_document_id,
                            "method": "EFFECTIVE_DATE_ORDER",
                            "confidence": 0.90,
                        }
                    },
                )
                superseded.append(old_id)

        return SupersessionResult(
            applied=len(superseded) > 0,
            superseded_document_ids=superseded,
            warnings=warnings,
            summary={
                "document_type": document_type,
                "new_effective_from": str(new_effective_from),
                "candidate_count": len(candidates),
                "superseded_count": len(superseded),
            },
        )
