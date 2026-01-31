from __future__ import annotations
from app.services.semantic.semantic_proposal_models import SemanticProposal


class SemanticExtractor:
    """
    STEP 2.5 Semantic Layer
    - MUST NEVER throw
    - MUST ALWAYS return SemanticProposal
    """

    def propose(self, *args, **kwargs) -> SemanticProposal:
        try:
            return SemanticProposal()
        except Exception as e:
            return SemanticProposal(
                warnings=[f"semantic_error:{e}"]
            )
