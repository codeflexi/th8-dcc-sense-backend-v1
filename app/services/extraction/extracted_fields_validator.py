from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional


ALLOWED_METHODS = {
    "LLM_HEADER",
    "RULE_ENRICH",
    "VALIDITY_RANGE_PATTERN",
    "EFFECTIVE_FROM_TO_PATTERN",
    "EFFECTIVE_SINGLE_PATTERN",
    "UNTIL_FURTHER_NOTICE_PATTERN",
    "TERM_DERIVED",
    "TITLE_PATTERN",
    "CONTRACT_NO_PATTERN",
    "PARTY_PATTERN",
    "NORMALIZATION",
}

ALLOWED_TRACE_FIELDS = {
    "doc_title",
    "doc_number",
    "doc_type",
    "effective_from",
    "effective_to",
    "parties",
    "supersession",
}

@dataclass
class ValidationResult:
    ok: bool
    errors: List[str]
    sanitized: Dict[str, Any]


def validate_extracted_fields(extracted_fields: Optional[Dict[str, Any]]) -> ValidationResult:
    """
    Minimal CLM-grade validator for dcc_document_headers.extracted_fields

    Shape:
    {
      "traces": { "<field>": { "value":..., "raw":..., "page_number":..., "method":..., "confidence":... }, ... },
      "signals": { "<signal>": true/false, ... },
      "normalization": { ... },
      "quality": { "warnings": [...], "notes": [...] }
    }
    """
    if not extracted_fields:
        return ValidationResult(ok=True, errors=[], sanitized={})

    if not isinstance(extracted_fields, dict):
        return ValidationResult(ok=False, errors=["extracted_fields must be object"], sanitized={})

    errors: List[str] = []
    out: Dict[str, Any] = {}

    # --- traces ---
    traces = extracted_fields.get("traces")
    if traces is not None:
        if not isinstance(traces, dict):
            errors.append("traces must be object")
        else:
            out_traces: Dict[str, Any] = {}
            for k, v in traces.items():
                if k not in ALLOWED_TRACE_FIELDS:
                    # fail-soft: keep but mark
                    errors.append(f"traces.{k} is not in allowed fields")
                if not isinstance(v, dict):
                    errors.append(f"traces.{k} must be object")
                    continue

                page_number = v.get("page_number")
                if page_number is not None and not isinstance(page_number, int):
                    errors.append(f"traces.{k}.page_number must be int")

                method = v.get("method")
                if method is not None and method not in ALLOWED_METHODS:
                    errors.append(f"traces.{k}.method not allowed: {method}")

                conf = v.get("confidence")
                if conf is not None and not isinstance(conf, (int, float)):
                    errors.append(f"traces.{k}.confidence must be number")

                # sanitized trace (keep only known keys)
                out_traces[k] = {
                    "value": v.get("value"),
                    "raw": v.get("raw"),
                    "page_number": page_number,
                    "method": method,
                    "confidence": conf,
                }
            out["traces"] = out_traces

    # --- signals ---
    signals = extracted_fields.get("signals")
    if signals is not None:
        if not isinstance(signals, dict):
            errors.append("signals must be object")
        else:
            out["signals"] = {k: bool(v) for k, v in signals.items()}

    # --- normalization ---
    norm = extracted_fields.get("normalization")
    if norm is not None:
        if not isinstance(norm, dict):
            errors.append("normalization must be object")
        else:
            out["normalization"] = norm

    # --- quality ---
    quality = extracted_fields.get("quality")
    if quality is not None:
        if not isinstance(quality, dict):
            errors.append("quality must be object")
        else:
            warnings = quality.get("warnings") or []
            notes = quality.get("notes") or []
            if not isinstance(warnings, list):
                errors.append("quality.warnings must be list")
                warnings = []
            if not isinstance(notes, list):
                errors.append("quality.notes must be list")
                notes = []
            out["quality"] = {"warnings": warnings[:50], "notes": notes[:50]}

    ok = len(errors) == 0
    return ValidationResult(ok=ok, errors=errors, sanitized=out if ok else out)
