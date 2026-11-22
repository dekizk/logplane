"""Result shapes for each stage, in the strict form OpenAI structured output needs.

Every object lists all its properties as required and allows no others;
fields that may not apply are nullable. Claude accepts the same schemas.
"""

from __future__ import annotations


def _obj(properties: dict) -> dict:
    return {"type": "object", "properties": properties, "required": sorted(properties),
            "additionalProperties": False}


_STR = {"type": "string"}
_STRS = {"type": "array", "items": _STR}
_NULLABLE_STR = {"type": ["string", "null"]}

MAP_SCHEMA = _obj({
    "areas": {"type": "array", "items": _obj({
        "id": _STR, "title": _STR, "files": _STRS, "focus": _STR, "critical_paths": _STR,
    })},
    "invariants": _STRS,
    "notes": _NULLABLE_STR,
})

CANDIDATE = _obj({
    "title": _STR,
    "location": _STR,
    "root_cause": _STR,
    "attack_path": _STR,
    "preconditions": _STR,
    "impact": _STR,
    "funds_at_risk": _STR,
    "why_new": _STR,
    "confidence": {"type": "string", "enum": ["low", "medium", "high"]},
    "poc_outline": _STR,
})

HUNT_SCHEMA = _obj({
    "candidates": {"type": "array", "items": CANDIDATE},
    "checked": _STRS,
    "notes": _NULLABLE_STR,
})

TRIAGE_SCHEMA = _obj({
    "findings": {"type": "array", "items": _obj({
        "rank": {"type": "integer"},
        "title": _STR,
        "candidate_ids": _STRS,
        "verdict": {"type": "string", "enum": ["likely_critical", "needs_poc", "unlikely_critical"]},
        "location": _STR,
        "attack_path": _STR,
        "why_new": _STR,
        "poc_plan": _STR,
        "reviewer_notes": _STR,
    })},
    "rejected": {"type": "array", "items": _obj({"candidate_id": _STR, "reason": _STR})},
    "summary": _STR,
})

SCHEMAS = {"map": MAP_SCHEMA, "hunt": HUNT_SCHEMA, "triage": TRIAGE_SCHEMA}
REQUIRED_KEYS = {"map": ("areas",), "hunt": ("candidates",), "triage": ("findings", "rejected")}
