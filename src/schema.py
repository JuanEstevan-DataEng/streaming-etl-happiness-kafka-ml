"""Event schema validation. Returns processing status for each Kafka payload."""

from typing import Optional

# Required JSON fields for a happiness event
REQUIRED_FIELDS = ["country", "year", "gdp", "family", "health",
                   "freedom", "generosity", "corruption", "actual_happiness_score"]

# Feature order MUST match the order used during training (Step A.4)
MODEL_FEATURE_ORDER = ["gdp", "family", "health", "freedom", "generosity", "corruption"]

# Acceptable numeric ranges (loose, dataset-driven)
_NUMERIC_RANGES = {
    "gdp": (0.0, 5.0), "family": (0.0, 5.0), "health": (0.0, 5.0),
    "freedom": (0.0, 5.0), "generosity": (0.0, 5.0), "corruption": (0.0, 5.0),
    "actual_happiness_score": (0.0, 10.0),
}

def validate_event(payload: dict) -> tuple[str, Optional[str]]:
    """Return ('VALID', None) or ('INVALID_SCHEMA'|'INVALID_VALUES', detail)."""
    if not isinstance(payload, dict):
        return "INVALID_SCHEMA", f"payload no es dict: {type(payload).__name__}"

    # Check required fields presence
    missing = [f for f in REQUIRED_FIELDS if f not in payload]
    if missing:
        return "INVALID_SCHEMA", f"campos faltantes: {missing}"

    # Year must be int-like and within plausible range
    try:
        year = int(payload["year"])
    except (TypeError, ValueError):
        return "INVALID_SCHEMA", "year no es entero"
    if not (2010 <= year <= 2030):
        return "INVALID_VALUES", f"year fuera de rango: {year}"

    # Numeric features must cast to float and lie in range
    for field, (lo, hi) in _NUMERIC_RANGES.items():
        try:
            v = float(payload[field])
        except (TypeError, ValueError):
            return "INVALID_SCHEMA", f"{field} no es numérico"
        if not (lo <= v <= hi):
            return "INVALID_VALUES", f"{field}={v} fuera de [{lo}, {hi}]"

    # Country must be a non-empty string
    if not isinstance(payload["country"], str) or not payload["country"].strip():
        return "INVALID_SCHEMA", "country vacío o no string"

    return "VALID", None
