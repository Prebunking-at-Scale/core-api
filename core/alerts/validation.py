"""What makes an alert valid, as the frontend's validateAlertRule (utils/alertRules.ts)
checks it, and the filters kept for each condition."""

from typing import Any
from uuid import UUID

from core.alerts.models import (
    ALLOWED_FILTERS,
    CONDITION_TYPES,
    MAX_CONDITIONS,
    NAME_MAX_LENGTH,
    AlertConditionInput,
    AlertRuleInput,
    AlertValidationError,
)
from core.models import NarrativeSpreadPattern

_UUID_FILTERS = ("topic_id", "entity_id")
_SPREAD_PATTERNS = {p.value for p in NarrativeSpreadPattern}


def _is_empty(value: Any) -> bool:
    return value is None or value == [] or value == ""


def clean_filters(type_: str, filters: dict[str, Any]) -> tuple[dict[str, Any], list[str]]:
    """The filters worth keeping (empty values dropped; keyword_mode only with
    keywords) and the errors found in the rest."""
    errors: list[str] = []
    kept: dict[str, Any] = {}
    allowed = ALLOWED_FILTERS[type_]
    for key, value in filters.items():
        if key == "keyword_mode" or _is_empty(value):
            continue
        if key not in allowed:
            errors.append("filter_not_allowed")
            continue
        if not isinstance(value, list) or not all(isinstance(v, str) and v.strip() for v in value):
            errors.append("invalid_filter")
            continue
        values = list(dict.fromkeys(v.strip() for v in value))
        if key in _UUID_FILTERS:
            try:
                values = [str(UUID(v)) for v in values]
            except ValueError:
                errors.append("invalid_filter")
                continue
        if key == "spread_pattern" and not set(values) <= _SPREAD_PATTERNS:
            errors.append("invalid_filter")
            continue
        kept[key] = values
    if "keyword" in kept and filters.get("keyword_mode") == "all":
        kept["keyword_mode"] = "all"
    return kept, errors


def validate(data: AlertRuleInput) -> tuple[str, list[AlertConditionInput]]:
    """The trimmed name and the cleaned conditions, or AlertValidationError."""
    errors: list[str] = []
    name = data.name.strip()
    if not name:
        errors.append("name_required")
    elif len(name) > NAME_MAX_LENGTH:
        errors.append("name_too_long")
    if not data.conditions:
        errors.append("conditions_required")
    elif len(data.conditions) > MAX_CONDITIONS:
        errors.append("too_many_conditions")

    conditions: list[AlertConditionInput] = []
    for condition in data.conditions:
        if condition.type not in CONDITION_TYPES:
            errors.append("invalid_type")
            continue
        in_narrative = condition.type == "new_claim_in_narrative"
        if in_narrative and condition.narrative_id is None:
            errors.append("narrative_required")
        if not in_narrative and condition.narrative_id is not None:
            errors.append("narrative_not_allowed")
        filters, filter_errors = clean_filters(condition.type, condition.filters)
        errors.extend(filter_errors)
        # A condition that follows a narrative may have no filters (every new claim of
        # that narrative); the others need at least one.
        if not in_narrative and not filters:
            errors.append("empty_condition")
        conditions.append(
            AlertConditionInput(type=condition.type, narrative_id=condition.narrative_id, filters=filters)
        )

    if errors:
        raise AlertValidationError(list(dict.fromkeys(errors)))
    return name, conditions
