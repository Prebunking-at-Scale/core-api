"""The alerts of the frontend's docs/alerts.md: an alert belongs to the person who
created it, is e-mailed to them only, and has conditions combined with OR. Each
condition watches new narratives, new claims, or new claims in one narrative, with the
search's filters."""

from datetime import datetime
from typing import Any, Literal
from uuid import UUID

from litestar.exceptions import HTTPException
from pydantic import BaseModel

ConditionType = Literal["new_narrative", "new_claim", "new_claim_in_narrative"]
CONDITION_TYPES: tuple[ConditionType, ...] = ("new_narrative", "new_claim", "new_claim_in_narrative")

NAME_MAX_LENGTH = 120
MAX_CONDITIONS = 20

LIST_FILTERS = ("topic_id", "keyword", "language", "platform", "channel", "entity_id", "spread_pattern")
_CLAIM_FILTERS = ("topic_id", "keyword", "language", "platform", "channel", "min_score", "max_score")

# Which filters each type of condition may use (docs/alerts.md, "Filters available
# per type"), as in the frontend's utils/alertRules.ts.
ALLOWED_FILTERS: dict[str, tuple[str, ...]] = {
    "new_narrative": ("topic_id", "keyword", "language", "platform", "channel", "entity_id", "spread_pattern"),
    "new_claim": (*_CLAIM_FILTERS, "entity_id"),
    "new_claim_in_narrative": _CLAIM_FILTERS,
}


class AlertCondition(BaseModel):
    id: UUID
    type: ConditionType
    narrative_id: UUID | None = None
    filters: dict[str, Any] = {}


class AlertRule(BaseModel):
    id: UUID
    name: str
    enabled: bool
    position: int
    conditions: list[AlertCondition]
    created_at: datetime
    # When it last reported something, for the list
    last_match_at: datetime | None = None


class AlertConditionInput(BaseModel):
    type: str
    narrative_id: UUID | None = None
    filters: dict[str, Any] = {}


class AlertRuleInput(BaseModel):
    name: str
    enabled: bool = True
    conditions: list[AlertConditionInput]


class AlertOrderInput(BaseModel):
    ids: list[UUID]


class AlertValidationError(HTTPException):
    """422 with the codes of utils/alertRules.ts in `extra.errors`: name_required,
    name_too_long, conditions_required, too_many_conditions, invalid_type,
    narrative_required, narrative_not_allowed, empty_condition, filter_not_allowed,
    invalid_filter."""

    status_code = 422

    def __init__(self, errors: list[str]) -> None:
        super().__init__(detail=errors[0], extra={"errors": errors})


# The daily e-mail ------------------------------------------------------------------


class DigestItem(BaseModel):
    id: UUID
    title: str
    conditions: list[int]


class DigestSection(BaseModel):
    items: list[DigestItem] = []
    total: int = 0


class DigestNarrativeGroup(DigestSection):
    narrative_id: UUID
    narrative_title: str


class DigestEntry(BaseModel):
    """One triggered alert: new narratives, new claims in each followed narrative,
    then the other new claims. Up to DIGEST_SECTION_LIMIT items per section."""

    alert_id: UUID
    alert_name: str
    narratives: DigestSection = DigestSection()
    in_narratives: list[DigestNarrativeGroup] = []
    claims: DigestSection = DigestSection()

    @property
    def total(self) -> int:
        return self.narratives.total + self.claims.total + sum(g.total for g in self.in_narratives)


DIGEST_SECTION_LIMIT = 5
