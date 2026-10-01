"""The search's matching rules as SQL (docs/search.md).

Each attribute has one home: channel, platform and upload date on the video; topic,
language and text on the claim; entities on the narrative. A filter checked on claims
must hold on the same claim as every other claim-level filter, which is why each tab
puts them all inside one EXISTS over a claim and its video.

Only fixed SQL fragments are assembled here; every value goes through a parameter.
"""

from dataclasses import dataclass, field
from typing import Any

from core.search.models import SearchFilters


def _like_escape(value: str) -> str:
    """A keyword is matched literally: % and _ are not wildcards."""
    return value.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")


@dataclass
class Query:
    """A WHERE clause under construction, with its parameters."""

    filters: SearchFilters
    params: dict[str, Any] = field(default_factory=dict)
    _seq: int = 0
    # Placeholders reused wherever the same values appear more than once
    _keyword_params: list[str] | None = None
    _entity_placeholder: str | None = None

    def param(self, value: Any) -> str:
        self._seq += 1
        name = f"p{self._seq}"
        self.params[name] = value
        return f"%({name})s"

    # Keywords -----------------------------------------------------------------

    def keywords_in(self, text_sql: str) -> str | None:
        """Keywords in one text, ignoring case, accents and hyphens: any of them, or
        with keyword_mode "all" every one of them in that same text."""
        if not self.filters.keywords:
            return None
        if self._keyword_params is None:
            self._keyword_params = [
                self.param(_like_escape(k)) for k in self.filters.keywords
            ]
        parts = [
            f"normalize_text({text_sql}) LIKE ('%%' || normalize_text({p}) || '%%')"
            for p in self._keyword_params
        ]
        joiner = " AND " if self.filters.keyword_mode == "all" else " OR "
        return "(" + joiner.join(parts) + ")"

    # Claim level ---------------------------------------------------------------

    def claim_topic(self, c: str) -> str | None:
        """The claim's own topics: those the narratives service's classifier assigned
        (claim_topics), not the claim finder's metadata.topics."""
        if not self.filters.topic_ids:
            return None
        return (
            f"EXISTS (SELECT 1 FROM claim_topics ct WHERE ct.claim_id = {c}.id"
            f" AND ct.topic_id = ANY({self.param(self.filters.topic_ids)}))"
        )

    def claim_language(self, c: str) -> str | None:
        if not self.filters.languages:
            return None
        return f"{c}.metadata->>'language' = ANY({self.param(self.filters.languages)})"

    def claim_score(self, c: str) -> list[str]:
        out = []
        if self.filters.min_score is not None:
            out.append(f"({c}.metadata->>'score')::float >= {self.param(self.filters.min_score)}")
        if self.filters.max_score is not None:
            out.append(f"({c}.metadata->>'score')::float <= {self.param(self.filters.max_score)}")
        return out

    def claim_entity(self, c: str) -> str | None:
        """Entities live on narratives: a claim has one when one of its narratives does."""
        if not self.filters.entity_ids:
            return None
        return (
            "EXISTS (SELECT 1 FROM claim_narratives ecn"
            " JOIN narrative_entities ene ON ene.narrative_id = ecn.narrative_id"
            f" WHERE ecn.claim_id = {c}.id"
            f" AND ene.entity_id = ANY({self._entity_param()}))"
        )

    # Video level ---------------------------------------------------------------

    def video_conditions(self, v: str) -> list[str]:
        out = []
        if self.filters.platforms:
            out.append(f"{v}.platform = ANY({self.param(self.filters.platforms)})")
        if self.filters.channels:
            lowered = [c.lower() for c in self.filters.channels]
            out.append(f"lower({v}.channel) = ANY({self.param(lowered)})")
        if self.filters.start_date:
            out.append(f"{v}.uploaded_at >= {self.param(self.filters.start_date)}")
        if self.filters.end_date:
            out.append(f"{v}.uploaded_at <= {self.param(self.filters.end_date)}")
        return out

    # Narrative level -----------------------------------------------------------

    def narrative_topic(self, n: str) -> str | None:
        if not self.filters.topic_ids:
            return None
        return (
            "EXISTS (SELECT 1 FROM narrative_topics nt"
            f" WHERE nt.narrative_id = {n}.id"
            f" AND nt.topic_id = ANY({self.param(self.filters.topic_ids)}))"
        )

    def narrative_entity(self, n: str) -> str | None:
        if not self.filters.entity_ids:
            return None
        return (
            "EXISTS (SELECT 1 FROM narrative_entities ne"
            f" WHERE ne.narrative_id = {n}.id"
            f" AND ne.entity_id = ANY({self._entity_param()}))"
        )

    def _entity_param(self) -> str:
        if self._entity_placeholder is None:
            self._entity_placeholder = self.param(self.filters.entity_ids)
        return self._entity_placeholder


def _and(parts: list[str | None]) -> str:
    kept = [p for p in parts if p]
    return " AND ".join(kept) if kept else "TRUE"


@dataclass
class TabQuery:
    """FROM and WHERE for one tab, with how each row matched and its order."""

    from_sql: str
    where_sql: str
    match_source_sql: str
    order_sql: str
    params: dict[str, Any]


def claims_query(filters: SearchFilters) -> TabQuery:
    """A claim matches when every filter holds on it, its video, or (entities) its
    narrative. Newest upload first; claims without a video last."""
    q = Query(filters)
    where = _and(
        [
            q.claim_topic("c"),
            q.keywords_in("c.claim"),
            q.claim_language("c"),
            *q.video_conditions("v"),
            *q.claim_score("c"),
            q.claim_entity("c"),
        ]
    )
    return TabQuery(
        from_sql="video_claims c LEFT JOIN videos v ON v.id = c.video_id",
        where_sql=where,
        # Refined per row once the page is known: see SearchRepository.
        match_source_sql="'direct'",
        order_sql="v.uploaded_at DESC NULLS LAST, v.id DESC, c.id",
        params=q.params,
    )


def videos_query(filters: SearchFilters) -> TabQuery:
    """A video matches its own filters (platform, channel, date) and has one claim
    meeting every claim-level filter. A keyword can match the title instead of that
    claim, and then the video needs a claim only for the other claim-level filters."""
    q = Query(filters)
    others = [
        p
        for p in [q.claim_topic("c"), q.claim_language("c"), q.claim_entity("c")]
        if p
    ]

    def a_claim(conditions: list[str]) -> str:
        return (
            "EXISTS (SELECT 1 FROM video_claims c WHERE c.video_id = v.id AND "
            + _and(list(conditions))
            + ")"
        )

    title = q.keywords_in("v.title")
    match_source = "'direct'"
    if title:
        claim_text = q.keywords_in("c.claim")
        assert claim_text
        with_title = f"({title} AND {a_claim(others) if others else 'TRUE'})"
        claim_part = f"({with_title} OR {a_claim([*others, claim_text])})"
        match_source = f"CASE WHEN {title} THEN 'direct' ELSE 'claims' END"
    elif others:
        claim_part = a_claim(others)
    else:
        claim_part = None

    return TabQuery(
        from_sql="videos v",
        where_sql=_and([*q.video_conditions("v"), claim_part]),
        match_source_sql=match_source,
        order_sql="v.uploaded_at DESC NULLS LAST, v.id DESC",
        params=q.params,
    )


def narratives_query(filters: SearchFilters) -> TabQuery:
    """A narrative matches when it has one claim such that every filter holds on the
    narrative or on that claim: topic and keywords on either, entities and spread
    pattern on the narrative, language, platform, channel and dates on the claim (and
    its video). Newest first."""
    q = Query(filters)
    own_topic = q.narrative_topic("n")
    own_title = q.keywords_in("n.title")

    per_claim: list[str | None] = [
        f"({own_topic} OR {q.claim_topic('c')})" if own_topic else None,
        f"({own_title} OR {q.keywords_in('c.claim')})" if own_title else None,
        q.claim_language("c"),
        *q.video_conditions("v"),
    ]
    a_claim = None
    if any(per_claim):
        a_claim = (
            "EXISTS (SELECT 1 FROM claim_narratives cn"
            " JOIN video_claims c ON c.id = cn.claim_id"
            " LEFT JOIN videos v ON v.id = c.video_id"
            " WHERE cn.narrative_id = n.id AND " + _and(per_claim) + ")"
        )

    spread = None
    if filters.spread_patterns:
        spread = f"n.spread_pattern = ANY({q.param([s.value for s in filters.spread_patterns])})"

    # "claims" when the topic or the keywords needed a claim; entities are always the
    # narrative's own, and the claim-only filters don't count (docs/search.md).
    needed_claim = [f"NOT {x}" for x in (own_topic, own_title) if x]
    match_source = (
        f"CASE WHEN {' OR '.join(needed_claim)} THEN 'claims' ELSE 'direct' END"
        if needed_claim
        else "'direct'"
    )

    return TabQuery(
        from_sql="narratives n",
        where_sql=_and([spread, q.narrative_entity("n"), a_claim]),
        match_source_sql=match_source,
        order_sql="n.created_at DESC, n.id DESC",
        params=q.params,
    )


QUERIES = {
    "narratives": narratives_query,
    "claims": claims_query,
    "videos": videos_query,
}
