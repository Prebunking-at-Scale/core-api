"""Models for the entity knowledge-graph proxy endpoints.

These mirror the response shape of the prebunking-narratives ``/graph/*``
endpoints. The backend does not own this data — it lives in Neo4j and is
queried by the narratives service — but re-declaring the shape here keeps
the contract typed and makes it visible in the OpenAPI schema the frontend
consumes.
"""

from pydantic import BaseModel


class GraphNode(BaseModel):
    """One entity node of the knowledge graph."""

    name: str
    labels: list[str] = []
    primary_label: str
    wikidata_id: str | None = None
    wikidata_label: str | None = None
    wikidata_description: str | None = None
    narrative_count: int
    depth: int


class GraphEdge(BaseModel):
    """A directed, typed relationship between two entities."""

    source: str
    target: str
    type: str


class EntityGraph(BaseModel):
    """The typed knowledge-graph neighborhood around one entity."""

    center: str
    depth: int
    nodes: list[GraphNode]
    edges: list[GraphEdge]


class EntitySearchResult(BaseModel):
    """One entity match for the center-entity picker / autocomplete."""

    name: str
    primary_label: str
    narrative_count: int
