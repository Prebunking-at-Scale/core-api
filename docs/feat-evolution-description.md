# Feature: Evolution Description for Narratives

## Summary

Added a new optional field `evolution_description` to narratives. This field stores a detailed text description of the narrative's classification process and evolution over time. It is designed to be synced with the external narratives API and displayed in the frontend.

## Motivation

The classification process of a narrative involves multiple stages. Until now, there was no way to record how a narrative evolved through these stages. The `evolution_description` field provides a persistent, append-only log of this process with timestamped entries.

## Design Decisions

| Decision | Choice | Rationale |
|---|---|---|
| Storage | Dedicated `TEXT` column | The field has its own merge logic, syncs with external API, and will be displayed in the frontend — too structured for `metadata` JSONB |
| Size limit | None (`TEXT`) | No practical limit needed |
| Presence in listings | Detail only | Large text field — excluded from `NarrativeSummary`, viral, and prevalent endpoints to keep list responses lightweight |
| Merge strategy | Concatenation with timestamp | On deduplication/merge, new content is appended with `\n\n--{ISO timestamp}--\n\n` separator |
| External sync | Yes | Synced via `PATCH /narrative/{id}` to the external narratives API |
| Text search | No | Not included in the existing `text` filter that searches `title` and `description` |

## Changes

### Database

**New migration:** `18.add-evolution-description-to-narratives.up.sql`

```sql
ALTER TABLE narratives ADD COLUMN evolution_description TEXT;
```

Nullable, no default. Instant operation in PostgreSQL — no table rewrite, no downtime.

### Files Modified

#### `core/models.py`
- Added `evolution_description: str | None = None` to the `Narrative` base model.

#### `core/narratives/models.py`
- `NarrativeInput`: added optional `evolution_description` field for creation.
- `NarrativePatchInput`: added optional `evolution_description` field for updates.
- `NarrativeDetail`: added `evolution_description` field (returned in detail views).
- `NarrativeSummary`: **unchanged** — field intentionally excluded from list views.

#### `core/narratives/repo.py`
- `create_narrative()`: accepts and inserts `evolution_description` into the `INSERT` statement.
- `update_narrative()`: accepts `evolution_description` and includes it in dynamic `SET` clauses.
- `get_narrative_detail()`: selects `evolution_description` from the `narrative_base` CTE and maps it to `NarrativeDetail`.

#### `core/narratives/service.py`
- Added `_merge_evolution_description(existing, new)` helper function that concatenates descriptions with a timestamped separator: `\n\n--{ISO 8601 timestamp}--\n\n`.
- `create_narrative()`: passes `evolution_description` to the repo. On deduplication merge, concatenates with existing description.
- `update_narrative()`: concatenates new `evolution_description` with existing one via `_merge_evolution_description()`. Syncs to external API when the field is updated.
- `_sync_external_narrative()`: now accepts and forwards `evolution_description` to the external API.

#### `core/narratives/api.py`
- `update_narrative_title()` refactored to delegate to a new generic `update_narrative()` method.
- New `update_narrative(external_narrative_id, title, evolution_description)`: sends a `PATCH` request with any combination of `title` and/or `evolution_description` to the external narratives API.

#### `core/app.py`
- `MIGRATION_TARGET_VERSION` updated from `17` to `18`.

### Files Created

- `core/migrations/18.add-evolution-description-to-narratives.up.sql`

## API Contract

### POST /api/narratives/

Request body now accepts:

```json
{
  "title": "string (required)",
  "description": "string (required)",
  "evolution_description": "string (optional)",
  "claim_ids": [],
  "topic_ids": [],
  "entities": [],
  "metadata": {}
}
```

### PATCH /api/narratives/{id}

Request body now accepts:

```json
{
  "evolution_description": "string (optional)"
}
```

When provided, the new value is **concatenated** to the existing one:

```
Existing description

--2026-03-23T15:52:00.123456--

New description
```

### GET /api/narratives/{id}

Response includes `evolution_description` in the `NarrativeDetail` schema.

### GET /api/narratives/ (list), /viral, /prevalent

Response does **not** include `evolution_description`. These use `NarrativeSummary`.

## External API Sync

When `evolution_description` is created or updated, and the narrative has an `narrative_id` in its metadata (external system reference), the field is synced via:

```
PATCH {NARRATIVES_BASE_URL}/narrative/{external_id}
{
  "evolution_description": "..."
}
```

The external API does not yet accept this field — it needs to be implemented on the consuming repository.

## Merge / Deduplication Behavior

When a narrative is created with a title or `metadata.narrative_id` that matches an existing narrative:

1. `claim_ids`, `topic_ids`, `entity_ids` are merged (union of sets).
2. `evolution_description` is **concatenated** with a timestamped separator.
3. `title`, `description`, and `metadata` are overwritten with the new values.
