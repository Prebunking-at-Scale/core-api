# Search

One search over narratives, claims and videos with the same filters, for the
frontend's Research page, plus saved selections for its filters. The rules come from
the frontend's `docs/filters.md`; the plan is its `docs/research-implementation.md`.

Code: `core/search/` (rules in `query.py`), `core/saved_selections/`, migrations 24
and 25. Tests: `tests/search/`, `tests/saved_selections/`.

## Endpoints

| Route | Returns |
|---|---|
| `GET /api/search/narratives` | `{data, total, total_capped, page, size}`, newest narrative first |
| `GET /api/search/claims` | same, newest video upload first (claims without a video last) |
| `GET /api/search/videos` | same, newest upload first |
| `GET /api/search/counts` | `{data: {narratives: {total, capped}, claims: {…}, videos: {…}}}` |
| `GET /api/search/channels?text=&platform=&limit=` | channel options |
| `GET /api/search/languages` | claim languages with counts, most common first |
| `GET /api/saved-selections?kind=keyword\|entity_id\|channel` | the organisation's defaults, then your own |
| `POST /api/saved-selections` `{kind, name, values}` | the saved selection |
| `DELETE /api/saved-selections/{id}` | 204 |

Results and counts are global: they don't depend on the organisation.

### Filters

The same query parameters on the three lists and the counts. Values within a filter
are ORed; filters are ANDed.

| Param | |
|---|---|
| `topic_id` (repeated) | a claim's own topics (`claim_topics`, from the narratives service's classifier), or a narrative's (`narrative_topics`) |
| `entity_id` (repeated) | a narrative's entities; claims and videos through their narratives |
| `keyword` (repeated), `keyword_mode=any\|all` | each keyword a phrase, anywhere in claim text, video titles or narrative titles, ignoring case, accents and hyphens; `all`: every keyword in the same text |
| `language` (repeated) | the claim's language |
| `platform`, `channel` (repeated) | the video's; channel exact, ignoring case |
| `start_date`, `end_date` | the video's upload date, inclusive |
| `min_score`, `max_score` | claims only |
| `spread_pattern` (repeated) | narratives only |
| `limit` (1–100, default 12), `offset` | paging |

### Matching

Each attribute is checked where it lives (video: platform, channel, upload date and
title; claim: topic, language, text; narrative: entities, title, topic) and carries
through claim → video → narrative. Filters checked on claims must hold on **the same
claim**.

- **Claims:** every filter holds on the claim, its video, or (entities) one of its
  narratives. `match_source: "narrative"` with `via_entities` when entities only held
  through the narrative and the claim's text doesn't name any of them.
- **Videos:** platform, channel and dates on the video, and one claim meeting the other
  filters. A keyword can match the title instead; `match_source: "claims"` when it only
  matched a claim.
- **Narratives:** one claim such that every filter holds on the narrative or on that
  claim: topic and keywords on either, entities and spread pattern on the narrative,
  the rest on the claim. With a language, platform, channel or date filter, keywords
  must be in that same claim's text, not the title: a narrative about the keyword isn't
  listed for content that doesn't mention it. `match_source: "claims"` when the topic
  or keywords needed the claim.

### Counts

Exact up to 10,000 (`COUNT_CAP`); above that, `total` is 10,000 with `capped: true`
(`total_capped` on the lists), shown as "10,000+". Paging stops there too.

### Options

- **Channels:** without `text`, the organisation's own channels (its channel feeds, for
  the "Ours" switch); with `text`, collected channels whose name contains it, the
  organisation's own first, then by number of videos. The frontend searches from 3
  characters: shorter texts can't use the trigram index.
- **Languages:** read from every claim, cached for an hour per process.

### Saved selections

- A person's selections are visible to that person only, within their organisation.
- The organisation's defaults are built from its non-archived feeds on each request:
  `<short_name>-channels` (`id: "default-channels"`) and one `<short_name>-<Topic>` per
  keyword feed (`id: "default-topic-<topic_id>"`). Deleting one gives 403; they change
  with the feeds.
- `POST` errors are 422 with `detail` `invalid_kind`, `name_required`, `name_too_long`
  (over 60), `values_required` or `name_taken` (per person and kind, ignoring case,
  defaults included). Values are trimmed and deduplicated.

## Narrative list dates

`GET /api/narratives` has two date filters. `start_date`/`end_date` (from the date-filter
branch) are about when the narrative's videos were posted: a narrative is in range if
any of them was. `created_start`/`created_end` are about when the narrative was created
(`n.created_at`), which is what `start_date`/`end_date` meant before. The frontend's
Narratives list uses the former (its Date Range); the dashboard's timeframe uses the
latter.

## Deploying

The indexes of migrations 23 and 25 are large in production (4M claims, 1.1M videos
in September 2026), and a plain `CREATE INDEX` blocks writes while it builds. Before
deploying this version:

```sh
psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -f scripts/search-indexes-concurrently.sql
```

It creates the extensions and `normalize_text()` and builds every index with
`CREATE INDEX CONCURRENTLY`; it can be run again safely. At startup, migrations 23–25
then find everything in place. The file's last query lists indexes left invalid by a
failed build (it should return nothing).

## Known limits

- Claims have no entities of their own in practice (`claim_entities` is empty), so
  entity filters on claims and videos go through narratives, which hold 2–9% of claims.
- A video collected for several organisations gives one claim per organisation.
- Keywords under three characters can't use the trigram index and read every row.
- Claim topics come from `claim_topics`, which the narratives service writes only for
  the claims it keeps (score of about 2.5 or more): roughly 4–9% of claims in September
  2026. A topic filter on claims finds those; `metadata.topics` (the claim finder's
  guess from the feeds' keywords) is not used.
