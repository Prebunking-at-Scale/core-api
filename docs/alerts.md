# Alerts

E-mail alerts on new narratives and claims. The design and every decision are in the
frontend's `docs/alerts.md` and `docs/alerts-implementation.md`; this is the backend.

Code: `core/alerts/` (models, `validation.py`, `repo.py` with the matching queries,
`service.py`, `summary.py`), `core/email/messages.py` (`alert_digest_message`),
migrations 26–27. Tests: `tests/alerts/`.

## Model

- An alert (`alert_rules`) belongs to the person who created it, in one organisation,
  and is e-mailed to them only. Their alerts are listed newest first, in the panel and
  in the e-mail; the order can't be changed.
- Its conditions (`alert_rule_conditions`), combined with OR, are `new_narrative`,
  `new_claim` or `new_claim_in_narrative` (with `narrative_id`), each with the search's
  filters (`filters` JSON: `topic_id`, `keyword` + `keyword_mode`, `language`,
  `platform`, `channel`, `entity_id`, `spread_pattern`; claims are never filtered by
  their score; which
  ones each type allows is `ALLOWED_FILTERS`).
- `counting_since` is the starting point: only what appears after it is reported. It's
  reset when the conditions change or the alert is enabled again.
- `alert_rule_reports` holds every element an alert has reported: once only.

## Matching

Each condition runs the search's own query (`core/search/query.py`) on the elements
that appeared after `counting_since` and in the last 7 days:

| Condition | Appeared |
|---|---|
| `new_narrative` | `narratives.created_at` |
| `new_claim` | `video_claims.created_at` |
| `new_claim_in_narrative` | when the claim joined that narrative (`claim_narratives.created_at`, migration 26) |

Links made before migration 26 have no date and never count. `update_narrative` only adds
and removes the links that change, so a link keeps its date.

## The daily e-mail

`uv run python -m core.cli send-alert-digest`, run by the `core-api-process-alerts`
CronJob at 08:00 Europe/Madrid. Per person (active, in an active organisation), one
English e-mail with their triggered alerts, newest first: new narratives, new claims in
each followed narrative, new claims; each element once with the conditions it met; 5 per
section and "See all" to `/alerts/{id}`. Reports are recorded in the same transaction as
the e-mail is sent: if sending fails, the matches go out next time.

## API

`/api/alerts` (`GET`, `POST`), `/api/alerts/{id}` (`GET`, `PUT`, `DELETE`),
`GET /api/alerts/digest-preview`, and `GET /api/alerts/narratives?text=` (narratives to
follow, by title only, for the "Belongs to this narrative" picker). Only your own alerts; anything
else is 404. Invalid input is 422 with the codes in `extra.errors`
(`core/alerts/models.py`, `AlertValidationError`).

`POST /api/narratives/{id}/merge` `{"into": …}` (super admins) moves a narrative's
claims, topics and entities into another, points the conditions that followed it to
the target, and deletes it.

## Migration of today's alerts (27)

Topic and keyword alerts become alerts with one `new_narrative` condition (the topic, or
the keyword, now matched like the search: title or any claim). Threshold alerts are not
carried over. Today's `alerts`, `alerts_triggered` and `alert_executions` are kept until
the new alerts have been checked in production.
