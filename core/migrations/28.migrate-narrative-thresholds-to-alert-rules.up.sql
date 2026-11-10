BEGIN;
-- Today's threshold alerts that follow one narrative (scope 'specific': its views, claims
-- or videos over a number) become alerts with one "New claim in this narrative"
-- condition and no filters: every claim that joins the narrative from now on. Threshold
-- alerts on every narrative (scope 'general') are still not carried over (see 27).
-- Counting starts now, so nobody gets old matches. Today's tables are left as they are.
CREATE TEMP TABLE carried_narratives ON COMMIT DROP AS
SELECT gen_random_uuid() AS new_id, a.*
FROM alerts a
WHERE a.alert_type IN ('narrative_views', 'narrative_claims_count', 'narrative_videos_count')
  AND a.scope = 'specific'
  AND a.narrative_id IS NOT NULL
  AND length(trim(a.name)) > 0;

INSERT INTO alert_rules (id, organisation_id, user_id, name, enabled, created_at)
SELECT new_id, organisation_id, user_id, left(trim(name), 120), enabled, created_at
FROM carried_narratives;

INSERT INTO alert_rule_conditions (alert_id, position, type, narrative_id, filters)
SELECT new_id, 1, 'new_claim_in_narrative', narrative_id, '{}'::jsonb
FROM carried_narratives;
COMMIT;
