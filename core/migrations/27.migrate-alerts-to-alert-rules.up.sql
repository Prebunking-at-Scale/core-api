BEGIN;

-- Today's topic and keyword alerts become alerts with one New narrative condition:
-- the topic, or the keyword (matched as in the search: a narrative's title or any of
-- its claims). Threshold alerts (views, claims or videos) are not carried over.
-- Counting starts now, so nobody gets old matches. Today's tables are left as they are.

CREATE TEMP TABLE carried ON COMMIT DROP AS
SELECT
    gen_random_uuid() AS new_id,
    a.*,
    row_number() OVER (PARTITION BY a.user_id, a.organisation_id ORDER BY a.created_at, a.id) AS pos
FROM alerts a
WHERE a.alert_type IN ('narrative_with_topic', 'keyword');

INSERT INTO alert_rules (id, organisation_id, user_id, name, enabled, position, created_at)
SELECT new_id, organisation_id, user_id, left(trim(name), 120), enabled, pos, created_at
FROM carried
WHERE length(trim(name)) > 0;

INSERT INTO alert_rule_conditions (alert_id, position, type, filters)
SELECT
    new_id,
    1,
    'new_narrative',
    CASE alert_type
        WHEN 'narrative_with_topic' THEN jsonb_build_object('topic_id', jsonb_build_array(topic_id::text))
        ELSE jsonb_build_object('keyword', jsonb_build_array(keyword))
    END
FROM carried
WHERE length(trim(name)) > 0;

COMMIT;
