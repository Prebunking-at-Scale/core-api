BEGIN;

-- "Alert level" is renamed to "spread pattern" throughout.
--
-- The old name described the consumer (someone gets alerted) rather than the thing
-- being measured (how a narrative has spread), and it collided with the unrelated
-- `alerts` feature -- alerts, alert_executions, alerts_triggered, AlertService -- which
-- is about e-mail and Slack notifications and is untouched by this rename.
--
-- "Pattern" rather than "level" because the four labels are regions of a plane, not
-- rungs of a ladder: `consolidated` is not a milder `viral`, it is a different shape of
-- spread. "Level" invited a severity reading the taxonomy does not have.
--
-- Pure renames: no data is rewritten and the enum's member values (none, viral,
-- early_surge, alert, watch, trending, consolidated) are unchanged. Note that `alert`
-- survives as a *member* of the type; it is a retired classification kept so that a
-- consumer still filtering on it gets an empty result rather than a 400. That member
-- has nothing to do with the type's old name.
--
-- Breaking for API consumers: the column backs the `spread_pattern` field and the
-- ?spread_pattern= query parameter from this version on.

ALTER TYPE narrative_alert_level RENAME TO narrative_spread_pattern;

ALTER TABLE narratives RENAME COLUMN alert_level TO spread_pattern;

COMMIT;
