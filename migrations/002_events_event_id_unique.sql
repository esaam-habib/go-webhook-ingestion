-- The events table only had a non-unique index on event_id, so concurrent or
-- redelivered webhooks with the same event_id could both insert successfully,
-- causing duplicate call records and inflated account_stats. A UNIQUE
-- constraint makes InsertEvent's ON CONFLICT (event_id) DO NOTHING atomic.
DROP INDEX IF EXISTS idx_events_event_id;
ALTER TABLE events ADD CONSTRAINT events_event_id_key UNIQUE (event_id);
