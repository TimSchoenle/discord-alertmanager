-- Cards keyed by identity and re-armed in place. The PostgreSQL backend carries the same number
-- and filename, so the two are diffable side by side.

-- When a card last moved to resolved, while it still is. A re-fire is measured against it: inside
-- the regroup window the card is re-armed, past it a new card replaces this one. A card resolved
-- before this migration takes its last write as the nearest record of when.
ALTER TABLE notifications ADD COLUMN resolved_at TEXT;
UPDATE notifications SET resolved_at = updated_at WHERE state = 'resolved';

-- A per-alert key no longer carries a firing episode. It names the card an alert is shown on now,
-- and a replaced card hands its key to the replacement. Every card but the newest for one alert in
-- one channel retires its key first, and the newest then drops its episode suffix, so the unique
-- index on (channel_id, dedupe_key) holds after each statement. The newest card is never renamed,
-- which is what makes the first statement independent of the order SQLite visits rows in.
UPDATE notifications
SET dedupe_key = 'superseded:' || id || ':' || dedupe_key
WHERE dedupe_key LIKE 'a:%'
  AND EXISTS (
      SELECT 1 FROM notifications AS newer
      WHERE newer.channel_id = notifications.channel_id
        AND newer.dedupe_key LIKE 'a:%'
        AND newer.id > notifications.id
        AND (CASE WHEN instr(newer.dedupe_key, '#') > 0
                  THEN substr(newer.dedupe_key, 1, instr(newer.dedupe_key, '#') - 1)
                  ELSE newer.dedupe_key END)
          = (CASE WHEN instr(notifications.dedupe_key, '#') > 0
                  THEN substr(notifications.dedupe_key, 1, instr(notifications.dedupe_key, '#') - 1)
                  ELSE notifications.dedupe_key END)
  );

UPDATE notifications SET dedupe_key = substr(dedupe_key, 1, instr(dedupe_key, '#') - 1)
WHERE dedupe_key LIKE 'a:%#%';

-- The episode lived on the alert only to give a new card a key of its own. The card now decides
-- that from its own resolution time, so the column has nothing left to record.
ALTER TABLE alerts DROP COLUMN episode;

-- The read behind a resolve on a route whose cards merge fingerprints: which alerts under this
-- name are still firing. Partial on the same terms the read uses, so it covers the firing set and
-- not the history it sits in.
CREATE INDEX alerts_firing_by_name ON alerts (json_extract(labels, '$.alertname'))
    WHERE status = 'firing';
