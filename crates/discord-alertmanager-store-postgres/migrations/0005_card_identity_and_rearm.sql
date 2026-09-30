-- Cards keyed by identity and re-armed in place. The SQLite backend carries the same number and
-- filename, so the two are diffable side by side.

-- When a card last moved to resolved, while it still is. A re-fire is measured against it: inside
-- the regroup window the card is re-armed, past it a new card replaces this one. A card resolved
-- before this migration takes its last write as the nearest record of when.
ALTER TABLE notifications ADD COLUMN resolved_at TIMESTAMPTZ;
UPDATE notifications SET resolved_at = updated_at WHERE state = 'resolved';

-- A per-alert key no longer carries a firing episode. It names the card an alert is shown on now,
-- and a replaced card hands its key to the replacement. Every card but the newest for one alert in
-- one channel retires its key first, and the newest then drops its episode suffix, so the unique
-- index on (channel_id, dedupe_key) holds after each statement.
UPDATE notifications AS older
SET dedupe_key = 'superseded:' || older.id || ':' || older.dedupe_key
WHERE older.dedupe_key LIKE 'a:%'
  AND EXISTS (
      SELECT 1 FROM notifications AS newer
      WHERE newer.channel_id = older.channel_id
        AND newer.dedupe_key LIKE 'a:%'
        AND split_part(newer.dedupe_key, '#', 1) = split_part(older.dedupe_key, '#', 1)
        AND newer.id > older.id
  );

UPDATE notifications SET dedupe_key = split_part(dedupe_key, '#', 1)
WHERE dedupe_key LIKE 'a:%#%';

-- The episode lived on the alert only to give a new card a key of its own. The card now decides
-- that from its own resolution time, so the column has nothing left to record.
ALTER TABLE alerts DROP COLUMN episode;

-- The read behind a resolve on a route whose cards merge fingerprints: which alerts under this
-- name are still firing. Partial on the same terms the read uses, so it covers the firing set and
-- not the history it sits in.
CREATE INDEX alerts_firing_by_name ON alerts ((labels ->> 'alertname'))
    WHERE status = 'firing';
