-- Card-scoped outbox coalescing. The PostgreSQL backend carries the same number and filename, so
-- the two are diffable side by side.

-- The card an effect acts on, denormalised out of `payload` so the coalescing fold can name it in
-- a predicate. Null for the three effects that act on no card — the two silence calls and the
-- administrative notice — and null on every row written before this migration, which makes those
-- rows unfoldable rather than wrongly foldable: an extra edit costs one request, a lost one costs
-- a card that never catches up.
ALTER TABLE outbox ADD COLUMN notification_id INTEGER;

-- The fold's predicate, and the only reason the column exists. Partial on the same terms the fold
-- uses, so a queue whose depth is mostly claimed or card-less rows stays cheap to search.
CREATE INDEX outbox_coalesce ON outbox (kind, notification_id)
    WHERE claimed_at IS NULL AND notification_id IS NOT NULL;
