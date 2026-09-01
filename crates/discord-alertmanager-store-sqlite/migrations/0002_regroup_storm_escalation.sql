-- Regrouping, storm digests and escalation. The PostgreSQL backend carries the same number and
-- filename, so the two are diffable side by side.

-- Which firing episode the alert is in. A fingerprint that resolves and fires again inside the
-- regroup window stays in its episode and counts a flap; one that fires again long afterwards
-- starts a new episode, and the episode is what puts the new card under a key of its own instead
-- of reviving a card nobody has looked at since last week.
ALTER TABLE alerts ADD COLUMN episode INTEGER NOT NULL DEFAULT 0;

-- The card this one replaced, so the new episode's card can link back to the one before it.
-- Nullable and unconstrained on purpose: the row it names may be pruned long before this one is,
-- and losing the link is a card without a back-reference rather than a card that cannot be drawn.
ALTER TABLE notifications ADD COLUMN supersedes INTEGER;

-- When the escalation sweep last mentioned somebody about this card. Null means it has not, which
-- is what makes the sweep's claim idempotent: two sweeps racing on one card produce one mention.
ALTER TABLE notifications ADD COLUMN escalated_at TEXT;

-- The route's escalation policy, as written. Null on a route that does not escalate, which is
-- every route until somebody configures one.
ALTER TABLE routes ADD COLUMN escalation TEXT;

-- The escalation sweep reads only cards it has not escalated yet, which on a healthy deployment
-- is a small and shrinking set inside a table that is neither.
CREATE INDEX notifications_pending_escalation ON notifications (created_at)
    WHERE escalated_at IS NULL;
