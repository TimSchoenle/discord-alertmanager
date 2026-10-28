-- Orphaned cards hold no pin. The SQLite backend carries the same number and filename, so the two
-- are diffable side by side.

-- Orphaning used to leave the pin flag set. A deleted message holds no pin, and the pin limit
-- counts flagged rows, so a card orphaned before this migration would hold one of its route's
-- slots for as long as the row is kept.
UPDATE notifications SET pinned = FALSE WHERE state = 'orphaned' AND pinned;
