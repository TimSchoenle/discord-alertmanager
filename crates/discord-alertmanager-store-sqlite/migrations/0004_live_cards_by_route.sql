-- The index behind `/cards resync`. The PostgreSQL backend carries the same number and filename,
-- so the two are diffable side by side.

-- A resync enumerates the cards of a handful of routes that still describe something happening,
-- inside a table that also holds every card those routes ever posted. Partial on the same terms
-- the read uses, so the index covers the live set and not the history it sits in: on a deployment
-- that keeps a year of cards, the live set is the small part.
CREATE INDEX notifications_live_by_route ON notifications (route_id, created_at)
    WHERE message_id IS NOT NULL AND state NOT IN ('resolved', 'orphaned');
