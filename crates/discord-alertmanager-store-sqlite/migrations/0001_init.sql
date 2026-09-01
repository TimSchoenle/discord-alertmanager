-- Initial schema for the SQLite backend, adapted to SQLite's weaker typing: timestamps are TEXT
-- (RFC 3339, fixed six-digit subsecond so lexicographic order matches chronological order), JSON
-- documents are TEXT, booleans are INTEGER 0/1, and every auto-incrementing key is INTEGER
-- PRIMARY KEY AUTOINCREMENT.
--
-- The PostgreSQL backend carries the same numbers and filenames, so the two are diffable side by
-- side and a test asserts neither dialect is missing a migration the other has.

CREATE TABLE alerts (
    fingerprint      TEXT PRIMARY KEY,
    labels_hash      TEXT NOT NULL,
    group_key        TEXT,
    labels           TEXT NOT NULL,
    annotations      TEXT NOT NULL,
    starts_at        TEXT NOT NULL,
    ends_at          TEXT,
    generator_url    TEXT,
    status           TEXT NOT NULL,
    am_state         TEXT NOT NULL,
    severity         TEXT NOT NULL,
    silenced_by      TEXT NOT NULL DEFAULT '[]',
    inhibited_by     TEXT NOT NULL DEFAULT '[]',
    first_seen_at    TEXT NOT NULL,
    last_seen_at     TEXT NOT NULL,
    resolved_at      TEXT,
    flap_count       INTEGER NOT NULL DEFAULT 0,
    updated_at       TEXT NOT NULL
);
CREATE INDEX alerts_status_last_seen ON alerts (status, last_seen_at DESC);
CREATE INDEX alerts_severity          ON alerts (severity);
CREATE INDEX alerts_group_key        ON alerts (group_key);

CREATE TABLE alert_events (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    fingerprint   TEXT NOT NULL REFERENCES alerts (fingerprint) ON DELETE CASCADE,
    kind          TEXT NOT NULL,
    source        TEXT NOT NULL,
    starts_at     TEXT NOT NULL,
    ends_at       TEXT,
    payload       TEXT NOT NULL,
    received_at   TEXT NOT NULL
);
CREATE UNIQUE INDEX alert_events_dedupe
    ON alert_events (fingerprint, kind, starts_at, COALESCE(ends_at, '1970-01-01T00:00:00.000000Z'));

CREATE TABLE notifications (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    dedupe_key    TEXT NOT NULL,
    fingerprint   TEXT NOT NULL,
    route_id      INTEGER NOT NULL,
    guild_id      INTEGER NOT NULL,
    channel_id    INTEGER NOT NULL,
    message_id    INTEGER,
    thread_id     INTEGER,
    state         TEXT NOT NULL,
    render_hash   TEXT,
    applied_tags  TEXT NOT NULL DEFAULT '[]',
    tags_hash     TEXT,
    pinned        INTEGER NOT NULL DEFAULT 0,
    archived      INTEGER NOT NULL DEFAULT 0,
    responded_at  TEXT,
    reply_count   INTEGER NOT NULL DEFAULT 0,
    created_at    TEXT NOT NULL,
    updated_at    TEXT NOT NULL,
    UNIQUE (channel_id, dedupe_key)
);
CREATE UNIQUE INDEX notifications_message ON notifications (message_id)
    WHERE message_id IS NOT NULL;
CREATE INDEX notifications_thread ON notifications (thread_id);

CREATE TABLE outbox (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    lane         INTEGER NOT NULL,
    kind         TEXT NOT NULL,
    dedupe_key   TEXT NOT NULL,
    payload      TEXT NOT NULL,
    not_before   TEXT NOT NULL,
    attempts     INTEGER NOT NULL DEFAULT 0,
    claimed_by   TEXT,
    claimed_at   TEXT,
    last_error   TEXT,
    created_at   TEXT NOT NULL
);
CREATE INDEX outbox_claimable ON outbox (lane, not_before, id) WHERE claimed_at IS NULL;

CREATE TABLE acknowledgements (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    fingerprint  TEXT NOT NULL,
    user_id      INTEGER NOT NULL,
    kind         TEXT NOT NULL,
    note         TEXT,
    created_at   TEXT NOT NULL,
    revoked_at   TEXT
);
CREATE UNIQUE INDEX ack_active ON acknowledgements (fingerprint) WHERE revoked_at IS NULL;

CREATE TABLE ignore_rules (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    scope        TEXT NOT NULL,
    guild_id     INTEGER NOT NULL,
    channel_id   INTEGER,
    matcher_source TEXT NOT NULL,
    reason       TEXT NOT NULL,
    created_by   INTEGER NOT NULL,
    created_at   TEXT NOT NULL,
    expires_at   TEXT,
    revoked_at   TEXT
);
CREATE INDEX ignore_rules_guild ON ignore_rules (guild_id);

CREATE TABLE silences (
    am_id            TEXT PRIMARY KEY,
    matchers         TEXT NOT NULL,
    starts_at        TEXT NOT NULL,
    ends_at          TEXT NOT NULL,
    created_by       TEXT NOT NULL,
    discord_user_id  INTEGER,
    origin_message   TEXT,
    comment          TEXT NOT NULL,
    state            TEXT NOT NULL,
    synced_at        TEXT NOT NULL
);

CREATE TABLE routes (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    guild_id       INTEGER NOT NULL,
    name           TEXT NOT NULL,
    matcher_source TEXT NOT NULL,
    min_severity   TEXT,
    target         TEXT NOT NULL,
    group_strategy TEXT NOT NULL,
    mentions       TEXT NOT NULL,
    priority       INTEGER NOT NULL DEFAULT 100,
    continue_to_next INTEGER NOT NULL DEFAULT 0,
    source         TEXT NOT NULL,
    enabled        INTEGER NOT NULL DEFAULT 1,
    created_by     INTEGER,
    created_at     TEXT NOT NULL,
    UNIQUE (guild_id, name)
);

CREATE TABLE forum_tags (
    channel_id  INTEGER NOT NULL,
    tag_name    TEXT NOT NULL,
    tag_id      INTEGER NOT NULL,
    moderated   INTEGER NOT NULL,
    synced_at   TEXT NOT NULL,
    PRIMARY KEY (channel_id, tag_name)
);

CREATE TABLE subscriptions (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id      INTEGER NOT NULL,
    matcher_source TEXT NOT NULL,
    min_severity TEXT,
    created_at   TEXT NOT NULL
);

CREATE TABLE audit_log (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    actor       INTEGER,
    guild_id    INTEGER,
    action      TEXT NOT NULL,
    subject     TEXT,
    detail      TEXT NOT NULL,
    result      TEXT NOT NULL,
    created_at  TEXT NOT NULL
);
CREATE INDEX audit_created ON audit_log (created_at DESC);
