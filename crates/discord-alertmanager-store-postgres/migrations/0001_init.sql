-- Initial schema for the PostgreSQL backend. The SQLite backend carries the same numbers and
-- filenames, so the two are diffable side by side and a test asserts neither dialect is missing a
-- migration the other has.
--
-- Where SQLite stores a timestamp as text, a JSON document as text and a boolean as an integer,
-- this dialect has all three, and the row mapper is correspondingly shorter.

CREATE TABLE alerts (
    fingerprint      TEXT PRIMARY KEY,
    labels_hash      TEXT NOT NULL,
    group_key        TEXT,
    labels           JSONB NOT NULL,
    annotations      JSONB NOT NULL,
    starts_at        TIMESTAMPTZ NOT NULL,
    ends_at          TIMESTAMPTZ,
    generator_url    TEXT,
    status           TEXT NOT NULL,
    am_state         TEXT NOT NULL,
    severity         TEXT NOT NULL,
    silenced_by      JSONB NOT NULL DEFAULT '[]',
    inhibited_by     JSONB NOT NULL DEFAULT '[]',
    first_seen_at    TIMESTAMPTZ NOT NULL,
    last_seen_at     TIMESTAMPTZ NOT NULL,
    resolved_at      TIMESTAMPTZ,
    flap_count       INTEGER NOT NULL DEFAULT 0,
    updated_at       TIMESTAMPTZ NOT NULL
);
CREATE INDEX alerts_status_last_seen ON alerts (status, last_seen_at DESC);
CREATE INDEX alerts_severity          ON alerts (severity);
CREATE INDEX alerts_group_key        ON alerts (group_key);

CREATE TABLE alert_events (
    id            BIGSERIAL PRIMARY KEY,
    fingerprint   TEXT NOT NULL REFERENCES alerts (fingerprint) ON DELETE CASCADE,
    kind          TEXT NOT NULL,
    source        TEXT NOT NULL,
    starts_at     TIMESTAMPTZ NOT NULL,
    ends_at       TIMESTAMPTZ,
    payload       JSONB NOT NULL,
    received_at   TIMESTAMPTZ NOT NULL
);
CREATE UNIQUE INDEX alert_events_dedupe
    ON alert_events (fingerprint, kind, starts_at, COALESCE(ends_at, '1970-01-01T00:00:00Z'));

CREATE TABLE notifications (
    id            BIGSERIAL PRIMARY KEY,
    dedupe_key    TEXT NOT NULL,
    fingerprint   TEXT NOT NULL,
    route_id      BIGINT NOT NULL,
    guild_id      BIGINT NOT NULL,
    channel_id    BIGINT NOT NULL,
    message_id    BIGINT,
    thread_id     BIGINT,
    state         TEXT NOT NULL,
    render_hash   TEXT,
    applied_tags  JSONB NOT NULL DEFAULT '[]',
    tags_hash     TEXT,
    pinned        BOOLEAN NOT NULL DEFAULT FALSE,
    archived      BOOLEAN NOT NULL DEFAULT FALSE,
    responded_at  TIMESTAMPTZ,
    reply_count   INTEGER NOT NULL DEFAULT 0,
    created_at    TIMESTAMPTZ NOT NULL,
    updated_at    TIMESTAMPTZ NOT NULL,
    UNIQUE (channel_id, dedupe_key)
);
CREATE UNIQUE INDEX notifications_message ON notifications (message_id)
    WHERE message_id IS NOT NULL;
CREATE INDEX notifications_thread ON notifications (thread_id);

CREATE TABLE outbox (
    id           BIGSERIAL PRIMARY KEY,
    lane         INTEGER NOT NULL,
    kind         TEXT NOT NULL,
    dedupe_key   TEXT NOT NULL,
    payload      JSONB NOT NULL,
    not_before   TIMESTAMPTZ NOT NULL,
    attempts     INTEGER NOT NULL DEFAULT 0,
    claimed_by   TEXT,
    claimed_at   TIMESTAMPTZ,
    last_error   TEXT,
    created_at   TIMESTAMPTZ NOT NULL
);
CREATE INDEX outbox_claimable ON outbox (lane, not_before, id) WHERE claimed_at IS NULL;

CREATE TABLE acknowledgements (
    id           BIGSERIAL PRIMARY KEY,
    fingerprint  TEXT NOT NULL,
    user_id      BIGINT NOT NULL,
    kind         TEXT NOT NULL,
    note         TEXT,
    created_at   TIMESTAMPTZ NOT NULL,
    revoked_at   TIMESTAMPTZ
);
CREATE UNIQUE INDEX ack_active ON acknowledgements (fingerprint) WHERE revoked_at IS NULL;

CREATE TABLE ignore_rules (
    id             BIGSERIAL PRIMARY KEY,
    scope          TEXT NOT NULL,
    guild_id       BIGINT NOT NULL,
    channel_id     BIGINT,
    matcher_source TEXT NOT NULL,
    reason         TEXT NOT NULL,
    created_by     BIGINT NOT NULL,
    created_at     TIMESTAMPTZ NOT NULL,
    expires_at     TIMESTAMPTZ,
    revoked_at     TIMESTAMPTZ
);
CREATE INDEX ignore_rules_guild ON ignore_rules (guild_id);

CREATE TABLE silences (
    am_id            TEXT PRIMARY KEY,
    matchers         TEXT NOT NULL,
    starts_at        TIMESTAMPTZ NOT NULL,
    ends_at          TIMESTAMPTZ NOT NULL,
    created_by       TEXT NOT NULL,
    discord_user_id  BIGINT,
    origin_message   TEXT,
    comment          TEXT NOT NULL,
    state            TEXT NOT NULL,
    synced_at        TIMESTAMPTZ NOT NULL
);

CREATE TABLE routes (
    id               BIGSERIAL PRIMARY KEY,
    guild_id         BIGINT NOT NULL,
    name             TEXT NOT NULL,
    matcher_source   TEXT NOT NULL,
    min_severity     TEXT,
    target           JSONB NOT NULL,
    group_strategy   TEXT NOT NULL,
    mentions         JSONB NOT NULL,
    priority         INTEGER NOT NULL DEFAULT 100,
    continue_to_next BOOLEAN NOT NULL DEFAULT FALSE,
    source           TEXT NOT NULL,
    enabled          BOOLEAN NOT NULL DEFAULT TRUE,
    created_by       BIGINT,
    created_at       TIMESTAMPTZ NOT NULL,
    UNIQUE (guild_id, name)
);

CREATE TABLE forum_tags (
    channel_id  BIGINT NOT NULL,
    tag_name    TEXT NOT NULL,
    tag_id      BIGINT NOT NULL,
    moderated   BOOLEAN NOT NULL,
    synced_at   TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (channel_id, tag_name)
);

CREATE TABLE subscriptions (
    id             BIGSERIAL PRIMARY KEY,
    user_id        BIGINT NOT NULL,
    matcher_source TEXT NOT NULL,
    min_severity   TEXT,
    created_at     TIMESTAMPTZ NOT NULL
);

CREATE TABLE audit_log (
    id          BIGSERIAL PRIMARY KEY,
    actor       BIGINT,
    guild_id    BIGINT,
    action      TEXT NOT NULL,
    subject     TEXT,
    detail      JSONB NOT NULL,
    result      TEXT NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL
);
CREATE INDEX audit_created ON audit_log (created_at DESC);
