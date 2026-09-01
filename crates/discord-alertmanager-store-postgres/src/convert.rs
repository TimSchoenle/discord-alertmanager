//! The one place a `PostgreSQL` row becomes a domain value.
//!
//! Shorter than the `SQLite` counterpart, and for one reason: this dialect has a timestamp type, a
//! JSON type and a boolean, so the three encodings that module performs by hand are the driver's
//! job here. What is left is the same work either way — turning a stored discriminant back into an
//! enum, compiling a matcher expression, and giving a decode failure a name.

use chrono::{DateTime, Utc};
use dam_core::{
    Alert, AlertStatus, AmState, Annotations, DedupeKey, Fingerprint, GroupKey, Labels, LabelsHash,
    MatcherSet, NotificationState, Severity,
};
use dam_store::{
    AckKind, Acknowledgement, AlertRecord, ChannelId, Effect, ForumTag, GuildId, IgnoreId,
    IgnoreRule, IgnoreScope, Mentions, MessageId, Notification, NotificationId, OutboxId,
    OutboxItem, Route, RouteId, RouteSource, RouteTarget, SilenceLifecycle, SilenceLink,
    StoreError, Subscription, SubscriptionId, TagId, UserId, WorkerId,
};
use serde::de::DeserializeOwned;
use sqlx::Row;
use sqlx::postgres::PgRow;

/// Reads a JSON column.
fn json_at<T: DeserializeOwned>(row: &PgRow, column: &'static str) -> Result<T, StoreError> {
    let raw: serde_json::Value = row.try_get(column).map_err(backend)?;

    serde_json::from_value(raw).map_err(|error| StoreError::Decode {
        kind: column,
        detail: error.to_string(),
    })
}

/// Reads a nullable JSON column, where SQL null and a JSON null mean the same thing.
fn json_opt_at<T: DeserializeOwned>(
    row: &PgRow,
    column: &'static str,
) -> Result<Option<T>, StoreError> {
    let raw: Option<serde_json::Value> = row.try_get(column).map_err(backend)?;
    let Some(raw) = raw.filter(|value| !value.is_null()) else {
        return Ok(None);
    };

    serde_json::from_value(raw).map_err(|error| StoreError::Decode {
        kind: column,
        detail: error.to_string(),
    })
}

/// Reads a column holding one of the domain's stored discriminants.
fn parsed_at<T>(row: &PgRow, column: &'static str) -> Result<T, StoreError>
where
    T: std::str::FromStr,
    T::Err: std::fmt::Display,
{
    let raw: String = row.try_get(column).map_err(backend)?;

    raw.parse().map_err(|error: T::Err| StoreError::Decode {
        kind: column,
        detail: error.to_string(),
    })
}

/// Reads a nullable snowflake column into the newtype `wrap` produces.
fn id_opt_at<T>(
    row: &PgRow,
    column: &'static str,
    wrap: fn(i64) -> T,
) -> Result<Option<T>, StoreError> {
    let raw: Option<i64> = row.try_get(column).map_err(backend)?;

    Ok(raw.map(wrap))
}

/// Reads a count column that the domain holds as a `u32`.
///
/// A negative count is not representable and is clamped rather than refused: the column is a
/// counter the database maintains, and failing the read of a whole card because a reply count went
/// strange would hide the card as well as the counter.
fn count_at(row: &PgRow, column: &'static str) -> Result<u32, StoreError> {
    let raw: i32 = row.try_get(column).map_err(backend)?;

    Ok(u32::try_from(raw).unwrap_or(0))
}

/// Maps a driver failure onto the store's vocabulary.
///
/// A unique violation is separated out because it is the expected outcome of two workers racing to
/// create one card, and the caller's response to it — re-read and edit the winner's row — is
/// nothing like its response to an unreachable database.
#[expect(
    clippy::needless_pass_by_value,
    reason = "used as a function item in `map_err`, which hands over an owned error; taking a               reference would put a closure at every one of its call sites"
)]
pub(crate) fn backend(error: sqlx::Error) -> StoreError {
    if let sqlx::Error::Database(ref db) = error
        && db.is_unique_violation()
    {
        return StoreError::Conflict {
            // This dialect names the index it rejected the write on, which is the most useful
            // thing a log can carry about a race.
            constraint: db.constraint().unwrap_or(db.message()).to_owned(),
        };
    }

    StoreError::Backend {
        detail: error.to_string(),
    }
}

/// Builds an alert and its ledger fields from an `alerts` row.
pub(crate) fn alert_record(row: &PgRow) -> Result<AlertRecord, StoreError> {
    let fingerprint: String = row.try_get("fingerprint").map_err(backend)?;
    let group_key: Option<String> = row.try_get("group_key").map_err(backend)?;
    let labels_hash: String = row.try_get("labels_hash").map_err(backend)?;

    let alert = Alert {
        fingerprint: Fingerprint::new(fingerprint).map_err(|error| StoreError::Decode {
            kind: "fingerprint",
            detail: error.to_string(),
        })?,
        labels: json_at::<Labels>(row, "labels")?,
        annotations: json_at::<Annotations>(row, "annotations")?,
        starts_at: row.try_get("starts_at").map_err(backend)?,
        ends_at: row.try_get("ends_at").map_err(backend)?,
        generator_url: row.try_get("generator_url").map_err(backend)?,
        status: parsed_at::<AlertStatus>(row, "status")?,
        am_state: parsed_at::<AmState>(row, "am_state")?,
        silenced_by: json_at(row, "silenced_by")?,
        inhibited_by: json_at(row, "inhibited_by")?,
        group_key: group_key.map(GroupKey::new),
    };

    Ok(AlertRecord {
        alert,
        labels_hash: LabelsHash::from_stored(labels_hash),
        first_seen_at: row.try_get("first_seen_at").map_err(backend)?,
        last_seen_at: row.try_get("last_seen_at").map_err(backend)?,
        resolved_at: row.try_get("resolved_at").map_err(backend)?,
        flap_count: count_at(row, "flap_count")?,
        episode: count_at(row, "episode")?,
        updated_at: row.try_get("updated_at").map_err(backend)?,
    })
}

/// Builds a card from a `notifications` row.
pub(crate) fn notification(row: &PgRow) -> Result<Notification, StoreError> {
    let dedupe_key: String = row.try_get("dedupe_key").map_err(backend)?;
    let route_id: i64 = row.try_get("route_id").map_err(backend)?;
    let guild_id: i64 = row.try_get("guild_id").map_err(backend)?;
    let channel_id: i64 = row.try_get("channel_id").map_err(backend)?;
    let id: i64 = row.try_get("id").map_err(backend)?;

    let fingerprint: String = row.try_get("fingerprint").map_err(backend)?;

    Ok(Notification {
        id: NotificationId::new(id),
        dedupe_key: DedupeKey::from_stored(dedupe_key),
        fingerprint: Fingerprint::new(fingerprint).map_err(|error| StoreError::Decode {
            kind: "notification fingerprint",
            detail: error.to_string(),
        })?,
        route_id: RouteId::new(route_id),
        guild_id: GuildId::from_db(guild_id),
        channel_id: ChannelId::from_db(channel_id),
        message_id: id_opt_at(row, "message_id", MessageId::from_db)?,
        thread_id: id_opt_at(row, "thread_id", ChannelId::from_db)?,
        state: parsed_at::<NotificationState>(row, "state")?,
        render_hash: row.try_get("render_hash").map_err(backend)?,
        applied_tags: json_at(row, "applied_tags")?,
        tags_hash: row.try_get("tags_hash").map_err(backend)?,
        pinned: row.try_get("pinned").map_err(backend)?,
        archived: row.try_get("archived").map_err(backend)?,
        responded_at: row.try_get("responded_at").map_err(backend)?,
        escalated_at: row.try_get("escalated_at").map_err(backend)?,
        supersedes: id_opt_at(row, "supersedes", NotificationId::new)?,
        reply_count: count_at(row, "reply_count")?,
        created_at: row.try_get("created_at").map_err(backend)?,
        updated_at: row.try_get("updated_at").map_err(backend)?,
    })
}

/// Builds a queued effect from an `outbox` row.
pub(crate) fn outbox_item(row: &PgRow) -> Result<OutboxItem, StoreError> {
    let id: i64 = row.try_get("id").map_err(backend)?;
    let lane: i32 = row.try_get("lane").map_err(backend)?;
    let dedupe_key: String = row.try_get("dedupe_key").map_err(backend)?;
    let claimed_by: Option<String> = row.try_get("claimed_by").map_err(backend)?;

    Ok(OutboxItem {
        id: OutboxId::new(id),
        lane: u16::try_from(lane).unwrap_or(0),
        effect: json_at::<Effect>(row, "payload")?,
        dedupe_key: DedupeKey::from_stored(dedupe_key),
        not_before: row.try_get("not_before").map_err(backend)?,
        attempts: count_at(row, "attempts")?,
        claimed_by: claimed_by.map(WorkerId::new),
        claimed_at: row.try_get("claimed_at").map_err(backend)?,
        last_error: row.try_get("last_error").map_err(backend)?,
        created_at: row.try_get("created_at").map_err(backend)?,
    })
}

/// Builds a route from a `routes` row, compiling its matchers.
///
/// The compiled set is built here rather than on use: evaluating a route against an alert happens
/// once per alert per route, and compiling a regex there would put the regex compiler on the hot
/// path of every notification.
pub(crate) fn route(row: &PgRow) -> Result<Route, StoreError> {
    let id: i64 = row.try_get("id").map_err(backend)?;
    let guild_id: i64 = row.try_get("guild_id").map_err(backend)?;
    let matcher_source: String = row.try_get("matcher_source").map_err(backend)?;
    let min_severity: Option<String> = row.try_get("min_severity").map_err(backend)?;
    let priority: i32 = row.try_get("priority").map_err(backend)?;

    let matchers = MatcherSet::parse(&matcher_source).map_err(|error| StoreError::Decode {
        kind: "route matchers",
        detail: error.to_string(),
    })?;

    let min_severity = min_severity
        .map(|value| {
            value
                .parse::<Severity>()
                .map_err(|error| StoreError::Decode {
                    kind: "route severity",
                    detail: error.to_string(),
                })
        })
        .transpose()?;

    Ok(Route {
        id: RouteId::new(id),
        guild_id: GuildId::from_db(guild_id),
        name: row.try_get("name").map_err(backend)?,
        matcher_source,
        matchers,
        min_severity,
        target: json_at::<RouteTarget>(row, "target")?,
        group_strategy: parsed_at(row, "group_strategy")?,
        mentions: json_at::<Mentions>(row, "mentions")?,
        escalation: json_opt_at::<dam_store::Escalation>(row, "escalation")?,
        priority,
        continue_to_next: row.try_get("continue_to_next").map_err(backend)?,
        source: parsed_at::<RouteSource>(row, "source")?,
        enabled: row.try_get("enabled").map_err(backend)?,
        created_by: id_opt_at(row, "created_by", UserId::from_db)?,
        created_at: row.try_get("created_at").map_err(backend)?,
    })
}

/// Builds an ignore rule from an `ignore_rules` row, compiling its matchers.
pub(crate) fn ignore_rule(row: &PgRow) -> Result<IgnoreRule, StoreError> {
    let id: i64 = row.try_get("id").map_err(backend)?;
    let guild_id: i64 = row.try_get("guild_id").map_err(backend)?;
    let created_by: i64 = row.try_get("created_by").map_err(backend)?;
    let matcher_source: String = row.try_get("matcher_source").map_err(backend)?;

    let matchers = MatcherSet::parse(&matcher_source).map_err(|error| StoreError::Decode {
        kind: "ignore matchers",
        detail: error.to_string(),
    })?;

    Ok(IgnoreRule {
        id: IgnoreId::new(id),
        scope: parsed_at::<IgnoreScope>(row, "scope")?,
        guild_id: GuildId::from_db(guild_id),
        channel_id: id_opt_at(row, "channel_id", ChannelId::from_db)?,
        matcher_source,
        matchers,
        reason: row.try_get("reason").map_err(backend)?,
        created_by: UserId::from_db(created_by),
        created_at: row.try_get("created_at").map_err(backend)?,
        expires_at: row.try_get("expires_at").map_err(backend)?,
        revoked_at: row.try_get("revoked_at").map_err(backend)?,
    })
}

/// Builds a silence link from a `silences` row.
pub(crate) fn silence_link(row: &PgRow) -> Result<SilenceLink, StoreError> {
    Ok(SilenceLink {
        am_id: row.try_get("am_id").map_err(backend)?,
        matchers: row.try_get("matchers").map_err(backend)?,
        starts_at: row.try_get("starts_at").map_err(backend)?,
        ends_at: row.try_get("ends_at").map_err(backend)?,
        created_by: row.try_get("created_by").map_err(backend)?,
        discord_user_id: id_opt_at(row, "discord_user_id", UserId::from_db)?,
        origin_message: row.try_get("origin_message").map_err(backend)?,
        comment: row.try_get("comment").map_err(backend)?,
        state: parsed_at::<SilenceLifecycle>(row, "state")?,
        synced_at: row.try_get("synced_at").map_err(backend)?,
    })
}

/// Builds a cached forum tag from a `forum_tags` row.
pub(crate) fn forum_tag(row: &PgRow) -> Result<ForumTag, StoreError> {
    let channel_id: i64 = row.try_get("channel_id").map_err(backend)?;
    let tag_id: i64 = row.try_get("tag_id").map_err(backend)?;

    Ok(ForumTag {
        channel_id: ChannelId::from_db(channel_id),
        name: row.try_get("tag_name").map_err(backend)?,
        id: TagId::from_db(tag_id),
        moderated: row.try_get("moderated").map_err(backend)?,
        synced_at: row.try_get("synced_at").map_err(backend)?,
    })
}

/// Builds a subscription from a `subscriptions` row, compiling its matchers.
pub(crate) fn subscription(row: &PgRow) -> Result<Subscription, StoreError> {
    let id: i64 = row.try_get("id").map_err(backend)?;
    let user_id: i64 = row.try_get("user_id").map_err(backend)?;
    let matcher_source: String = row.try_get("matcher_source").map_err(backend)?;
    let min_severity: Option<String> = row.try_get("min_severity").map_err(backend)?;

    let matchers = MatcherSet::parse(&matcher_source).map_err(|error| StoreError::Decode {
        kind: "subscription matchers",
        detail: error.to_string(),
    })?;

    let min_severity = min_severity
        .map(|value| {
            value
                .parse::<Severity>()
                .map_err(|error| StoreError::Decode {
                    kind: "subscription severity",
                    detail: error.to_string(),
                })
        })
        .transpose()?;

    Ok(Subscription {
        id: SubscriptionId::new(id),
        user_id: UserId::from_db(user_id),
        matcher_source,
        matchers,
        min_severity,
        created_at: row.try_get("created_at").map_err(backend)?,
    })
}

/// Renders a value as the JSON document a column holds.
///
/// Serialisation of these types cannot fail — they are maps, vectors and enums of owned strings —
/// so a failure here is a bug rather than a condition, and the null document keeps a bug from also
/// being a write of invalid JSON.
pub(crate) fn json<T: serde::Serialize>(value: &T) -> serde_json::Value {
    serde_json::to_value(value).unwrap_or(serde_json::Value::Null)
}

/// The timestamp type this dialect binds, named so the store reads the same as its counterpart.
pub(crate) type Timestamp = DateTime<Utc>;

/// Builds the live acknowledgement from an `acknowledgements` row.
pub(crate) fn acknowledgement(row: &PgRow) -> Result<Acknowledgement, StoreError> {
    let user_id: i64 = row.try_get("user_id").map_err(backend)?;

    Ok(Acknowledgement {
        user_id: UserId::from_db(user_id),
        kind: parsed_at::<AckKind>(row, "kind")?,
        note: row.try_get("note").map_err(backend)?,
        at: row.try_get("created_at").map_err(backend)?,
    })
}
