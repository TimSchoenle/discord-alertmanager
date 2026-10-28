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

/// Decodes a JSON column.
fn from_json<T: DeserializeOwned>(
    raw: serde_json::Value,
    kind: &'static str,
) -> Result<T, StoreError> {
    serde_json::from_value(raw).map_err(|error| StoreError::Decode {
        kind,
        detail: error.to_string(),
    })
}

/// Decodes a nullable JSON column, where SQL null and a JSON null mean the same thing.
fn from_json_opt<T: DeserializeOwned>(
    raw: Option<serde_json::Value>,
    kind: &'static str,
) -> Result<Option<T>, StoreError> {
    raw.filter(|value| !value.is_null())
        .map(|raw| from_json(raw, kind))
        .transpose()
}

/// Decodes a column holding one of the domain's stored discriminants.
fn parsed<T>(raw: &str, kind: &'static str) -> Result<T, StoreError>
where
    T: std::str::FromStr,
    T::Err: std::fmt::Display,
{
    raw.parse().map_err(|error: T::Err| StoreError::Decode {
        kind,
        detail: error.to_string(),
    })
}

/// Decodes a count column that the domain holds as a `u32`.
///
/// A negative count is not representable and is clamped rather than refused: the column is a
/// counter the database maintains, and failing the read of a whole card because a reply count went
/// strange would hide the card as well as the counter.
fn count(raw: i32) -> u32 {
    u32::try_from(raw).unwrap_or(0)
}

/// Compiles a stored matcher expression.
fn matchers(source: &str, kind: &'static str) -> Result<MatcherSet, StoreError> {
    MatcherSet::parse(source).map_err(|error| StoreError::Decode {
        kind,
        detail: error.to_string(),
    })
}

/// Decodes a nullable severity column.
fn severity_opt(raw: Option<&str>, kind: &'static str) -> Result<Option<Severity>, StoreError> {
    raw.map(|value| parsed::<Severity>(value, kind)).transpose()
}

/// Decodes a fingerprint column.
fn fingerprint(raw: String, kind: &'static str) -> Result<Fingerprint, StoreError> {
    Fingerprint::new(raw).map_err(|error| StoreError::Decode {
        kind,
        detail: error.to_string(),
    })
}

/// Maps a driver failure onto the store's vocabulary.
///
/// A unique violation is separated out because it is the expected outcome of two workers racing to
/// create one card, and the caller's response to it — re-read and edit the winner's row — is
/// nothing like its response to an unreachable database.
#[expect(
    clippy::needless_pass_by_value,
    reason = "used as a function item in `map_err`, which hands over an owned error; taking a reference would put a closure at every one of its call sites"
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

/// One `alerts` row, as [`crate::store`] selects it.
pub(crate) struct AlertRow {
    pub(crate) fingerprint: String,
    pub(crate) labels_hash: String,
    pub(crate) group_key: Option<String>,
    pub(crate) labels: serde_json::Value,
    pub(crate) annotations: serde_json::Value,
    pub(crate) starts_at: Timestamp,
    pub(crate) ends_at: Option<Timestamp>,
    pub(crate) generator_url: Option<String>,
    pub(crate) status: String,
    pub(crate) am_state: String,
    pub(crate) silenced_by: serde_json::Value,
    pub(crate) inhibited_by: serde_json::Value,
    pub(crate) first_seen_at: Timestamp,
    pub(crate) last_seen_at: Timestamp,
    pub(crate) resolved_at: Option<Timestamp>,
    pub(crate) flap_count: i32,
    pub(crate) updated_at: Timestamp,
}

impl TryFrom<AlertRow> for AlertRecord {
    type Error = StoreError;

    fn try_from(row: AlertRow) -> Result<Self, StoreError> {
        let alert = Alert {
            fingerprint: fingerprint(row.fingerprint, "fingerprint")?,
            labels: from_json::<Labels>(row.labels, "labels")?,
            annotations: from_json::<Annotations>(row.annotations, "annotations")?,
            starts_at: row.starts_at,
            ends_at: row.ends_at,
            generator_url: row.generator_url,
            status: parsed::<AlertStatus>(&row.status, "status")?,
            am_state: parsed::<AmState>(&row.am_state, "am_state")?,
            silenced_by: from_json(row.silenced_by, "silenced_by")?,
            inhibited_by: from_json(row.inhibited_by, "inhibited_by")?,
            group_key: row.group_key.map(GroupKey::new),
        };

        Ok(Self {
            alert,
            labels_hash: LabelsHash::from_stored(row.labels_hash),
            first_seen_at: row.first_seen_at,
            last_seen_at: row.last_seen_at,
            resolved_at: row.resolved_at,
            flap_count: count(row.flap_count),
            updated_at: row.updated_at,
        })
    }
}

/// One `notifications` row.
pub(crate) struct NotificationRow {
    pub(crate) id: i64,
    pub(crate) dedupe_key: String,
    pub(crate) fingerprint: String,
    pub(crate) route_id: i64,
    pub(crate) guild_id: i64,
    pub(crate) channel_id: i64,
    pub(crate) message_id: Option<i64>,
    pub(crate) thread_id: Option<i64>,
    pub(crate) state: String,
    pub(crate) render_hash: Option<String>,
    pub(crate) applied_tags: serde_json::Value,
    pub(crate) tags_hash: Option<String>,
    pub(crate) pinned: bool,
    pub(crate) archived: bool,
    pub(crate) responded_at: Option<Timestamp>,
    pub(crate) escalated_at: Option<Timestamp>,
    pub(crate) supersedes: Option<i64>,
    pub(crate) resolved_at: Option<Timestamp>,
    pub(crate) reply_count: i32,
    pub(crate) created_at: Timestamp,
    pub(crate) updated_at: Timestamp,
}

impl TryFrom<NotificationRow> for Notification {
    type Error = StoreError;

    fn try_from(row: NotificationRow) -> Result<Self, StoreError> {
        Ok(Self {
            id: NotificationId::new(row.id),
            dedupe_key: DedupeKey::from_stored(row.dedupe_key),
            fingerprint: fingerprint(row.fingerprint, "notification fingerprint")?,
            route_id: RouteId::new(row.route_id),
            guild_id: GuildId::from_db(row.guild_id),
            channel_id: ChannelId::from_db(row.channel_id),
            message_id: row.message_id.map(MessageId::from_db),
            thread_id: row.thread_id.map(ChannelId::from_db),
            state: parsed::<NotificationState>(&row.state, "state")?,
            render_hash: row.render_hash,
            applied_tags: from_json(row.applied_tags, "applied_tags")?,
            tags_hash: row.tags_hash,
            pinned: row.pinned,
            archived: row.archived,
            responded_at: row.responded_at,
            escalated_at: row.escalated_at,
            supersedes: row.supersedes.map(NotificationId::new),
            resolved_at: row.resolved_at,
            reply_count: count(row.reply_count),
            created_at: row.created_at,
            updated_at: row.updated_at,
        })
    }
}

/// One `outbox` row.
pub(crate) struct OutboxRow {
    pub(crate) id: i64,
    pub(crate) lane: i32,
    pub(crate) dedupe_key: String,
    pub(crate) payload: serde_json::Value,
    pub(crate) not_before: Timestamp,
    pub(crate) attempts: i32,
    pub(crate) claimed_by: Option<String>,
    pub(crate) claimed_at: Option<Timestamp>,
    pub(crate) last_error: Option<String>,
    pub(crate) created_at: Timestamp,
}

impl TryFrom<OutboxRow> for OutboxItem {
    type Error = StoreError;

    fn try_from(row: OutboxRow) -> Result<Self, StoreError> {
        Ok(Self {
            id: OutboxId::new(row.id),
            lane: u16::try_from(row.lane).unwrap_or(0),
            effect: from_json::<Effect>(row.payload, "payload")?,
            dedupe_key: DedupeKey::from_stored(row.dedupe_key),
            not_before: row.not_before,
            attempts: count(row.attempts),
            claimed_by: row.claimed_by.map(WorkerId::new),
            claimed_at: row.claimed_at,
            last_error: row.last_error,
            created_at: row.created_at,
        })
    }
}

/// One `routes` row.
pub(crate) struct RouteRow {
    pub(crate) id: i64,
    pub(crate) guild_id: i64,
    pub(crate) name: String,
    pub(crate) matcher_source: String,
    pub(crate) min_severity: Option<String>,
    pub(crate) target: serde_json::Value,
    pub(crate) group_strategy: String,
    pub(crate) mentions: serde_json::Value,
    pub(crate) escalation: Option<serde_json::Value>,
    pub(crate) priority: i32,
    pub(crate) continue_to_next: bool,
    pub(crate) source: String,
    pub(crate) enabled: bool,
    pub(crate) created_by: Option<i64>,
    pub(crate) created_at: Timestamp,
}

/// Builds a route, compiling its matchers.
///
/// The compiled set is built here rather than on use: evaluating a route against an alert happens
/// once per alert per route, and compiling a regex there would put the regex compiler on the hot
/// path of every notification.
impl TryFrom<RouteRow> for Route {
    type Error = StoreError;

    fn try_from(row: RouteRow) -> Result<Self, StoreError> {
        Ok(Self {
            id: RouteId::new(row.id),
            guild_id: GuildId::from_db(row.guild_id),
            name: row.name,
            matchers: matchers(&row.matcher_source, "route matchers")?,
            matcher_source: row.matcher_source,
            min_severity: severity_opt(row.min_severity.as_deref(), "route severity")?,
            target: from_json::<RouteTarget>(row.target, "target")?,
            group_strategy: parsed(&row.group_strategy, "group_strategy")?,
            mentions: from_json::<Mentions>(row.mentions, "mentions")?,
            escalation: from_json_opt(row.escalation, "escalation")?,
            priority: row.priority,
            continue_to_next: row.continue_to_next,
            source: parsed::<RouteSource>(&row.source, "source")?,
            enabled: row.enabled,
            created_by: row.created_by.map(UserId::from_db),
            created_at: row.created_at,
        })
    }
}

/// One `ignore_rules` row.
pub(crate) struct IgnoreRow {
    pub(crate) id: i64,
    pub(crate) scope: String,
    pub(crate) guild_id: i64,
    pub(crate) channel_id: Option<i64>,
    pub(crate) matcher_source: String,
    pub(crate) reason: String,
    pub(crate) created_by: i64,
    pub(crate) created_at: Timestamp,
    pub(crate) expires_at: Option<Timestamp>,
    pub(crate) revoked_at: Option<Timestamp>,
}

impl TryFrom<IgnoreRow> for IgnoreRule {
    type Error = StoreError;

    fn try_from(row: IgnoreRow) -> Result<Self, StoreError> {
        Ok(Self {
            id: IgnoreId::new(row.id),
            scope: parsed::<IgnoreScope>(&row.scope, "scope")?,
            guild_id: GuildId::from_db(row.guild_id),
            channel_id: row.channel_id.map(ChannelId::from_db),
            matchers: matchers(&row.matcher_source, "ignore matchers")?,
            matcher_source: row.matcher_source,
            reason: row.reason,
            created_by: UserId::from_db(row.created_by),
            created_at: row.created_at,
            expires_at: row.expires_at,
            revoked_at: row.revoked_at,
        })
    }
}

/// One `silences` row.
pub(crate) struct SilenceRow {
    pub(crate) am_id: String,
    pub(crate) matchers: String,
    pub(crate) starts_at: Timestamp,
    pub(crate) ends_at: Timestamp,
    pub(crate) created_by: String,
    pub(crate) discord_user_id: Option<i64>,
    pub(crate) origin_message: Option<String>,
    pub(crate) comment: String,
    pub(crate) state: String,
    pub(crate) synced_at: Timestamp,
}

impl TryFrom<SilenceRow> for SilenceLink {
    type Error = StoreError;

    fn try_from(row: SilenceRow) -> Result<Self, StoreError> {
        Ok(Self {
            am_id: row.am_id,
            matchers: row.matchers,
            starts_at: row.starts_at,
            ends_at: row.ends_at,
            created_by: row.created_by,
            discord_user_id: row.discord_user_id.map(UserId::from_db),
            origin_message: row.origin_message,
            comment: row.comment,
            state: parsed::<SilenceLifecycle>(&row.state, "state")?,
            synced_at: row.synced_at,
        })
    }
}

/// One `forum_tags` row.
pub(crate) struct ForumTagRow {
    pub(crate) channel_id: i64,
    pub(crate) tag_name: String,
    pub(crate) tag_id: i64,
    pub(crate) moderated: bool,
    pub(crate) synced_at: Timestamp,
}

impl From<ForumTagRow> for ForumTag {
    fn from(row: ForumTagRow) -> Self {
        Self {
            channel_id: ChannelId::from_db(row.channel_id),
            name: row.tag_name,
            id: TagId::from_db(row.tag_id),
            moderated: row.moderated,
            synced_at: row.synced_at,
        }
    }
}

/// One `subscriptions` row.
pub(crate) struct SubscriptionRow {
    pub(crate) id: i64,
    pub(crate) user_id: i64,
    pub(crate) matcher_source: String,
    pub(crate) min_severity: Option<String>,
    pub(crate) created_at: Timestamp,
}

impl TryFrom<SubscriptionRow> for Subscription {
    type Error = StoreError;

    fn try_from(row: SubscriptionRow) -> Result<Self, StoreError> {
        Ok(Self {
            id: SubscriptionId::new(row.id),
            user_id: UserId::from_db(row.user_id),
            matchers: matchers(&row.matcher_source, "subscription matchers")?,
            matcher_source: row.matcher_source,
            min_severity: severity_opt(row.min_severity.as_deref(), "subscription severity")?,
            created_at: row.created_at,
        })
    }
}

/// The live acknowledgement on one alert, from an `acknowledgements` row.
pub(crate) struct AckRow {
    pub(crate) user_id: i64,
    pub(crate) kind: String,
    pub(crate) note: Option<String>,
    pub(crate) created_at: Timestamp,
}

impl TryFrom<AckRow> for Acknowledgement {
    type Error = StoreError;

    fn try_from(row: AckRow) -> Result<Self, StoreError> {
        Ok(Self {
            user_id: UserId::from_db(row.user_id),
            kind: parsed::<AckKind>(&row.kind, "kind")?,
            note: row.note,
            at: row.created_at,
        })
    }
}

/// Converts every row of a result set, stopping at the first that does not decode.
pub(crate) fn decode_all<R, T>(rows: Vec<R>) -> Result<Vec<T>, StoreError>
where
    T: TryFrom<R, Error = StoreError>,
{
    rows.into_iter().map(T::try_from).collect()
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
