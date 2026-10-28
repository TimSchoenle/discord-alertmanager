//! The one place a `SQLite` row becomes a domain value, and a domain value becomes a bound
//! parameter.
//!
//! `SQLite` has five storage classes and no dedicated timestamp, boolean or JSON type, so every
//! column of those three kinds is encoded on the way in and decoded on the way out. Doing that
//! per query would put the encoding in forty places and guarantee that two of them disagree about
//! the subsecond width, which is the one detail the ordering of the whole outbox rests on.
//!
//! Timestamps are RFC 3339 with a fixed six-digit subsecond and a literal `Z`. The width is fixed
//! rather than minimal because the column is ordered lexicographically: without it,
//! `…:00.5Z` sorts after `…:00.45Z` and a claim returns rows in the wrong order.

use chrono::{DateTime, SecondsFormat, Utc};
use dam_core::{
    Alert, AlertStatus, AmState, Annotations, DedupeKey, Fingerprint, GroupKey, Labels, LabelsHash,
    MatcherSet, NotificationState, Severity,
};
use dam_store::{
    AckKind, Acknowledgement, AlertRecord, ChannelId, Effect, ForumTag, GuildId, IgnoreId,
    IgnoreRule, IgnoreScope, Mentions, MessageId, Notification, NotificationId, OutboxId,
    OutboxItem, Route, RouteId, RouteSource, RouteTarget, SilenceLifecycle, SilenceLink,
    StoreError, Subscription, SubscriptionId, UserId, WorkerId,
};
use serde::Serialize;
use serde::de::DeserializeOwned;

/// Renders a timestamp for storage.
pub(crate) fn encode_time(value: DateTime<Utc>) -> String {
    value.to_rfc3339_opts(SecondsFormat::Micros, true)
}

/// Renders an optional timestamp for storage.
pub(crate) fn encode_time_opt(value: Option<DateTime<Utc>>) -> Option<String> {
    value.map(encode_time)
}

/// Decodes a timestamp column.
fn time(raw: &str, kind: &'static str) -> Result<DateTime<Utc>, StoreError> {
    DateTime::parse_from_rfc3339(raw)
        .map(|value| value.with_timezone(&Utc))
        .map_err(|error| StoreError::Decode {
            kind,
            detail: error.to_string(),
        })
}

/// Decodes a nullable timestamp column.
fn time_opt(raw: Option<&str>, kind: &'static str) -> Result<Option<DateTime<Utc>>, StoreError> {
    raw.map(|raw| time(raw, kind)).transpose()
}

/// Renders a value as the JSON text a column holds.
///
/// Serialisation of these types cannot fail — they are maps, vectors and enums of owned strings —
/// so a failure here is a bug rather than a condition, and the empty document keeps a bug from
/// also being a write of invalid JSON.
pub(crate) fn encode_json<T: Serialize>(value: &T) -> String {
    serde_json::to_string(value).unwrap_or_else(|_| "null".to_owned())
}

/// Decodes a JSON column.
fn from_json<T: DeserializeOwned>(raw: &str, kind: &'static str) -> Result<T, StoreError> {
    serde_json::from_str(raw).map_err(|error| StoreError::Decode {
        kind,
        detail: error.to_string(),
    })
}

/// Decodes a nullable JSON column, where null and an absent document mean the same thing.
fn from_json_opt<T: DeserializeOwned>(
    raw: Option<&str>,
    kind: &'static str,
) -> Result<Option<T>, StoreError> {
    raw.map(|raw| from_json(raw, kind)).transpose()
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
/// A negative or oversized count is not representable and is clamped rather than refused: the
/// column is a counter the database itself maintains, and failing a read of a whole card because
/// a reply count went strange would hide the card as well as the counter.
fn count(raw: i64) -> u32 {
    u32::try_from(raw).unwrap_or(0)
}

/// Decodes a boolean, which this dialect stores as an integer.
fn flag(raw: i64) -> bool {
    raw != 0
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
/// A unique violation is separated out because it is the expected outcome of two workers racing
/// to create one card, and the caller's response to it — re-read and edit the winner's row — is
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
            // `SQLite` names the columns rather than the index in its message and exposes no
            // constraint name, so the message is the most specific thing available.
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
    pub(crate) labels: String,
    pub(crate) annotations: String,
    pub(crate) starts_at: String,
    pub(crate) ends_at: Option<String>,
    pub(crate) generator_url: Option<String>,
    pub(crate) status: String,
    pub(crate) am_state: String,
    pub(crate) silenced_by: String,
    pub(crate) inhibited_by: String,
    pub(crate) first_seen_at: String,
    pub(crate) last_seen_at: String,
    pub(crate) resolved_at: Option<String>,
    pub(crate) flap_count: i64,
    pub(crate) updated_at: String,
}

impl TryFrom<AlertRow> for AlertRecord {
    type Error = StoreError;

    fn try_from(row: AlertRow) -> Result<Self, StoreError> {
        let alert = Alert {
            fingerprint: fingerprint(row.fingerprint, "fingerprint")?,
            labels: from_json::<Labels>(&row.labels, "labels")?,
            annotations: from_json::<Annotations>(&row.annotations, "annotations")?,
            starts_at: time(&row.starts_at, "starts_at")?,
            ends_at: time_opt(row.ends_at.as_deref(), "ends_at")?,
            generator_url: row.generator_url,
            status: parsed::<AlertStatus>(&row.status, "status")?,
            am_state: parsed::<AmState>(&row.am_state, "am_state")?,
            silenced_by: from_json(&row.silenced_by, "silenced_by")?,
            inhibited_by: from_json(&row.inhibited_by, "inhibited_by")?,
            group_key: row.group_key.map(GroupKey::new),
        };

        Ok(Self {
            alert,
            labels_hash: LabelsHash::from_stored(row.labels_hash),
            first_seen_at: time(&row.first_seen_at, "first_seen_at")?,
            last_seen_at: time(&row.last_seen_at, "last_seen_at")?,
            resolved_at: time_opt(row.resolved_at.as_deref(), "resolved_at")?,
            flap_count: count(row.flap_count),
            updated_at: time(&row.updated_at, "updated_at")?,
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
    pub(crate) applied_tags: String,
    pub(crate) tags_hash: Option<String>,
    pub(crate) pinned: i64,
    pub(crate) archived: i64,
    pub(crate) responded_at: Option<String>,
    pub(crate) escalated_at: Option<String>,
    pub(crate) supersedes: Option<i64>,
    pub(crate) resolved_at: Option<String>,
    pub(crate) reply_count: i64,
    pub(crate) created_at: String,
    pub(crate) updated_at: String,
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
            applied_tags: from_json(&row.applied_tags, "applied_tags")?,
            tags_hash: row.tags_hash,
            pinned: flag(row.pinned),
            archived: flag(row.archived),
            responded_at: time_opt(row.responded_at.as_deref(), "responded_at")?,
            escalated_at: time_opt(row.escalated_at.as_deref(), "escalated_at")?,
            supersedes: row.supersedes.map(NotificationId::new),
            resolved_at: time_opt(row.resolved_at.as_deref(), "resolved_at")?,
            reply_count: count(row.reply_count),
            created_at: time(&row.created_at, "created_at")?,
            updated_at: time(&row.updated_at, "updated_at")?,
        })
    }
}

/// One `outbox` row.
pub(crate) struct OutboxRow {
    pub(crate) id: i64,
    pub(crate) lane: i64,
    pub(crate) dedupe_key: String,
    pub(crate) payload: String,
    pub(crate) not_before: String,
    pub(crate) attempts: i64,
    pub(crate) claimed_by: Option<String>,
    pub(crate) claimed_at: Option<String>,
    pub(crate) last_error: Option<String>,
    pub(crate) created_at: String,
}

impl TryFrom<OutboxRow> for OutboxItem {
    type Error = StoreError;

    fn try_from(row: OutboxRow) -> Result<Self, StoreError> {
        Ok(Self {
            id: OutboxId::new(row.id),
            lane: u16::try_from(row.lane).unwrap_or(0),
            effect: from_json::<Effect>(&row.payload, "payload")?,
            dedupe_key: DedupeKey::from_stored(row.dedupe_key),
            not_before: time(&row.not_before, "not_before")?,
            attempts: count(row.attempts),
            claimed_by: row.claimed_by.map(WorkerId::new),
            claimed_at: time_opt(row.claimed_at.as_deref(), "claimed_at")?,
            last_error: row.last_error,
            created_at: time(&row.created_at, "created_at")?,
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
    pub(crate) target: String,
    pub(crate) group_strategy: String,
    pub(crate) mentions: String,
    pub(crate) escalation: Option<String>,
    pub(crate) priority: i64,
    pub(crate) continue_to_next: i64,
    pub(crate) source: String,
    pub(crate) enabled: i64,
    pub(crate) created_by: Option<i64>,
    pub(crate) created_at: String,
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
            target: from_json::<RouteTarget>(&row.target, "target")?,
            group_strategy: parsed::<dam_store::GroupStrategy>(
                &row.group_strategy,
                "group_strategy",
            )?,
            mentions: from_json::<Mentions>(&row.mentions, "mentions")?,
            escalation: from_json_opt(row.escalation.as_deref(), "escalation")?,
            priority: i32::try_from(row.priority).unwrap_or(i32::MAX),
            continue_to_next: flag(row.continue_to_next),
            source: parsed::<RouteSource>(&row.source, "source")?,
            enabled: flag(row.enabled),
            created_by: row.created_by.map(UserId::from_db),
            created_at: time(&row.created_at, "created_at")?,
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
    pub(crate) created_at: String,
    pub(crate) expires_at: Option<String>,
    pub(crate) revoked_at: Option<String>,
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
            created_at: time(&row.created_at, "created_at")?,
            expires_at: time_opt(row.expires_at.as_deref(), "expires_at")?,
            revoked_at: time_opt(row.revoked_at.as_deref(), "revoked_at")?,
        })
    }
}

/// One `silences` row.
pub(crate) struct SilenceRow {
    pub(crate) am_id: String,
    pub(crate) matchers: String,
    pub(crate) starts_at: String,
    pub(crate) ends_at: String,
    pub(crate) created_by: String,
    pub(crate) discord_user_id: Option<i64>,
    pub(crate) origin_message: Option<String>,
    pub(crate) comment: String,
    pub(crate) state: String,
    pub(crate) synced_at: String,
}

impl TryFrom<SilenceRow> for SilenceLink {
    type Error = StoreError;

    fn try_from(row: SilenceRow) -> Result<Self, StoreError> {
        Ok(Self {
            am_id: row.am_id,
            matchers: row.matchers,
            starts_at: time(&row.starts_at, "starts_at")?,
            ends_at: time(&row.ends_at, "ends_at")?,
            created_by: row.created_by,
            discord_user_id: row.discord_user_id.map(UserId::from_db),
            origin_message: row.origin_message,
            comment: row.comment,
            state: parsed::<SilenceLifecycle>(&row.state, "state")?,
            synced_at: time(&row.synced_at, "synced_at")?,
        })
    }
}

/// One `forum_tags` row.
pub(crate) struct ForumTagRow {
    pub(crate) channel_id: i64,
    pub(crate) tag_name: String,
    pub(crate) tag_id: i64,
    pub(crate) moderated: i64,
    pub(crate) synced_at: String,
}

impl TryFrom<ForumTagRow> for ForumTag {
    type Error = StoreError;

    fn try_from(row: ForumTagRow) -> Result<Self, StoreError> {
        Ok(Self {
            channel_id: ChannelId::from_db(row.channel_id),
            name: row.tag_name,
            id: dam_store::TagId::from_db(row.tag_id),
            moderated: flag(row.moderated),
            synced_at: time(&row.synced_at, "synced_at")?,
        })
    }
}

/// One `subscriptions` row.
pub(crate) struct SubscriptionRow {
    pub(crate) id: i64,
    pub(crate) user_id: i64,
    pub(crate) matcher_source: String,
    pub(crate) min_severity: Option<String>,
    pub(crate) created_at: String,
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
            created_at: time(&row.created_at, "created_at")?,
        })
    }
}

/// The live acknowledgement on one alert, from an `acknowledgements` row.
pub(crate) struct AckRow {
    pub(crate) user_id: i64,
    pub(crate) kind: String,
    pub(crate) note: Option<String>,
    pub(crate) created_at: String,
}

impl TryFrom<AckRow> for Acknowledgement {
    type Error = StoreError;

    fn try_from(row: AckRow) -> Result<Self, StoreError> {
        Ok(Self {
            user_id: UserId::from_db(row.user_id),
            kind: parsed::<AckKind>(&row.kind, "kind")?,
            note: row.note,
            at: time(&row.created_at, "created_at")?,
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

/// Decodes a timestamp read back by a query that selects one on its own.
pub(crate) fn decode_time(raw: &str, kind: &'static str) -> Result<DateTime<Utc>, StoreError> {
    time(raw, kind)
}
