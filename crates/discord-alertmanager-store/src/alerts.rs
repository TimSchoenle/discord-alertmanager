//! What goes into the alert tables, and what comes back out of them.

use std::str::FromStr;

use chrono::{DateTime, Utc};
use dam_core::{
    Alert, AlertDelta, AlertStatus, CoreError, EventSource, Fingerprint, GroupKey, LabelsHash,
    MatchOp, NotificationState, Severity,
};
use serde::{Deserialize, Serialize};

use crate::ids::UserId;

/// One delivery of alerts, from either source, as it arrives at the store.
///
/// The webhook and the reconciler produce the same shape, so `ingest_batch` is written once. What
/// differs is `source`, which every event row carries, and `truncated`, which only a webhook can
/// report.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IngestBatch {
    /// Where the batch came from.
    pub source: EventSource,

    /// The group Alertmanager delivered it under, when it delivered one.
    pub group_key: Option<GroupKey>,

    /// Alerts Alertmanager dropped from the payload before sending it.
    ///
    /// Above zero means the batch is not the whole truth, so the reconciler is nudged rather than
    /// the batch being trusted on its own.
    pub truncated: u32,

    /// The alerts themselves.
    pub alerts: Vec<Alert>,

    /// When the batch was received.
    pub received_at: DateTime<Utc>,
}

impl IngestBatch {
    /// A batch with no group and nothing truncated.
    #[must_use]
    pub fn new(source: EventSource, alerts: Vec<Alert>, received_at: DateTime<Utc>) -> Self {
        Self {
            source,
            group_key: None,
            truncated: 0,
            alerts,
            received_at,
        }
    }

    /// Whether Alertmanager admitted to dropping alerts from this batch.
    #[must_use]
    pub fn is_truncated(&self) -> bool {
        self.truncated > 0
    }
}

/// What ingesting a batch produced.
///
/// Deltas carry only the changes that survived deduplication, so a redelivered webhook yields an
/// empty list and a duplicate count rather than a second round of card edits.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct IngestOutcome {
    /// The accepted changes, in the order they were applied.
    pub deltas: Vec<AlertDelta>,

    /// Events discarded because an identical one was already stored.
    pub duplicates: u32,
}

/// The stored view of one alert: its current state plus the history the local tables keep.
///
/// Alertmanager garbage-collects a resolved alert within one collection interval, and every card
/// edit after that — a late thread reply, a button, a flap re-render — needs a label set and
/// timings it can no longer supply. This row is what keeps such a card renderable.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AlertRecord {
    /// The alert as last seen.
    pub alert: Alert,

    /// The locally computed hash of the label set, stored beside Alertmanager's fingerprint.
    pub labels_hash: LabelsHash,

    /// When this fingerprint was first seen, ever.
    pub first_seen_at: DateTime<Utc>,

    /// When it was last seen from either source.
    pub last_seen_at: DateTime<Utc>,

    /// When it last resolved.
    pub resolved_at: Option<DateTime<Utc>>,

    /// How many times it has re-fired after resolving, inside the current episode.
    pub flap_count: u32,

    /// Which firing episode the alert is in.
    ///
    /// Incremented by a re-fire that arrives after a whole regroup window of quiet, and by
    /// nothing else. The card for one episode is a different card from the card for the last.
    pub episode: u32,

    /// When the row was last written.
    pub updated_at: DateTime<Utc>,
}

impl AlertRecord {
    /// The alert's fingerprint.
    #[must_use]
    pub fn fingerprint(&self) -> &Fingerprint {
        &self.alert.fingerprint
    }

    /// The alert's severity.
    #[must_use]
    pub fn severity(&self) -> Severity {
        self.alert.severity()
    }
}

/// A filtered, paginated read of the alert table.
///
/// Every field is optional and they conjoin. This is the one query shape that cannot be checked
/// at compile time, because the SQL it produces depends on which fields are set.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AlertQuery {
    /// Keep only alerts in these statuses. Empty means every status.
    pub statuses: Vec<AlertStatus>,

    /// Keep only alerts at or above this severity.
    pub min_severity: Option<Severity>,

    /// Keep only alerts whose labels satisfy every one of these.
    ///
    /// Carried as name, operator and value rather than as a compiled `MatcherSet`, because a
    /// backend turns them into SQL predicates where it can and evaluates the rest in memory.
    pub matchers: Vec<QueryMatcher>,

    /// Keep only alerts with a notification in this state.
    pub notification_state: Option<NotificationState>,

    /// Where to start.
    pub offset: u32,

    /// How many rows to return.
    pub limit: u32,
}

/// One matcher in an [`AlertQuery`], flattened for transport.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueryMatcher {
    /// The label to read.
    pub name: String,

    /// The comparison.
    pub op: MatchOp,

    /// The right-hand side.
    pub value: String,
}

/// One page of results, and enough context to render "12–24 of 137".
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Page<T> {
    /// The rows on this page.
    pub items: Vec<T>,

    /// How many rows the filter matches in total.
    pub total: u64,

    /// Where this page started.
    pub offset: u32,

    /// How many rows were asked for.
    pub limit: u32,
}

impl<T> Page<T> {
    /// Index one past the last row on this page.
    #[must_use]
    pub fn end(&self) -> u64 {
        u64::from(self.offset) + u64::try_from(self.items.len()).unwrap_or(u64::MAX)
    }

    /// Whether another page exists after this one.
    #[must_use]
    pub fn has_more(&self) -> bool {
        self.end() < self.total
    }
}

/// The bot's record of one Alertmanager silence.
///
/// Alertmanager owns the silence. This row is the link between it and the Discord action that
/// created it, which Alertmanager has no place to keep and `/silence list` has to show.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SilenceLink {
    /// The id Alertmanager assigned.
    pub am_id: String,

    /// The matcher expression as it was sent.
    pub matchers: String,

    /// When the silence starts.
    pub starts_at: DateTime<Utc>,

    /// When it expires.
    pub ends_at: DateTime<Utc>,

    /// The `createdBy` string sent to Alertmanager, carrying the Discord identity.
    pub created_by: String,

    /// The Discord user behind it, when one is known.
    pub discord_user_id: Option<UserId>,

    /// Permalink to the card the silence was created from.
    pub origin_message: Option<String>,

    /// The operator's comment.
    pub comment: String,

    /// Where the silence is in its life.
    pub state: SilenceLifecycle,

    /// When this row last agreed with Alertmanager.
    pub synced_at: DateTime<Utc>,
}

/// Where a silence is in its life, as Alertmanager reports it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SilenceLifecycle {
    /// Created, but its start time has not arrived.
    Pending,

    /// In force.
    Active,

    /// Over, whether by expiry or by being expired early.
    Expired,
}

impl SilenceLifecycle {
    /// The state as the lowercase word stored in the database.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Active => "active",
            Self::Expired => "expired",
        }
    }

    /// Whether the silence is suppressing anything right now.
    #[must_use]
    pub fn is_in_force(self) -> bool {
        matches!(self, Self::Active)
    }
}

impl FromStr for SilenceLifecycle {
    type Err = CoreError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "pending" => Ok(Self::Pending),
            "active" => Ok(Self::Active),
            "expired" => Ok(Self::Expired),
            other => Err(CoreError::UnknownVariant {
                kind: "silence state",
                value: other.to_owned(),
            }),
        }
    }
}

/// One silence as Alertmanager currently reports it, for the syncer to diff against.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SilenceState {
    /// The id Alertmanager assigned.
    pub am_id: String,

    /// The fingerprints this silence currently suppresses.
    ///
    /// Supplied by the caller from Alertmanager's alert list rather than evaluated locally: the
    /// bot's matcher implementation agreeing with Alertmanager's is a property worth testing, not
    /// one worth relying on to decide which cards to recolour.
    pub suppresses: Vec<Fingerprint>,

    /// Where the silence is in its life.
    pub state: SilenceLifecycle,

    /// When it expires.
    pub ends_at: DateTime<Utc>,

    /// When this snapshot was taken.
    pub observed_at: DateTime<Utc>,
}
