//! The `Store` trait, the row types it moves, and the conformance suite both backends run.
//!
//! The trait's methods are use cases, not rows. `ingest_batch` appends events, upserts alert rows
//! and enqueues outbox work; a caller cannot do two of those three. Anything that has to be
//! atomic is one method, so the transaction lives inside the backend and never appears in a
//! signature.
//!
//! # Two shapes that were rejected
//!
//! `sqlx::Any` gives one code path and takes back `RETURNING`, `FOR UPDATE SKIP LOCKED`, `JSONB`
//! and partial-index behaviour. Those are the concurrency primitives the outbox is built on, so
//! the abstraction would leak exactly where the two engines differ most.
//!
//! A trait per table — `AlertRepo`, `OutboxRepo` — forces cross-repository transactions, which
//! cannot be expressed over `dyn` without a `Transaction` object that then infects every
//! signature it touches.
//!
//! # Why `dyn` and not a generic parameter
//!
//! Monomorphising the application twice doubles compile time and binary size to save a vtable
//! dispatch, which is noise next to the network round trip on the other side of every call.
//! `Arc<dyn Store>` is also what lets the engine's unit tests supply an in-memory fake.
//!
//! # Compile-time checking is not the conformance suite
//!
//! `sqlx::query!` validates SQL against a schema. It says nothing about whether
//! `FOR UPDATE SKIP LOCKED` and `BEGIN IMMEDIATE` claim the same row once, whether two backends
//! order equal timestamps the same way, or whether both map a unique violation onto the same
//! `StoreError`. One generic test module here exercises every trait method against a `dyn Store`
//! and runs twice, once per backend.

#[cfg(feature = "conformance")]
pub mod conformance;

pub mod alerts;
pub mod audit;
pub mod ids;
pub mod notifications;
pub mod outbox;
pub mod routing;

mod error;

pub use alerts::{
    AlertQuery, AlertRecord, IngestBatch, IngestOutcome, Page, QueryMatcher, SilenceLifecycle,
    SilenceLink, SilenceState,
};
pub use audit::{AuditEntry, AuditResult, PruneReport, RetentionPolicy};
pub use error::StoreError;
pub use ids::{
    AckId, ChannelId, GuildId, IgnoreId, MessageId, NotificationId, OutboxId, RoleId, RouteId,
    Snowflake, SubscriptionId, TagId, UserId, WorkerId,
};
pub use notifications::{
    AckCommand, AckKind, AckOutcome, NewNotification, Notification, ThreadReply,
};
pub use outbox::{AppliedEffect, ClaimRequest, Effect, NewOutboxItem, OutboxItem, SilenceEffect};
pub use routing::{
    ForumPolicy, ForumTag, GroupStrategy, IgnoreRule, IgnoreScope, Mentions, Route, RouteSource,
    RouteTarget, StateTags, Subscription, ThreadKind, ThreadPolicy, ThreadTrigger,
};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use dam_core::{AlertDelta, DedupeKey, Fingerprint, NotificationState};

/// Everything the rest of the application asks of a database.
///
/// Every method is a use case. Anything that has to happen atomically is one call, so the
/// transaction opens and closes inside the backend and no signature ever mentions one. That is
/// what makes `Arc<dyn Store>` workable: a trait whose methods each need a caller-held
/// transaction cannot be made object-safe without threading a transaction type through every
/// signature it touches.
#[async_trait]
pub trait Store: Send + Sync + 'static {
    /// Appends events, upserts alert rows and enqueues nothing.
    ///
    /// The webhook's whole write path. Returns the changes that were not duplicates, so a
    /// redelivery costs one transaction and produces no card edits. Deciding what to do about
    /// those changes is the pipeline's job, not this one's.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::Backend`] when the database is unreachable or refuses the write.
    async fn ingest_batch(&self, batch: &IngestBatch) -> Result<IngestOutcome, StoreError>;

    /// The stored view of one alert.
    ///
    /// # Errors
    ///
    /// As [`Store::ingest_batch`].
    async fn alert(&self, fingerprint: &Fingerprint) -> Result<Option<AlertRecord>, StoreError>;

    /// A filtered, paginated read of the alert table.
    ///
    /// The fallback for `/alerts list` when Alertmanager is unreachable, and the only way to see
    /// an alert Alertmanager has already garbage-collected.
    ///
    /// # Errors
    ///
    /// As [`Store::ingest_batch`].
    async fn query_alerts(&self, query: &AlertQuery) -> Result<Page<AlertRecord>, StoreError>;

    /// Fingerprints the local state calls firing that Alertmanager no longer reports.
    ///
    /// The reconciler's half of converging after an outage. Alertmanager forgetting an alert is
    /// how a lost `resolved` webhook is detected, and requiring two consecutive polls to agree is
    /// what stops a single failed request from resolving everything at once.
    ///
    /// # Errors
    ///
    /// As [`Store::ingest_batch`].
    async fn firing_not_in(
        &self,
        present: &[Fingerprint],
        now: DateTime<Utc>,
    ) -> Result<Vec<AlertRecord>, StoreError>;

    /// Records the decision to notify, and enqueues the effects, in one transaction.
    ///
    /// Creating the notification row and enqueuing its post have to be atomic. Enqueuing first
    /// risks an effect referring to a row that does not exist; writing the row first risks a card
    /// nobody ever posts.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::Conflict`] when another worker created the same card first, which
    /// the caller resolves by re-reading rather than by retrying.
    async fn apply_decision(&self, decision: &Decision) -> Result<Vec<NotificationId>, StoreError>;

    /// Takes up to `request.limit` claimable items for this worker.
    ///
    /// # Errors
    ///
    /// As [`Store::ingest_batch`].
    async fn claim_outbox(
        &self,
        worker: &WorkerId,
        request: ClaimRequest,
        now: DateTime<Utc>,
    ) -> Result<Vec<OutboxItem>, StoreError>;

    /// Clears a finished item and writes what it changed, in one transaction.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::LeaseLost`] when the claim expired and another worker took the item.
    async fn complete_outbox(
        &self,
        worker: &WorkerId,
        id: OutboxId,
        applied: &AppliedEffect,
    ) -> Result<(), StoreError>;

    /// Releases a failed item for a later attempt, or abandons it when `retry_at` is `None`.
    ///
    /// # Errors
    ///
    /// As [`Store::complete_outbox`].
    async fn fail_outbox(
        &self,
        worker: &WorkerId,
        id: OutboxId,
        error: &str,
        retry_at: Option<DateTime<Utc>>,
    ) -> Result<(), StoreError>;

    /// Returns leases older than `older_than` to the claimable pool.
    ///
    /// # Errors
    ///
    /// As [`Store::ingest_batch`].
    async fn reclaim_expired(
        &self,
        older_than: DateTime<Utc>,
        now: DateTime<Utc>,
    ) -> Result<u64, StoreError>;

    /// How many items are waiting, by effect kind, for the backpressure metric.
    ///
    /// # Errors
    ///
    /// As [`Store::ingest_batch`].
    async fn outbox_depth(&self) -> Result<Vec<(String, u64)>, StoreError>;

    /// Records an acknowledgement and returns every card it changes.
    ///
    /// Written and read in one transaction, so two people pressing the button at the same moment
    /// produce one acknowledgement and one set of edits.
    ///
    /// # Errors
    ///
    /// As [`Store::ingest_batch`].
    async fn acknowledge(&self, command: &AckCommand) -> Result<AckOutcome, StoreError>;

    /// Records a human reply in a card's thread.
    ///
    /// Returns the card when this reply changed it, so the first reply marks it responded and
    /// later ones only move a counter.
    ///
    /// # Errors
    ///
    /// As [`Store::ingest_batch`].
    async fn record_reply(&self, reply: &ThreadReply) -> Result<Option<Notification>, StoreError>;

    /// The card for a dedupe key in a channel, if one exists.
    ///
    /// # Errors
    ///
    /// As [`Store::ingest_batch`].
    async fn notification_for(
        &self,
        key: &DedupeKey,
        channel: ChannelId,
    ) -> Result<Option<Notification>, StoreError>;

    /// One card by its surrogate key, which is what a button carries.
    ///
    /// # Errors
    ///
    /// As [`Store::ingest_batch`].
    async fn notification(&self, id: NotificationId) -> Result<Option<Notification>, StoreError>;

    /// Moves a card to a new state and records when.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::NotFound`] when the card is gone.
    async fn set_notification_state(
        &self,
        id: NotificationId,
        state: NotificationState,
        now: DateTime<Utc>,
    ) -> Result<(), StoreError>;

    /// Records a silence created through the bot.
    ///
    /// # Errors
    ///
    /// As [`Store::ingest_batch`].
    async fn record_silence(&self, link: &SilenceLink) -> Result<(), StoreError>;

    /// Reconciles the local silence rows against Alertmanager's, and returns the alerts whose
    /// suppression changed.
    ///
    /// # Errors
    ///
    /// As [`Store::ingest_batch`].
    async fn sync_silences(
        &self,
        snapshot: &[SilenceState],
        now: DateTime<Utc>,
    ) -> Result<Vec<AlertDelta>, StoreError>;

    /// The silences the bot knows about, most recent first.
    ///
    /// # Errors
    ///
    /// As [`Store::ingest_batch`].
    async fn silences(&self, active_only: bool) -> Result<Vec<SilenceLink>, StoreError>;

    /// Creates or replaces an ignore rule.
    ///
    /// # Errors
    ///
    /// As [`Store::ingest_batch`].
    async fn upsert_ignore(&self, rule: &IgnoreRule) -> Result<IgnoreId, StoreError>;

    /// Revokes an ignore rule.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::NotFound`] when no such rule exists in that guild.
    async fn revoke_ignore(
        &self,
        id: IgnoreId,
        guild: GuildId,
        now: DateTime<Utc>,
    ) -> Result<(), StoreError>;

    /// The ignore rules in force in a guild.
    ///
    /// # Errors
    ///
    /// As [`Store::ingest_batch`].
    async fn active_ignores(
        &self,
        guild: GuildId,
        now: DateTime<Utc>,
    ) -> Result<Vec<IgnoreRule>, StoreError>;

    /// Creates or replaces a route.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::Conflict`] when the guild already has a route of that name.
    async fn upsert_route(&self, route: &Route) -> Result<RouteId, StoreError>;

    /// Every route, across every guild, for building the routing snapshot at startup.
    ///
    /// # Errors
    ///
    /// As [`Store::ingest_batch`].
    async fn routes(&self) -> Result<Vec<Route>, StoreError>;

    /// Disables the config-sourced routes whose names are not in `keep`.
    ///
    /// Disabling rather than deleting is what lets a route removed from the file keep the
    /// notifications it created, along with their history.
    ///
    /// # Errors
    ///
    /// As [`Store::ingest_batch`].
    async fn disable_missing_config_routes(&self, keep: &[String]) -> Result<u64, StoreError>;

    /// Replaces the cached tag list of a forum channel.
    ///
    /// # Errors
    ///
    /// As [`Store::ingest_batch`].
    async fn sync_forum_tags(
        &self,
        channel: ChannelId,
        tags: &[ForumTag],
    ) -> Result<(), StoreError>;

    /// The cached tags of a forum channel.
    ///
    /// # Errors
    ///
    /// As [`Store::ingest_batch`].
    async fn forum_tags(&self, channel: ChannelId) -> Result<Vec<ForumTag>, StoreError>;

    /// Appends one audit entry.
    ///
    /// # Errors
    ///
    /// As [`Store::ingest_batch`].
    async fn append_audit(&self, entry: &AuditEntry) -> Result<(), StoreError>;

    /// Deletes rows past their retention horizon, in one bounded batch.
    ///
    /// # Errors
    ///
    /// As [`Store::ingest_batch`].
    async fn prune(
        &self,
        policy: &RetentionPolicy,
        now: DateTime<Utc>,
    ) -> Result<PruneReport, StoreError>;

    /// Whether the database answers.
    ///
    /// What `/readyz` calls. Deliberately a query rather than a pool statistic: a pool can hold
    /// connections a failed-over server will refuse.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::Backend`] when it does not.
    async fn health(&self) -> Result<(), StoreError>;
}

/// What the pipeline decided to do about one change, ready to be applied atomically.
///
/// Carries the rows to write and the effects to enqueue together, because they are one
/// transaction. Splitting them into two calls would reintroduce exactly the crash window the
/// outbox exists to close: a card row with no queued post, or a queued post naming a row that
/// was never written.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Decision {
    /// Cards that do not exist yet.
    pub new_cards: Vec<PlannedCard>,

    /// Cards that do.
    pub updates: Vec<CardUpdate>,

    /// When the decision was made.
    pub at: DateTime<Utc>,
}

impl Decision {
    /// Whether there is nothing to apply.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.new_cards.is_empty() && self.updates.is_empty()
    }
}

/// A card to create, and the post that follows it.
///
/// The post is described rather than enqueued, because the row's key does not exist until the
/// insert happens. The backend writes the row and enqueues the effect with the id it just
/// received, inside one transaction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlannedCard {
    /// The row to insert.
    pub card: NewNotification,

    /// Whether the post mentions the route's roles and users.
    ///
    /// True only for a transition into firing at or above the route's mention severity.
    pub mention: bool,

    /// Earliest time the post may be sent.
    pub not_before: DateTime<Utc>,
}

/// A change to a card that already exists.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CardUpdate {
    /// The card.
    pub id: NotificationId,

    /// The state it moves to, when it moves at all.
    ///
    /// `None` for an update that re-renders without transitioning, which is what an annotation
    /// change produces.
    pub state: Option<NotificationState>,

    /// The effects to enqueue for it.
    pub effects: Vec<NewOutboxItem>,
}
