//! The trait implementation: one method per use case, one transaction per method.
//!
//! Every method that writes more than one row opens `BEGIN IMMEDIATE` and closes it before
//! returning, so no transaction ever crosses a trait boundary. `SQLite`'s default deferred
//! transaction takes its write lock at the first write rather than at `BEGIN`, which turns two
//! read-then-write transactions into a busy error instead of a queue. The outbox claim is
//! exactly that shape.
//!
//! The `Store` impl below only forwards. Each use case is a function named after the trait method
//! it implements, in the module for its concern: `alerts`, `notifications`, `outbox`, `routing`
//! or `audit`, the same split as `dam_store`. `health` is the exception, as it touches no table.

/// Selects `alerts` rows into [`crate::convert::AlertRow`], with `$tail` appended after the table.
///
/// The column list lives here once. The query macros take only string literals, so a `const`
/// cannot be spliced in; a literal passed through `macro_rules!` can, and the result is still
/// checked against the schema like any other query.
macro_rules! select_alerts {
    ($tail:literal $(, $arg:expr)* $(,)?) => {
        sqlx::query_as!(
            crate::convert::AlertRow,
            "SELECT fingerprint AS \"fingerprint!\", labels_hash, group_key, labels, annotations, starts_at, ends_at, \
             generator_url, status, am_state, silenced_by, inhibited_by, first_seen_at, \
             last_seen_at, resolved_at, flap_count, updated_at FROM alerts " + $tail
            $(, $arg)*
        )
    };
}

/// Selects `notifications` rows into [`crate::convert::NotificationRow`].
macro_rules! select_notifications {
    ($tail:literal $(, $arg:expr)* $(,)?) => {
        sqlx::query_as!(
            crate::convert::NotificationRow,
            "SELECT id, dedupe_key, fingerprint, route_id, guild_id, channel_id, message_id, \
             thread_id, state, render_hash, applied_tags, tags_hash, pinned, archived, \
             responded_at, escalated_at, supersedes, resolved_at, reply_count, created_at, \
             updated_at FROM notifications " + $tail
            $(, $arg)*
        )
    };
}

/// Selects `outbox` rows into [`crate::convert::OutboxRow`].
macro_rules! select_outbox {
    ($tail:literal $(, $arg:expr)* $(,)?) => {
        sqlx::query_as!(
            crate::convert::OutboxRow,
            "SELECT id, lane, dedupe_key, payload, not_before, attempts, claimed_by, claimed_at, \
             last_error, created_at FROM outbox " + $tail
            $(, $arg)*
        )
    };
}

/// Selects `silences` rows into [`crate::convert::SilenceRow`].
macro_rules! select_silences {
    ($tail:literal $(, $arg:expr)* $(,)?) => {
        sqlx::query_as!(
            crate::convert::SilenceRow,
            "SELECT am_id AS \"am_id!\", matchers, starts_at, ends_at, created_by, discord_user_id, \
             origin_message, comment, state, synced_at FROM silences " + $tail
            $(, $arg)*
        )
    };
}

mod alerts;
mod audit;
mod notifications;
mod outbox;
mod routing;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use dam_core::{AlertDelta, DedupeKey, Fingerprint, NotificationState};
use dam_store::{
    AckCommand, AckOutcome, Acknowledgement, AlertQuery, AlertRecord, AppliedEffect, AuditEntry,
    ChannelId, ClaimRequest, Decision, ForumTag, GuildId, IgnoreId, IgnoreRule, IngestBatch,
    IngestOutcome, NewOutboxItem, Notification, NotificationId, OutboxId, OutboxItem, Page,
    PruneReport, RetentionPolicy, Route, RouteId, SilenceLink, SilenceState, Store, StoreError,
    Subscription, SubscriptionId, ThreadReply, UserId, WorkerId,
};
use sqlx::{Sqlite, Transaction};

use crate::SqliteStore;
use crate::convert::backend;

impl SqliteStore {
    /// Opens a write transaction.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::Backend`] when no connection can be acquired.
    async fn write(&self) -> Result<Transaction<'static, Sqlite>, StoreError> {
        self.pool
            .begin_with("BEGIN IMMEDIATE")
            .await
            .map_err(backend)
    }
}

#[async_trait]
impl Store for SqliteStore {
    async fn ingest_batch(&self, batch: &IngestBatch) -> Result<IngestOutcome, StoreError> {
        alerts::ingest_batch(self, batch).await
    }

    async fn alert(&self, fingerprint: &Fingerprint) -> Result<Option<AlertRecord>, StoreError> {
        alerts::alert(self, fingerprint).await
    }

    async fn query_alerts(&self, query: &AlertQuery) -> Result<Page<AlertRecord>, StoreError> {
        alerts::query_alerts(self, query).await
    }

    async fn firing_alerts_named(
        &self,
        alertname: Option<&str>,
    ) -> Result<Vec<AlertRecord>, StoreError> {
        alerts::firing_alerts_named(self, alertname).await
    }

    async fn firing_not_in(
        &self,
        present: &[Fingerprint],
        cutoff: DateTime<Utc>,
    ) -> Result<Vec<AlertRecord>, StoreError> {
        alerts::firing_not_in(self, present, cutoff).await
    }

    async fn apply_decision(&self, decision: &Decision) -> Result<Vec<NotificationId>, StoreError> {
        notifications::apply_decision(self, decision).await
    }

    async fn enqueue_effects(
        &self,
        items: &[NewOutboxItem],
        at: DateTime<Utc>,
    ) -> Result<(), StoreError> {
        outbox::enqueue_effects(self, items, at).await
    }

    async fn claim_outbox(
        &self,
        worker: &WorkerId,
        request: ClaimRequest,
        now: DateTime<Utc>,
    ) -> Result<Vec<OutboxItem>, StoreError> {
        outbox::claim_outbox(self, worker, request, now).await
    }

    async fn complete_outbox(
        &self,
        worker: &WorkerId,
        id: OutboxId,
        applied: &AppliedEffect,
    ) -> Result<(), StoreError> {
        outbox::complete_outbox(self, worker, id, applied).await
    }

    async fn fail_outbox(
        &self,
        worker: &WorkerId,
        id: OutboxId,
        error: &str,
        retry_at: Option<DateTime<Utc>>,
    ) -> Result<(), StoreError> {
        outbox::fail_outbox(self, worker, id, error, retry_at).await
    }

    async fn reclaim_expired(
        &self,
        older_than: DateTime<Utc>,
        now: DateTime<Utc>,
    ) -> Result<u64, StoreError> {
        outbox::reclaim_expired(self, older_than, now).await
    }

    async fn outbox_depth(&self) -> Result<Vec<(String, u64)>, StoreError> {
        outbox::outbox_depth(self).await
    }

    async fn acknowledge(&self, command: &AckCommand) -> Result<AckOutcome, StoreError> {
        notifications::acknowledge(self, command).await
    }

    async fn acknowledgement(
        &self,
        fingerprint: &Fingerprint,
    ) -> Result<Option<Acknowledgement>, StoreError> {
        notifications::acknowledgement(self, fingerprint).await
    }

    async fn record_reply(&self, reply: &ThreadReply) -> Result<Option<Notification>, StoreError> {
        notifications::record_reply(self, reply).await
    }

    async fn notification_for(
        &self,
        key: &DedupeKey,
        channel: ChannelId,
    ) -> Result<Option<Notification>, StoreError> {
        notifications::notification_for(self, key, channel).await
    }

    async fn notification_for_thread(
        &self,
        thread: ChannelId,
    ) -> Result<Option<Notification>, StoreError> {
        notifications::notification_for_thread(self, thread).await
    }

    async fn notification(&self, id: NotificationId) -> Result<Option<Notification>, StoreError> {
        notifications::notification(self, id).await
    }

    async fn set_notification_state(
        &self,
        id: NotificationId,
        state: NotificationState,
        now: DateTime<Utc>,
    ) -> Result<(), StoreError> {
        notifications::set_notification_state(self, id, state, now).await
    }

    async fn orphan_notification(
        &self,
        id: NotificationId,
        now: DateTime<Utc>,
    ) -> Result<(), StoreError> {
        notifications::orphan_notification(self, id, now).await
    }

    async fn pinned_posts(&self, channel: ChannelId) -> Result<u32, StoreError> {
        notifications::pinned_posts(self, channel).await
    }

    async fn live_cards(
        &self,
        routes: &[RouteId],
        limit: u32,
    ) -> Result<Vec<Notification>, StoreError> {
        notifications::live_cards(self, routes, limit).await
    }

    async fn pending_escalations(
        &self,
        created_before: DateTime<Utc>,
        limit: u32,
    ) -> Result<Vec<Notification>, StoreError> {
        notifications::pending_escalations(self, created_before, limit).await
    }

    async fn mark_escalated(
        &self,
        id: NotificationId,
        at: DateTime<Utc>,
    ) -> Result<bool, StoreError> {
        notifications::mark_escalated(self, id, at).await
    }

    async fn record_silence(&self, link: &SilenceLink) -> Result<(), StoreError> {
        alerts::record_silence(self, link).await
    }

    async fn sync_silences(
        &self,
        snapshot: &[SilenceState],
        now: DateTime<Utc>,
    ) -> Result<Vec<AlertDelta>, StoreError> {
        alerts::sync_silences(self, snapshot, now).await
    }

    async fn silences(&self, active_only: bool) -> Result<Vec<SilenceLink>, StoreError> {
        alerts::silences(self, active_only).await
    }

    async fn upsert_ignore(&self, rule: &IgnoreRule) -> Result<IgnoreId, StoreError> {
        routing::upsert_ignore(self, rule).await
    }

    async fn revoke_ignore(
        &self,
        id: IgnoreId,
        guild: GuildId,
        now: DateTime<Utc>,
    ) -> Result<(), StoreError> {
        routing::revoke_ignore(self, id, guild, now).await
    }

    async fn active_ignores(
        &self,
        guild: GuildId,
        now: DateTime<Utc>,
    ) -> Result<Vec<IgnoreRule>, StoreError> {
        routing::active_ignores(self, guild, now).await
    }

    async fn upsert_route(&self, route: &Route) -> Result<RouteId, StoreError> {
        routing::upsert_route(self, route).await
    }

    async fn routes(&self) -> Result<Vec<Route>, StoreError> {
        routing::routes(self).await
    }

    async fn disable_missing_config_routes(&self, keep: &[String]) -> Result<u64, StoreError> {
        routing::disable_missing_config_routes(self, keep).await
    }

    async fn upsert_subscription(
        &self,
        subscription: &Subscription,
    ) -> Result<SubscriptionId, StoreError> {
        routing::upsert_subscription(self, subscription).await
    }

    async fn subscriptions(&self) -> Result<Vec<Subscription>, StoreError> {
        routing::subscriptions(self).await
    }

    async fn remove_subscription(
        &self,
        id: SubscriptionId,
        user: UserId,
    ) -> Result<(), StoreError> {
        routing::remove_subscription(self, id, user).await
    }

    async fn sync_forum_tags(
        &self,
        channel: ChannelId,
        tags: &[ForumTag],
    ) -> Result<(), StoreError> {
        routing::sync_forum_tags(self, channel, tags).await
    }

    async fn forum_tags(&self, channel: ChannelId) -> Result<Vec<ForumTag>, StoreError> {
        routing::forum_tags(self, channel).await
    }

    async fn append_audit(&self, entry: &AuditEntry) -> Result<(), StoreError> {
        audit::append_audit(self, entry).await
    }

    async fn prune(
        &self,
        policy: &RetentionPolicy,
        now: DateTime<Utc>,
    ) -> Result<PruneReport, StoreError> {
        audit::prune(self, policy, now).await
    }

    async fn health(&self) -> Result<(), StoreError> {
        sqlx::query!("SELECT 1 AS alive")
            .fetch_one(&self.pool)
            .await
            .map_err(backend)?;

        Ok(())
    }
}
