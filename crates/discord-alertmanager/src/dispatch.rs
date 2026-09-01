//! The workers that turn queued effects into Discord and Alertmanager calls.
//!
//! Everything reaching Discord goes through here, and nothing here decides anything. The pipeline
//! already worked out that a card is to be created rather than edited, that a thread has to be
//! reopened first, and which tags the post ends up with; a worker claims the row, makes the call,
//! and writes back what happened in the same transaction that clears the row.
//!
//! # Why the claim, the call and the write-back are three separate things
//!
//! A crash between deciding to post and posting leaves a claimable row rather than a notification
//! nobody sees. A crash between posting and recording it would post twice, which is why the
//! completion carries what Discord returned and clears the item atomically. And the lease is what
//! covers the third case, a worker that dies holding a row: the janitor releases it, and the
//! worker that comes back finds its claim gone rather than writing over somebody else's work.

use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use dam_core::NotificationState;
use dam_discord::Renderer;
use dam_engine::{
    AlertmanagerApi, DiscordSink, Mention, MessageRef, Note, PostFlags, SharedRouting,
    SilenceRequest, SinkError,
};
use dam_store::{
    AppliedEffect, ClaimRequest, Effect, LaneAssignment, MessageId, OutboxItem, SilenceEffect,
    Store, StoreError, WorkerId,
};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::admin::AdminChannel;
use crate::cards::Cards;

/// How long a worker waits after finding the queue empty.
///
/// Short enough that a card posted by a webhook does not visibly lag, long enough that an idle bot
/// is not a query per millisecond. The reconciler's cadence is measured in tens of seconds, so
/// nothing here needs to be faster than a person can notice.
const IDLE_PAUSE: Duration = Duration::from_millis(500);

/// The longest a failed item waits before its next attempt.
const MAX_BACKOFF_SECS: i64 = 300;

/// Attempts after which an item is abandoned rather than retried.
///
/// A queue that retries forever is a queue whose depth only goes up, and by the tenth attempt the
/// failure is not transient whatever it claims to be.
const MAX_ATTEMPTS: u32 = 10;

/// One dispatcher worker.
pub(crate) struct Dispatcher {
    store: Arc<dyn Store>,
    sink: Arc<dyn DiscordSink>,
    alertmanager: Arc<dyn AlertmanagerApi>,
    cards: Arc<Cards>,
    renderer: Arc<Renderer>,
    routing: Arc<SharedRouting>,
    admin: Arc<AdminChannel>,
    worker: WorkerId,
    lane: Option<LaneAssignment>,
    lease: Duration,
    batch: u32,
}

impl Dispatcher {
    /// Builds a worker that owns one slice of the lane space.
    #[expect(
        clippy::too_many_arguments,
        reason = "the composition root hands a worker every collaborator it owns; folding them \
                  into a struct would move the same list one line up"
    )]
    pub(crate) fn new(
        store: Arc<dyn Store>,
        sink: Arc<dyn DiscordSink>,
        alertmanager: Arc<dyn AlertmanagerApi>,
        cards: Arc<Cards>,
        renderer: Arc<Renderer>,
        routing: Arc<SharedRouting>,
        admin: Arc<AdminChannel>,
        worker: WorkerId,
        lane: Option<LaneAssignment>,
        lease: Duration,
        batch: u32,
    ) -> Self {
        Self {
            store,
            sink,
            alertmanager,
            cards,
            renderer,
            routing,
            admin,
            worker,
            lane,
            lease,
            batch,
        }
    }

    /// Claims and runs work until the token is cancelled.
    pub(crate) async fn run(self, shutdown: CancellationToken) {
        info!(worker = %self.worker, "dispatcher started");

        loop {
            if shutdown.is_cancelled() {
                info!(worker = %self.worker, "dispatcher stopped");
                return;
            }

            let claimed = self
                .store
                .claim_outbox(
                    &self.worker,
                    ClaimRequest {
                        lane: self.lane,
                        lease_secs: u32::try_from(self.lease.as_secs()).unwrap_or(30),
                        limit: self.batch,
                    },
                    Utc::now(),
                )
                .await;

            let items = match claimed {
                Ok(items) => items,
                Err(error) => {
                    warn!(worker = %self.worker, %error, "cannot claim outbox work");
                    Vec::new()
                }
            };

            if items.is_empty() {
                tokio::select! {
                    () = shutdown.cancelled() => {
                        info!(worker = %self.worker, "dispatcher stopped");
                        return;
                    }
                    () = tokio::time::sleep(IDLE_PAUSE) => continue,
                }
            }

            // The batch is finished even when shutdown is asked for mid-way: an item this worker
            // holds is invisible to everyone else until its lease expires, and finishing it is
            // faster than waiting out the lease.
            for item in items {
                self.handle(&item).await;
            }
        }
    }

    /// Runs one item and records what it did.
    async fn handle(&self, item: &OutboxItem) {
        let outcome = self.apply(item).await;

        let result = match outcome {
            Ok(applied) => {
                self.store
                    .complete_outbox(&self.worker, item.id, &applied)
                    .await
            }
            Err(Failure::Drop(reason)) => {
                debug!(item = %item.id, reason, "dropping an effect that cannot succeed");
                self.store
                    .fail_outbox(&self.worker, item.id, &reason, None)
                    .await
            }
            Err(Failure::Blocked(reason)) => {
                warn!(item = %item.id, reason, "a route cannot deliver");
                self.report_route_health(item, &reason).await;
                self.store
                    .fail_outbox(&self.worker, item.id, &reason, None)
                    .await
            }
            Err(Failure::Retry(detail, after)) => {
                let attempts = item.attempts;
                if attempts >= MAX_ATTEMPTS {
                    warn!(item = %item.id, attempts, detail, "abandoning an effect after too many attempts");
                    self.report_route_health(item, &detail).await;
                    self.store
                        .fail_outbox(&self.worker, item.id, &detail, None)
                        .await
                } else {
                    // The larger of what the failure asked for and what the attempt count says.
                    // A rate limit names its own delay; everything else backs off.
                    let delay = after.max(backoff_secs(attempts));
                    let retry_at = Utc::now() + chrono::Duration::seconds(delay);
                    warn!(item = %item.id, attempts, detail, "retrying an effect");
                    self.store
                        .fail_outbox(&self.worker, item.id, &detail, Some(retry_at))
                        .await
                }
            }
        };

        if let Err(error) = result {
            // A lost lease is the expected end of a slow worker's item, not a fault: somebody else
            // has it now, and the work is not lost.
            match error {
                StoreError::LeaseLost { .. } => {
                    debug!(item = %item.id, "the lease expired while this item was running");
                }
                error => warn!(item = %item.id, %error, "cannot record an effect's outcome"),
            }
        }
    }

    /// Carries out one effect.
    async fn apply(&self, item: &OutboxItem) -> Result<AppliedEffect, Failure> {
        match &item.effect {
            Effect::PostCard {
                notification,
                mention,
            } => self.post(*notification, *mention).await,
            Effect::EditCard { notification } => self.edit(*notification).await,
            Effect::OpenThread { notification, name } => {
                self.open_thread(*notification, name).await
            }
            Effect::ThreadNote { notification, text } => self.note(*notification, text).await,
            Effect::SetTags { notification, tags } => {
                let thread = self.thread(*notification).await?;
                self.sink
                    .set_post_tags(thread, tags)
                    .await
                    .map(|()| AppliedEffect {
                        applied_tags: Some(tags.clone()),
                        tags_hash: Some(tags_hash(tags)),
                        ..AppliedEffect::default()
                    })
                    .map_err(Failure::from_sink)
            }
            Effect::SetFlags {
                notification,
                archived,
                locked,
                auto_archive_minutes,
            } => {
                let thread = self.thread(*notification).await?;
                self.sink
                    .set_post_flags(
                        thread,
                        PostFlags {
                            archived: *archived,
                            locked: *locked,
                            auto_archive_minutes: *auto_archive_minutes,
                        },
                    )
                    .await
                    .map(|()| AppliedEffect {
                        archived: Some(*archived),
                        ..AppliedEffect::default()
                    })
                    .map_err(Failure::from_sink)
            }
            Effect::SetPinned {
                notification,
                pinned,
            } => {
                let thread = self.thread(*notification).await?;

                match self.sink.set_post_pinned(thread, *pinned).await {
                    Ok(()) => Ok(AppliedEffect {
                        pinned: Some(*pinned),
                        ..AppliedEffect::default()
                    }),
                    // A pin is a convenience — the "needs attention" tray at the top of a forum
                    // index — and never a reason to hold up or lose a notification.
                    Err(error) => {
                        warn!(%error, "cannot change a post's pin; carrying on");
                        Ok(AppliedEffect::default())
                    }
                }
            }
            Effect::DisableComponents { notification } => {
                let reference = self.message(*notification).await?;
                self.sink
                    .disable_components(&reference)
                    .await
                    .map(|()| AppliedEffect::default())
                    .map_err(Failure::from_sink)
            }
            Effect::Escalate {
                notification,
                roles,
                users,
            } => self.escalate(*notification, roles, users).await,
            Effect::AdminNotice { channel, text } => self
                .sink
                .post_thread_note(*channel, &Note { text: text.clone() })
                .await
                .map(|()| AppliedEffect::default())
                .map_err(Failure::from_sink),
            Effect::CreateSilence { request } => self.create_silence(request).await,
            Effect::ExpireSilence { am_id } => self
                .alertmanager
                .expire_silence(am_id)
                .await
                .map(|()| AppliedEffect::default())
                .map_err(Failure::from_am),
        }
    }

    /// Posts a card that has no message yet.
    async fn post(
        &self,
        notification: dam_store::NotificationId,
        mention: bool,
    ) -> Result<AppliedEffect, Failure> {
        let Some(assembled) = self.assemble(notification, mention).await? else {
            return Err(Failure::Drop("the card can no longer be drawn".to_owned()));
        };

        if assembled.card.is_posted() {
            // Somebody already posted it — a redelivered effect, or a worker whose lease expired
            // after the call succeeded. Posting again would put two cards in the channel.
            return Err(Failure::Drop("the card is already posted".to_owned()));
        }

        let posted = if assembled.target.target.is_forum() {
            self.sink
                .create_forum_post(&assembled.target, &assembled.data)
                .await
        } else {
            self.sink
                .post_card(&assembled.target, &assembled.data)
                .await
        }
        .map_err(Failure::from_sink)?;

        let rendered = self.renderer.render(&assembled.data);

        Ok(AppliedEffect {
            message_id: Some(posted.message),
            thread_id: posted.thread,
            render_hash: Some(rendered.hash),
            applied_tags: Some(assembled.target.tags.clone()),
            tags_hash: Some(tags_hash(&assembled.target.tags)),
            ..AppliedEffect::default()
        })
    }

    /// Re-renders a card, skipping the call when nothing a viewer can see has changed.
    async fn edit(
        &self,
        notification: dam_store::NotificationId,
    ) -> Result<AppliedEffect, Failure> {
        let Some(assembled) = self.assemble(notification, false).await? else {
            return Err(Failure::Drop("the card can no longer be drawn".to_owned()));
        };

        let Some(message) = assembled.card.message_id else {
            return Err(Failure::Drop("the card has not been posted yet".to_owned()));
        };

        let rendered = self.renderer.render(&assembled.data);

        if !assembled.card.needs_edit(&rendered.hash) {
            // The skip is what keeps an alert storm inside Discord's per-channel edit limits: a
            // burst of updates that change nothing visible costs no requests at all.
            debug!(notification = %notification, "skipping an edit that would change nothing");
            metrics::counter!("dam_render_skipped_total").increment(1);
            return Ok(AppliedEffect::default());
        }

        let reference = MessageRef {
            channel: assembled
                .card
                .thread_id
                .unwrap_or(assembled.card.channel_id),
            message,
        };

        match self.sink.edit_card(&reference, &assembled.data).await {
            Ok(()) => Ok(AppliedEffect {
                render_hash: Some(rendered.hash),
                ..AppliedEffect::default()
            }),
            // The card was deleted. Retrying the edit fails for as long as the alert lasts, so the
            // row is released instead and the next change posts a fresh card.
            Err(SinkError::UnknownMessage) => {
                if let Err(error) = self
                    .store
                    .orphan_notification(notification, Utc::now())
                    .await
                {
                    warn!(%error, "cannot release a deleted card");
                }

                Err(Failure::Drop("the message was deleted".to_owned()))
            }
            Err(error) => Err(Failure::from_sink(error)),
        }
    }

    /// Opens the thread a route's policy asks for.
    async fn open_thread(
        &self,
        notification: dam_store::NotificationId,
        name: &str,
    ) -> Result<AppliedEffect, Failure> {
        let reference = self.message(notification).await?;

        self.sink
            .open_thread(&reference, name)
            .await
            .map(|thread| AppliedEffect {
                thread_id: Some(thread),
                ..AppliedEffect::default()
            })
            .map_err(Failure::from_sink)
    }

    /// Posts a one-line note into a card's thread.
    async fn note(
        &self,
        notification: dam_store::NotificationId,
        text: &str,
    ) -> Result<AppliedEffect, Failure> {
        let thread = self.thread(notification).await?;

        match self
            .sink
            .post_thread_note(
                thread,
                &Note {
                    text: text.to_owned(),
                },
            )
            .await
        {
            Ok(()) => Ok(AppliedEffect::default()),
            // A resolved post is archived and a flap has to reopen one, so this is an ordinary
            // step rather than a retry after a failure.
            Err(SinkError::ThreadArchived) => {
                self.sink
                    .set_post_flags(
                        thread,
                        PostFlags {
                            archived: false,
                            locked: false,
                            auto_archive_minutes: 10_080,
                        },
                    )
                    .await
                    .map_err(Failure::from_sink)?;

                self.sink
                    .post_thread_note(
                        thread,
                        &Note {
                            text: text.to_owned(),
                        },
                    )
                    .await
                    .map(|()| AppliedEffect {
                        archived: Some(false),
                        ..AppliedEffect::default()
                    })
                    .map_err(Failure::from_sink)
            }
            Err(error) => Err(Failure::from_sink(error)),
        }
    }

    /// Mentions a route's escalation targets about a card nobody has taken.
    ///
    /// Into the card's thread where it has one, and beside the card in its channel where it does
    /// not: a route with no thread policy would otherwise be the one route that cannot escalate,
    /// which is also the route where a missed alert is hardest to notice.
    async fn escalate(
        &self,
        notification: dam_store::NotificationId,
        roles: &[dam_store::RoleId],
        users: &[dam_store::UserId],
    ) -> Result<AppliedEffect, Failure> {
        let card = self
            .store
            .notification(notification)
            .await
            .map_err(|error| Failure::Retry(error.to_string(), 5))?
            .ok_or_else(|| Failure::Drop("the card is gone".to_owned()))?;

        // Somebody answered it between the sweep claiming the card and this running. The claim is
        // not given back: the alert has been seen, which is the whole thing an escalation asks
        // for.
        if card.state != NotificationState::Firing {
            return Err(Failure::Drop(
                "the card was answered before its escalation ran".to_owned(),
            ));
        }

        let mentions: Vec<Mention> = roles
            .iter()
            .copied()
            .map(Mention::Role)
            .chain(users.iter().copied().map(Mention::User))
            .collect();

        let text = format!(
            "Still firing and unacknowledged since <t:{}:R>.",
            card.created_at.timestamp()
        );

        self.sink
            .post_escalation(card.thread_id.unwrap_or(card.channel_id), &mentions, &text)
            .await
            .map(|()| AppliedEffect::default())
            .map_err(Failure::from_sink)
    }

    /// Tells an administrator that a route has stopped delivering, once per route.
    ///
    /// Named by route rather than by card, because the condition is the route's: a missing
    /// permission swallows every card the route produces, and one message per card would fill the
    /// channel an operator is trying to read.
    async fn report_route_health(&self, item: &OutboxItem, reason: &str) {
        let Some(notification) = item.effect.notification() else {
            return;
        };

        let Ok(Some(card)) = self.store.notification(notification).await else {
            return;
        };

        let snapshot = self.routing.load();
        let name = snapshot
            .route(card.route_id)
            .map_or_else(|| card.route_id.to_string(), |route| route.name.clone());

        self.admin
            .say_once(
                format!("route-health:{}", card.route_id),
                format!(
                    "Route `{name}` is not delivering: {reason}. Cards for it are being dropped \
                     until this is fixed."
                ),
            )
            .await;
    }

    /// Creates or replaces an Alertmanager silence.
    async fn create_silence(&self, request: &SilenceEffect) -> Result<AppliedEffect, Failure> {
        let matchers = dam_core::MatcherSet::parse(&request.matchers).map_err(|error| {
            Failure::Drop(format!("the silence's matchers no longer parse: {error}"))
        })?;

        let am_id = self
            .alertmanager
            .upsert_silence(&SilenceRequest {
                // Set on a retry, which Alertmanager treats as an update. Creating is not
                // idempotent: without the id, every retry would leave another silence behind.
                id: request.am_id.clone(),
                matchers,
                starts_at: request.starts_at,
                ends_at: request.ends_at,
                created_by: request.created_by.clone(),
                comment: request.comment.clone(),
            })
            .await
            .map_err(Failure::from_am)?;

        Ok(AppliedEffect {
            am_silence_id: Some(am_id),
            ..AppliedEffect::default()
        })
    }

    /// Reads a card, mapping a database failure into a retry.
    async fn assemble(
        &self,
        notification: dam_store::NotificationId,
        mention: bool,
    ) -> Result<Option<crate::cards::Assembled>, Failure> {
        self.cards
            .assemble(notification, mention)
            .await
            .map_err(|error| Failure::Retry(error.to_string(), 5))
    }

    /// Where a card's message lives.
    async fn message(
        &self,
        notification: dam_store::NotificationId,
    ) -> Result<MessageRef, Failure> {
        let card = self
            .store
            .notification(notification)
            .await
            .map_err(|error| Failure::Retry(error.to_string(), 5))?
            .ok_or_else(|| Failure::Drop("the card is gone".to_owned()))?;

        let message: MessageId = card
            .message_id
            .ok_or_else(|| Failure::Drop("the card has not been posted yet".to_owned()))?;

        Ok(MessageRef {
            channel: card.thread_id.unwrap_or(card.channel_id),
            message,
        })
    }

    /// A card's thread, which for a forum post is the post itself.
    async fn thread(
        &self,
        notification: dam_store::NotificationId,
    ) -> Result<dam_store::ChannelId, Failure> {
        let card = self
            .store
            .notification(notification)
            .await
            .map_err(|error| Failure::Retry(error.to_string(), 5))?
            .ok_or_else(|| Failure::Drop("the card is gone".to_owned()))?;

        card.thread_id
            .ok_or_else(|| Failure::Drop("the card has no thread".to_owned()))
    }
}

/// What to do about an effect that did not succeed.
enum Failure {
    /// Try again after this many seconds.
    Retry(String, i64),

    /// Never try again, for the stated reason.
    Drop(String),

    /// Never try again, and tell an administrator.
    ///
    /// Separate from `Drop` because the two are different problems. A dropped effect is one whose
    /// subject has moved on — a deleted message, an alert somebody already answered. A blocked
    /// one is the bot being unable to do its job on this route at all, and nothing about the
    /// route will fix itself.
    Blocked(String),
}

#[expect(
    clippy::needless_pass_by_value,
    reason = "both are used as function items in `map_err`, which hands over an owned error;               taking a reference would put a closure at every one of their call sites"
)]
impl Failure {
    /// Maps a Discord failure onto retry or drop.
    fn from_sink(error: SinkError) -> Self {
        let detail = error.to_string();

        if let Some(retry_after_ms) = error.retry_after_ms() {
            metrics::counter!("dam_discord_rate_limited_total").increment(1);
            return Self::Retry(
                detail,
                i64::try_from(retry_after_ms / 1000).unwrap_or(1).max(1),
            );
        }

        if error.is_retryable() {
            return Self::Retry(detail, 5);
        }

        // A permission and a channel of the wrong kind are configuration, not weather. Retrying
        // costs requests and changes nothing, and nobody finds out unless somebody is told.
        if matches!(
            error,
            SinkError::MissingPermissions { .. } | SinkError::WrongChannelType { .. }
        ) {
            return Self::Blocked(detail);
        }

        Self::Drop(detail)
    }

    /// Maps an Alertmanager failure onto retry or drop.
    ///
    /// A 4xx is never retried: the request is wrong, and repeating it verbatim produces the same
    /// answer more expensively.
    fn from_am(error: dam_engine::AmError) -> Self {
        let detail = error.to_string();

        if error.is_retryable() {
            Self::Retry(detail, 10)
        } else {
            Self::Drop(detail)
        }
    }
}

/// Hashes a tag set, so a card that ticks over from firing to acknowledged is one tag change and
/// an unchanged one is none.
fn tags_hash(tags: &[dam_store::TagId]) -> String {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;

    for tag in tags {
        for byte in tag.get().to_be_bytes() {
            hash ^= u64::from(byte);
            hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
        }
    }

    format!("{hash:016x}")
}

/// The backoff an item waits after its `attempts`-th failure.
///
/// Doubling, capped, and deliberately not jittered per item: the queue is claimed in batches and
/// two workers cannot hold the same row, so the thundering herd this would otherwise smooth out
/// does not exist here.
fn backoff_secs(attempts: u32) -> i64 {
    let doubled = 1_i64
        .checked_shl(attempts.min(20))
        .unwrap_or(MAX_BACKOFF_SECS);

    doubled.min(MAX_BACKOFF_SECS)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_unchanged_tag_set_hashes_the_same() {
        let tags = vec![dam_store::TagId::new(1), dam_store::TagId::new(2)];

        assert_eq!(tags_hash(&tags), tags_hash(&tags.clone()));
        assert_ne!(tags_hash(&tags), tags_hash(&[dam_store::TagId::new(1)]));
    }

    #[test]
    fn the_backoff_doubles_and_then_stops() {
        assert_eq!(backoff_secs(0), 1);
        assert_eq!(backoff_secs(4), 16);
        assert_eq!(backoff_secs(20), MAX_BACKOFF_SECS);
    }
}
