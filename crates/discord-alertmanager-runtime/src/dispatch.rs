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

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use dam_core::{Labels, NotificationState};
use dam_discord::Renderer;
use dam_engine::{
    AlertmanagerApi, DiscordSink, Mention, MessageRef, Note, PostFlags, RoutingSnapshot,
    SharedRouting, SilenceRequest, SinkError, TagSpec, label_tag_names, load_snapshot,
};
use dam_store::{
    AppliedEffect, ClaimRequest, Effect, ForumPolicy, LaneAssignment, MessageId, OutboxItem,
    RouteTarget, SilenceEffect, Store, StoreError, WorkerId,
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

/// The inactivity window a thread reopened mid-incident is given: Discord's longest, one week.
///
/// A thread that has to be reopened to carry a change is one the pipeline is still writing to,
/// and a shorter window only brings the same refusal back on the next edit. The route's own
/// window is applied again when the alert resolves, which is the point at which a post is meant
/// to go quiet.
const REOPENED_AUTO_ARCHIVE_MINUTES: u32 = 10_080;

/// One dispatcher worker.
pub struct Dispatcher {
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
    pub fn new(
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
    ///
    /// # Panics
    ///
    /// Panics if polled outside a Tokio runtime with the time driver enabled, once the queue
    /// is empty and the loop waits before claiming again.
    pub async fn run(self, shutdown: CancellationToken) {
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
    ///
    /// The span is the root of everything one effect does, so a trace collector sees one unit of
    /// work per outbox row rather than a flat stream of calls with nothing tying them together.
    /// `kind` and `lane` are bounded sets; the identifiers are not, and are fields rather than part
    /// of the name so the spans still group.
    #[tracing::instrument(
        name = "outbox item",
        skip_all,
        fields(
            worker = %self.worker,
            item = %item.id,
            kind = item.effect.kind(),
            lane = item.lane,
            attempts = item.attempts,
        )
    )]
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
            Effect::ResyncCard { notification } => self.resync(*notification).await,
            Effect::OpenThread { notification, name } => {
                self.open_thread(*notification, name).await
            }
            Effect::ThreadNote { notification, text } => self.note(*notification, text).await,
            Effect::SetTags { notification, tags } => self.retag(*notification, tags).await,
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
            } => self.pin(*notification, *pinned).await,
            Effect::DisableComponents { notification } => {
                let reference = self.message(*notification).await?;
                reopening(self.sink.as_ref(), reference.channel, || {
                    self.sink.disable_components(&reference)
                })
                .await
                .map(|((), reopened)| AppliedEffect {
                    archived: reopened.then_some(false),
                    ..AppliedEffect::default()
                })
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
        let Some(mut assembled) = self.assemble(notification, mention).await? else {
            return Err(Failure::Drop("the card can no longer be drawn".to_owned()));
        };

        if assembled.card.is_posted() {
            // Somebody already posted it — a redelivered effect, or a worker whose lease expired
            // after the call succeeded. Posting again would put two cards in the channel.
            return Err(Failure::Drop("the card is already posted".to_owned()));
        }

        // A label tag can only be created once an alert carries the value it is named after, so
        // this is the first point at which it can exist. When one was created the card is
        // assembled again, because its tag ids were resolved against the routing snapshot from
        // before the tag existed.
        if self.create_label_tags(&assembled).await {
            let Some(again) = self.assemble(notification, mention).await? else {
                return Err(Failure::Drop("the card can no longer be drawn".to_owned()));
            };
            assembled = again;
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

        match reopening(self.sink.as_ref(), reference.channel, || {
            self.sink.edit_card(&reference, &assembled.data)
        })
        .await
        {
            Ok(((), reopened)) => Ok(AppliedEffect {
                render_hash: Some(rendered.hash),
                archived: reopened.then_some(false),
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

    /// Re-renders a card and re-applies its tags, whatever the stored hashes say.
    ///
    /// The repair path. Everything an edit or a tag change normally skips on is a hash written by
    /// the last worker that thought it had succeeded, and a deployment whose cards drifted out of
    /// line — a queue emptied by hand, a restore from a backup taken behind Discord, a channel
    /// somebody rebuilt — is one where those hashes describe posts that no longer exist in that
    /// shape. So both calls go out unconditionally.
    ///
    /// Two calls in one effect, which is the exception the [`Effect`] documentation names: the
    /// desired tag set falls out of the same join that produces the card, and there is nowhere
    /// else in the system holding both.
    async fn resync(
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
        let reference = MessageRef {
            channel: assembled
                .card
                .thread_id
                .unwrap_or(assembled.card.channel_id),
            message,
        };

        let reopened = match reopening(self.sink.as_ref(), reference.channel, || {
            self.sink.edit_card(&reference, &assembled.data)
        })
        .await
        {
            Ok(((), reopened)) => reopened,
            // As for an ordinary edit: the message is gone, so the row is released and the next
            // change posts a fresh card rather than chasing one nobody can see.
            Err(SinkError::UnknownMessage) => {
                if let Err(error) = self
                    .store
                    .orphan_notification(notification, Utc::now())
                    .await
                {
                    warn!(%error, "cannot release a deleted card");
                }

                return Err(Failure::Drop("the message was deleted".to_owned()));
            }
            Err(error) => return Err(Failure::from_sink(error)),
        };

        let mut applied = AppliedEffect {
            render_hash: Some(rendered.hash),
            archived: reopened.then_some(false),
            ..AppliedEffect::default()
        };

        // Only a forum post has tags, and only a forum post has a thread to set them on. A text
        // card is finished at the edit above.
        if assembled.target.target.is_forum() {
            // Discord makes a post's id its thread's id, so a posted forum card always carries
            // one. A card that does not is a broken invariant rather than a state to handle: it
            // is logged, and the re-render is still recorded rather than the whole repair being
            // thrown away over the half of it that cannot run.
            let Some(post) = assembled.card.thread_id else {
                warn!(notification = %notification, "a posted forum card has no thread to tag");
                return Ok(applied);
            };

            let tags = assembled.target.tags.clone();

            // The edit above is idempotent, so a tag call that fails costs one redundant edit on
            // the retry rather than a card left half repaired.
            let ((), retagged) = reopening(self.sink.as_ref(), post, || {
                self.sink.set_post_tags(post, &tags)
            })
            .await
            .map_err(Failure::from_sink)?;

            applied.tags_hash = Some(tags_hash(&tags));
            applied.applied_tags = Some(tags);
            applied.archived = (reopened || retagged).then_some(false);
        }

        Ok(applied)
    }

    /// Creates the label tags a forum post needs that its channel does not have yet.
    ///
    /// Only for a route whose policy sets `manage_tags`. Returns whether a tag was created and the
    /// routing snapshot rebuilt with it. Every failure is logged and ends in `false`: a missing tag
    /// is a worse reason to lose a notification than any tag is a reason to have one, so the post
    /// goes out with the tags that already resolve.
    ///
    /// Discord replaces a channel's tag list wholesale, so two workers creating tags in one channel
    /// at once can each drop the other's new tag. If Discord refuses a post for naming a dropped
    /// tag, the retry assembles the card again and creates the tag again.
    async fn create_label_tags(&self, assembled: &crate::cards::Assembled) -> bool {
        let RouteTarget::Forum { channel, policy } = &assembled.target.target else {
            return false;
        };

        if !policy.manage_tags {
            return false;
        }

        let missing = missing_label_tags(
            &self.routing.load(),
            *channel,
            policy,
            &assembled.data.alert.labels,
        );

        if missing.is_empty() {
            return false;
        }

        let tags = match self.sink.ensure_forum_tags(*channel, &missing).await {
            Ok(tags) => tags,
            Err(error) => {
                warn!(%error, channel = channel.get(), "cannot create a forum's label tags");
                return false;
            }
        };

        // Without `MANAGE_CHANNELS`, or at the tag ceiling, the sink returns the tags that were
        // already there, and there is nothing new to resolve the card against.
        if !missing
            .iter()
            .any(|spec| tags.iter().any(|tag| tag.name == spec.name))
        {
            return false;
        }

        if let Err(error) = self.store.sync_forum_tags(*channel, &tags).await {
            warn!(%error, channel = channel.get(), "cannot cache forum tags");
            return false;
        }

        match load_snapshot(self.store.as_ref(), Utc::now()).await {
            Ok(rebuilt) => {
                self.routing.store(rebuilt);
                true
            }
            Err(error) => {
                warn!(%error, "cannot rebuild the routing snapshot");
                false
            }
        }
    }

    /// Applies a post's tags, skipping the call when the post already carries that set.
    ///
    /// Compared here rather than when the effect is planned. Tag changes coalesce in the queue, so
    /// the row this worker holds is the latest desired set, and the stored hash is the set the
    /// last completed call applied. A comparison at planning time would read the stored hash while
    /// an older change was still queued, skip a revert to the applied set, and let the queued
    /// change land last.
    async fn retag(
        &self,
        notification: dam_store::NotificationId,
        tags: &[dam_store::TagId],
    ) -> Result<AppliedEffect, Failure> {
        let card = self.card(notification).await?;

        let thread = card
            .thread_id
            .ok_or_else(|| Failure::Drop("the card has no thread".to_owned()))?;

        let hash = tags_hash(tags);
        if !card.needs_tag_update(&hash) {
            debug!(notification = %notification, "skipping a tag write that would change nothing");
            return Ok(AppliedEffect::default());
        }

        reopening(self.sink.as_ref(), thread, || {
            self.sink.set_post_tags(thread, tags)
        })
        .await
        .map(|((), reopened)| AppliedEffect {
            applied_tags: Some(tags.to_vec()),
            tags_hash: Some(hash),
            archived: reopened.then_some(false),
            ..AppliedEffect::default()
        })
        .map_err(Failure::from_sink)
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
        let note = Note {
            text: text.to_owned(),
        };

        // A resolved post is archived and a flap has to reopen one, so the reopen is an ordinary
        // step here rather than a retry after a failure.
        reopening(self.sink.as_ref(), thread, || {
            self.sink.post_thread_note(thread, &note)
        })
        .await
        .map(|((), reopened)| AppliedEffect {
            archived: reopened.then_some(false),
            ..AppliedEffect::default()
        })
        .map_err(Failure::from_sink)
    }

    /// Moves a post in or out of a forum's pinned tray.
    ///
    /// A pin is a convenience (the "needs attention" row at the top of a forum index) and never
    /// a reason to hold up or lose a notification, so every refusal here ends in `Ok` and the card
    /// carries on with the pin it already had.
    async fn pin(
        &self,
        notification: dam_store::NotificationId,
        pinned: bool,
    ) -> Result<AppliedEffect, Failure> {
        let card = self.card(notification).await?;

        let thread = card
            .thread_id
            .ok_or_else(|| Failure::Drop("the card has no thread".to_owned()))?;

        if pinned && !self.pin_allowed(&card).await? {
            debug!(%notification, "the route's pin limit is reached; carrying on");
            return Ok(AppliedEffect::default());
        }

        match self.sink.set_post_pinned(thread, pinned).await {
            Ok(()) => Ok(AppliedEffect {
                pinned: Some(pinned),
                ..AppliedEffect::default()
            }),
            // Discord gives a forum one pinned post, which is fewer than a busy channel asks for,
            // and a full channel stays full for as long as the post holding the slot is open.
            // Expected rather than broken, so not a warning: the card keeps its unpinned state,
            // and the plan asks again the next time the card changes state.
            Err(SinkError::PinLimitReached) => {
                debug!(%notification, "the channel has no free pin; carrying on");
                Ok(AppliedEffect::default())
            }
            Err(error) => {
                warn!(%notification, %error, "cannot change a post's pin; carrying on");
                Ok(AppliedEffect::default())
            }
        }
    }

    /// Whether a card's route leaves room for one more pinned post in its channel.
    ///
    /// Counted from the stored flags, so two workers pinning in the same channel at once can both
    /// see room. Discord's own limit of one pinned post per forum still holds in that case, and
    /// its refusal is handled by the caller like any other.
    async fn pin_allowed(&self, card: &dam_store::Notification) -> Result<bool, Failure> {
        let snapshot = self.routing.load();
        let Some(RouteTarget::Forum { policy, .. }) =
            snapshot.route(card.route_id).map(|route| &route.target)
        else {
            return Ok(true);
        };

        let pinned = self
            .store
            .pinned_posts(card.channel_id)
            .await
            .map_err(|error| Failure::Retry(error.to_string(), 5))?;

        Ok(has_pin_room(pinned, policy.max_pinned))
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
        let card = self.card(notification).await?;

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

        let channel = card.thread_id.unwrap_or(card.channel_id);

        reopening(self.sink.as_ref(), channel, || {
            self.sink.post_escalation(channel, &mentions, &text)
        })
        .await
        .map(|((), reopened)| AppliedEffect {
            archived: reopened.then_some(false),
            ..AppliedEffect::default()
        })
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
        let card = self.card(notification).await?;

        let message: MessageId = card
            .message_id
            .ok_or_else(|| Failure::Drop("the card has not been posted yet".to_owned()))?;

        Ok(MessageRef {
            channel: card.thread_id.unwrap_or(card.channel_id),
            message,
        })
    }

    /// Reads a card's row, dropping the effect when the row is gone.
    async fn card(
        &self,
        notification: dam_store::NotificationId,
    ) -> Result<dam_store::Notification, Failure> {
        self.store
            .notification(notification)
            .await
            .map_err(|error| Failure::Retry(error.to_string(), 5))?
            .ok_or_else(|| Failure::Drop("the card is gone".to_owned()))
    }

    /// A card's thread, which for a forum post is the post itself.
    async fn thread(
        &self,
        notification: dam_store::NotificationId,
    ) -> Result<dam_store::ChannelId, Failure> {
        let card = self.card(notification).await?;

        card.thread_id
            .ok_or_else(|| Failure::Drop("the card has no thread".to_owned()))
    }
}

/// Runs a call against a thread, reopening the thread once when it turns out to be archived.
///
/// Discord archives a thread on its own once its inactivity window passes, and tells nobody:
/// editing a message is not activity, so a long-lived card's post closes underneath the pipeline
/// while the stored flag still says open. The plan therefore carries no reopen step, and every
/// write the card needs is refused. Retrying that refusal cannot help — an archived thread stays
/// archived until something reopens it — so the reopen happens here, and the `bool` says whether
/// it did, for the caller to record against the card.
///
/// A reopen that is itself refused surfaces as the failure it is: a locked thread, or a missing
/// `MANAGE_THREADS`, reaches an operator rather than being spent on ten identical attempts.
async fn reopening<T, F, Fut>(
    sink: &dyn DiscordSink,
    thread: dam_store::ChannelId,
    call: F,
) -> Result<(T, bool), SinkError>
where
    F: Fn() -> Fut,
    Fut: Future<Output = Result<T, SinkError>>,
{
    match call().await {
        Err(SinkError::ThreadArchived) => {
            sink.set_post_flags(
                thread,
                PostFlags {
                    archived: false,
                    locked: false,
                    auto_archive_minutes: REOPENED_AUTO_ARCHIVE_MINUTES,
                },
            )
            .await?;

            metrics::counter!("dam_thread_reopened_total").increment(1);

            call().await.map(|value| (value, true))
        }
        other => other.map(|value| (value, false)),
    }
}

/// What to do about an effect that did not succeed.
enum Failure {
    /// A failure worth another attempt, after the given number of seconds.
    Retry(String, i64),

    /// A failure no attempt can fix, for the stated reason.
    Drop(String),

    /// A failure no attempt can fix, which an administrator has to hear about.
    ///
    /// Separate from `Drop` because the two are different problems. A dropped effect is one whose
    /// subject has moved on, such as a deleted message or an alert somebody already answered. A blocked
    /// one is the bot being unable to do its job on this route at all, and nothing about the
    /// route will fix itself.
    Blocked(String),
}

#[expect(
    clippy::needless_pass_by_value,
    reason = "both are used as function items in `map_err`, which hands over an owned error; taking a reference would put a closure at every one of their call sites"
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

/// Whether a channel holding `pinned` of the bot's posts may take one more under `max_pinned`.
///
/// A `max_pinned` of zero turns pinning off for the route.
fn has_pin_room(pinned: u32, max_pinned: u32) -> bool {
    pinned < max_pinned
}

/// The label tags a forum post wants that its channel does not have, each named once.
///
/// Created non-moderated, so the bot can apply them as the thread's owner without
/// `MANAGE_THREADS`.
fn missing_label_tags(
    snapshot: &RoutingSnapshot,
    channel: dam_store::ChannelId,
    policy: &ForumPolicy,
    labels: &Labels,
) -> Vec<TagSpec> {
    let mut missing: Vec<TagSpec> = Vec::new();

    for name in label_tag_names(policy, labels) {
        if snapshot.tag_id(channel, &name).is_none()
            && !missing.iter().any(|spec| spec.name == name)
        {
            missing.push(TagSpec {
                name,
                moderated: false,
            });
        }
    }

    missing
}

/// Hashes a tag set into the fingerprint stored as `tags_hash` when tags are written.
///
/// Order-sensitive: `desired_tags` returns a set in a fixed priority order, so the same set always
/// hashes the same.
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
    use std::sync::Mutex;

    use async_trait::async_trait;
    use dam_engine::{CardData, CardTarget, PostedMessage, TagSpec};
    use dam_store::{ChannelId, ForumTag};

    use super::*;

    /// A sink that records what it was asked to do and refuses notes until the thread is reopened.
    ///
    /// Written by hand rather than mocked: the whole point of the test is the order of three
    /// calls, which a recorded list states more plainly than an expectation does.
    #[derive(Default)]
    struct FakeSink {
        calls: Mutex<Vec<String>>,
        archived: Mutex<bool>,
        reopen: Option<SinkError>,
    }

    impl FakeSink {
        /// A sink whose thread is archived, and which lets it be reopened.
        fn archived() -> Self {
            Self {
                archived: Mutex::new(true),
                ..Self::default()
            }
        }

        /// A sink whose thread is archived and refuses to reopen, as a locked one does.
        fn sealed(error: SinkError) -> Self {
            Self {
                archived: Mutex::new(true),
                reopen: Some(error),
                ..Self::default()
            }
        }

        /// What it was asked to do, in order.
        fn calls(&self) -> Vec<String> {
            self.calls
                .lock()
                .expect("the test holds no poisoned lock")
                .clone()
        }

        /// Records one call.
        fn record(&self, call: &str) {
            self.calls
                .lock()
                .expect("the test holds no poisoned lock")
                .push(call.to_owned());
        }
    }

    #[async_trait]
    impl DiscordSink for FakeSink {
        async fn post_thread_note(
            &self,
            _thread: ChannelId,
            _note: &Note,
        ) -> Result<(), SinkError> {
            self.record("note");

            if *self
                .archived
                .lock()
                .expect("the test holds no poisoned lock")
            {
                return Err(SinkError::ThreadArchived);
            }

            Ok(())
        }

        async fn set_post_flags(
            &self,
            _thread: ChannelId,
            flags: PostFlags,
        ) -> Result<(), SinkError> {
            self.record("flags");

            if let Some(error) = self.reopen.clone() {
                return Err(error);
            }

            *self
                .archived
                .lock()
                .expect("the test holds no poisoned lock") = flags.archived;

            Ok(())
        }

        async fn post_card(
            &self,
            _target: &CardTarget,
            _card: &CardData,
        ) -> Result<PostedMessage, SinkError> {
            unimplemented!("the reopen tests post no cards")
        }

        async fn create_forum_post(
            &self,
            _target: &CardTarget,
            _card: &CardData,
        ) -> Result<PostedMessage, SinkError> {
            unimplemented!("the reopen tests post no cards")
        }

        async fn edit_card(
            &self,
            _message: &MessageRef,
            _card: &CardData,
        ) -> Result<(), SinkError> {
            unimplemented!("the reopen tests edit no cards")
        }

        async fn open_thread(
            &self,
            _message: &MessageRef,
            _name: &str,
        ) -> Result<dam_store::ChannelId, SinkError> {
            unimplemented!("the reopen tests open no threads")
        }

        async fn post_escalation(
            &self,
            _channel: ChannelId,
            _mentions: &[Mention],
            _text: &str,
        ) -> Result<(), SinkError> {
            unimplemented!("the reopen tests escalate nothing")
        }

        async fn disable_components(&self, _message: &MessageRef) -> Result<(), SinkError> {
            unimplemented!("the reopen tests disable nothing")
        }

        async fn set_post_tags(
            &self,
            _thread: ChannelId,
            _tags: &[dam_store::TagId],
        ) -> Result<(), SinkError> {
            unimplemented!("the reopen tests set no tags")
        }

        async fn set_post_pinned(
            &self,
            _thread: ChannelId,
            _pinned: bool,
        ) -> Result<(), SinkError> {
            unimplemented!("the reopen tests pin nothing")
        }

        async fn forum_tags(&self, _forum: ChannelId) -> Result<Vec<ForumTag>, SinkError> {
            unimplemented!("the reopen tests read no tags")
        }

        async fn ensure_forum_tags(
            &self,
            _forum: ChannelId,
            _want: &[TagSpec],
        ) -> Result<Vec<ForumTag>, SinkError> {
            unimplemented!("the reopen tests create no tags")
        }
    }

    /// The note the reopen tests post.
    fn note() -> Note {
        Note {
            text: "still firing".to_owned(),
        }
    }

    #[tokio::test]
    async fn an_archived_thread_is_reopened_and_the_call_made_again() {
        let sink = FakeSink::archived();
        let thread = ChannelId::new(1);

        let note = note();

        let outcome = reopening(&sink, thread, || sink.post_thread_note(thread, &note)).await;

        assert_eq!(outcome, Ok(((), true)));
        assert_eq!(sink.calls(), vec!["note", "flags", "note"]);
    }

    #[tokio::test]
    async fn an_open_thread_is_left_alone() {
        let sink = FakeSink::default();
        let thread = ChannelId::new(1);

        let note = note();

        let outcome = reopening(&sink, thread, || sink.post_thread_note(thread, &note)).await;

        assert_eq!(outcome, Ok(((), false)));
        assert_eq!(sink.calls(), vec!["note"]);
    }

    #[tokio::test]
    async fn a_thread_that_will_not_reopen_reports_why_instead_of_trying_again() {
        let sink = FakeSink::sealed(SinkError::ThreadLocked);
        let thread = ChannelId::new(1);

        let note = note();

        let outcome = reopening(&sink, thread, || sink.post_thread_note(thread, &note)).await;

        assert_eq!(outcome, Err(SinkError::ThreadLocked));
        assert_eq!(sink.calls(), vec!["note", "flags"]);
    }

    #[test]
    fn an_unchanged_tag_set_hashes_the_same() {
        let tags = vec![dam_store::TagId::new(1), dam_store::TagId::new(2)];

        assert_eq!(tags_hash(&tags), tags_hash(&tags.clone()));
        assert_ne!(tags_hash(&tags), tags_hash(&[dam_store::TagId::new(1)]));
    }

    /// A forum policy that names two labels as tags.
    fn labelled_policy() -> ForumPolicy {
        ForumPolicy {
            title_template: "{{ labels.alertname }}".to_owned(),
            manage_tags: true,
            state_tags: dam_store::StateTags {
                firing: "firing".to_owned(),
                acked: "acked".to_owned(),
                silenced: "silenced".to_owned(),
                resolved: "resolved".to_owned(),
            },
            severity_tags: true,
            label_tags: vec!["namespace".to_owned(), "team".to_owned()],
            default_tag: None,
            auto_archive_minutes: 10_080,
            archive_on_resolve: true,
            lock_on_resolve: false,
            pin_min_severity: None,
            max_pinned: 1,
            bump_on_state_change: true,
        }
    }

    /// Builds a label set.
    fn labels(pairs: &[(&str, &str)]) -> Labels {
        pairs
            .iter()
            .map(|(name, value)| {
                (
                    dam_core::LabelName::new(*name).expect("the test label name is valid"),
                    (*value).to_owned(),
                )
            })
            .collect()
    }

    #[test]
    fn only_label_tags_the_channel_lacks_are_created_and_each_once() {
        let forum = ChannelId::new(200);
        let snapshot = RoutingSnapshot::new(
            Vec::new(),
            Vec::new(),
            vec![ForumTag {
                channel_id: forum,
                name: "payments".to_owned(),
                id: dam_store::TagId::new(1),
                moderated: false,
                synced_at: Utc::now(),
            }],
        );
        let mut policy = labelled_policy();
        policy.label_tags.push("owner".to_owned());

        let missing = missing_label_tags(
            &snapshot,
            forum,
            &policy,
            &labels(&[("namespace", "payments"), ("team", "sre"), ("owner", "sre")]),
        );

        assert_eq!(
            missing,
            vec![TagSpec {
                name: "sre".to_owned(),
                moderated: false,
            }],
            "an existing tag is not created again, and two labels sharing a value create one tag"
        );
    }

    #[test]
    fn a_tag_in_another_channel_does_not_count() {
        let snapshot = RoutingSnapshot::new(
            Vec::new(),
            Vec::new(),
            vec![ForumTag {
                channel_id: ChannelId::new(300),
                name: "payments".to_owned(),
                id: dam_store::TagId::new(1),
                moderated: false,
                synced_at: Utc::now(),
            }],
        );

        let missing = missing_label_tags(
            &snapshot,
            ChannelId::new(200),
            &labelled_policy(),
            &labels(&[("namespace", "payments")]),
        );

        assert_eq!(missing.len(), 1);
    }

    #[test]
    fn pins_stop_at_the_route_limit_and_zero_turns_them_off() {
        assert!(has_pin_room(0, 1));
        assert!(!has_pin_room(1, 1));
        assert!(has_pin_room(4, 5));
        assert!(!has_pin_room(0, 0));
    }

    #[test]
    fn the_backoff_doubles_and_then_stops() {
        assert_eq!(backoff_secs(0), 1);
        assert_eq!(backoff_secs(4), 16);
        assert_eq!(backoff_secs(20), MAX_BACKOFF_SECS);
    }
}
