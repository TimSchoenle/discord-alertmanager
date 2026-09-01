//! Everything that joins the store, the engine and the two clients.
//!
//! The listener knows how to speak HTTP and nothing else; the engine knows what an alert change
//! means and does no I/O. This is the piece that joins them: it reads what the decision needs,
//! calls it, and writes back what it decided, which is exactly the work neither of those crates
//! should be doing.
//!
//! Every source of change ends in the same place. A webhook, a reconciler pass and a silence sync
//! all produce a list of `AlertDelta`, and all three hand it to [`PipelineService::apply_deltas`].
//! Anything a card does about a change is therefore decided once, whichever way the change
//! arrived.

use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use dam_core::{AlertDelta, AlertStatus, DedupeKey, EventSource, Fingerprint, NotificationState};
use dam_engine::{
    AlertFilter, AlertmanagerApi, AmError, DecisionSettings, ExistingCards, SharedRouting,
    SharedStorm, StormCounter, decide, dedupe_keys, delivery_channel, suppressed_fingerprints,
};
use dam_ingest::{IngestAccepted, IngestRequest, Readiness, ServiceError, WebhookService};
use dam_store::{IngestBatch, RetentionPolicy, SilenceState, Store, StoreError, WorkerId};
use tracing::{debug, warn};

use crate::admin::AdminChannel;

/// Cards one escalation sweep may claim.
///
/// A bound rather than a page: the sweep runs on a cadence, so what it does not reach this time
/// it reaches on the next tick, and a storm that has left thousands of cards unanswered should not
/// produce thousands of mentions in one breath.
const ESCALATION_BATCH: u32 = 50;

/// The key the deadman's notice is remembered under.
///
/// One condition, so one key: the bot has gone silent, and it says so once until it stops being
/// true.
const DEADMAN_KEY: &str = "deadman";

/// The pipeline behind the webhook, the reconciler and the command handlers.
///
/// Holds the collaborators rather than constructing them, so one instance serves every source of
/// change: the decision is the same one whichever produced it.
pub(crate) struct PipelineService {
    store: Arc<dyn Store>,
    alertmanager: Arc<dyn AlertmanagerApi>,
    routing: Arc<SharedRouting>,
    storm: Arc<SharedStorm>,
    counter: Mutex<StormCounter>,
    admin: Arc<AdminChannel>,
    settings: DecisionSettings,
    retention: RetentionPolicy,
    lease: Duration,
    deadman: chrono::Duration,
    gateway_connected: Arc<AtomicBool>,
    last_poll: Arc<AtomicI64>,
    previous_poll: Arc<AtomicI64>,
    last_webhook: AtomicI64,
}

impl PipelineService {
    /// Builds the service around an already-open store, the clients and an initial snapshot.
    ///
    /// The snapshot is passed in rather than loaded here, because the same one is published again
    /// on every route change and the thing that publishes it is not this type.
    #[expect(
        clippy::too_many_arguments,
        reason = "the composition root hands the pipeline every collaborator it owns; folding \
                  them into a struct would move the same list one line up"
    )]
    pub(crate) fn new(
        store: Arc<dyn Store>,
        alertmanager: Arc<dyn AlertmanagerApi>,
        routing: Arc<SharedRouting>,
        storm_state: Arc<SharedStorm>,
        counter: StormCounter,
        admin: Arc<AdminChannel>,
        settings: DecisionSettings,
        retention: RetentionPolicy,
        lease: Duration,
        deadman: chrono::Duration,
    ) -> Self {
        Self {
            store,
            alertmanager,
            routing,
            storm: storm_state,
            counter: Mutex::new(counter),
            admin,
            settings,
            retention,
            lease,
            deadman,
            gateway_connected: Arc::new(AtomicBool::new(false)),
            last_poll: Arc::new(AtomicI64::new(0)),
            previous_poll: Arc::new(AtomicI64::new(0)),
            last_webhook: AtomicI64::new(0),
        }
    }

    /// The flag the gateway task sets while it holds a session.
    ///
    /// Shared rather than queried: readiness is asked far more often than the connection changes,
    /// and asking the gateway would answer a question about the network with a question about the
    /// network.
    pub(crate) fn gateway_flag(&self) -> Arc<AtomicBool> {
        Arc::clone(&self.gateway_connected)
    }

    /// Decides what a batch of accepted changes does, and writes the decisions down.
    ///
    /// Reads the cards each delta might touch, calls the pure decision, and applies it. Every
    /// source of change comes through here, so a webhook and a reconciler pass cannot disagree
    /// about what a resolve means.
    ///
    /// # Errors
    ///
    /// Returns the store's error, so the webhook can answer 503 for a database that is merely
    /// down.
    pub(crate) async fn apply_deltas(&self, deltas: &[AlertDelta]) -> Result<(), StoreError> {
        let snapshot = self.routing.load();
        let now = Utc::now();
        let storm = self.count_storm(&snapshot, deltas, now);

        for delta in deltas {
            let severity = delta.alert.severity();
            let mut existing = ExistingCards::new();

            for route in snapshot.resolve(&delta.alert.labels, severity) {
                // The engine's own mapping, not `RouteTarget::channel`: a direct message has no
                // channel and stands in the user's id, and reading the pair differently here from
                // the way the decision reads it would hide every existing card on a DM route and
                // post a second one on every change.
                let channel = delivery_channel(&route.target);

                // The decision's own key function, for the same reason. A group route, a route in
                // digest mode and an alert in its second firing episode are each keyed
                // differently, and computing a key a second way here would look up a card that
                // does not exist and then create one the unique index refuses.
                let mut keys = dedupe_keys(delta, route, &storm, &self.settings, now);

                // The episode before this one, so a card that replaces it can link back. Read
                // here rather than inside the decision, which does no I/O by design.
                if let Some(previous) = delta.episode.checked_sub(1) {
                    keys.push(DedupeKey::per_alert(&delta.alert.fingerprint, previous));
                }

                for key in keys {
                    if let Some(card) = self.store.notification_for(&key, channel).await? {
                        existing.insert((channel, key), card);
                    }
                }
            }

            // Whether the alert is acknowledged is a property of the alert, not of one card. A
            // re-fire has to preserve it, or an alert somebody is already working flaps back to
            // unclaimed and pages the channel again.
            let acknowledged = self
                .store
                .acknowledgement(&delta.alert.fingerprint)
                .await?
                .is_some()
                || existing
                    .values()
                    .any(|card| card.state == NotificationState::Acked);

            let decision = decide(
                delta,
                &snapshot,
                &storm,
                &existing,
                acknowledged,
                &self.settings,
                now,
            );

            if decision.is_empty() {
                continue;
            }

            match self.store.apply_decision(&decision).await {
                Ok(_) => {}
                // Another worker created the same card first. Re-reading is the whole answer: the
                // winner's row is there, and the next change edits it.
                Err(StoreError::Conflict { constraint }) => {
                    debug!(constraint, "another worker created this card first");
                }
                Err(error) => return Err(error),
            }
        }

        Ok(())
    }

    /// Folds a batch into the storm counter and publishes what it now says.
    ///
    /// Once per batch rather than once per alert: every delta in one delivery has to reach the
    /// same verdict, or half of a webhook's alerts land on cards of their own and the other half
    /// on a digest.
    fn count_storm(
        &self,
        snapshot: &dam_engine::RoutingSnapshot,
        deltas: &[AlertDelta],
        now: DateTime<Utc>,
    ) -> Arc<dam_engine::StormState> {
        {
            let mut counter = match self.counter.lock() {
                Ok(counter) => counter,
                // The counter is an observation, not a ledger. A poisoned lock means a previous
                // holder panicked; refusing to notify anybody about it would be the worse
                // outcome.
                Err(poisoned) => poisoned.into_inner(),
            };

            for delta in deltas {
                for route in snapshot.resolve(&delta.alert.labels, delta.alert.severity()) {
                    counter.observe(route.id, delta.observed_at);
                }
            }

            self.storm.store(counter.snapshot(now));
        }

        self.storm.load()
    }

    /// Mentions the escalation targets of every card that has gone unanswered too long.
    ///
    /// The deadline belongs to the route, so the store is asked for everything older than the
    /// shortest deadline any route sets — the widest net any of them could need — and each card is
    /// then judged against its own route's. A route with no escalation policy takes no part, which
    /// is every route until somebody configures one, and the query is skipped outright when that
    /// is all of them.
    ///
    /// # Errors
    ///
    /// Returns the store's error.
    pub(crate) async fn escalate(&self) -> Result<u64, StoreError> {
        let snapshot = self.routing.load();
        let now = Utc::now();

        let Some(soonest) = snapshot
            .routes()
            .iter()
            .filter_map(|route| route.escalation.as_ref())
            .map(dam_store::Escalation::after)
            .min()
        else {
            return Ok(0);
        };

        let candidates = self
            .store
            .pending_escalations(now - soonest, ESCALATION_BATCH)
            .await?;

        let mut escalated = 0;

        for card in candidates {
            let Some(route) = snapshot.route(card.route_id) else {
                continue;
            };

            let Some(policy) = route.escalation.as_ref() else {
                continue;
            };

            if now - card.created_at < policy.after() {
                continue;
            }

            // The claim is what makes this idempotent. Two processes sweeping at once both see
            // the card and one of them writes the mention.
            if !self.store.mark_escalated(card.id, now).await? {
                continue;
            }

            self.store
                .enqueue_effects(
                    &[dam_store::NewOutboxItem::now(
                        dam_store::Effect::Escalate {
                            notification: card.id,
                            roles: policy.roles.clone(),
                            users: policy.users.clone(),
                        },
                        card.dedupe_key.clone(),
                        now,
                    )],
                    now,
                )
                .await?;

            escalated += 1;
        }

        if escalated > 0 {
            metrics::counter!("dam_escalations_total").increment(escalated);
        }

        Ok(escalated)
    }

    /// Says so, once, when the bot has gone quiet and cannot tell whether that is the truth.
    ///
    /// Both halves are needed. No webhook inside the window alone is a quiet night; an
    /// unreachable Alertmanager alone is an outage the reconciler is already logging. Together
    /// they are the case nothing else can report, because the channel that would report it is the
    /// one that has gone quiet.
    async fn check_deadman(&self, alertmanager_reachable: bool, now: DateTime<Utc>) {
        let stamped = self.last_webhook.load(Ordering::Relaxed);

        // A process that has never received one has nothing to compare against. A deployment
        // whose Alertmanager has no webhook receiver configured would otherwise be told it had
        // gone silent every minute from boot.
        let quiet = stamped != 0
            && DateTime::from_timestamp(stamped, 0).is_some_and(|last| now - last > self.deadman);

        if quiet && !alertmanager_reachable {
            self.admin
                .say_once(
                    DEADMAN_KEY.to_owned(),
                    format!(
                        "No webhook has arrived in {} minutes and Alertmanager cannot be reached. \
                         Alerts may be firing without anybody being told.",
                        self.deadman.num_minutes()
                    ),
                )
                .await;

            return;
        }

        // Cleared on recovery rather than left set, so the next time this happens it is reported
        // again. A deadman that fires once per process lifetime is a deadman for one incident.
        self.admin.forget(DEADMAN_KEY);
    }

    /// Polls Alertmanager and converges the local state onto what it holds.
    ///
    /// Two halves. Everything Alertmanager has goes through the same ingest path a webhook uses,
    /// so anything it holds that the database does not becomes a synthetic event. Then anything
    /// the database still calls firing that Alertmanager has not mentioned since the *previous*
    /// poll is resolved — two consecutive polls have to agree, or one failed request would resolve
    /// every alert at once.
    ///
    /// # Errors
    ///
    /// Returns [`AmError`] when Alertmanager cannot be reached or its answer does not decode. A
    /// database failure is logged rather than returned, because the poll clock that readiness
    /// reads should move whenever Alertmanager itself answered.
    pub(crate) async fn reconcile(&self) -> Result<usize, AmError> {
        let started = Utc::now();
        let alerts = match self
            .alertmanager
            .list_alerts(&AlertFilter::everything())
            .await
        {
            Ok(alerts) => alerts,
            Err(error) => {
                // The one place that knows Alertmanager is unreachable, which is half of what the
                // deadman needs. The other half is how long ago a webhook last arrived.
                self.check_deadman(false, started).await;
                return Err(error);
            }
        };

        self.check_deadman(true, started).await;

        let count = alerts.len();
        let present: Vec<Fingerprint> = alerts
            .iter()
            .map(|alert| alert.fingerprint.clone())
            .collect();

        match self
            .store
            .ingest_batch(&IngestBatch::new(EventSource::Reconciler, alerts, started))
            .await
        {
            Ok(outcome) => {
                if !outcome.deltas.is_empty() {
                    metrics::counter!("dam_reconcile_repairs_total", "kind" => "changed")
                        .increment(outcome.deltas.len() as u64);
                }
                if let Err(error) = self.apply_deltas(&outcome.deltas).await {
                    warn!(%error, "cannot apply reconciled changes");
                }
            }
            Err(error) => warn!(%error, "cannot persist a reconciler batch"),
        }

        if let Err(error) = self.resolve_missing(&present, started).await {
            warn!(%error, "cannot resolve the alerts Alertmanager stopped reporting");
        }

        self.previous_poll
            .store(self.last_poll.load(Ordering::Relaxed), Ordering::Relaxed);
        self.last_poll.store(started.timestamp(), Ordering::Relaxed);

        Ok(count)
    }

    /// Resolves what the database still calls firing and Alertmanager has forgotten.
    async fn resolve_missing(
        &self,
        present: &[Fingerprint],
        now: DateTime<Utc>,
    ) -> Result<(), StoreError> {
        let cutoff = self.previous_poll.load(Ordering::Relaxed);
        if cutoff == 0 {
            // The first poll of a process has nothing to compare against. Resolving on it would
            // mean every restart resolved every alert Alertmanager happened not to return.
            return Ok(());
        }

        let Some(cutoff) = DateTime::from_timestamp(cutoff, 0) else {
            return Ok(());
        };

        let missing = self.store.firing_not_in(present, cutoff).await?;
        if missing.is_empty() {
            return Ok(());
        }

        metrics::counter!("dam_reconcile_repairs_total", "kind" => "resolved")
            .increment(missing.len() as u64);

        let resolved = missing
            .into_iter()
            .map(|record| dam_core::Alert {
                status: AlertStatus::Resolved,
                ends_at: record.alert.ends_at.or(Some(now)),
                ..record.alert
            })
            .collect();

        let outcome = self
            .store
            .ingest_batch(&IngestBatch::new(EventSource::Reconciler, resolved, now))
            .await?;

        self.apply_deltas(&outcome.deltas).await
    }

    /// Diffs the local silence rows against Alertmanager's, and re-renders the cards that changed.
    ///
    /// # Errors
    ///
    /// Returns [`AmError`] when Alertmanager cannot be reached.
    pub(crate) async fn sync_silences(&self) -> Result<usize, AmError> {
        let now = Utc::now();
        let silences = self.alertmanager.list_silences(&[]).await?;
        let alerts = self
            .alertmanager
            .list_alerts(&AlertFilter::everything())
            .await?;

        let snapshot: Vec<SilenceState> = silences
            .iter()
            .map(|silence| SilenceState {
                am_id: silence.id.clone(),
                // Taken from Alertmanager's own answer rather than by evaluating the silence's
                // matchers here. This bot agreeing with Alertmanager's matcher semantics is worth
                // a test; it is not worth deciding from which cards change colour.
                suppresses: suppressed_fingerprints(&alerts, &silence.id),
                state: silence.state,
                ends_at: silence.ends_at,
                observed_at: now,
            })
            .collect();

        let count = snapshot.len();

        match self.store.sync_silences(&snapshot, now).await {
            Ok(deltas) => {
                if let Err(error) = self.apply_deltas(&deltas).await {
                    warn!(%error, "cannot apply silence changes");
                }
            }
            Err(error) => warn!(%error, "cannot sync silences"),
        }

        Ok(count)
    }

    /// Returns the leases of workers that died holding one.
    ///
    /// # Errors
    ///
    /// Returns the store's error.
    pub(crate) async fn reclaim_leases(&self) -> Result<u64, StoreError> {
        // Published on the janitor's cadence rather than on one of its own: the gauge describes
        // the same queue the janitor is already reading, and a second timer would be a second
        // thing to reason about for no extra information.
        self.publish_queue_depth().await;

        let now = Utc::now();
        // Three times the lease: the margin for a worker that is slow rather than dead. Reclaiming
        // at exactly the lease would take items away from workers still running them.
        let older_than = now
            - chrono::Duration::from_std(self.lease * 3)
                .unwrap_or_else(|_| chrono::Duration::seconds(90));

        self.store.reclaim_expired(older_than, now).await
    }

    /// Deletes rows past their retention horizon.
    ///
    /// # Errors
    ///
    /// Returns the store's error.
    pub(crate) async fn prune(&self) -> Result<u64, StoreError> {
        let report = self.store.prune(&self.retention, Utc::now()).await?;

        Ok(report.events + report.alerts + report.notifications + report.audit)
    }

    /// Publishes the depth of the queue, by effect kind.
    ///
    /// The one metric that says the bot is falling behind rather than failing: a rising
    /// `post_card` depth during an incident means Discord is the bottleneck, and nothing else
    /// reports that.
    async fn publish_queue_depth(&self) {
        match self.store.outbox_depth().await {
            Ok(depths) => {
                for (kind, depth) in depths {
                    // A queue deep enough to lose precision here is one that has been broken for
                    // days; the gauge is read for its shape, not for its last digit.
                    #[expect(
                        clippy::cast_precision_loss,
                        reason = "a gauge is a f64 and a queue never reaches 2^53 items"
                    )]
                    let depth = depth as f64;

                    metrics::gauge!("dam_outbox_depth", "kind" => kind).set(depth);
                }
            }
            Err(error) => warn!(%error, "cannot read the outbox depth"),
        }
    }

    /// A worker identifier unique to this process and this index.
    ///
    /// The process id is in it deliberately: rows claimed by a previous process must look
    /// abandoned to the janitor rather than reclaimable by whoever happens to start with the same
    /// name.
    pub(crate) fn worker_id(index: u16) -> WorkerId {
        WorkerId::new(format!("{}-{index}", std::process::id()))
    }
}

#[async_trait]
impl WebhookService for PipelineService {
    async fn ingest(&self, request: IngestRequest) -> Result<IngestAccepted, ServiceError> {
        let truncated = request.truncated;

        // Stamped on arrival rather than on acceptance: the deadman asks whether Alertmanager is
        // still talking to this process, and a batch of duplicates answers that as well as a
        // batch of changes does.
        self.last_webhook
            .store(request.received_at.timestamp(), Ordering::Relaxed);

        let batch = IngestBatch {
            source: request.source,
            group_key: request.group_key,
            truncated,
            alerts: request.alerts,
            received_at: request.received_at,
        };

        let outcome = self.store.ingest_batch(&batch).await.map_err(|error| {
            warn!(%error, "cannot persist a webhook batch");
            let detail = error.to_string();
            if error.is_retryable() {
                // Alertmanager retries a 503 and gives up on a 400, so a database that is merely
                // down has to be reported as the transient failure it is.
                ServiceError::Unavailable { detail }
            } else {
                ServiceError::Internal { detail }
            }
        })?;

        let accepted = u32::try_from(outcome.deltas.len()).unwrap_or(u32::MAX);

        self.apply_deltas(&outcome.deltas)
            .await
            .map_err(|error| ServiceError::Unavailable {
                detail: error.to_string(),
            })?;

        // A truncated group means Alertmanager did not send everything it holds, so the local
        // state cannot be brought up to date from this batch alone. The reconciler's next pass
        // closes the gap; saying so here is what makes the gap explicable afterwards.
        if truncated > 0 {
            debug!(truncated, "Alertmanager dropped alerts from this batch");
            metrics::counter!("dam_webhook_truncated_total").increment(1);
        }

        metrics::counter!("dam_webhook_duplicates_total").increment(u64::from(outcome.duplicates));

        Ok(IngestAccepted {
            accepted,
            duplicates: outcome.duplicates,
        })
    }

    async fn readiness(&self) -> Readiness {
        Readiness {
            store_reachable: self.store.health().await.is_ok(),
            gateway_connected: self.gateway_connected.load(Ordering::Relaxed),
            last_poll_age: last_poll_age(&self.last_poll),
        }
    }
}

/// How long ago the last successful poll was, or `None` if there has not been one.
fn last_poll_age(clock: &AtomicI64) -> Option<Duration> {
    let stamped = clock.load(Ordering::Relaxed);
    if stamped == 0 {
        return None;
    }

    let elapsed = Utc::now().timestamp() - stamped;

    // A negative age means the clock stepped backwards, which is a reason to treat the poll as
    // fresh rather than as impossibly old: reporting not-ready because NTP moved the clock would
    // take a healthy replica out of service.
    Some(Duration::from_secs(elapsed.max(0).unsigned_abs()))
}
