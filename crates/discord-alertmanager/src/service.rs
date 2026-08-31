//! The webhook listener's view of the pipeline.
//!
//! The listener knows how to speak HTTP and nothing else; the engine knows what an alert change
//! means and does no I/O. This is the piece that joins them: it reads what the decision needs,
//! calls it, and writes back what it decided, which is exactly the work neither of those crates
//! should be doing.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use chrono::Utc;
use dam_core::NotificationState;
use dam_engine::{
    AlertFilter, AlertmanagerApi, DecisionSettings, ExistingCards, RoutingSnapshot, SharedRouting,
    decide,
};
use dam_ingest::{IngestAccepted, IngestRequest, Readiness, ServiceError, WebhookService};
use dam_store::{IngestBatch, Store};
use tracing::{debug, warn};

/// The pipeline behind the webhook.
///
/// Holds the collaborators rather than constructing them, so the same type serves the reconciler
/// and the command handlers once those are wired: the decision is the same one whichever source
/// produced the change.
pub(crate) struct PipelineService {
    store: Arc<dyn Store>,
    alertmanager: Arc<dyn AlertmanagerApi>,
    routing: Arc<SharedRouting>,
    settings: DecisionSettings,
    gateway_connected: Arc<AtomicBool>,
    last_poll: Arc<AtomicI64>,
}

impl PipelineService {
    /// Builds the service around an already-open store, a client and an initial routing snapshot.
    ///
    /// The snapshot is passed in rather than loaded here, because the same one is published again
    /// on every route change and the thing that publishes it is not this type.
    pub(crate) fn new(
        store: Arc<dyn Store>,
        alertmanager: Arc<dyn AlertmanagerApi>,
        routing: RoutingSnapshot,
    ) -> Self {
        Self {
            store,
            alertmanager,
            routing: Arc::new(SharedRouting::new(routing)),
            settings: DecisionSettings::default(),
            gateway_connected: Arc::new(AtomicBool::new(false)),
            last_poll: Arc::new(AtomicI64::new(0)),
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
}

#[async_trait]
impl WebhookService for PipelineService {
    async fn ingest(&self, request: IngestRequest) -> Result<IngestAccepted, ServiceError> {
        let batch = IngestBatch {
            source: request.source,
            group_key: request.group_key,
            truncated: request.truncated,
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
        let snapshot = self.routing.load();
        let now = Utc::now();

        for delta in &outcome.deltas {
            let severity = delta.alert.severity();
            let mut existing = ExistingCards::new();

            for route in snapshot.resolve(&delta.alert.labels, severity) {
                let Some(channel) = route.target.channel() else {
                    continue;
                };
                let key = delta.per_alert_key();

                match self.store.notification_for(&key, channel).await {
                    Ok(Some(card)) => {
                        existing.insert((channel, key), card);
                    }
                    Ok(None) => {}
                    Err(error) => {
                        warn!(%error, route = route.name, "cannot read a card");
                        return Err(ServiceError::Unavailable {
                            detail: error.to_string(),
                        });
                    }
                }
            }

            // Whether the alert is acknowledged is a property of the alert, not of one card, and
            // the cards are where it is recorded. Any card showing it acknowledged means somebody
            // took the alert, which is what a re-fire has to preserve.
            let acknowledged = existing
                .values()
                .any(|card| card.state == NotificationState::Acked);

            let decision = decide(
                delta,
                &snapshot,
                &existing,
                acknowledged,
                &self.settings,
                now,
            );
            if decision.is_empty() {
                continue;
            }

            if let Err(error) = self.store.apply_decision(&decision).await {
                warn!(%error, "cannot apply a decision");
                return Err(ServiceError::Unavailable {
                    detail: error.to_string(),
                });
            }
        }

        // A truncated group means Alertmanager did not send everything it holds, so the local
        // state cannot be brought up to date from this batch alone. The reconciler's next pass
        // closes the gap; nudging it here only shortens the window.
        if request.truncated > 0 {
            debug!(truncated = request.truncated, "batch was truncated");
        }

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

impl PipelineService {
    /// Reads Alertmanager once and stamps the poll clock.
    ///
    /// Here rather than in the reconciler task so that readiness and the reconciler agree on what
    /// a successful poll is: the clock only moves when Alertmanager actually answered.
    ///
    /// # Errors
    ///
    /// Returns the client's error unchanged, so the caller can tell an unreachable server from a
    /// rejected request.
    pub(crate) async fn poll_alertmanager(&self) -> Result<usize, dam_engine::AmError> {
        let alerts = self
            .alertmanager
            .list_alerts(&AlertFilter::everything())
            .await?;

        self.last_poll
            .store(Utc::now().timestamp(), Ordering::Relaxed);

        Ok(alerts.len())
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
