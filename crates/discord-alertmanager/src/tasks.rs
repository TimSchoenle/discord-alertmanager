//! The periodic work: reconciling against Alertmanager, syncing silences, reclaiming leases,
//! escalating what nobody answered, and pruning history.
//!
//! Each of these is a loop around one method on [`crate::service::PipelineService`], and each is a
//! child of the same cancellation token as the listener, so `serve` returns only once every one of
//! them has stopped.
//!
//! The reconciler is the one that matters most. The webhook is the low-latency path and it is
//! lossy — restarts, partitions, a receiver that never sent `send_resolved` — and the only thing
//! that finds out is a comparison against what Alertmanager currently holds.

use std::sync::Arc;
use std::time::Duration;

use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};

use crate::service::PipelineService;

/// What one periodic task does when it fires.
///
/// An enum rather than five near-identical spawn functions, so the cadence, the cancellation and
/// the "a failure is logged and the next tick is the retry" rule are written once.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Job {
    /// Poll Alertmanager and converge the local state onto it.
    Reconcile,

    /// Diff the local silence rows against Alertmanager's.
    SyncSilences,

    /// Return the leases of workers that died holding one.
    ReclaimLeases,

    /// Mention the escalation targets of cards nobody has answered.
    Escalate,

    /// Delete rows past their retention horizon.
    Prune,
}

impl Job {
    /// The word this job is logged under.
    fn name(self) -> &'static str {
        match self {
            Self::Reconcile => "reconcile",
            Self::SyncSilences => "sync-silences",
            Self::ReclaimLeases => "reclaim-leases",
            Self::Escalate => "escalate",
            Self::Prune => "prune",
        }
    }

    /// Runs one pass.
    ///
    /// The span is the root of that pass, so a trace collector sees the reconciler's Alertmanager
    /// poll and the pruner's deletes as separate units of work rather than as one long-lived task.
    #[tracing::instrument(name = "periodic task", skip_all, fields(job = self.name()))]
    async fn run(self, service: &PipelineService) -> Result<u64, String> {
        match self {
            Self::Reconcile => service
                .reconcile()
                .await
                .map_err(|error| error.to_string())
                .map(|count| count as u64),
            Self::SyncSilences => service
                .sync_silences()
                .await
                .map_err(|error| error.to_string())
                .map(|count| count as u64),
            Self::ReclaimLeases => service
                .reclaim_leases()
                .await
                .map_err(|error| error.to_string()),
            Self::Escalate => service.escalate().await.map_err(|error| error.to_string()),
            Self::Prune => service.prune().await.map_err(|error| error.to_string()),
        }
    }
}

/// Runs `job` every `interval` until the token is cancelled.
///
/// A failed pass is logged and not retried early: the next tick is the retry, and hammering an
/// Alertmanager that is already struggling helps nobody.
pub(crate) async fn run(
    job: Job,
    service: Arc<PipelineService>,
    interval: Duration,
    shutdown: CancellationToken,
) {
    // `max(1)`: a zero interval in the configuration would otherwise spin this loop as fast as
    // the database and Alertmanager can answer.
    let mut ticker = tokio::time::interval(interval.max(Duration::from_secs(1)));
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    loop {
        tokio::select! {
            () = shutdown.cancelled() => {
                debug!(job = job.name(), "task stopped");
                return;
            }
            _ = ticker.tick() => match job.run(&service).await {
                Ok(count) => debug!(job = job.name(), count, "task ran"),
                Err(detail) => warn!(job = job.name(), detail, "task failed"),
            },
        }
    }
}
