//! The inbound port: everything the HTTP layer asks of the pipeline behind it.
//!
//! One trait, deliberately. The listener knows how to authenticate a request, how to reject a
//! malformed envelope and how to answer a probe; it does not know that a database exists. That is
//! why this crate's manifest has no `dam-store` line, and it is what lets every route be tested
//! against an in-memory fake rather than against a schema.

use std::time::Duration;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use dam_core::{Alert, EventSource, GroupKey};
use thiserror::Error;

/// Name `/readyz` reports when the store cannot be reached.
pub const CHECK_STORE: &str = "store";

/// Name `/readyz` reports when the Discord gateway is not connected.
pub const CHECK_GATEWAY: &str = "gateway";

/// Name `/readyz` reports when the last successful Alertmanager poll is too old.
pub const CHECK_ALERTMANAGER: &str = "alertmanager_poll";

/// One delivery of alerts, as it leaves the HTTP layer.
///
/// The same shape the reconciler produces, so the write path behind the trait is written once and
/// the two sources differ only in `source` and in whether `truncated` can be above zero.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IngestRequest {
    /// Where the batch came from. Always [`EventSource::Webhook`] on this path.
    pub source: EventSource,

    /// The group Alertmanager delivered the batch under, when it sent one.
    pub group_key: Option<GroupKey>,

    /// Alerts Alertmanager dropped from the body before sending it.
    ///
    /// Above zero means the batch is not the whole truth. It is carried through rather than
    /// logged and forgotten, because the only correct response is to make the reconciler poll
    /// now instead of trusting a body that admits to being incomplete.
    pub truncated: u32,

    /// The alerts themselves, already converted into domain values.
    pub alerts: Vec<Alert>,

    /// When the listener received the batch.
    ///
    /// Taken once in the handler rather than inside the implementation, so every event a batch
    /// produces shares one timestamp and two alerts from one body cannot be ordered by clock
    /// jitter.
    pub received_at: DateTime<Utc>,
}

impl IngestRequest {
    /// Whether Alertmanager admitted to dropping alerts from this batch.
    #[must_use]
    pub fn is_truncated(&self) -> bool {
        self.truncated > 0
    }
}

/// What accepting a batch produced.
///
/// Returned to the handler only so it can be counted and answered with. The handler makes no
/// decision from it: the work the batch implies is already durable by the time this exists.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct IngestAccepted {
    /// Changes that survived deduplication and will be acted on.
    pub accepted: u32,

    /// Changes discarded because an identical one was already stored.
    ///
    /// Alertmanager's webhook is at-least-once, so a redelivery is routine rather than an error.
    /// Counting them is how a retry storm is told apart from a genuine alert storm.
    pub duplicates: u32,
}

/// What `/readyz` needs to know, gathered by the implementation.
///
/// Three independent facts rather than one boolean, so the probe can name the dependency that is
/// down. An operator reading a 503 that says only "not ready" has to go and find out which of the
/// three it was, and that lookup happens during an incident.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Readiness {
    /// Whether the database answered its last health query.
    pub store_reachable: bool,

    /// Whether the Discord gateway session is established.
    pub gateway_connected: bool,

    /// How long ago the reconciler last completed a poll of Alertmanager.
    ///
    /// [`None`] means no poll has ever succeeded, which is the state a freshly started process is
    /// in and is not ready either.
    pub last_poll_age: Option<Duration>,
}

impl Readiness {
    /// Names of the checks currently failing, given the oldest poll still considered fresh.
    ///
    /// Ordered store, gateway, poll, so two consecutive 503 bodies for one cause compare equal
    /// and a log scraper can group on them.
    #[must_use]
    pub fn failures(&self, max_poll_age: Duration) -> Vec<&'static str> {
        let mut failures = Vec::new();

        if !self.store_reachable {
            failures.push(CHECK_STORE);
        }
        if !self.gateway_connected {
            failures.push(CHECK_GATEWAY);
        }
        if self.last_poll_age.is_none_or(|age| age > max_poll_age) {
            failures.push(CHECK_ALERTMANAGER);
        }

        failures
    }

    /// Whether every dependency is healthy at `max_poll_age`.
    #[must_use]
    pub fn is_ready(&self, max_poll_age: Duration) -> bool {
        self.failures(max_poll_age).is_empty()
    }
}

/// Why a batch could not be accepted.
///
/// Three variants because the HTTP layer has three answers to give, and Alertmanager reads them
/// differently: it retries a 503, and it does not retry a 400. Collapsing the first two would
/// either lose a batch a restart would have accepted, or make Alertmanager redeliver a body that
/// will never be accepted.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum ServiceError {
    /// A dependency the write path needs is down, and the same batch would succeed later.
    #[error("the ingest path is unavailable: {detail}")]
    Unavailable {
        /// What was unreachable, for the log line rather than for the caller.
        detail: String,
    },

    /// The batch itself is unacceptable and redelivering it will not help.
    #[error("the batch was rejected: {detail}")]
    Rejected {
        /// Why the batch cannot be stored.
        detail: String,
    },

    /// Anything else, which is a defect here rather than a problem at the caller.
    #[error("the ingest path failed: {detail}")]
    Internal {
        /// What went wrong.
        detail: String,
    },
}

/// The seam between the listener and the decision pipeline.
///
/// Object-safe on purpose: the router holds `Arc<dyn WebhookService>`, so the binary supplies one
/// implementation over the engine and the tests supply another that records what it was handed,
/// without the router being generic over either.
#[async_trait]
pub trait WebhookService: Send + Sync + 'static {
    /// Persists a batch and returns once it is durable.
    ///
    /// This is the whole of the webhook's work. An implementation must not perform outbound
    /// Discord or Alertmanager I/O here: Alertmanager's webhook has a short timeout, and a
    /// rate-limit stall inline would time the request out, earn a redelivery, and multiply the
    /// load during exactly the incident that produced the batch.
    ///
    /// # Errors
    ///
    /// Returns [`ServiceError::Unavailable`] when a dependency is down and the batch is worth
    /// redelivering, [`ServiceError::Rejected`] when it never will be, and
    /// [`ServiceError::Internal`] for anything else.
    async fn ingest(&self, batch: IngestRequest) -> Result<IngestAccepted, ServiceError>;

    /// The current state of the three dependencies `/readyz` reports on.
    ///
    /// Infallible by design. A readiness probe that can fail to answer has to decide what a
    /// failure means, and the honest answer — not ready — is already expressible.
    async fn readiness(&self) -> Readiness;
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Twice a one-minute reconcile interval, which is the bound the router applies.
    const MAX_POLL_AGE: Duration = Duration::from_mins(2);

    fn healthy() -> Readiness {
        Readiness {
            store_reachable: true,
            gateway_connected: true,
            last_poll_age: Some(Duration::from_secs(5)),
        }
    }

    #[test]
    fn a_healthy_process_reports_no_failures() {
        assert!(healthy().is_ready(MAX_POLL_AGE));
        assert!(healthy().failures(MAX_POLL_AGE).is_empty());
    }

    #[test]
    fn a_never_polled_process_is_not_ready() {
        let readiness = Readiness {
            last_poll_age: None,
            ..healthy()
        };

        assert_eq!(readiness.failures(MAX_POLL_AGE), vec![CHECK_ALERTMANAGER]);
    }

    #[test]
    fn every_failing_check_is_named() {
        let readiness = Readiness {
            store_reachable: false,
            gateway_connected: false,
            last_poll_age: Some(Duration::from_mins(10)),
        };

        assert_eq!(
            readiness.failures(MAX_POLL_AGE),
            vec![CHECK_STORE, CHECK_GATEWAY, CHECK_ALERTMANAGER]
        );
    }

    #[test]
    fn a_poll_exactly_at_the_bound_is_still_fresh() {
        let readiness = Readiness {
            last_poll_age: Some(MAX_POLL_AGE),
            ..healthy()
        };

        assert!(readiness.is_ready(MAX_POLL_AGE));
    }
}
