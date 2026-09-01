//! What the handlers share, and the task that runs them.
//!
//! The state is one `Arc` behind a `Clone` wrapper because axum clones it per request. Holding
//! the configuration section itself rather than a copy of six of its fields means a key added to
//! `Ingest` is available here without a second struct to keep in step.

use std::future::IntoFuture;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use dam_config::{Config, Ingest};
use metrics_exporter_prometheus::PrometheusHandle;
use secrecy::SecretString;
use thiserror::Error;
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;

use crate::router::router;
use crate::service::WebhookService;

/// Everything the handlers need, cheap to clone because it is one pointer.
#[derive(Clone)]
pub struct AppState {
    inner: Arc<Inner>,
}

/// The state itself, behind the pointer.
struct Inner {
    service: Arc<dyn WebhookService>,
    ingest: Ingest,
    max_poll_age: Duration,
    metrics: Option<PrometheusHandle>,
}

impl AppState {
    /// Assembles the state from its parts.
    ///
    /// `metrics` is what mounts `/metrics`: [`None`] leaves the route off the router entirely, so
    /// a disabled exporter answers 404 rather than an empty exposition that a scraper would
    /// happily record as zero for every series.
    #[must_use]
    pub fn new(
        service: Arc<dyn WebhookService>,
        ingest: Ingest,
        max_poll_age: Duration,
        metrics: Option<PrometheusHandle>,
    ) -> Self {
        Self {
            inner: Arc::new(Inner {
                service,
                ingest,
                max_poll_age,
                metrics,
            }),
        }
    }

    /// Assembles the state from a whole loaded configuration.
    ///
    /// Reads three sections rather than one: the listener from `ingest`, whether `/metrics` is
    /// mounted from `observability`, and the reconciler's cadence from `engine`. The staleness
    /// bound `/readyz` applies is twice that cadence, so one poll may be lost — to a restart, a
    /// slow Alertmanager, a dropped connection — without the pod being pulled out of service.
    ///
    /// The handle is dropped when metrics are disabled, so passing one unconditionally is safe
    /// and the caller does not have to duplicate the check.
    #[must_use]
    pub fn from_config(
        config: &Config,
        service: Arc<dyn WebhookService>,
        metrics: Option<PrometheusHandle>,
    ) -> Self {
        let max_poll_age =
            Duration::from_secs(config.engine.reconcile_interval_secs.saturating_mul(2));
        let metrics = metrics.filter(|_| config.observability.metrics_enabled);

        Self::new(service, config.ingest.clone(), max_poll_age, metrics)
    }

    /// The pipeline behind the listener.
    #[must_use]
    pub fn service(&self) -> &Arc<dyn WebhookService> {
        &self.inner.service
    }

    /// The listener's configuration section.
    #[must_use]
    pub fn ingest(&self) -> &Ingest {
        &self.inner.ingest
    }

    /// The oldest successful Alertmanager poll `/readyz` still calls fresh.
    #[must_use]
    pub fn max_poll_age(&self) -> Duration {
        self.inner.max_poll_age
    }

    /// The Prometheus exposition handle, when metrics are enabled.
    #[must_use]
    pub fn metrics(&self) -> Option<&PrometheusHandle> {
        self.inner.metrics.as_ref()
    }

    /// The bearer token every webhook request has to carry, when one is configured.
    #[must_use]
    pub fn webhook_token(&self) -> Option<&SecretString> {
        self.inner.ingest.webhook_token.as_ref()
    }
}

/// Why the listener could not run.
#[derive(Debug, Error)]
pub enum ServeError {
    /// The configured address could not be bound.
    ///
    /// Almost always a port already in use or a bind address that does not exist in the
    /// container's network namespace, and in both cases the process should not start.
    #[error("cannot bind {address}")]
    Bind {
        /// The address that was refused.
        address: SocketAddr,
        /// The operating system's reason.
        #[source]
        source: std::io::Error,
    },

    /// The server stopped on an error rather than on the cancellation token.
    #[error("the listener stopped")]
    Stopped {
        /// What the accept loop reported.
        #[source]
        source: std::io::Error,
    },
}

/// Binds the configured address and serves until `cancel` fires.
///
/// Returns only once the server has stopped, so a caller that awaits this has awaited the
/// listener's shutdown rather than merely requested it. In-flight requests are given
/// `shutdown_drain_secs` to finish; past that the remaining connections are dropped, because a
/// client holding one open must not be able to keep a terminating pod alive indefinitely.
///
/// # Errors
///
/// Returns [`ServeError::Bind`] when the address cannot be bound and [`ServeError::Stopped`] when
/// the accept loop fails.
pub async fn serve(state: AppState, cancel: CancellationToken) -> Result<(), ServeError> {
    let address = state.ingest().bind;
    let drain = Duration::from_secs(state.ingest().shutdown_drain_secs);

    let listener = TcpListener::bind(address)
        .await
        .map_err(|source| ServeError::Bind { address, source })?;

    // The bound address rather than the configured one, so a configuration asking for port 0
    // logs the port it actually got instead of the zero nothing can connect to.
    let bound = listener.local_addr().unwrap_or(address);
    tracing::info!(address = %bound, path = %state.ingest().webhook_path, "ingest listener started");

    let served = axum::serve(listener, router(state))
        .with_graceful_shutdown(cancelled(cancel.clone()))
        .into_future();

    let outcome = tokio::select! {
        result = served => result.map_err(|source| ServeError::Stopped { source }),
        () = drain_deadline(cancel, drain) => {
            tracing::warn!(
                drain_secs = drain.as_secs(),
                "requests were still in flight at the drain deadline; dropping their connections"
            );
            Ok(())
        }
    };

    tracing::info!(address = %bound, "ingest listener stopped");

    outcome
}

/// Completes when the token is cancelled.
async fn cancelled(cancel: CancellationToken) {
    cancel.cancelled().await;
    tracing::info!("ingest listener draining");
}

/// Completes once the drain window has elapsed, and never before cancellation.
///
/// Kept separate from the graceful shutdown signal so the two race rather than nest: axum waits
/// for every connection to close, and this is the bound on that wait.
async fn drain_deadline(cancel: CancellationToken, drain: Duration) {
    cancel.cancelled().await;
    tokio::time::sleep(drain).await;
}
