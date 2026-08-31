//! The one thing `oneshot` cannot check: that the server stops when it is told to.
//!
//! This binds an ephemeral loopback port, because the property under test is the shutdown
//! handshake between the cancellation token and the accept loop, and there is no accept loop
//! without a socket. Port 0 is used so the suite never collides with anything.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use dam_config::Ingest;
use dam_ingest::{
    AppState, IngestAccepted, IngestRequest, Readiness, ServiceError, WebhookService, serve,
};
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;

/// How long the test waits for a shutdown that should be immediate.
const PATIENCE: Duration = Duration::from_secs(5);

/// A service nothing in this file asks anything of.
struct IdleService;

#[async_trait]
impl WebhookService for IdleService {
    async fn ingest(&self, _batch: IngestRequest) -> Result<IngestAccepted, ServiceError> {
        Ok(IngestAccepted::default())
    }

    async fn readiness(&self) -> Readiness {
        Readiness {
            store_reachable: true,
            gateway_connected: true,
            last_poll_age: Some(Duration::ZERO),
        }
    }
}

fn state() -> AppState {
    let ingest = Ingest {
        // Port 0 asks the operating system for a free one, so two runs of this suite never fight
        // over a fixed port.
        bind: SocketAddr::from(([127, 0, 0, 1], 0)),
        shutdown_drain_secs: 1,
        ..Ingest::default()
    };

    AppState::new(Arc::new(IdleService), ingest, Duration::from_mins(2), None)
}

#[tokio::test]
async fn the_listener_returns_once_the_token_is_cancelled() {
    let cancel = CancellationToken::new();
    let listener = tokio::spawn(serve(state(), cancel.clone()));

    cancel.cancel();

    let outcome = timeout(PATIENCE, listener)
        .await
        .expect("the listener stops inside the drain window")
        .expect("the listener task does not panic");

    assert!(outcome.is_ok(), "the listener reported {outcome:?}");
}

#[tokio::test]
async fn an_unbindable_address_is_reported_rather_than_panicked() {
    let ingest = Ingest {
        // A port below 1024 on an address the test process does not own. Either the bind is
        // refused or the address is unavailable; both are the error path under test.
        bind: SocketAddr::from(([203, 0, 113, 1], 80)),
        ..Ingest::default()
    };

    let state = AppState::new(Arc::new(IdleService), ingest, Duration::from_mins(2), None);

    let outcome = timeout(PATIENCE, serve(state, CancellationToken::new()))
        .await
        .expect("binding fails without hanging");

    assert!(outcome.is_err());
}
