//! The axum listener: the Alertmanager webhook, the health probes and `/metrics`.
//!
//! The webhook handler writes to the database and returns. It performs no Discord I/O, because
//! Alertmanager's webhook is at-least-once with a short timeout: a Discord rate-limit stall would
//! time the request out, Alertmanager would retry, and the load would multiply during the
//! incident the bot exists to report.
//!
//! # Readiness is not liveness
//!
//! `/healthz` answers whether the process is alive. `/readyz` answers whether the store is
//! reachable, the gateway is connected, and the last successful Alertmanager poll is inside twice
//! the poll interval. Wiring `/readyz` to the Kubernetes readiness probe is what stops a bot that
//! cannot reach Alertmanager from continuing to look healthy.
//!
//! # A rejected payload is rejected loudly
//!
//! A `version` other than `4` is a 400 and a log line, not a lenient deserialisation. When
//! `truncatedAlerts` is above zero Alertmanager has dropped alerts from the body, so the batch is
//! not trustworthy on its own and an immediate reconcile is enqueued.
//!
//! The bearer token is compared with `subtle::ConstantTimeEq`, and a mismatch is a 401 carrying
//! no detail about which half of it was wrong.
//!
//! # One inbound port, and no database
//!
//! Everything behind the handlers is reached through [`WebhookService`]. This crate does not
//! depend on `dam-store`, so the routes are testable against an in-memory fake and the listener
//! cannot grow a second way of writing an alert.
//!
//! The envelope itself is `dam_am`'s. It is the same document the reconciler's client already
//! models, and a second definition of it here would be a second thing to update the next time
//! Alertmanager changes a field.

pub mod app;
pub mod router;
pub mod service;

pub use app::{AppState, ServeError, serve};
pub use router::{WEBHOOK_DUPLICATES, WEBHOOK_REQUESTS, router};
pub use service::{
    CHECK_ALERTMANAGER, CHECK_GATEWAY, CHECK_STORE, IngestAccepted, IngestRequest, Readiness,
    ServiceError, WebhookService,
};
