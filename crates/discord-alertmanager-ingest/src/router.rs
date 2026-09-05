//! The four routes, the bearer check in front of one of them, and the layers around all of them.
//!
//! The webhook handler parses, converts and hands the batch to the service. It answers as soon as
//! the batch is durable and does nothing else, because every millisecond spent here is a
//! millisecond of Alertmanager's short webhook timeout, and a timeout earns a redelivery of the
//! same body.
//!
//! # Why the bearer check is a layer and not the first line of the handler
//!
//! A handler runs after its extractors, so authenticating inside one means an unauthenticated
//! caller has already had a body buffered on its behalf. The check runs as a `route_layer`
//! instead: it sees the headers, it never sees the body, and an unauthenticated request costs
//! nothing beyond a constant-time comparison.

use std::sync::Once;
use std::time::Duration;

use axum::Router;
use axum::body::Bytes;
use axum::extract::{DefaultBodyLimit, Request, State};
use axum::http::{HeaderMap, StatusCode, header};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Json, Response};
use axum::routing::{get, post};
use chrono::Utc;
use dam_am::WebhookPayload;
use dam_core::{EventSource, GroupKey};
use secrecy::{ExposeSecret, SecretString};
use serde::Serialize;
use subtle::ConstantTimeEq;
use tower_http::limit::RequestBodyLimitLayer;
use tower_http::timeout::TimeoutLayer;
use tower_http::trace::TraceLayer;

use crate::app::AppState;
use crate::service::{IngestRequest, ServiceError};

/// Webhook requests, labelled by how each one ended.
pub const WEBHOOK_REQUESTS: &str = "dam_webhook_requests_total";

/// Alert changes discarded because an identical one was already stored.
pub const WEBHOOK_DUPLICATES: &str = "dam_webhook_duplicates_total";

/// Content type of the Prometheus text exposition format.
const PROMETHEUS_CONTENT_TYPE: &str = "text/plain; version=0.0.4; charset=utf-8";

/// A batch that reached the service.
const RESULT_ACCEPTED: &str = "accepted";

/// A request whose bearer token was missing or wrong.
const RESULT_UNAUTHORIZED: &str = "unauthorized";

/// A body that was not the envelope.
const RESULT_MALFORMED: &str = "malformed";

/// An envelope declaring a version this listener does not implement.
const RESULT_BAD_VERSION: &str = "bad_version";

/// An envelope carrying an alert that cannot become a domain value.
const RESULT_INVALID_ALERT: &str = "invalid_alert";

/// A batch the service could not take because a dependency is down.
const RESULT_UNAVAILABLE: &str = "unavailable";

/// A batch the service refused outright.
const RESULT_REJECTED: &str = "rejected";

/// A failure that is a defect in this process.
const RESULT_ERROR: &str = "error";

/// The listener's routes, with the body limit, timeout and tracing layers around them.
///
/// `/metrics` is present only when the state carries an exposition handle, so a deployment with
/// metrics switched off answers 404 there rather than serving an empty exposition that a scraper
/// would record as zero for every series.
///
/// # Panics
///
/// Panics when the configured webhook path is not a path axum can route on. A leading slash is
/// added when it is missing; anything else — a brace, a wildcard in a position axum reserves — is
/// a configuration error that must stop the process rather than silently serve a different path.
pub fn router(state: AppState) -> Router {
    describe_metrics();

    let path = routable_path(&state.ingest().webhook_path);
    let body_limit = state.ingest().body_limit_bytes;
    let timeout = Duration::from_secs(state.ingest().request_timeout_secs);

    let webhook = Router::new()
        .route(&path, post(ingest_webhook))
        .route_layer(middleware::from_fn_with_state(
            state.clone(),
            require_bearer,
        ))
        // `DefaultBodyLimit::disable()` first, or axum's own 2 MiB cap would silently override a
        // larger configured one and the configuration key would be a lie above that size.
        .layer(DefaultBodyLimit::disable())
        .layer(RequestBodyLimitLayer::new(body_limit));

    let mut probes = Router::new()
        .route("/healthz", get(healthz))
        .route("/readyz", get(readyz));

    if state.metrics().is_some() {
        probes = probes.route("/metrics", get(render_metrics));
    }

    webhook
        .merge(probes)
        // 408 rather than a dropped connection: Alertmanager logs a status and retries, where a
        // reset socket looks like a network fault and is harder to attribute to this listener.
        .layer(TimeoutLayer::with_status_code(
            StatusCode::REQUEST_TIMEOUT,
            timeout,
        ))
        .layer(TraceLayer::new_for_http())
        .with_state(state)
}

/// What a 200 carries back to Alertmanager.
///
/// Alertmanager ignores the body of a 2xx entirely. It is here for the operator running `curl`
/// against the listener, for whom "accepted 3, 1 duplicate" answers the question that brought
/// them.
#[derive(Debug, Serialize)]
struct AcceptedBody {
    accepted: u32,
    duplicates: u32,
}

/// What a rejection carries back.
#[derive(Debug, Serialize)]
struct ErrorBody {
    error: String,
}

/// What `/readyz` carries back, ready or not.
#[derive(Debug, Serialize)]
struct ReadinessBody {
    status: &'static str,
    failed: Vec<&'static str>,
}

/// Accepts one version-4 envelope.
///
/// The span is the root of everything one delivery causes, so a trace collector sees one unit of
/// work per batch. `TraceLayer` above it makes its own span at `debug`, which is below what the
/// exporters this process is wired to record, so this one is the root rather than a child of it.
#[tracing::instrument(
    name = "webhook",
    skip_all,
    fields(
        bytes = body.len(),
        group_key = tracing::field::Empty,
        alerts = tracing::field::Empty,
    )
)]
async fn ingest_webhook(State(state): State<AppState>, body: Bytes) -> Response {
    let payload = match serde_json::from_slice::<WebhookPayload>(&body) {
        Ok(payload) => payload,
        Err(error) => {
            tracing::warn!(%error, bytes = body.len(), "rejected a webhook body");
            return refuse(
                RESULT_MALFORMED,
                StatusCode::BAD_REQUEST,
                &error.to_string(),
            );
        }
    };

    if let Err(error) = payload.ensure_supported() {
        // Loud rather than lenient. A version this listener does not implement means the fields
        // it just read may mean something else, and quietly ingesting them would put wrong alerts
        // on cards instead of putting an error in front of whoever upgraded.
        tracing::warn!(
            version = %payload.version,
            "refused a webhook envelope of an unsupported version"
        );

        return refuse(
            RESULT_BAD_VERSION,
            StatusCode::BAD_REQUEST,
            &error.to_string(),
        );
    }

    let truncated = payload.truncated_alerts;
    // Kept at the batch level as well as on every alert: a route that posts one card per group
    // has nothing else to key on, and the envelope is the only place Alertmanager reveals it.
    let group_key = Some(GroupKey::new(payload.group_key.clone()));

    // Recorded rather than declared, because neither is known until the body has parsed, and a
    // span opened after the parse would not cover the rejection when it does not.
    let span = tracing::Span::current();
    span.record("group_key", payload.group_key.as_str());
    span.record("alerts", payload.alerts.len());

    let alerts = match payload.into_alerts() {
        Ok(alerts) => alerts,
        Err(error) => {
            tracing::warn!(%error, "refused a webhook envelope this bot cannot represent");
            return refuse(
                RESULT_INVALID_ALERT,
                StatusCode::BAD_REQUEST,
                &error.to_string(),
            );
        }
    };

    if truncated > 0 {
        // Carried into the batch as well as logged. The pipeline turns it into an immediate
        // reconcile, because a body that admits to dropping alerts cannot be trusted to say what
        // is no longer firing.
        tracing::warn!(
            truncated,
            delivered = alerts.len(),
            "Alertmanager truncated the batch"
        );
    }

    let request = IngestRequest {
        source: EventSource::Webhook,
        group_key,
        truncated,
        alerts,
        received_at: Utc::now(),
    };

    match state.service().ingest(request).await {
        Ok(accepted) => {
            record(RESULT_ACCEPTED);
            if accepted.duplicates > 0 {
                metrics::counter!(WEBHOOK_DUPLICATES).increment(u64::from(accepted.duplicates));
            }

            (
                StatusCode::OK,
                Json(AcceptedBody {
                    accepted: accepted.accepted,
                    duplicates: accepted.duplicates,
                }),
            )
                .into_response()
        }
        Err(error @ ServiceError::Unavailable { .. }) => {
            tracing::error!(%error, "could not accept a webhook batch");
            // 503 rather than 500: Alertmanager retries this one, and the batch is worth
            // redelivering once whatever is down is back.
            refuse(
                RESULT_UNAVAILABLE,
                StatusCode::SERVICE_UNAVAILABLE,
                &error.to_string(),
            )
        }
        Err(error @ ServiceError::Rejected { .. }) => {
            tracing::warn!(%error, "the pipeline refused a webhook batch");
            refuse(RESULT_REJECTED, StatusCode::BAD_REQUEST, &error.to_string())
        }
        Err(error @ ServiceError::Internal { .. }) => {
            tracing::error!(%error, "a webhook batch failed");
            refuse(
                RESULT_ERROR,
                StatusCode::INTERNAL_SERVER_ERROR,
                &error.to_string(),
            )
        }
    }
}

/// Answers whether the process is alive.
///
/// Deliberately asks nothing of anything. Wiring a liveness probe to a dependency check is how a
/// database outage turns into a restart loop that cannot fix it.
async fn healthz() -> StatusCode {
    StatusCode::OK
}

/// Answers whether the process can do its job, naming whatever cannot.
async fn readyz(State(state): State<AppState>) -> Response {
    let readiness = state.service().readiness().await;
    let failed = readiness.failures(state.max_poll_age());

    if failed.is_empty() {
        return (
            StatusCode::OK,
            Json(ReadinessBody {
                status: "ready",
                failed,
            }),
        )
            .into_response();
    }

    tracing::warn!(?failed, "readiness probe reported a failing dependency");

    (
        StatusCode::SERVICE_UNAVAILABLE,
        Json(ReadinessBody {
            status: "not ready",
            failed,
        }),
    )
        .into_response()
}

/// Renders the Prometheus exposition.
async fn render_metrics(State(state): State<AppState>) -> Response {
    state.metrics().map_or_else(
        // Unreachable while the route is mounted only alongside a handle, and cheaper than an
        // invariant maintained in two places.
        || StatusCode::NOT_FOUND.into_response(),
        |handle| {
            (
                [(header::CONTENT_TYPE, PROMETHEUS_CONTENT_TYPE)],
                handle.render(),
            )
                .into_response()
        },
    )
}

/// Rejects a request unless it carries the configured bearer token.
///
/// A missing token and a wrong token get the same empty 401. Telling them apart would tell a
/// caller which of the two mistakes it made, and the only caller that benefits from knowing is
/// the one guessing.
async fn require_bearer(State(state): State<AppState>, request: Request, next: Next) -> Response {
    let Some(expected) = state.webhook_token() else {
        // No token configured, so no check. Defensible only where the listener is unreachable
        // from outside the namespace, which is a deployment decision made in the configuration.
        return next.run(request).await;
    };

    let presented = bearer_token(request.headers());
    if !presented.is_some_and(|token| token_matches(expected, token)) {
        record(RESULT_UNAUTHORIZED);
        tracing::warn!(
            presented = presented.is_some(),
            "rejected a webhook request with a missing or wrong bearer token"
        );

        return StatusCode::UNAUTHORIZED.into_response();
    }

    next.run(request).await
}

/// The token out of an `Authorization: Bearer …` header, if there is one.
///
/// The scheme is compared case-insensitively because RFC 7235 defines it that way, and a client
/// that sends `bearer` is not the problem worth failing a delivery over.
fn bearer_token(headers: &HeaderMap) -> Option<&str> {
    let value = headers.get(header::AUTHORIZATION)?.to_str().ok()?;
    let (scheme, token) = value.split_once(' ')?;

    scheme
        .eq_ignore_ascii_case("bearer")
        .then(|| token.trim_start())
}

/// Compares the presented token with the configured one in constant time.
///
/// A byte-by-byte comparison that stops at the first difference leaks the token one byte at a
/// time to anyone who can measure the response. The length is not hidden — `ct_eq` answers false
/// for slices of different lengths without comparing them — and does not need to be.
fn token_matches(expected: &SecretString, presented: &str) -> bool {
    expected
        .expose_secret()
        .as_bytes()
        .ct_eq(presented.as_bytes())
        .into()
}

/// Counts a rejection and builds the response carrying its reason.
fn refuse(result: &'static str, status: StatusCode, detail: &str) -> Response {
    record(result);

    (
        status,
        Json(ErrorBody {
            error: detail.to_owned(),
        }),
    )
        .into_response()
}

/// Counts one webhook request under how it ended.
fn record(result: &'static str) {
    metrics::counter!(WEBHOOK_REQUESTS, "result" => result).increment(1);
}

/// Registers the help text for both counters, once per process.
///
/// Descriptions are global state in the `metrics` facade, so re-registering them on every router
/// build would be harmless and pointless. A router is built once in the binary and once per test.
fn describe_metrics() {
    static DESCRIBED: Once = Once::new();

    DESCRIBED.call_once(|| {
        metrics::describe_counter!(WEBHOOK_REQUESTS, "Webhook requests, by how each one ended.");
        metrics::describe_counter!(
            WEBHOOK_DUPLICATES,
            "Alert changes discarded because an identical one was already stored."
        );
    });
}

/// The configured webhook path, in the form axum routes on.
///
/// A path without a leading slash is the one mistake worth correcting rather than rejecting: it
/// is what an operator writes when copying the value out of Alertmanager's own `url` key, and the
/// intent is never ambiguous.
fn routable_path(configured: &str) -> String {
    if configured.starts_with('/') {
        configured.to_owned()
    } else {
        format!("/{configured}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_path_without_a_leading_slash_is_corrected() {
        assert_eq!(routable_path("webhook"), "/webhook");
        assert_eq!(routable_path("/webhook"), "/webhook");
    }

    #[test]
    fn a_bearer_header_yields_its_token_whatever_the_scheme_case() {
        let mut headers = HeaderMap::new();
        headers.insert(header::AUTHORIZATION, "bearer secret".parse().unwrap());

        assert_eq!(bearer_token(&headers), Some("secret"));
    }

    #[test]
    fn a_header_of_another_scheme_yields_nothing() {
        let mut headers = HeaderMap::new();
        headers.insert(header::AUTHORIZATION, "Basic secret".parse().unwrap());

        assert_eq!(bearer_token(&headers), None);
        assert_eq!(bearer_token(&HeaderMap::new()), None);
    }

    #[test]
    fn a_token_matches_only_itself() {
        let expected = SecretString::from("s3cret");

        assert!(token_matches(&expected, "s3cret"));
        assert!(!token_matches(&expected, "s3cre"));
        assert!(!token_matches(&expected, "s3crett"));
        assert!(!token_matches(&expected, ""));
    }
}
