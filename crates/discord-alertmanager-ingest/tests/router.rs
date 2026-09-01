//! The routes, driven through `tower::ServiceExt::oneshot` rather than over a socket.
//!
//! No listener is bound and no port is taken, so the suite runs in parallel with itself and with
//! everything else in the workspace. What is exercised is the router as assembled: the bearer
//! layer, the body limit, the version check and the handlers, in the order a real request meets
//! them.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use axum::body::{Body, to_bytes};
use axum::http::{Request, StatusCode, header};
use dam_config::Ingest;
use dam_core::EventSource;
use dam_ingest::{
    AppState, IngestAccepted, IngestRequest, Readiness, ServiceError, WebhookService, router,
};
use metrics_exporter_prometheus::PrometheusBuilder;
use secrecy::SecretString;
use tower::ServiceExt;

/// The bearer token the fixtures configure.
const TOKEN: &str = "s3cret-token";

/// How stale a poll may be before `/readyz` calls it a failure, in these tests.
const MAX_POLL_AGE: Duration = Duration::from_mins(2);

/// A version-4 envelope with the version and truncated count left as placeholders.
const ENVELOPE: &str = r#"{
    "version": "VERSION",
    "groupKey": "group-1",
    "truncatedAlerts": TRUNCATED,
    "status": "firing",
    "receiver": "discord",
    "groupLabels": { "alertname": "Boom" },
    "commonLabels": { "alertname": "Boom" },
    "commonAnnotations": {},
    "externalURL": "https://alertmanager.example",
    "alerts": [
        {
            "status": "firing",
            "labels": { "alertname": "Boom", "severity": "critical" },
            "annotations": { "summary": "it broke" },
            "startsAt": "2026-01-01T00:00:00Z",
            "endsAt": "0001-01-01T00:00:00Z",
            "generatorURL": "https://prometheus.example/graph",
            "fingerprint": "0123456789abcdef"
        }
    ]
}"#;

/// A service that records what it was handed and answers with whatever it was built with.
struct FakeService {
    received: Mutex<Vec<IngestRequest>>,
    outcome: Result<IngestAccepted, ServiceError>,
    readiness: Readiness,
}

impl FakeService {
    fn new(outcome: Result<IngestAccepted, ServiceError>, readiness: Readiness) -> Self {
        Self {
            received: Mutex::new(Vec::new()),
            outcome,
            readiness,
        }
    }

    fn accepting() -> Arc<Self> {
        Arc::new(Self::new(
            Ok(IngestAccepted {
                accepted: 1,
                duplicates: 2,
            }),
            healthy(),
        ))
    }

    fn failing(error: ServiceError) -> Arc<Self> {
        Arc::new(Self::new(Err(error), healthy()))
    }

    fn reporting(readiness: Readiness) -> Arc<Self> {
        Arc::new(Self::new(Ok(IngestAccepted::default()), readiness))
    }

    fn received(&self) -> Vec<IngestRequest> {
        self.received
            .lock()
            .expect("the fake is not poisoned")
            .clone()
    }
}

#[async_trait]
impl WebhookService for FakeService {
    async fn ingest(&self, batch: IngestRequest) -> Result<IngestAccepted, ServiceError> {
        self.received
            .lock()
            .expect("the fake is not poisoned")
            .push(batch);

        self.outcome.clone()
    }

    async fn readiness(&self) -> Readiness {
        self.readiness
    }
}

fn healthy() -> Readiness {
    Readiness {
        store_reachable: true,
        gateway_connected: true,
        last_poll_age: Some(Duration::from_secs(10)),
    }
}

fn settings() -> Ingest {
    Ingest {
        webhook_token: Some(SecretString::from(TOKEN)),
        ..Ingest::default()
    }
}

fn state(service: Arc<FakeService>, ingest: Ingest) -> AppState {
    AppState::new(service, ingest, MAX_POLL_AGE, None)
}

fn envelope(version: &str, truncated: u32) -> String {
    ENVELOPE
        .replace("VERSION", version)
        .replace("TRUNCATED", &truncated.to_string())
}

fn webhook_request(token: Option<&str>, body: String) -> Request<Body> {
    let mut builder = Request::builder()
        .method("POST")
        .uri("/webhook")
        .header(header::CONTENT_TYPE, "application/json")
        .header(header::CONTENT_LENGTH, body.len());

    if let Some(token) = token {
        builder = builder.header(header::AUTHORIZATION, format!("Bearer {token}"));
    }

    builder
        .body(Body::from(body))
        .expect("the request is well formed")
}

fn get_request(path: &str) -> Request<Body> {
    Request::builder()
        .uri(path)
        .body(Body::empty())
        .expect("the request is well formed")
}

async fn body_text(response: axum::response::Response) -> String {
    let bytes = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("the response body is readable");

    String::from_utf8(bytes.to_vec()).expect("the response body is text")
}

#[tokio::test]
async fn a_wrong_token_is_rejected_without_saying_why() {
    let service = FakeService::accepting();
    let response = router(state(Arc::clone(&service), settings()))
        .oneshot(webhook_request(Some("not-the-token"), envelope("4", 0)))
        .await
        .expect("the router answers");

    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    assert!(body_text(response).await.is_empty());
    assert!(service.received().is_empty());
}

#[tokio::test]
async fn a_missing_token_is_rejected_the_same_way() {
    let service = FakeService::accepting();
    let response = router(state(Arc::clone(&service), settings()))
        .oneshot(webhook_request(None, envelope("4", 0)))
        .await
        .expect("the router answers");

    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    assert!(body_text(response).await.is_empty());
    assert!(service.received().is_empty());
}

#[tokio::test]
async fn an_unconfigured_token_leaves_the_check_off() {
    let service = FakeService::accepting();
    let ingest = Ingest {
        webhook_token: None,
        ..Ingest::default()
    };

    let response = router(state(Arc::clone(&service), ingest))
        .oneshot(webhook_request(None, envelope("4", 0)))
        .await
        .expect("the router answers");

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(service.received().len(), 1);
}

#[tokio::test]
async fn an_envelope_of_another_version_is_a_bad_request() {
    let service = FakeService::accepting();
    let response = router(state(Arc::clone(&service), settings()))
        .oneshot(webhook_request(Some(TOKEN), envelope("5", 0)))
        .await
        .expect("the router answers");

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    assert!(body_text(response).await.contains('5'));
    assert!(service.received().is_empty());
}

#[tokio::test]
async fn a_body_that_is_not_the_envelope_is_a_bad_request() {
    let service = FakeService::accepting();
    let response = router(state(Arc::clone(&service), settings()))
        .oneshot(webhook_request(Some(TOKEN), "not json".to_owned()))
        .await
        .expect("the router answers");

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    assert!(service.received().is_empty());
}

#[tokio::test]
async fn a_valid_batch_reaches_the_service_and_is_answered_at_once() {
    let service = FakeService::accepting();
    let response = router(state(Arc::clone(&service), settings()))
        .oneshot(webhook_request(Some(TOKEN), envelope("4", 0)))
        .await
        .expect("the router answers");

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        body_text(response).await,
        r#"{"accepted":1,"duplicates":2}"#
    );

    let received = service.received();
    let batch = received.first().expect("the service was handed the batch");

    assert_eq!(batch.source, EventSource::Webhook);
    assert_eq!(
        batch.group_key.as_ref().map(dam_core::GroupKey::as_str),
        Some("group-1")
    );
    assert_eq!(batch.alerts.len(), 1);
    assert_eq!(batch.alerts[0].fingerprint.as_str(), "0123456789abcdef");
    assert_eq!(batch.alerts[0].labels.get("severity"), Some("critical"));
    assert!(!batch.is_truncated());
}

#[tokio::test]
async fn a_firing_alert_reaches_the_service_without_an_end_time() {
    let service = FakeService::accepting();
    let response = router(state(Arc::clone(&service), settings()))
        .oneshot(webhook_request(Some(TOKEN), envelope("4", 0)))
        .await
        .expect("the router answers");

    assert_eq!(response.status(), StatusCode::OK);

    let received = service.received();
    let batch = received.first().expect("the service was handed the batch");

    // Alertmanager writes Go's zero time rather than null while an alert is still firing, and a
    // batch that reached the pipeline claiming to have ended in the year one would resolve a
    // card that is still on fire.
    assert_eq!(batch.alerts[0].ends_at, None);
    assert!(batch.alerts[0].is_firing());
}

#[tokio::test]
async fn an_alert_the_domain_refuses_is_a_bad_request() {
    let service = FakeService::accepting();
    let body = envelope("4", 0).replace("0123456789abcdef", "not-hex");

    let response = router(state(Arc::clone(&service), settings()))
        .oneshot(webhook_request(Some(TOKEN), body))
        .await
        .expect("the router answers");

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    assert!(service.received().is_empty());
}

#[tokio::test]
async fn truncated_alerts_are_passed_through_to_the_service() {
    let service = FakeService::accepting();
    let response = router(state(Arc::clone(&service), settings()))
        .oneshot(webhook_request(Some(TOKEN), envelope("4", 7)))
        .await
        .expect("the router answers");

    assert_eq!(response.status(), StatusCode::OK);

    let received = service.received();
    let batch = received.first().expect("the service was handed the batch");

    assert_eq!(batch.truncated, 7);
    assert!(batch.is_truncated());
}

#[tokio::test]
async fn a_body_over_the_configured_limit_is_refused() {
    let service = FakeService::accepting();
    let ingest = Ingest {
        body_limit_bytes: 128,
        ..settings()
    };

    let response = router(state(Arc::clone(&service), ingest))
        .oneshot(webhook_request(Some(TOKEN), envelope("4", 0)))
        .await
        .expect("the router answers");

    assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
    assert!(service.received().is_empty());
}

#[tokio::test]
async fn a_body_inside_the_configured_limit_is_accepted() {
    let service = FakeService::accepting();
    let body = envelope("4", 0);
    let ingest = Ingest {
        body_limit_bytes: body.len(),
        ..settings()
    };

    let response = router(state(Arc::clone(&service), ingest))
        .oneshot(webhook_request(Some(TOKEN), body))
        .await
        .expect("the router answers");

    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn an_unavailable_pipeline_asks_alertmanager_to_come_back() {
    let service = FakeService::failing(ServiceError::Unavailable {
        detail: "the database is unreachable".to_owned(),
    });

    let response = router(state(service, settings()))
        .oneshot(webhook_request(Some(TOKEN), envelope("4", 0)))
        .await
        .expect("the router answers");

    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
}

#[tokio::test]
async fn healthz_answers_while_the_process_is_alive() {
    let service = FakeService::reporting(Readiness {
        store_reachable: false,
        gateway_connected: false,
        last_poll_age: None,
    });

    let response = router(state(service, settings()))
        .oneshot(get_request("/healthz"))
        .await
        .expect("the router answers");

    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn readyz_is_ready_when_every_dependency_is_up() {
    let service = FakeService::reporting(healthy());
    let response = router(state(service, settings()))
        .oneshot(get_request("/readyz"))
        .await
        .expect("the router answers");

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        body_text(response).await,
        r#"{"status":"ready","failed":[]}"#
    );
}

#[tokio::test]
async fn readyz_names_the_dependency_that_is_down() {
    let service = FakeService::reporting(Readiness {
        store_reachable: false,
        ..healthy()
    });

    let response = router(state(service, settings()))
        .oneshot(get_request("/readyz"))
        .await
        .expect("the router answers");

    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);

    let body = body_text(response).await;
    assert!(body.contains(dam_ingest::CHECK_STORE), "body was {body}");
    assert!(!body.contains(dam_ingest::CHECK_GATEWAY), "body was {body}");
}

#[tokio::test]
async fn readyz_refuses_a_stale_alertmanager_poll() {
    let service = FakeService::reporting(Readiness {
        last_poll_age: Some(MAX_POLL_AGE + Duration::from_secs(1)),
        ..healthy()
    });

    let response = router(state(service, settings()))
        .oneshot(get_request("/readyz"))
        .await
        .expect("the router answers");

    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert!(
        body_text(response)
            .await
            .contains(dam_ingest::CHECK_ALERTMANAGER)
    );
}

#[tokio::test]
async fn metrics_are_absent_when_the_exporter_is_off() {
    let service = FakeService::accepting();
    let response = router(state(service, settings()))
        .oneshot(get_request("/metrics"))
        .await
        .expect("the router answers");

    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn metrics_are_served_when_the_exporter_is_on() {
    let handle = PrometheusBuilder::new().build_recorder().handle();
    let state = AppState::new(
        FakeService::accepting(),
        settings(),
        MAX_POLL_AGE,
        Some(handle),
    );

    let response = router(state)
        .oneshot(get_request("/metrics"))
        .await
        .expect("the router answers");

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response
            .headers()
            .get(header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok()),
        Some("text/plain; version=0.0.4; charset=utf-8")
    );
}

#[tokio::test]
async fn an_unknown_path_is_not_an_authentication_failure() {
    let service = FakeService::accepting();
    let response = router(state(service, settings()))
        .oneshot(get_request("/nothing-here"))
        .await
        .expect("the router answers");

    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}
