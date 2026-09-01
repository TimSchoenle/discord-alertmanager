//! The client against a stub Alertmanager: what it sends, what it decodes, and what it retries.
//!
//! The behaviour worth pinning here is not that a `GET` returns a list. It is the three decisions
//! the client makes on its own — fail over to the next peer, retry a 5xx, refuse to retry a 4xx —
//! because each of them is invisible from the call site and each of them is wrong in a way that
//! only shows up during an incident.

use std::net::TcpListener;
use std::sync::atomic::{AtomicUsize, Ordering};

use chrono::{TimeZone, Utc};
use dam_am::AlertmanagerClient;
use dam_config::{Alertmanager, Retry};
use dam_core::{AlertStatus, AmState, MatcherSet};
use dam_engine::{AlertFilter, AlertmanagerApi, AmError, SilenceRequest};
use secrecy::SecretString;
use url::Url;
use wiremock::matchers::{body_string_contains, header, method, path, query_param};
use wiremock::{Mock, MockServer, Request, Respond, ResponseTemplate};

const ALERTS: &str = include_str!("fixtures/alerts_v2.json");
const SILENCES: &str = include_str!("fixtures/silences_v2.json");
const STATUS: &str = include_str!("fixtures/status_v2.json");
const RECEIVERS: &str = include_str!("fixtures/receivers_v2.json");

/// A configuration pointing at the given endpoints, with the waits shrunk to test length.
fn config(endpoints: &[&str]) -> Alertmanager {
    let mut parsed = Vec::new();
    for endpoint in endpoints {
        parsed.push(Url::parse(endpoint).expect("the test endpoint parses"));
    }

    Alertmanager {
        endpoints: parsed,
        timeout_secs: 5,
        connect_timeout_secs: 2,
        retry: Retry {
            initial_backoff_ms: 5,
            max_backoff_secs: 1,
            max_elapsed_secs: 10,
        },
        ..Alertmanager::default()
    }
}

/// A client for the given endpoints.
fn client(endpoints: &[&str]) -> AlertmanagerClient {
    AlertmanagerClient::new(&config(endpoints)).expect("the client builds")
}

/// An address nothing is listening on.
///
/// Bound and immediately released rather than picked as a constant, so the test cannot collide
/// with whatever else the machine happens to be running.
fn dead_endpoint() -> String {
    let listener = TcpListener::bind("127.0.0.1:0").expect("a loopback port is available");
    let port = listener
        .local_addr()
        .expect("the listener has an address")
        .port();
    drop(listener);

    format!("http://127.0.0.1:{port}")
}

/// A silence request with one literal and one regex matcher.
fn silence_request(id: Option<&str>) -> SilenceRequest {
    SilenceRequest {
        id: id.map(ToOwned::to_owned),
        matchers: MatcherSet::parse("namespace=prod, alertname=~Pod.*")
            .expect("the expression parses"),
        starts_at: Utc
            .with_ymd_and_hms(2026, 3, 4, 9, 0, 0)
            .single()
            .expect("the timestamp is unambiguous"),
        ends_at: Utc
            .with_ymd_and_hms(2026, 3, 4, 11, 0, 0)
            .single()
            .expect("the timestamp is unambiguous"),
        created_by: "discord:tim#4212".to_owned(),
        comment: "rolling the prod deployment".to_owned(),
    }
}

/// A responder that fails the first call and succeeds afterwards.
///
/// A pair of mocks with call limits would depend on the order the mock server matches them in,
/// which is an implementation detail of the mock server rather than of the client under test.
struct FailsOnce {
    calls: AtomicUsize,
    status: u16,
    body: &'static str,
}

impl Respond for FailsOnce {
    fn respond(&self, _: &Request) -> ResponseTemplate {
        if self.calls.fetch_add(1, Ordering::SeqCst) == 0 {
            ResponseTemplate::new(self.status).set_body_string("the cluster is not ready yet")
        } else {
            ResponseTemplate::new(200).set_body_raw(self.body, "application/json")
        }
    }
}

/// A `200` carrying a JSON fixture.
fn json(body: &'static str) -> ResponseTemplate {
    ResponseTemplate::new(200).set_body_raw(body, "application/json")
}

#[tokio::test]
async fn listing_alerts_sends_the_four_flags_and_decodes_the_answer() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/api/v2/alerts"))
        .and(query_param("active", "true"))
        .and(query_param("silenced", "true"))
        .and(query_param("inhibited", "true"))
        .and(query_param("unprocessed", "true"))
        .respond_with(json(ALERTS))
        .expect(1)
        .mount(&server)
        .await;

    let alerts = client(&[&server.uri()])
        .list_alerts(&AlertFilter::everything())
        .await
        .expect("the alerts are listed");

    assert_eq!(alerts.len(), 3);
    assert_eq!(alerts[0].name(), "PodDown");
    assert_eq!(alerts[0].status, AlertStatus::Firing);
    assert!(alerts[0].annotations.runbook_url().is_some());
    assert_eq!(alerts[1].am_state, AmState::Suppressed);
    assert_eq!(
        alerts[1].silenced_by,
        vec!["b3f4e0d1-6a2c-4c31-9f0b-77a1f2c8d5e9".to_owned()]
    );
    // Its `endsAt` is in the past, unlike the two carrying a resolve timeout.
    assert_eq!(alerts[2].status, AlertStatus::Resolved);

    server.verify().await;
}

#[tokio::test]
async fn a_matcher_filter_is_passed_through_as_a_repeated_query_parameter() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/api/v2/alerts"))
        .and(query_param("filter", "severity=critical"))
        .respond_with(json("[]"))
        .expect(1)
        .mount(&server)
        .await;

    let alerts = client(&[&server.uri()])
        .list_alerts(&AlertFilter::matching("severity=critical"))
        .await
        .expect("the alerts are listed");

    assert!(alerts.is_empty());
    server.verify().await;
}

#[tokio::test]
async fn listing_silences_compiles_the_matchers_back_out_of_the_two_booleans() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/api/v2/silences"))
        .respond_with(json(SILENCES))
        .expect(1)
        .mount(&server)
        .await;

    let silences = client(&[&server.uri()])
        .list_silences(&[])
        .await
        .expect("the silences are listed");

    assert_eq!(silences.len(), 2);
    assert_eq!(
        silences[0].matchers.to_string(),
        "namespace=prod, alertname=~Pod.*"
    );
    assert_eq!(silences[1].matchers.to_string(), "instance!=node-3");
    server.verify().await;
}

#[tokio::test]
async fn creating_a_silence_sends_structured_matchers_and_returns_the_new_id() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/v2/silences"))
        .and(body_string_contains(r#""isRegex":false"#))
        .and(body_string_contains(r#""isRegex":true"#))
        .and(body_string_contains(r#""isEqual":true"#))
        .respond_with(json(
            r#"{"silenceID":"b3f4e0d1-6a2c-4c31-9f0b-77a1f2c8d5e9"}"#,
        ))
        .expect(1)
        .mount(&server)
        .await;

    let id = client(&[&server.uri()])
        .upsert_silence(&silence_request(None))
        .await
        .expect("the silence is created");

    assert_eq!(id, "b3f4e0d1-6a2c-4c31-9f0b-77a1f2c8d5e9");
    server.verify().await;
}

#[tokio::test]
async fn an_update_carries_the_id_that_makes_it_one() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/v2/silences"))
        .and(body_string_contains(r#""id":"9d1a""#))
        .respond_with(json(r#"{"silenceID":"9d1a"}"#))
        .expect(1)
        .mount(&server)
        .await;

    let id = client(&[&server.uri()])
        .upsert_silence(&silence_request(Some("9d1a")))
        .await
        .expect("the silence is replaced");

    assert_eq!(id, "9d1a");
    server.verify().await;
}

#[tokio::test]
async fn expiring_a_silence_uses_the_singular_path() {
    let server = MockServer::start().await;
    Mock::given(method("DELETE"))
        .and(path("/api/v2/silence/b3f4e0d1-6a2c-4c31-9f0b-77a1f2c8d5e9"))
        .respond_with(ResponseTemplate::new(200))
        .expect(1)
        .mount(&server)
        .await;

    client(&[&server.uri()])
        .expire_silence("b3f4e0d1-6a2c-4c31-9f0b-77a1f2c8d5e9")
        .await
        .expect("the silence is expired");

    server.verify().await;
}

#[tokio::test]
async fn the_status_reduces_to_what_the_deadman_watches() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/api/v2/status"))
        .respond_with(json(STATUS))
        .expect(1)
        .mount(&server)
        .await;

    let status = client(&[&server.uri()])
        .status()
        .await
        .expect("the status is read");

    assert_eq!(status.version, "0.27.0");
    assert!(status.cluster_ready);
    assert_eq!(status.peers.len(), 2);
    assert!(status.uptime.is_some());
    assert!(status.config_hash.is_some());
    server.verify().await;
}

#[tokio::test]
async fn the_receivers_come_back_by_name() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/api/v2/receivers"))
        .respond_with(json(RECEIVERS))
        .expect(1)
        .mount(&server)
        .await;

    let receivers = client(&[&server.uri()])
        .receivers()
        .await
        .expect("the receivers are read");

    assert_eq!(receivers.len(), 3);
    assert_eq!(receivers[0].name, "discord");
    server.verify().await;
}

#[tokio::test]
async fn a_bearer_token_is_sent_on_every_request() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/api/v2/receivers"))
        .and(header("authorization", "Bearer s3cret-token"))
        .respond_with(json(RECEIVERS))
        .expect(1)
        .mount(&server)
        .await;

    let authenticated = Alertmanager {
        bearer_token: Some(SecretString::from("s3cret-token")),
        ..config(&[&server.uri()])
    };

    AlertmanagerClient::new(&authenticated)
        .expect("the client builds")
        .receivers()
        .await
        .expect("the receivers are read");

    server.verify().await;
}

#[tokio::test]
async fn a_four_hundred_is_reported_once_and_never_retried() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/v2/silences"))
        .respond_with(
            ResponseTemplate::new(400).set_body_string("invalid silence: end time in the past"),
        )
        .expect(1)
        .mount(&server)
        .await;

    let error = client(&[&server.uri()])
        .upsert_silence(&silence_request(None))
        .await
        .expect_err("the server refused the silence");

    match error {
        AmError::Status { status, ref body } => {
            assert_eq!(status, 400);
            assert!(body.contains("end time in the past"), "{body}");
        }
        other => panic!("expected a status failure, got {other}"),
    }
    assert!(!error.is_retryable());
    assert!(!error.is_unavailable());

    // The `expect(1)` above is the assertion that matters: a second request would fail it.
    server.verify().await;
}

#[tokio::test]
async fn a_five_hundred_is_retried_and_the_second_attempt_is_kept() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/api/v2/alerts"))
        .respond_with(FailsOnce {
            calls: AtomicUsize::new(0),
            status: 503,
            body: ALERTS,
        })
        .expect(2)
        .mount(&server)
        .await;

    let alerts = client(&[&server.uri()])
        .list_alerts(&AlertFilter::everything())
        .await
        .expect("the retry succeeded");

    assert_eq!(alerts.len(), 3);
    server.verify().await;
}

#[tokio::test]
async fn a_dead_first_endpoint_fails_over_to_a_live_second_one() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/api/v2/alerts"))
        .respond_with(json(ALERTS))
        .expect(1)
        .mount(&server)
        .await;

    let alerts = client(&[&dead_endpoint(), &server.uri()])
        .list_alerts(&AlertFilter::everything())
        .await
        .expect("the second endpoint answered");

    assert_eq!(alerts.len(), 3);
    server.verify().await;
}

#[tokio::test]
async fn no_endpoint_answering_is_reported_as_unreachable() {
    // A budget of zero so the assertion is about the failure and not about how long the retry
    // schedule is willing to keep trying.
    let hopeless = Alertmanager {
        connect_timeout_secs: 1,
        retry: Retry {
            max_elapsed_secs: 0,
            ..config(&[]).retry
        },
        ..config(&[&dead_endpoint(), &dead_endpoint()])
    };

    let error = AlertmanagerClient::new(&hopeless)
        .expect("the client builds")
        .status()
        .await
        .expect_err("nothing is listening");

    assert!(matches!(error, AmError::Unreachable { .. }), "{error}");
    assert!(error.is_unavailable());
}

#[tokio::test]
async fn an_exhausted_budget_stops_after_one_round_of_endpoints() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/api/v2/status"))
        .respond_with(ResponseTemplate::new(502).set_body_string("bad gateway"))
        .expect(1)
        .mount(&server)
        .await;

    let impatient = Alertmanager {
        retry: Retry {
            max_elapsed_secs: 0,
            ..config(&[]).retry
        },
        ..config(&[&server.uri()])
    };

    let error = AlertmanagerClient::new(&impatient)
        .expect("the client builds")
        .status()
        .await
        .expect_err("the server is broken");

    assert!(matches!(error, AmError::Status { status: 502, .. }));
    assert!(error.is_unavailable());
    server.verify().await;
}

#[tokio::test]
async fn an_answer_that_does_not_decode_is_not_retried() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/api/v2/status"))
        .respond_with(json(r#"{"cluster":{"status":"ready"}}"#))
        .expect(1)
        .mount(&server)
        .await;

    let error = client(&[&server.uri()])
        .status()
        .await
        .expect_err("the version block is missing");

    assert!(matches!(error, AmError::Decode { .. }));
    assert!(!error.is_retryable());
    server.verify().await;
}
