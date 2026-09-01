//! The HTTP client: peers tried in order, and retries confined to what is worth retrying.
//!
//! Two loops, nested, doing different jobs. The inner one walks the configured endpoints and is
//! failover: a peer that will not answer is abandoned for the next one immediately, which is what
//! the short connect timeout exists for. The outer one is backoff: once every peer has failed the
//! same way, waiting and asking again is the only thing left, and it happens on a full-jitter
//! exponential schedule bounded by the configured elapsed budget.
//!
//! What is never retried is a 4xx. It means the request was wrong, sending it again produces the
//! same answer, and the only thing gained is load on somebody else's server during whatever
//! incident prompted the request.

use std::fs;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use chrono::Utc;
use dam_config::{Alertmanager as AlertmanagerConfig, Retry};
use dam_core::Alert;
use dam_engine::{
    AlertFilter, AlertmanagerApi, AmError, AmStatus, Receiver, SilenceRecord, SilenceRequest,
};
use reqwest::header::{AUTHORIZATION, CONTENT_TYPE, HeaderValue};
use reqwest::{Certificate, Client, Method, RequestBuilder};
use secrecy::{ExposeSecret, SecretString};
use serde::de::DeserializeOwned;
use tracing::{debug, warn};
use url::Url;

use crate::model::{
    AlertmanagerStatus, GettableAlert, GettableSilence, PostableSilence, SilenceCreated,
    WireReceiver,
};

/// Identifies this client in Alertmanager's access log.
const USER_AGENT: &str = concat!("discord-alertmanager/", env!("CARGO_PKG_VERSION"));

/// Longest error body kept for an [`AmError::Status`].
///
/// Alertmanager answers a bad matcher with a sentence and a 500 with a stack of Go text. The
/// first is the whole diagnosis and the second is unbounded, so the message is cut here rather
/// than in whatever log line or Discord reply eventually shows it.
const MAX_ERROR_BODY: usize = 512;

/// A client for one Alertmanager high-availability set.
///
/// Built once at startup and shared, not built per call. The inner [`Client`] owns the connection
/// pool, so sharing it is what amortises the TLS handshake across every poll and every silence;
/// a client per request would open a connection per request.
#[derive(Debug)]
pub struct AlertmanagerClient {
    http: Client,
    endpoints: Vec<Url>,
    auth: Auth,
    retry: Schedule,
    jitter: Jitter,
}

impl AlertmanagerClient {
    /// Builds a client from the `alertmanager` section of the configuration.
    ///
    /// TLS is rustls, explicitly rather than by default, so the stack in use does not depend on
    /// which feature some other crate in the tree happened to enable. There is no option to skip
    /// verification; a private certificate authority goes in `ca_bundle` and is added to the
    /// system roots rather than replacing them.
    ///
    /// # Errors
    ///
    /// Returns [`AmError::Config`] when no endpoint is configured, when either timeout is zero,
    /// when the CA bundle cannot be read or contains no certificate, when the credentials cannot
    /// become a header value, or when the underlying HTTP client refuses to build.
    pub fn new(config: &AlertmanagerConfig) -> Result<Self, AmError> {
        if config.endpoints.is_empty() {
            return Err(config_error("no Alertmanager endpoint is configured"));
        }
        if config.timeout_secs == 0 || config.connect_timeout_secs == 0 {
            return Err(config_error(
                "timeout_secs and connect_timeout_secs must both be at least one second",
            ));
        }

        let mut builder = Client::builder()
            .use_rustls_tls()
            .user_agent(USER_AGENT)
            .timeout(Duration::from_secs(config.timeout_secs))
            .connect_timeout(Duration::from_secs(config.connect_timeout_secs));

        if let Some(path) = config.ca_bundle.as_ref() {
            let pem = fs::read(path).map_err(|source| {
                config_error(format!(
                    "cannot read CA bundle `{}`: {source}",
                    path.display()
                ))
            })?;

            let authorities = Certificate::from_pem_bundle(&pem).map_err(|source| {
                config_error(format!(
                    "cannot parse CA bundle `{}`: {source}",
                    path.display()
                ))
            })?;

            // An empty bundle is a configuration mistake that would otherwise present as a TLS
            // failure at the first poll, long after the deploy that caused it.
            if authorities.is_empty() {
                return Err(config_error(format!(
                    "CA bundle `{}` contains no certificate",
                    path.display()
                )));
            }

            for authority in authorities {
                builder = builder.add_root_certificate(authority);
            }
        }

        let http = builder
            .build()
            .map_err(|source| config_error(format!("cannot build the HTTP client: {source}")))?;

        Ok(Self {
            http,
            endpoints: config.endpoints.clone(),
            auth: Auth::from_config(config)?,
            retry: Schedule::from_config(&config.retry),
            jitter: Jitter::new(),
        })
    }

    /// Sends a call, failing over across endpoints and retrying what is retryable.
    async fn execute(&self, call: &Call) -> Result<Vec<u8>, AmError> {
        let deadline = Instant::now() + self.retry.budget;
        let mut attempt: u32 = 0;

        loop {
            attempt += 1;
            let mut last = None;

            for endpoint in &self.endpoints {
                match self.send(endpoint, call).await {
                    Ok(body) => return Ok(body),
                    // A 4xx or an undecodable answer is this client's problem, and the next peer
                    // holds the same gossiped state, so it would answer identically.
                    Err(error) if !error.is_retryable() => return Err(error),
                    Err(error) => {
                        warn!(
                            endpoint = %endpoint,
                            error = %error,
                            "Alertmanager endpoint failed, trying the next"
                        );
                        last = Some(error);
                    }
                }
            }

            let Some(error) = last else {
                return Err(config_error("no Alertmanager endpoint is configured"));
            };

            let wait_ms = self.backoff_ms(attempt);
            let wait = Duration::from_millis(wait_ms);
            // Giving up before sleeping rather than after: a wait that would outlast the budget
            // buys nothing, and the caller learns of the failure a whole backoff sooner.
            if Instant::now() + wait >= deadline {
                return Err(error);
            }

            debug!(attempt, wait_ms, "retrying Alertmanager");
            tokio::time::sleep(wait).await;
        }
    }

    /// One request to one endpoint.
    async fn send(&self, endpoint: &Url, call: &Call) -> Result<Vec<u8>, AmError> {
        let mut request = self.http.request(call.method.clone(), call.url(endpoint)?);
        request = self.auth.apply(request);
        if let Some(body) = call.body.as_ref() {
            request = request
                .header(CONTENT_TYPE, HeaderValue::from_static("application/json"))
                .body(body.clone());
        }

        let started = Instant::now();
        let response = request
            .send()
            .await
            .map_err(|source| transport_error(&source, started.elapsed()))?;

        let status = response.status();
        let body = response
            .bytes()
            .await
            .map_err(|source| transport_error(&source, started.elapsed()))?;

        if status.is_success() {
            return Ok(body.to_vec());
        }

        Err(AmError::Status {
            status: status.as_u16(),
            body: truncate(&String::from_utf8_lossy(&body)),
        })
    }

    /// How long to wait before attempt `attempt + 1`.
    ///
    /// Full jitter: the exponential term is a ceiling and the wait is drawn uniformly below it,
    /// rather than the exponential term itself. Every peer of a bot fleet that lost the same
    /// Alertmanager retries at the same moment otherwise, which is the thundering herd the
    /// backoff was supposed to prevent.
    fn backoff_ms(&self, attempt: u32) -> u64 {
        let doublings = attempt.saturating_sub(1).min(32);
        let exponential = self.retry.initial_ms.saturating_mul(1u64 << doublings);

        self.jitter.below(exponential.min(self.retry.ceiling_ms))
    }
}

#[async_trait]
impl AlertmanagerApi for AlertmanagerClient {
    async fn list_alerts(&self, filter: &AlertFilter) -> Result<Vec<Alert>, AmError> {
        let mut call = Call::get("api/v2/alerts");
        call.query.push(("active", filter.active.to_string()));
        call.query.push(("silenced", filter.silenced.to_string()));
        call.query.push(("inhibited", filter.inhibited.to_string()));
        call.query
            .push(("unprocessed", filter.unprocessed.to_string()));
        for matcher in &filter.matchers {
            call.query.push(("filter", matcher.clone()));
        }

        let wire: Vec<GettableAlert> = decode(&self.execute(&call).await?)?;

        // One clock reading for the whole page. Reading it per alert would let two alerts with
        // the same `endsAt` disagree about whether they have resolved.
        let now = Utc::now();
        let mut alerts = Vec::with_capacity(wire.len());
        for alert in wire {
            alerts.push(alert.into_core(now)?);
        }

        Ok(alerts)
    }

    async fn list_silences(&self, matchers: &[String]) -> Result<Vec<SilenceRecord>, AmError> {
        let mut call = Call::get("api/v2/silences");
        for matcher in matchers {
            call.query.push(("filter", matcher.clone()));
        }

        let wire: Vec<GettableSilence> = decode(&self.execute(&call).await?)?;

        let mut records = Vec::with_capacity(wire.len());
        for silence in wire {
            records.push(silence.into_record()?);
        }

        Ok(records)
    }

    async fn upsert_silence(&self, silence: &SilenceRequest) -> Result<String, AmError> {
        let call = Call::post("api/v2/silences", &PostableSilence::from_request(silence))?;
        let created: SilenceCreated = decode(&self.execute(&call).await?)?;

        Ok(created.silence_id)
    }

    async fn expire_silence(&self, id: &str) -> Result<(), AmError> {
        // Singular, and the only path on this API that is. Everything else is `/silences`.
        self.execute(&Call::delete("api/v2/silence", id)).await?;

        Ok(())
    }

    async fn status(&self) -> Result<AmStatus, AmError> {
        let wire: AlertmanagerStatus = decode(&self.execute(&Call::get("api/v2/status")).await?)?;

        Ok(wire.into_status())
    }

    async fn receivers(&self) -> Result<Vec<Receiver>, AmError> {
        let wire: Vec<WireReceiver> = decode(&self.execute(&Call::get("api/v2/receivers")).await?)?;

        let mut receivers = Vec::with_capacity(wire.len());
        for receiver in wire {
            receivers.push(Receiver::from(receiver));
        }

        Ok(receivers)
    }
}

/// One prepared request, built once and re-sent unchanged to each endpoint and on each retry.
///
/// The body is serialised bytes rather than a `reqwest` request, because a request is bound to
/// the URL it was built for and a retry against the next peer needs a different one.
#[derive(Debug)]
struct Call {
    method: Method,
    path: &'static str,
    segment: Option<String>,
    query: Vec<(&'static str, String)>,
    body: Option<Vec<u8>>,
}

impl Call {
    /// A `GET` of a fixed path.
    fn get(path: &'static str) -> Self {
        Self {
            method: Method::GET,
            path,
            segment: None,
            query: Vec::new(),
            body: None,
        }
    }

    /// A `POST` of a JSON document.
    fn post<T: serde::Serialize>(path: &'static str, body: &T) -> Result<Self, AmError> {
        let body = serde_json::to_vec(body).map_err(|source| AmError::Decode {
            detail: format!("cannot encode the request body: {source}"),
        })?;

        Ok(Self {
            method: Method::POST,
            path,
            segment: None,
            query: Vec::new(),
            body: Some(body),
        })
    }

    /// A `DELETE` of a fixed path with one caller-supplied final segment.
    fn delete(path: &'static str, segment: &str) -> Self {
        Self {
            method: Method::DELETE,
            path,
            segment: Some(segment.to_owned()),
            query: Vec::new(),
            body: None,
        }
    }

    /// Resolves the call against one base URL.
    ///
    /// # Errors
    ///
    /// Returns [`AmError::Config`] when the endpoint cannot carry a path, which is true of a URL
    /// with no host such as `mailto:` or `data:`.
    fn url(&self, endpoint: &Url) -> Result<Url, AmError> {
        // A configured endpoint may carry a path prefix, because Alertmanager is routinely served
        // under one behind a reverse proxy. `join` treats a base without a trailing slash as
        // naming a file and replaces its last segment, which would silently drop that prefix.
        let mut base = endpoint.clone();
        if !base.path().ends_with('/') {
            let with_slash = format!("{}/", base.path());
            base.set_path(&with_slash);
        }

        let mut url = base.join(self.path).map_err(|source| {
            config_error(format!("`{endpoint}` is not a usable base URL: {source}"))
        })?;

        if let Some(segment) = self.segment.as_ref() {
            let mut segments = url
                .path_segments_mut()
                .map_err(|()| config_error(format!("`{endpoint}` cannot carry a path")))?;
            // Percent-encodes, so an id containing a slash cannot reach a path it was not meant
            // to.
            segments.push(segment);
        }

        if !self.query.is_empty() {
            let mut pairs = url.query_pairs_mut();
            for (name, value) in &self.query {
                pairs.append_pair(name, value);
            }
        }

        Ok(url)
    }
}

/// How the client proves who it is.
#[derive(Debug)]
enum Auth {
    /// Alertmanager is unauthenticated, which is common behind a network boundary.
    None,

    /// A bearer token, rendered into a header once at construction.
    Bearer(HeaderValue),

    /// A username and password, encoded per request by `reqwest`.
    Basic {
        username: String,
        password: Option<SecretString>,
    },
}

impl Auth {
    /// Reads the credentials out of the configuration.
    ///
    /// # Errors
    ///
    /// Returns [`AmError::Config`] when the bearer token contains bytes no header can carry,
    /// which is worth catching at startup because the alternative is every request failing later
    /// for a reason that does not name the token.
    fn from_config(config: &AlertmanagerConfig) -> Result<Self, AmError> {
        if let Some(token) = config.bearer_token.as_ref() {
            let mut value = HeaderValue::from_str(&format!("Bearer {}", token.expose_secret()))
                .map_err(|_| {
                    config_error("the bearer token contains bytes that cannot go in a header")
                })?;
            // Keeps the value out of `Debug` output, so a dump of the request never carries it.
            value.set_sensitive(true);

            return Ok(Self::Bearer(value));
        }

        if let Some(username) = config.basic_username.as_ref() {
            return Ok(Self::Basic {
                username: username.clone(),
                password: config.basic_password.clone(),
            });
        }

        Ok(Self::None)
    }

    /// Attaches the credentials to a request.
    fn apply(&self, request: RequestBuilder) -> RequestBuilder {
        match self {
            Self::None => request,
            Self::Bearer(value) => request.header(AUTHORIZATION, value.clone()),
            Self::Basic { username, password } => {
                request.basic_auth(username, password.as_ref().map(ExposeSecret::expose_secret))
            }
        }
    }
}

/// The retry bounds, converted once out of the configuration's units.
#[derive(Debug)]
struct Schedule {
    initial_ms: u64,
    ceiling_ms: u64,
    budget: Duration,
}

impl Schedule {
    /// Reads the schedule out of the configuration.
    fn from_config(retry: &Retry) -> Self {
        Self {
            initial_ms: retry.initial_backoff_ms,
            ceiling_ms: retry.max_backoff_secs.saturating_mul(1_000),
            budget: Duration::from_secs(retry.max_elapsed_secs),
        }
    }
}

/// The source of the jitter added to each backoff.
///
/// An xorshift seeded from the clock, not a cryptographic generator and not asked to be one. Its
/// entire job is to make two processes that failed at the same instant retry at different ones,
/// and a predictable retry schedule is not a secret worth defending.
#[derive(Debug)]
struct Jitter(AtomicU64);

impl Jitter {
    /// Seeds the generator from the wall clock.
    fn new() -> Self {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0x2545_f491_4f6c_dd1d, |since| since.subsec_nanos().into());

        // xorshift is fixed at zero, so a clock that answered the epoch exactly must not seed it.
        Self(AtomicU64::new(nanos | 1))
    }

    /// A value uniformly distributed over `0..=ceiling`.
    fn below(&self, ceiling: u64) -> u64 {
        // Relaxed, and the load and store are not one atomic step: two threads racing here get
        // the same number, which costs a repeated backoff and nothing else.
        let mut state = self.0.load(Ordering::Relaxed);
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        self.0.store(state, Ordering::Relaxed);

        state % (ceiling + 1)
    }
}

/// Classifies a `reqwest` failure into the variant the caller decides on.
///
/// Connection before timeout, and the order is the whole point. A peer that never completes a
/// handshake is unreachable however the platform says so: a host that refuses reports a refusal,
/// and one that drops the packets reports the connect timeout instead, and calling the second a
/// server that answered slowly would misreport a dead peer as a busy one.
fn transport_error(source: &reqwest::Error, elapsed: Duration) -> AmError {
    if source.is_connect() {
        return AmError::Unreachable {
            detail: source.to_string(),
        };
    }

    if source.is_timeout() {
        return AmError::Timeout {
            elapsed_ms: u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX),
        };
    }

    if source.is_decode() {
        return AmError::Decode {
            detail: source.to_string(),
        };
    }

    // Everything left — a reset mid-response, a name that resolved to nothing, a rejected
    // certificate — is one peer being unusable, which is what failover and backoff are for.
    AmError::Unreachable {
        detail: source.to_string(),
    }
}

/// Parses a response body into the wire model.
///
/// # Errors
///
/// Returns [`AmError::Decode`], which is never retried: the same request would produce the same
/// unparseable answer, and the fix is a code change rather than another attempt.
fn decode<T: DeserializeOwned>(body: &[u8]) -> Result<T, AmError> {
    serde_json::from_slice(body).map_err(|source| AmError::Decode {
        detail: source.to_string(),
    })
}

/// Cuts an error body to [`MAX_ERROR_BODY`] on a character boundary.
///
/// Byte length, not character count: the limit exists to bound what is logged, and walking back
/// to the nearest boundary is what keeps the cut from splitting a multi-byte character into two
/// pieces of nothing.
fn truncate(body: &str) -> String {
    let trimmed = body.trim();
    if trimmed.len() <= MAX_ERROR_BODY {
        return trimmed.to_owned();
    }

    let mut end = MAX_ERROR_BODY;
    while !trimmed.is_char_boundary(end) {
        end -= 1;
    }

    trimmed[..end].to_owned()
}

/// Wraps a configuration complaint in the variant that says so.
fn config_error(detail: impl Into<String>) -> AmError {
    AmError::Config {
        detail: detail.into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config(endpoints: &[&str]) -> AlertmanagerConfig {
        let mut parsed = Vec::new();
        for endpoint in endpoints {
            parsed.push(Url::parse(endpoint).expect("the test endpoint parses"));
        }

        AlertmanagerConfig {
            endpoints: parsed,
            ..AlertmanagerConfig::default()
        }
    }

    #[test]
    fn a_client_without_an_endpoint_is_refused() {
        let error = AlertmanagerClient::new(&config(&[])).expect_err("there is nothing to talk to");

        assert!(matches!(error, AmError::Config { .. }));
    }

    #[test]
    fn a_zero_timeout_is_refused_rather_than_failing_every_request() {
        let error = AlertmanagerClient::new(&AlertmanagerConfig {
            timeout_secs: 0,
            ..config(&["http://localhost:9093"])
        })
        .expect_err("a zero timeout is not a configuration");

        assert!(matches!(error, AmError::Config { .. }));
    }

    #[test]
    fn a_missing_ca_bundle_names_the_path_it_looked_for() {
        let error = AlertmanagerClient::new(&AlertmanagerConfig {
            ca_bundle: Some("/nonexistent/ca.pem".into()),
            ..config(&["http://localhost:9093"])
        })
        .expect_err("the bundle is not there");

        assert!(matches!(error, AmError::Config { detail } if detail.contains("ca.pem")));
    }

    #[test]
    fn a_path_prefix_on_an_endpoint_survives() {
        let endpoint = Url::parse("https://example.com/alertmanager").expect("the URL parses");

        let url = Call::get("api/v2/alerts")
            .url(&endpoint)
            .expect("the URL resolves");

        assert_eq!(
            url.as_str(),
            "https://example.com/alertmanager/api/v2/alerts"
        );
    }

    #[test]
    fn a_silence_id_is_percent_encoded_into_the_path() {
        let endpoint = Url::parse("http://localhost:9093/").expect("the URL parses");

        let url = Call::delete("api/v2/silence", "a/b c")
            .url(&endpoint)
            .expect("the URL resolves");

        assert_eq!(
            url.as_str(),
            "http://localhost:9093/api/v2/silence/a%2Fb%20c"
        );
    }

    #[test]
    fn a_filter_may_appear_more_than_once() {
        let endpoint = Url::parse("http://localhost:9093").expect("the URL parses");
        let mut call = Call::get("api/v2/alerts");
        call.query.push(("filter", "severity=critical".to_owned()));
        call.query.push(("filter", "namespace=prod".to_owned()));

        let url = call.url(&endpoint).expect("the URL resolves");

        assert_eq!(
            url.query(),
            Some("filter=severity%3Dcritical&filter=namespace%3Dprod")
        );
    }

    #[test]
    fn the_backoff_never_exceeds_its_ceiling() {
        let client = AlertmanagerClient::new(&config(&["http://localhost:9093"]))
            .expect("the client builds");

        for attempt in 1..=12 {
            assert!(
                client.backoff_ms(attempt) <= client.retry.ceiling_ms,
                "{attempt}"
            );
        }
    }

    #[test]
    fn the_jitter_stays_inside_its_bounds_and_moves() {
        let jitter = Jitter::new();
        let mut seen = Vec::new();

        for _ in 0..64 {
            let value = jitter.below(1_000);
            assert!(value <= 1_000);
            seen.push(value);
        }

        assert!(seen.iter().any(|value| *value != seen[0]));
        assert_eq!(jitter.below(0), 0);
    }

    #[test]
    fn an_oversized_error_body_is_cut_without_splitting_a_character() {
        let body = "é".repeat(1_000);

        let cut = truncate(&body);

        assert!(cut.len() <= MAX_ERROR_BODY);
        assert!(cut.chars().all(|character| character == 'é'));
    }

    #[test]
    fn a_short_error_body_is_kept_whole() {
        assert_eq!(
            truncate("  invalid silence: end time in the past  "),
            "invalid silence: end time in the past"
        );
    }
}
