//! The Alertmanager port: what the pipeline asks of Alertmanager, in the pipeline's own types.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use dam_core::{Alert, Fingerprint, MatcherSet};
use dam_store::SilenceLifecycle;
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Which alerts Alertmanager should return.
///
/// The flags are Alertmanager's own query parameters. The reconciler asks for all four, because
/// an alert missing from its answer is what it treats as resolved, and a suppressed alert that
/// went unasked-for would be resolved by mistake on the next poll.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[expect(
    clippy::struct_excessive_bools,
    reason = "the four flags are Alertmanager's own independent query parameters, and any \
              combination of them is a request somebody makes"
)]
pub struct AlertFilter {
    /// Include alerts Alertmanager is notifying about.
    pub active: bool,

    /// Include alerts a silence is suppressing.
    pub silenced: bool,

    /// Include alerts an inhibition rule is suppressing.
    pub inhibited: bool,

    /// Include alerts Alertmanager has not processed yet.
    pub unprocessed: bool,

    /// Server-side label filters, in Alertmanager's matcher syntax.
    pub matchers: Vec<String>,
}

impl AlertFilter {
    /// Everything Alertmanager knows about.
    #[must_use]
    pub fn everything() -> Self {
        Self {
            active: true,
            silenced: true,
            inhibited: true,
            unprocessed: true,
            matchers: Vec::new(),
        }
    }

    /// Everything, narrowed by a matcher expression.
    #[must_use]
    pub fn matching(expression: impl Into<String>) -> Self {
        Self {
            matchers: vec![expression.into()],
            ..Self::everything()
        }
    }
}

impl Default for AlertFilter {
    fn default() -> Self {
        Self::everything()
    }
}

/// A silence to create or, when `id` is set, to replace.
///
/// Creating is not idempotent: every call without an id produces another silence. A retry
/// therefore looks for the silence the previous attempt recorded and sets `id`, which
/// Alertmanager treats as an update.
///
/// Not serialisable, and deliberately: it carries compiled matchers, so anything that has to
/// survive a restart holds the matcher expression as text and compiles it on the way back in.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SilenceRequest {
    /// The silence to replace, if this is an update or a retry.
    pub id: Option<String>,

    /// What the silence suppresses.
    ///
    /// Sent as structured matchers rather than as an expression for Alertmanager to parse: the
    /// server changed matcher parsers in 0.27, and handing it a string means choosing which
    /// parser to be wrong about.
    pub matchers: MatcherSet,

    /// When it starts.
    pub starts_at: DateTime<Utc>,

    /// When it expires.
    pub ends_at: DateTime<Utc>,

    /// Who created it, as it should appear in `amtool`.
    pub created_by: String,

    /// Why it exists.
    pub comment: String,
}

/// A silence as Alertmanager reports it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SilenceRecord {
    /// The id Alertmanager assigned.
    pub id: String,

    /// What it suppresses.
    pub matchers: MatcherSet,

    /// When it starts.
    pub starts_at: DateTime<Utc>,

    /// When it expires.
    pub ends_at: DateTime<Utc>,

    /// When it was last written.
    pub updated_at: DateTime<Utc>,

    /// Who created it.
    pub created_by: String,

    /// Why it exists.
    pub comment: String,

    /// Where it is in its life.
    pub state: SilenceLifecycle,
}

/// What `/am status` reports and the deadman watches.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AmStatus {
    /// The server's version.
    pub version: String,

    /// When the server started.
    pub uptime: Option<DateTime<Utc>>,

    /// Names of the peers in the gossip cluster.
    pub peers: Vec<String>,

    /// Whether the cluster considers itself settled.
    pub cluster_ready: bool,

    /// Hash of the loaded configuration, for spotting a peer that did not reload.
    pub config_hash: Option<String>,
}

/// A receiver Alertmanager will route to, reported by `/route test`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Receiver {
    /// The receiver's name in `alertmanager.yml`.
    pub name: String,
}

/// What a call to Alertmanager can fail with.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum AmError {
    /// No configured endpoint accepted a connection.
    #[error("no Alertmanager endpoint reachable: {detail}")]
    Unreachable {
        /// The last transport failure seen while trying them in order.
        detail: String,
    },

    /// The request was accepted but did not answer in time.
    #[error("Alertmanager timed out after {elapsed_ms}ms")]
    Timeout {
        /// How long was spent waiting.
        elapsed_ms: u64,
    },

    /// Alertmanager answered, and the answer was an error.
    ///
    /// A 4xx is never retried: the request is wrong, and repeating it verbatim only produces the
    /// same answer more expensively.
    #[error("Alertmanager returned {status}: {body}")]
    Status {
        /// The HTTP status.
        status: u16,
        /// The body, truncated for logging.
        body: String,
    },

    /// The response did not match the model.
    ///
    /// Almost always an Alertmanager upgrade that changed a field, which is why the fixtures are
    /// pinned to a server version and tested.
    #[error("cannot decode Alertmanager response: {detail}")]
    Decode {
        /// What the decoder complained about.
        detail: String,
    },

    /// The client could not be built from the configuration.
    #[error("Alertmanager client configuration is invalid: {detail}")]
    Config {
        /// What is wrong with it.
        detail: String,
    },
}

impl AmError {
    /// Whether retrying the same request could plausibly succeed.
    ///
    /// Connection failures, timeouts and 5xx only. Retrying a 4xx is how a client turns its own
    /// bug into load on somebody else's server.
    #[must_use]
    pub fn is_retryable(&self) -> bool {
        match self {
            Self::Unreachable { .. } | Self::Timeout { .. } => true,
            Self::Status { status, .. } => *status >= 500,
            Self::Decode { .. } | Self::Config { .. } => false,
        }
    }

    /// Whether this failure means Alertmanager is unreachable rather than unhappy.
    ///
    /// What `/readyz` and the deadman both ask. A 400 from a malformed silence says nothing
    /// about the server's health; a connection failure says everything.
    #[must_use]
    pub fn is_unavailable(&self) -> bool {
        matches!(self, Self::Unreachable { .. } | Self::Timeout { .. })
            || matches!(self, Self::Status { status, .. } if *status >= 500)
    }
}

/// Everything the pipeline asks of Alertmanager.
///
/// Alertmanager stays the source of truth for both alerts and silences. The bot reads the first
/// and is a client of the second; it never keeps a silence of its own, because a silence that
/// only the bot knows about would stop nothing and would disappear with the bot.
#[async_trait]
pub trait AlertmanagerApi: Send + Sync {
    /// The alerts Alertmanager currently holds.
    ///
    /// # Errors
    ///
    /// Returns [`AmError`] when no endpoint answers, or when the answer is an error or does not
    /// decode.
    async fn list_alerts(&self, filter: &AlertFilter) -> Result<Vec<Alert>, AmError>;

    /// The silences Alertmanager currently holds.
    ///
    /// # Errors
    ///
    /// As [`AlertmanagerApi::list_alerts`].
    async fn list_silences(&self, matchers: &[String]) -> Result<Vec<SilenceRecord>, AmError>;

    /// Creates a silence, or replaces the one named by [`SilenceRequest::id`].
    ///
    /// # Errors
    ///
    /// As [`AlertmanagerApi::list_alerts`]. A silence whose matchers Alertmanager rejects is a
    /// [`AmError::Status`] with a 4xx and is not retried.
    async fn upsert_silence(&self, silence: &SilenceRequest) -> Result<String, AmError>;

    /// Expires a silence before its end time.
    ///
    /// # Errors
    ///
    /// As [`AlertmanagerApi::list_alerts`].
    async fn expire_silence(&self, id: &str) -> Result<(), AmError>;

    /// The server's status.
    ///
    /// # Errors
    ///
    /// As [`AlertmanagerApi::list_alerts`].
    async fn status(&self) -> Result<AmStatus, AmError>;

    /// The receivers configured on the server.
    ///
    /// # Errors
    ///
    /// As [`AlertmanagerApi::list_alerts`].
    async fn receivers(&self) -> Result<Vec<Receiver>, AmError>;
}

/// The fingerprints a set of silences is suppressing, given the alerts Alertmanager returned.
///
/// Derived from Alertmanager's own answer rather than by evaluating the silence's matchers
/// locally. The bot's matcher implementation agreeing with the server's is worth testing; it is
/// not worth trusting to decide which cards change colour.
#[must_use]
pub fn suppressed_fingerprints(alerts: &[Alert], silence_id: &str) -> Vec<Fingerprint> {
    let mut fingerprints = Vec::new();
    for alert in alerts {
        if alert.silenced_by.iter().any(|id| id == silence_id) {
            fingerprints.push(alert.fingerprint.clone());
        }
    }
    fingerprints
}
