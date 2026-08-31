//! How to reach Alertmanager, authenticate to it, and retry it.

use std::path::PathBuf;

use secrecy::SecretString;
use serde::Deserialize;
use url::Url;

/// The Alertmanager peer set and the client's behaviour against it.
#[derive(Debug, Clone, Deserialize)]
#[cfg_attr(
    feature = "config-schema",
    derive(serde::Serialize, terrace_config::schema::Describe)
)]
#[serde(default, deny_unknown_fields)]
pub struct Alertmanager {
    /// Base URLs of the Alertmanager peers, tried in order.
    ///
    /// List every peer of a high-availability set. Silences gossip between peers, so any of them
    /// accepts a write and the first one that answers is used.
    pub endpoints: Vec<Url>,

    /// Bearer token sent to Alertmanager. Supply it through the secrets directory or `_FILE`.
    #[cfg_attr(feature = "config-schema", config(secret))]
    #[serde(skip_serializing)]
    pub bearer_token: Option<SecretString>,

    /// Username for basic authentication. Ignored when `bearer_token` is set.
    pub basic_username: Option<String>,

    /// Password for basic authentication. Supply it through the secrets directory or `_FILE`.
    #[cfg_attr(feature = "config-schema", config(secret))]
    #[serde(skip_serializing)]
    pub basic_password: Option<SecretString>,

    /// PEM bundle of certificate authorities to trust in addition to the system roots.
    ///
    /// There is deliberately no option to skip verification. An Alertmanager reachable only over
    /// a certificate nothing trusts is a certificate to fix, not a check to disable.
    pub ca_bundle: Option<PathBuf>,

    /// Seconds to wait for a whole request before giving up.
    pub timeout_secs: u64,

    /// Seconds to wait for a connection before trying the next endpoint.
    ///
    /// Short on purpose. This is how quickly a dead peer is abandoned for a live one.
    pub connect_timeout_secs: u64,

    /// Backoff applied to connection errors, timeouts and 5xx responses.
    #[cfg_attr(feature = "config-schema", config(nested))]
    pub retry: Retry,
}

impl Default for Alertmanager {
    fn default() -> Self {
        Self {
            endpoints: Vec::new(),
            bearer_token: None,
            basic_username: None,
            basic_password: None,
            ca_bundle: None,
            timeout_secs: 10,
            connect_timeout_secs: 2,
            retry: Retry::default(),
        }
    }
}

/// Retry schedule for the Alertmanager client.
///
/// Only connection errors, timeouts and 5xx responses are retried. A 4xx is never retried: it
/// means the request was wrong, and repeating it will keep being wrong.
#[derive(Debug, Clone, Deserialize)]
#[cfg_attr(
    feature = "config-schema",
    derive(serde::Serialize, terrace_config::schema::Describe)
)]
#[serde(default, deny_unknown_fields)]
pub struct Retry {
    /// Milliseconds to wait before the first retry.
    pub initial_backoff_ms: u64,

    /// Ceiling on a single wait, in seconds.
    pub max_backoff_secs: u64,

    /// Seconds to keep retrying one request before giving up on it.
    pub max_elapsed_secs: u64,
}

impl Default for Retry {
    fn default() -> Self {
        // Full jitter over these bounds. The ceiling is well inside the reconciler's own interval,
        // so a request never outlives the poll that issued it.
        Self {
            initial_backoff_ms: 200,
            max_backoff_secs: 10,
            max_elapsed_secs: 45,
        }
    }
}
