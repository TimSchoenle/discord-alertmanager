//! The HTTP listener Alertmanager posts to.

use std::net::SocketAddr;

use secrecy::SecretString;
use serde::Deserialize;

/// The webhook, health and metrics listener.
///
/// Bind this to the cluster network. `docs/operations.md` carries a `NetworkPolicy` restricting
/// ingress to the Alertmanager pod, which is the control that matters here; the bearer token
/// below is the second one, not the first.
#[derive(Debug, Clone, Deserialize)]
#[cfg_attr(
    feature = "config-schema",
    derive(serde::Serialize, terrace_config::schema::Describe)
)]
#[serde(default, deny_unknown_fields)]
pub struct Ingest {
    /// Address and port to listen on.
    pub bind: SocketAddr,

    /// Path Alertmanager posts the version-4 envelope to.
    pub webhook_path: String,

    /// Bearer token every webhook request has to carry.
    ///
    /// Compared in constant time. A mismatch is a 401 with no body, so a caller learns that the
    /// token was wrong and nothing about how it was wrong. Leaving this unset disables the check
    /// and is only defensible where the listener is unreachable from outside the namespace.
    #[cfg_attr(feature = "config-schema", config(secret))]
    #[serde(skip_serializing)]
    pub webhook_token: Option<SecretString>,

    /// Largest accepted request body, in bytes.
    pub body_limit_bytes: usize,

    /// Seconds a request may take before the listener abandons it.
    pub request_timeout_secs: u64,

    /// Requests handled at once. Further requests queue rather than being rejected.
    pub max_concurrent_requests: usize,

    /// Seconds to let in-flight requests finish during shutdown.
    pub shutdown_drain_secs: u64,
}

impl Default for Ingest {
    fn default() -> Self {
        Self {
            // 0.0.0.0, because the useful case is a container whose address is not known here.
            bind: SocketAddr::from(([0, 0, 0, 0], 9099)),
            webhook_path: "/webhook".to_owned(),
            webhook_token: None,
            body_limit_bytes: 1024 * 1024,
            request_timeout_secs: 10,
            max_concurrent_requests: 64,
            shutdown_drain_secs: 10,
        }
    }
}
