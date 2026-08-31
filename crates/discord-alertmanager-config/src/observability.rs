//! Metrics, and where the deadman reports.

use serde::Deserialize;

/// What the bot exposes about itself, and who hears when it goes quiet.
///
/// The log format and filter are not here. Both are read straight from `DAM_LOG_FORMAT` and
/// `DAM_LOG_LEVEL` before the configuration exists, because the subscriber is installed first and
/// is not rebuilt on reload. Both names are reserved, so supplying either through a secrets file
/// is an error rather than a value that never takes effect.
#[derive(Debug, Clone, Deserialize)]
#[cfg_attr(
    feature = "config-schema",
    derive(serde::Serialize, terrace_config::schema::Describe)
)]
#[serde(default, deny_unknown_fields)]
pub struct Observability {
    /// Serve Prometheus metrics at `/metrics` on the ingest listener.
    pub metrics_enabled: bool,

    /// Channel the deadman and route-health notices post to.
    ///
    /// The deadman fires when no webhook has arrived inside the deadman window *and*
    /// Alertmanager is unreachable. Pair it with a `Watchdog` alert in Prometheus, so that
    /// silence on one side is always noise on the other.
    pub admin_channel_id: Option<u64>,
}

impl Default for Observability {
    fn default() -> Self {
        Self {
            metrics_enabled: true,
            admin_channel_id: None,
        }
    }
}
