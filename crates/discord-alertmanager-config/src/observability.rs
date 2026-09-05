//! Metrics, and where the deadman reports.

use serde::Deserialize;

/// What the bot exposes about itself, and who hears when it goes quiet.
///
/// The log stream is not here. It lives under `telemetry`, next to the error reporter that reads
/// from it, because a Sentry threshold only ever narrows what `telemetry.log_level` already
/// allows. What stays here is the pull surface a scrape reads and the channel the bot posts to,
/// neither of which the subscriber is involved in.
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
