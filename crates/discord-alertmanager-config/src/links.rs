//! Templates for the link buttons on a card, and the allowlist they are checked against.

use serde::Deserialize;
use url::Url;

/// Link buttons, and the two safety rails every rendered URL passes through.
///
/// Label values come from metric targets, so in any environment that scrapes user workloads they
/// are attacker-influenced. Every substitution is percent-encoded, and the finished URL is parsed
/// and checked against `allowed_hosts` before it becomes a button. A template that renders a
/// `javascript:` URL, or one pointing at a host nobody listed, is dropped and logged rather than
/// posted.
#[derive(Debug, Clone, Deserialize)]
#[cfg_attr(
    feature = "config-schema",
    derive(serde::Serialize, terrace_config::schema::Describe)
)]
#[serde(default, deny_unknown_fields)]
pub struct Links {
    /// Prometheus base URL, available to templates as `links.prometheus_base`.
    pub prometheus_base: Option<Url>,

    /// Grafana base URL, available to templates as `links.grafana_base`.
    pub grafana_base: Option<Url>,

    /// Hosts a rendered button may point at.
    ///
    /// An empty list rejects every button, which is the safe reading of "nothing was configured".
    /// The scheme is checked separately and is always `http` or `https`.
    pub allowed_hosts: Vec<String>,

    /// Seconds of graph shown before the alert started.
    pub window_lead_secs: u64,

    /// Seconds of graph shown after the alert ended, or after now while it is firing.
    pub window_trail_secs: u64,

    /// The buttons themselves, rendered in order.
    pub buttons: Vec<LinkButton>,
}

impl Default for Links {
    fn default() -> Self {
        Self {
            prometheus_base: None,
            grafana_base: None,
            allowed_hosts: Vec::new(),
            window_lead_secs: 900,
            window_trail_secs: 300,
            buttons: Vec::new(),
        }
    }
}

/// One link-style button on a card.
///
/// Link buttons carry no interaction token, so they cost nothing to handle and never expire.
#[derive(Debug, Clone, Default, Deserialize)]
#[cfg_attr(
    feature = "config-schema",
    derive(serde::Serialize, terrace_config::schema::Describe)
)]
#[serde(default, deny_unknown_fields)]
pub struct LinkButton {
    /// Text on the button. Discord truncates past 80 characters.
    pub label: String,

    /// URL template, rendered with `minijinja` against `alert`, `labels`, `annotations`, `links`
    /// and `window`.
    ///
    /// Every substitution in a URL is percent-encoded. A template that interpolates a value
    /// without a filter is refused when the template is compiled, not when it is rendered, so the
    /// mistake surfaces at boot.
    pub url: String,

    /// Expression that has to be truthy for the button to appear.
    ///
    /// Leave it unset for a button that always appears. `annotations.runbook_url` is the common
    /// case: no runbook annotation, no runbook button.
    pub when: Option<String>,
}
