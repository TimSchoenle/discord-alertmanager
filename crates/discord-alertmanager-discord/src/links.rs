//! The link buttons on a card, and the two rails every one of them passes.
//!
//! A link button carries no interaction token, so it costs nothing to handle and never expires.
//! What it does carry is a URL built out of label values, and the whole of this module is about
//! that: every substitution is percent-encoded, and the finished URL is parsed and checked against
//! the configured host allowlist before it becomes something a person is invited to click.
//!
//! Both checks happen in different places on purpose. The encoding is enforced when the templates
//! are compiled, at boot, because a template that interpolates a raw label is a mistake in the
//! configuration and should stop the deployment rather than wait for the alert that exploits it.
//! The allowlist is enforced per render, because the value that decides the host is not known
//! until an alert arrives.

use chrono::{DateTime, Duration, Utc};
use dam_config::Links;
use dam_core::Alert;
use minijinja::Environment;
use thiserror::Error;
use tracing::warn;
use url::Url;

use crate::template;

/// A button ready to be put on a card.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RenderedLink {
    /// The text on the button.
    pub label: String,

    /// Where it points, already validated.
    pub url: String,
}

/// Why a link configuration could not be accepted.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum LinkError {
    /// A template does not compile.
    #[error("link button `{label}` has a template that does not compile: {detail}")]
    Template {
        /// Which button.
        label: String,
        /// What the compiler said.
        detail: String,
    },

    /// A template substitutes a value into a URL without encoding it.
    #[error(
        "link button `{label}` interpolates `{expression}` into a URL without `| urlencode`, \
         which lets a label value rewrite the address"
    )]
    Unfiltered {
        /// Which button.
        label: String,
        /// The substitution that was refused.
        expression: String,
    },
}

/// One compiled button.
struct Compiled {
    label: String,
    url: String,
    when: Option<String>,
}

/// Renders the configured link buttons for an alert.
pub struct LinkRenderer {
    environment: Environment<'static>,
    buttons: Vec<Compiled>,
    allowed_hosts: Vec<String>,
    lead: Duration,
    trail: Duration,
    bases: Vec<(&'static str, String)>,
}

impl LinkRenderer {
    /// Compiles every configured template, refusing an unsafe one.
    ///
    /// # Errors
    ///
    /// Returns [`LinkError::Template`] for a template that does not compile and
    /// [`LinkError::Unfiltered`] for one that substitutes a value into a URL without encoding it.
    pub fn new(config: &Links) -> Result<Self, LinkError> {
        let environment = template::environment();
        let mut buttons = Vec::with_capacity(config.buttons.len());

        for button in &config.buttons {
            check_encoded(&button.label, &button.url)?;

            // Compiled and thrown away: the environment holds no named templates, so this is the
            // only place a syntax error can be caught before an incident is the thing that finds
            // it. Rendering later uses `render_str` against the same environment.
            environment
                .template_from_str(&button.url)
                .map_err(|error| LinkError::Template {
                    label: button.label.clone(),
                    detail: error.to_string(),
                })?;

            if let Some(when) = &button.when {
                let guard = guard_template(when);
                environment
                    .template_from_str(&guard)
                    .map_err(|error| LinkError::Template {
                        label: button.label.clone(),
                        detail: error.to_string(),
                    })?;
            }

            buttons.push(Compiled {
                label: button.label.clone(),
                url: button.url.clone(),
                when: button.when.clone(),
            });
        }

        let mut bases = Vec::new();
        if let Some(base) = &config.prometheus_base {
            bases.push(("prometheus_base", trimmed(base)));
        }
        if let Some(base) = &config.grafana_base {
            bases.push(("grafana_base", trimmed(base)));
        }

        Ok(Self {
            environment,
            buttons,
            allowed_hosts: config
                .allowed_hosts
                .iter()
                .map(|host| host.to_ascii_lowercase())
                .collect(),
            lead: Duration::seconds(seconds(config.window_lead_secs)),
            trail: Duration::seconds(seconds(config.window_trail_secs)),
            bases,
        })
    }

    /// The buttons that apply to one alert, in configured order.
    ///
    /// A button whose guard is false is omitted, and one whose URL fails to render or fails
    /// validation is dropped with a warning. Neither is an error: losing a link is not a reason to
    /// lose the notification the link is attached to.
    #[must_use]
    pub fn render(&self, alert: &Alert, now: DateTime<Utc>) -> Vec<RenderedLink> {
        let window = self.window(alert, now);
        let context = template::context(alert, &self.bases, Some(window), now);
        let mut rendered = Vec::new();

        for button in &self.buttons {
            if let Some(when) = &button.when {
                let guard = self
                    .environment
                    .render_str(&guard_template(when), context.clone());

                if !matches!(guard.as_deref(), Ok("1")) {
                    continue;
                }
            }

            let url = match self.environment.render_str(&button.url, context.clone()) {
                Ok(url) => url,
                Err(error) => {
                    warn!(button = button.label, %error, "cannot render a link button");
                    continue;
                }
            };

            match self.validate(&url) {
                Ok(url) => rendered.push(RenderedLink {
                    label: button.label.clone(),
                    url,
                }),
                Err(reason) => {
                    warn!(button = button.label, reason, "refusing a link button");
                }
            }
        }

        rendered
    }

    /// The graph window an alert should open on, in milliseconds.
    ///
    /// From a little before the alert started to a little after it ended, or to a little after now
    /// while it is still firing, so the graph opens framed on the incident rather than on the last
    /// six hours of everything.
    fn window(&self, alert: &Alert, now: DateTime<Utc>) -> (i64, i64) {
        let from = alert.starts_at - self.lead;
        let to = alert.ends_at.unwrap_or(now) + self.trail;

        (from.timestamp_millis(), to.timestamp_millis())
    }

    /// Checks a rendered URL against the scheme and host rules.
    fn validate(&self, rendered: &str) -> Result<String, &'static str> {
        let url = Url::parse(rendered).map_err(|_| "the rendered URL does not parse")?;

        if !matches!(url.scheme(), "http" | "https") {
            return Err("the scheme is neither http nor https");
        }

        let host = url
            .host_str()
            .ok_or("the URL has no host")?
            .to_ascii_lowercase();

        if !self.allowed_hosts.contains(&host) {
            // An empty allowlist rejects everything, which is the safe reading of "nothing was
            // configured": a button nobody authorised is a button pointing wherever a label says.
            return Err("the host is not in links.allowed_hosts");
        }

        Ok(url.to_string())
    }
}

/// Wraps a guard expression in the smallest template that reports its truth.
///
/// `compile_expression` would say the same thing and hand back a value borrowing the environment,
/// which would put a lifetime through this whole type for one boolean.
fn guard_template(when: &str) -> String {
    format!("{{% if {when} %}}1{{% endif %}}")
}

/// Refuses a URL template that substitutes a value without encoding it.
///
/// The check is lexical rather than a walk of the parsed template, and deliberately strict: a
/// substitution passes when it ends in `| urlencode` or reads only from `links`, which is
/// configuration rather than anything an alert can influence. Anything else is a mistake worth
/// stopping the deployment for.
fn check_encoded(label: &str, template: &str) -> Result<(), LinkError> {
    let mut rest = template;

    while let Some(start) = rest.find("{{") {
        let after = &rest[start + 2..];
        let Some(end) = after.find("}}") else {
            // An unterminated block is a syntax error, and the compiler reports it better than
            // this function could.
            return Ok(());
        };

        let expression = after[..end].trim();
        let encoded = expression
            .rsplit('|')
            .next()
            .is_some_and(|filter| filter.trim() == "urlencode");
        let configuration_only = expression.starts_with("links.");

        if !encoded && !configuration_only {
            return Err(LinkError::Unfiltered {
                label: label.to_owned(),
                expression: expression.to_owned(),
            });
        }

        rest = &after[end + 2..];
    }

    Ok(())
}

/// A base URL without its trailing slash, so a template can write `{{ links.x }}/d/…`.
fn trimmed(base: &Url) -> String {
    base.as_str().trim_end_matches('/').to_owned()
}

/// Clamps a configured number of seconds to what a `Duration` can hold.
fn seconds(value: u64) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX / 1000)
}

#[cfg(test)]
mod tests {
    use chrono::TimeZone;
    use dam_config::LinkButton;
    use dam_core::{AlertStatus, AmState, Annotations, Fingerprint, LabelName, Labels};

    use super::*;

    fn alert(labels: &[(&str, &str)]) -> Alert {
        Alert {
            fingerprint: Fingerprint::new("deadbeef").expect("the fingerprint is hexadecimal"),
            labels: labels
                .iter()
                .map(|(name, value)| {
                    (
                        LabelName::new(*name).expect("the label name is valid"),
                        (*value).to_owned(),
                    )
                })
                .collect::<Labels>(),
            annotations: Annotations::new(),
            starts_at: Utc.timestamp_opt(1_700_000_000, 0).single().expect("valid"),
            ends_at: None,
            generator_url: None,
            status: AlertStatus::Firing,
            am_state: AmState::Active,
            silenced_by: Vec::new(),
            inhibited_by: Vec::new(),
            group_key: None,
        }
    }

    fn config(url: &str, hosts: &[&str]) -> Links {
        Links {
            grafana_base: Some(Url::parse("https://grafana.example.net/").expect("valid")),
            allowed_hosts: hosts.iter().map(|host| (*host).to_owned()).collect(),
            buttons: vec![LinkButton {
                label: "Dashboard".to_owned(),
                url: url.to_owned(),
                when: None,
            }],
            ..Links::default()
        }
    }

    #[test]
    fn an_unencoded_substitution_stops_the_deployment() {
        let refused = LinkRenderer::new(&config(
            "{{ links.grafana_base }}/d/x?pod={{ labels.pod }}",
            &["grafana.example.net"],
        ))
        .err();

        assert!(
            matches!(refused, Some(LinkError::Unfiltered { .. })),
            "a template that lets a label value rewrite the address has to stop the deployment"
        );
    }

    #[test]
    fn a_label_value_cannot_rewrite_the_address() {
        let renderer = LinkRenderer::new(&config(
            "{{ links.grafana_base }}/d/x?pod={{ labels.pod | urlencode }}",
            &["grafana.example.net"],
        ))
        .expect("the template is accepted");

        let produced = renderer.render(
            &alert(&[("pod", "../../evil?a=b")]),
            Utc.timestamp_opt(1_700_000_100, 0).single().expect("valid"),
        );

        assert_eq!(produced.len(), 1);
        assert!(
            produced[0]
                .url
                .starts_with("https://grafana.example.net/d/x?pod=..%2F..%2Fevil"),
            "the value stayed a value: {}",
            produced[0].url
        );
    }

    #[test]
    fn a_host_nobody_listed_is_dropped() {
        let renderer = LinkRenderer::new(&config(
            "{{ labels.target | urlencode }}",
            &["grafana.example.net"],
        ))
        .expect("the template is accepted");

        let produced = renderer.render(
            &alert(&[("target", "https://attacker.example.com/")]),
            Utc.timestamp_opt(1_700_000_100, 0).single().expect("valid"),
        );

        assert!(
            produced.is_empty(),
            "a URL whose host nobody authorised never becomes a button"
        );
    }

    #[test]
    fn a_guard_that_is_false_omits_the_button() {
        let mut links = config(
            "{{ annotations.runbook_url | urlencode }}",
            &["runbooks.example.net"],
        );
        links.buttons[0].when = Some("annotations.runbook_url is defined".to_owned());

        let renderer = LinkRenderer::new(&links).expect("the template is accepted");
        let produced = renderer.render(
            &alert(&[]),
            Utc.timestamp_opt(1_700_000_100, 0).single().expect("valid"),
        );

        assert!(
            produced.is_empty(),
            "no runbook annotation, no runbook button"
        );
    }

    #[test]
    fn the_window_frames_the_incident() {
        let renderer = LinkRenderer::new(&config(
            "{{ links.grafana_base }}/d/x?from={{ window.from_ms | urlencode }}",
            &["grafana.example.net"],
        ))
        .expect("the template is accepted");

        let produced = renderer.render(
            &alert(&[]),
            Utc.timestamp_opt(1_700_000_100, 0).single().expect("valid"),
        );

        assert_eq!(
            produced[0].url, "https://grafana.example.net/d/x?from=1699999100000",
            "the window opens fifteen minutes before the alert did"
        );
    }
}
