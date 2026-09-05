//! Where a crash is reported, how much of the traffic is traced, and what never leaves the process.

use secrecy::SecretString;
use serde::Deserialize;

/// The Sentry client, which stays off until a DSN is supplied.
///
/// Everything here is inert without `dsn`, and supplying one is the whole switch: the client is
/// linked into every build, so turning reporting on is a deployment decision rather than a rebuild.
/// A DSN that does not parse stops the process at boot, because error reporting that silently
/// failed to start is discovered during the incident it was meant to describe.
///
/// # What is sent
///
/// Log records at or above `event_level` become Sentry events, records at or above
/// `breadcrumb_level` are kept as breadcrumbs behind the next event, and this bot's own spans at
/// or above `span_level` become the tracing timeline — the last of those only once
/// `traces_sample_rate` is above zero. The three thresholds are ceilings on top of
/// `DAM_LOG_LEVEL`: the subscriber's filter runs first, so a record it drops never reaches Sentry
/// whatever these say.
#[derive(Debug, Clone, Deserialize)]
#[cfg_attr(
    feature = "config-schema",
    derive(serde::Serialize, terrace_config::schema::Describe)
)]
#[serde(default, deny_unknown_fields)]
pub struct Sentry {
    /// Project DSN. Reporting is off while this is unset.
    ///
    /// It is a credential: anyone holding it can write events into the project. Supply it through
    /// `DAM_SENTRY__DSN_FILE` or the secrets directory rather than as a plain environment
    /// variable.
    #[cfg_attr(feature = "config-schema", config(secret))]
    #[serde(skip_serializing)]
    pub dsn: Option<SecretString>,

    /// Deployment this process belongs to, which every event is tagged with.
    ///
    /// Set deliberately rather than left at the default. Sentry groups, alerts and retention are
    /// all keyed on it, and a staging replica reporting as `production` is worse than one that
    /// reports nothing.
    pub environment: String,

    /// Release every event is attributed to. Defaults to the bot's own version.
    ///
    /// Override it only where the image carries a version this binary does not know, such as a
    /// build that appends a commit to the tag.
    pub release: Option<String>,

    /// Host name reported with each event. Defaults to the machine's own.
    ///
    /// Worth setting where the machine name is an ephemeral pod hash, which groups nothing.
    pub server_name: Option<String>,

    /// Fraction of events that are sent, from `0.0` to `1.0`.
    ///
    /// A blunt instrument: it drops whole events at random, so an error that fires once is the one
    /// most likely to be lost. Leave it at `1.0` and shed volume with `event_level` instead.
    pub sample_rate: f32,

    /// Fraction of traces that are sent, from `0.0` to `1.0`. Tracing is off at `0.0`.
    ///
    /// Every webhook batch, every outbox item, every periodic pass and every slash command is one
    /// trace, so the rate multiplies the busiest path in the process rather than the rarest. Start
    /// low.
    pub traces_sample_rate: f32,

    /// Level at or above which a log record is sent as an event of its own.
    #[cfg_attr(feature = "config-schema", config(values))]
    pub event_level: CaptureLevel,

    /// Level at or above which a log record is kept as a breadcrumb behind the next event.
    ///
    /// Breadcrumbs never leave the process on their own. They are attached to the event that
    /// follows them, which is what turns "a dispatch failed" into the sequence that led there.
    #[cfg_attr(feature = "config-schema", config(values))]
    pub breadcrumb_level: CaptureLevel,

    /// Level at or above which a span joins the trace.
    ///
    /// Only this bot's own spans are traced, whatever this says, and there is no key to widen
    /// that. A dependency instruments its internals for its own logs, and the fields it attaches
    /// are debug renderings of whatever the call was holding — not something to hand to a third
    /// party, and enough of them to bury the trace they surround.
    #[cfg_attr(feature = "config-schema", config(values))]
    pub span_level: CaptureLevel,

    /// Attach a stack trace to every event, not only to panics.
    ///
    /// Useful while a fault is being chased and expensive afterwards: it captures and symbolicates
    /// a backtrace on every capture, including the ones that were only ever going to be counted.
    pub attach_stacktrace: bool,

    /// Send the IP addresses, headers and user identifiers Sentry calls default PII.
    ///
    /// Off, and worth leaving off. The alerts this bot handles carry infrastructure labels, and
    /// the Discord side carries user identifiers; neither needs to reach a third party for an
    /// error report to be actionable.
    pub send_default_pii: bool,

    /// How many breadcrumbs are kept behind an event.
    pub max_breadcrumbs: usize,

    /// Seconds to spend delivering whatever is still queued during shutdown.
    ///
    /// Spent on the way out, after the listener has stopped, so it lengthens a restart by at most
    /// this much. Zero abandons the queue, which loses exactly the report explaining why the
    /// process is stopping.
    pub shutdown_timeout_secs: u64,

    /// Log what the Sentry client itself is doing.
    ///
    /// For diagnosing reporting that is not arriving, and nothing else. It is noisy, and it prints
    /// the DSN.
    pub debug: bool,
}

impl Default for Sentry {
    fn default() -> Self {
        // `environment` is set here rather than left absent so that the client never falls back to
        // Sentry's own `SENTRY_ENVIRONMENT` lookup. A value arriving from an unprefixed variable
        // nothing in this configuration mentions is the shadowing the loader exists to refuse.
        Self {
            dsn: None,
            environment: "production".to_owned(),
            release: None,
            server_name: None,
            sample_rate: 1.0,
            traces_sample_rate: 0.0,
            event_level: CaptureLevel::Error,
            breadcrumb_level: CaptureLevel::Info,
            span_level: CaptureLevel::Info,
            attach_stacktrace: false,
            send_default_pii: false,
            max_breadcrumbs: 100,
            shutdown_timeout_secs: 2,
            debug: false,
        }
    }
}

/// A severity threshold, or nothing at all.
///
/// The same six values the log filter uses, plus `off`, which is what distinguishes "the quietest
/// level still counts" from "this stream is disabled". Without it, a threshold of `error` would be
/// the closest thing to silence and would still send every error.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[cfg_attr(
    feature = "config-schema",
    derive(serde::Serialize, terrace_config::schema::Describe)
)]
#[serde(rename_all = "lowercase")]
pub enum CaptureLevel {
    /// Nothing on this stream reaches Sentry.
    Off,

    /// Errors only.
    #[default]
    Error,

    /// Errors and warnings.
    Warn,

    /// Everything down to `info`.
    Info,

    /// Everything down to `debug`.
    Debug,

    /// Everything the subscriber emits.
    Trace,
}
