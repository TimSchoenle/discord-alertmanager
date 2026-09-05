//! The subscriber, and the Sentry client behind it.
//!
//! Both are built from `telemetry`, and both go up as early as a thing built from the
//! configuration can: immediately after it loads. Nothing is lost in that window — loading reads
//! files and environment variables and logs nothing — and the alternative is a subscriber
//! installed before the keys describing it have been read.
//!
//! # There is always a subscriber before a failure is reported
//!
//! Two failures happen while there is nothing to report them through: a configuration that will
//! not load, and a `log_level` that is not a filter. [`install_bootstrap`] answers both. It is the
//! same subscriber built from the defaults with `DAM_TELEMETRY__LOG_LEVEL` and
//! `DAM_TELEMETRY__LOG_FORMAT` read straight from the environment — the names the loader derives
//! for those two keys, so an operator raising the level to read a boot failure uses the name they
//! already know. [`install`] puts it up itself on the second failure, so a caller holding the
//! error it returns can always report it.
//!
//! # Nothing outside this file knows about Sentry
//!
//! The spans this process is traced by are ordinary `tracing` spans in the crates that do the
//! work. None of them names Sentry, sets a Sentry operation or carries a Sentry field: the layer
//! turns a root span into a transaction and a nested one into a child span on its own. Sentry is a
//! consumer of the instrumentation, not a reason for it, and replacing it is an edit to this file.

use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use dam_config::{CaptureLevel, LogFormat, Sentry as SentryConfig, Telemetry};
use secrecy::ExposeSecret;
use sentry::integrations::tracing::EventFilter;
use sentry::types::Dsn;
use tracing::{Level, Metadata, Subscriber};
use tracing_subscriber::EnvFilter;
use tracing_subscriber::prelude::*;
use tracing_subscriber::registry::LookupSpan;

/// Environment spelling of `telemetry.log_format`, read directly by the bootstrap subscriber.
const LOG_FORMAT_VAR: &str = dam_config::LOG_FORMAT_VAR;

/// Environment spelling of `telemetry.log_level`, read directly by the bootstrap subscriber.
const LOG_LEVEL_VAR: &str = dam_config::LOG_LEVEL_VAR;

/// Target prefixes that mark a span as this process's own work.
///
/// The binary and the `dam_…` libraries, which is every crate in this workspace and nothing else.
const OWN_TARGETS: [&str; 2] = ["discord_alertmanager", "dam_"];

/// The three levels that decide what a record becomes.
#[derive(Debug, Clone, Copy)]
struct Thresholds {
    /// At or above this, a record is an event of its own.
    event: Option<Level>,

    /// At or above this, a record is a breadcrumb behind the next event.
    breadcrumb: Option<Level>,

    /// At or above this, a span joins the trace.
    span: Option<Level>,
}

impl From<&SentryConfig> for Thresholds {
    fn from(config: &SentryConfig) -> Self {
        Self {
            event: level(config.event_level),
            breadcrumb: level(config.breadcrumb_level),
            span: level(config.span_level),
        }
    }
}

/// Installs the subscriber every crate logs through.
///
/// The Sentry layer is part of it from the first line, and reports nothing until
/// [`install_sentry`] binds a client to the hub it consults. That is what lets the subscriber be
/// installed once and never rebuilt.
///
/// # Errors
///
/// A `log_level` that is not a filter directive. The bootstrap subscriber goes up before the error
/// is returned, so whoever receives it can report it.
pub(crate) fn install(config: &Telemetry) -> Result<()> {
    let filter = match EnvFilter::try_new(&config.log_level) {
        Ok(filter) => filter,
        Err(error) => {
            // Nothing is installed yet, and the complaint about the filter that would not parse
            // has to reach someone. Quietly substituting the default here instead would start the
            // process on a verbosity nobody asked for.
            install_bootstrap();

            return Err(error).context(format!(
                "`telemetry.log_level` is not a filter directive: `{}`",
                config.log_level
            ));
        }
    };

    init(
        filter,
        config.log_format,
        Some(Thresholds::from(&config.sentry)),
    );

    Ok(())
}

/// Installs the subscriber that reports a configuration the process could not read.
///
/// The defaults, overridden by the two environment variables the loader would have derived for
/// those keys. Each override is ignored rather than refused when it does not parse: this is the
/// path where something has already failed to be read, and replacing that report with a second one
/// about the reporting would bury it.
pub(crate) fn install_bootstrap() {
    let mut settings = Telemetry::default();

    if let Ok(level) = std::env::var(LOG_LEVEL_VAR)
        && EnvFilter::try_new(&level).is_ok()
    {
        settings.log_level = level;
    }

    if std::env::var(LOG_FORMAT_VAR).is_ok_and(|format| format.eq_ignore_ascii_case("json")) {
        settings.log_format = LogFormat::Json;
    }

    // Infallible by construction: the directive is either the default, which a test holds to
    // parsing, or a string that parsed a few lines ago.
    let filter = EnvFilter::try_new(&settings.log_level)
        .unwrap_or_else(|_| EnvFilter::new(Telemetry::default().log_level));

    init(filter, settings.log_format, None);
}

/// Builds the layer stack and makes it the global subscriber.
///
/// `thresholds` is [`None`] for the bootstrap subscriber, which has no configuration and therefore
/// no DSN. Without a client the Sentry layer would do nothing; leaving it out says so.
fn init(filter: EnvFilter, format: LogFormat, thresholds: Option<Thresholds>) {
    // The filter is a layer rather than a per-layer filter, so it gates the Sentry layer as well
    // as the printed output: a record `log_level` drops is never reported, whatever the Sentry
    // thresholds say. That is the intended relationship — one dial for what the process observes
    // at all, three for how much of that a third party is told about.
    let registry = tracing_subscriber::registry()
        .with(filter)
        .with(thresholds.map(sentry_layer));

    match format {
        LogFormat::Json => registry
            .with(tracing_subscriber::fmt::layer().json())
            .init(),
        LogFormat::Plain => registry.with(tracing_subscriber::fmt::layer()).init(),
    }
}

/// Binds a Sentry client to the hub, and answers whether one was configured.
///
/// The client is linked into every build and started by configuration alone, for the same reason
/// both database backends are: one artefact answers every deployment, and a binary that had to be
/// rebuilt to turn reporting on would make the choice a property of the build rather than of the
/// environment it runs in.
///
/// # Errors
///
/// A DSN that does not parse, or a sample rate outside `0.0..=1.0`. Both stop the process. Error
/// reporting that quietly failed to start is discovered during the incident it was meant to
/// describe, and an out-of-range rate would otherwise panic inside the client's own builders.
pub(crate) fn install_sentry(config: &SentryConfig) -> Result<bool> {
    let Some(dsn) = config.dsn.as_ref() else {
        return Ok(false);
    };

    ensure_fraction("telemetry.sentry.sample_rate", config.sample_rate)?;
    ensure_fraction(
        "telemetry.sentry.traces_sample_rate",
        config.traces_sample_rate,
    )?;

    let dsn: Dsn = dsn
        .expose_secret()
        .parse()
        .context("parsing `telemetry.sentry.dsn`")?;

    let mut options = sentry::ClientOptions::new();
    options.dsn = Some(dsn);
    options.debug = config.debug;
    options.max_breadcrumbs = config.max_breadcrumbs;
    options.attach_stacktrace = config.attach_stacktrace;
    options.send_default_pii = config.send_default_pii;
    options.shutdown_timeout = Duration::from_secs(config.shutdown_timeout_secs);
    options.event_sampling_strategy = sentry::EventSamplingStrategy::FixedRate(config.sample_rate);
    // `Disabled` rather than a rate of zero: it is the difference between sampling every
    // transaction away and never starting one, and starting one costs an allocation on every
    // webhook, every outbox item and every command.
    options.traces_sampling_strategy = if config.traces_sample_rate > 0.0 {
        sentry::TracesSamplingStrategy::FixedRate(config.traces_sample_rate)
    } else {
        sentry::TracesSamplingStrategy::Disabled
    };
    options.environment = Some(config.environment.clone().into());
    // Always set, so the client never reaches for `SENTRY_RELEASE`. An unprefixed variable this
    // configuration does not mention is exactly the shadowing the loader refuses elsewhere.
    options.release = Some(
        config
            .release
            .clone()
            .unwrap_or_else(|| env!("CARGO_PKG_VERSION").to_owned())
            .into(),
    );
    options.server_name = config.server_name.clone().map(Into::into);

    // `apply_defaults` is what `sentry::init` would call: it installs the transport and the panic,
    // context and stack-trace integrations. It is called directly rather than through `init`
    // because `init` hands back a guard whose drop closes the client, and the client has to
    // outlive `run` so that the failure which stopped the process is reported rather than dropped
    // along with it.
    let mut options = sentry::apply_defaults(options);
    // `apply_defaults` reads `SSL_VERIFY` and switches off certificate verification for the
    // transport when it says so. There is deliberately no configuration key for that here, for the
    // same reason there is none for Alertmanager: a Sentry reachable only over a certificate
    // nothing trusts is a certificate to fix, not a check to disable.
    options.accept_invalid_certs = false;

    let client = Arc::new(sentry::Client::from_config(options));
    sentry::Hub::current().bind_client(Some(client));

    Ok(true)
}

/// Delivers whatever Sentry still holds, and closes the client.
///
/// Called after the last thing the process logs, so the error that stopped it is inside the flush
/// rather than one event behind it. A no-op when no client was ever bound.
pub(crate) fn flush_sentry() {
    let Some(client) = sentry::Hub::current().client() else {
        return;
    };

    // `None` is the timeout the client was built with, which is
    // `telemetry.sentry.shutdown_timeout_secs`.
    if !client.close(None) {
        tracing::warn!("Sentry did not deliver everything it had queued before the timeout");
    }
}

/// The Sentry layer, filtered by the configured thresholds.
///
/// A record is one thing or the other, never both: a record that already carries its own report
/// does not also need to be a breadcrumb behind the next one.
fn sentry_layer<S>(thresholds: Thresholds) -> sentry::integrations::tracing::SentryLayer<S>
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    sentry::integrations::tracing::layer()
        .event_filter(move |metadata| {
            if clears(thresholds.event, metadata) {
                EventFilter::Event
            } else if clears(thresholds.breadcrumb, metadata) {
                EventFilter::Breadcrumb
            } else {
                EventFilter::Ignore
            }
        })
        .span_filter(move |metadata| is_own_work(metadata) && clears(thresholds.span, metadata))
}

/// Whether a span belongs to this workspace rather than to a dependency.
///
/// Only this workspace's spans are traced, and there is deliberately no key to widen that. Two
/// reasons, both of which bite the moment `telemetry.log_level` is loosened to `info` for an
/// unrelated investigation. `serenity`, `reqwest` and `sqlx` instrument their internals densely, so
/// the trace stops being a record of what the bot did and the transaction volume rises by orders of
/// magnitude. Worse, their span fields are `Debug` renderings of whatever the call was holding — a
/// `Request`, a `Client`, a query — written for a local log and not for export to a third party. A
/// trace is the bot's own units of work: a webhook batch, an outbox item, a periodic pass, an
/// interaction.
///
/// Events are not filtered this way. A dependency logging at `error` is exactly what error
/// reporting is for, and an event carries a message rather than a dump of a caller's internals.
fn is_own_work(metadata: &Metadata<'_>) -> bool {
    OWN_TARGETS
        .iter()
        .any(|prefix| metadata.target().starts_with(prefix))
}

/// Whether a record at this metadata's level clears a threshold.
///
/// `tracing` orders levels by verbosity rather than by severity — `TRACE` is the greatest of them
/// — so "at or above `threshold` in severity" is `<=` here. [`None`] is the disabled stream, which
/// nothing clears.
fn clears(threshold: Option<Level>, metadata: &Metadata<'_>) -> bool {
    threshold.is_some_and(|threshold| *metadata.level() <= threshold)
}

/// The `tracing` level a configured threshold names, or [`None`] for `off`.
fn level(configured: CaptureLevel) -> Option<Level> {
    match configured {
        CaptureLevel::Off => None,
        CaptureLevel::Error => Some(Level::ERROR),
        CaptureLevel::Warn => Some(Level::WARN),
        CaptureLevel::Info => Some(Level::INFO),
        CaptureLevel::Debug => Some(Level::DEBUG),
        CaptureLevel::Trace => Some(Level::TRACE),
    }
}

/// Refuses a rate the Sentry client would only panic on.
fn ensure_fraction(key: &str, value: f32) -> Result<()> {
    if !(0.0..=1.0).contains(&value) {
        bail!("`{key}` is {value}, which is outside the range 0.0 to 1.0");
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `clears` reads `<=` as "at least this severe", which only holds while `tracing` keeps
    /// ordering its levels by verbosity. Asserted here rather than assumed in a comment.
    #[test]
    fn tracing_orders_levels_by_verbosity() {
        assert!(Level::ERROR <= Level::WARN);
        assert!(Level::TRACE > Level::DEBUG);
    }

    #[test]
    fn off_maps_to_no_level_and_every_other_value_to_its_own() {
        assert_eq!(level(CaptureLevel::Off), None);
        assert_eq!(level(CaptureLevel::Error), Some(Level::ERROR));
        assert_eq!(level(CaptureLevel::Warn), Some(Level::WARN));
        assert_eq!(level(CaptureLevel::Info), Some(Level::INFO));
        assert_eq!(level(CaptureLevel::Debug), Some(Level::DEBUG));
        assert_eq!(level(CaptureLevel::Trace), Some(Level::TRACE));
    }

    #[test]
    fn the_defaults_report_errors_and_keep_information_behind_them() {
        let thresholds = Thresholds::from(&Telemetry::default().sentry);

        assert_eq!(thresholds.event, Some(Level::ERROR));
        assert_eq!(thresholds.breadcrumb, Some(Level::INFO));
        assert_eq!(thresholds.span, Some(Level::INFO));
    }

    /// The default is what the bootstrap subscriber falls back to, so a typo in it would surface
    /// only on the path that reports a configuration nobody could load.
    #[test]
    fn the_default_log_level_is_a_filter_directive() {
        assert!(EnvFilter::try_new(Telemetry::default().log_level).is_ok());
    }

    /// What `install` refuses, and — just as much the point — what it cannot refuse.
    ///
    /// `EnvFilter` rejects a level it does not know and syntax it cannot parse, which is what the
    /// error path is for. It accepts a bare word as a target name, so `waarn` is a directive about
    /// a crate that does not exist rather than a misspelt level. Nothing here can tell those apart,
    /// which is why the key's documentation warns about it instead.
    #[test]
    fn a_log_level_is_refused_only_when_it_cannot_parse() {
        for directive in ["warn,dam_engine=debug", "info", "dam_ingest=trace,warn"] {
            assert!(
                EnvFilter::try_new(directive).is_ok(),
                "{directive} is a filter and must be accepted"
            );
        }

        for directive in ["dam_engine=verbose", "dam_engine=99", "=", "a=b=c", "["] {
            assert!(
                EnvFilter::try_new(directive).is_err(),
                "{directive} is not a filter and must be refused"
            );
        }

        // Accepted as a target name, not a level. The trap the key documents.
        assert!(EnvFilter::try_new("waarn").is_ok());
    }

    #[test]
    fn only_this_workspace_is_traced() {
        for own in [
            "discord_alertmanager::tasks",
            "dam_ingest::router",
            "dam_discord::bot",
        ] {
            assert!(OWN_TARGETS.iter().any(|prefix| own.starts_with(prefix)));
        }

        for foreign in [
            "serenity::http::request",
            "reqwest::connect",
            "sqlx::query",
            "hyper::client",
        ] {
            assert!(
                !OWN_TARGETS.iter().any(|prefix| foreign.starts_with(prefix)),
                "{foreign} is a dependency and must not be traced"
            );
        }
    }

    #[test]
    fn a_configuration_without_a_dsn_binds_nothing() {
        assert!(!install_sentry(&SentryConfig::default()).expect("no DSN is not a failure"));
    }

    #[test]
    fn a_rate_outside_the_unit_interval_is_refused() {
        assert!(ensure_fraction("telemetry.sentry.sample_rate", 0.0).is_ok());
        assert!(ensure_fraction("telemetry.sentry.sample_rate", 1.0).is_ok());
        assert!(ensure_fraction("telemetry.sentry.sample_rate", -0.1).is_err());
        assert!(ensure_fraction("telemetry.sentry.sample_rate", 1.1).is_err());
        assert!(ensure_fraction("telemetry.sentry.sample_rate", f32::NAN).is_err());
    }

    #[test]
    fn a_dsn_that_is_not_one_stops_the_process() {
        let config = SentryConfig {
            dsn: Some("not a dsn".into()),
            ..SentryConfig::default()
        };

        assert!(install_sentry(&config).is_err());
    }
}
