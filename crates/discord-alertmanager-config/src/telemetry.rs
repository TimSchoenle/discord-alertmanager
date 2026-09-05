//! What the process says about itself: the log stream, and where crashes and traces are reported.

mod sentry;

pub use sentry::{CaptureLevel, Sentry};

use serde::Deserialize;

/// The log stream, and the error reporter that reads from it.
///
/// One section rather than two, because they are one decision. The Sentry thresholds are ceilings
/// on top of `log_level`: the subscriber's filter runs first, so a record `log_level` drops is
/// never reported, whatever [`Sentry`] says. Splitting them across sections would put a key and
/// the key that silently overrules it in different tables.
///
/// # These keys describe the subscriber that reports on them
///
/// The subscriber is installed from `log_level` and `log_format`, which means it is installed
/// after the configuration has loaded rather than before. Nothing is lost in that window: loading
/// reads files and environment variables and logs nothing.
///
/// A configuration that will not load is the one exception, and it is the failure most worth
/// seeing. It is reported through a bootstrap subscriber that reads `DAM_TELEMETRY__LOG_LEVEL`
/// and `DAM_TELEMETRY__LOG_FORMAT` straight from the environment — the same two names the loader
/// derives for these keys, so each setting has one name and not two.
#[derive(Debug, Clone, Deserialize)]
#[cfg_attr(
    feature = "config-schema",
    derive(serde::Serialize, terrace_config::schema::Describe)
)]
#[serde(default, deny_unknown_fields)]
pub struct Telemetry {
    /// Filter directives for the log stream, in `RUST_LOG` syntax.
    ///
    /// A comma-separated list of `target=level` directives with an optional bare level as the
    /// default, so `warn,dam_engine=debug` raises one crate and leaves the rest alone. A directive
    /// that does not parse stops the process rather than being ignored: a filter that silently
    /// reverted to its default would be discovered while reading logs that should have been there.
    ///
    /// A bare word is a target name and not a level, so a misspelt `waarn` is read as a directive
    /// about a crate that does not exist. It parses, it stops nothing, and it filters nothing —
    /// check a level change took effect rather than assuming it did.
    pub log_level: String,

    /// How each log line is written.
    #[cfg_attr(feature = "config-schema", config(values))]
    pub log_format: LogFormat,

    /// Where crashes and traces are reported, and how much of the traffic reaches it.
    #[cfg_attr(feature = "config-schema", config(nested))]
    pub sentry: Sentry,
}

impl Default for Telemetry {
    fn default() -> Self {
        Self {
            // `info` for this workspace and `warn` for everything else, so a dependency's own
            // logging cannot bury the bot's. `dam_` covers every library crate here; the binary
            // is named in full because its target does not share that prefix.
            log_level: "warn,discord_alertmanager=info,dam_=info".to_owned(),
            log_format: LogFormat::Plain,
            sentry: Sentry::default(),
        }
    }
}

/// How a log line is written.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[cfg_attr(
    feature = "config-schema",
    derive(serde::Serialize, terrace_config::schema::Describe)
)]
#[serde(rename_all = "lowercase")]
pub enum LogFormat {
    /// One line per record, for a person reading a terminal.
    #[default]
    Plain,

    /// One JSON object per record, for a log aggregator that parses it.
    Json,
}
