//! The configuration surface, and the generator that turns it into `docs/config.md`.
//!
//! Five layers resolve one [`Config`]: the `serde` defaults on these types, `DAM_CONFIG` (a TOML
//! file or a directory of `*.toml` merged in sorted order), `DAM_`-prefixed environment variables
//! with `__` for nesting, files named after keys in `DAM_SECRETS_DIR`, and `DAM_<KEY>_FILE`
//! indirection. [`layers`] builds that stack; `terrace_config` implements it.
//!
//! The default shadow policy is kept. A key supplied by two of the last three layers fails the
//! load rather than resolving by precedence, because a stale `DAM_DISCORD__TOKEN` shadowing a
//! rotated mounted secret keeps the bot running on the old credential, and that surfaces during
//! an incident rather than during the deploy that caused it.
//!
//! # Every `///` here is published
//!
//! The doc comment on a public field in this crate is rendered into `docs/config.md`,
//! `config.example.toml` and the JSON contract by `examples/config-schema.rs`. It is written for
//! an operator setting the key, who is not reading this source. Reasoning aimed at whoever edits
//! the line goes in a plain `//` comment underneath, where it is not published.
//!
//! CI regenerates those files and fails on any difference, so a pull request that adds a key
//! without regenerating cannot merge.
//!
//! # Two departures from `docs/Design.MD`
//!
//! The design makes `storage` a `#[serde(tag = "backend")]` enum. `Describe` refuses an enum
//! whose variants carry data, on the grounds that such a variant is a shape rather than a value a
//! configuration file could hold, so a tagged enum would leave every storage key out of the
//! generated reference. [`Storage`] is a struct with a `backend` discriminant instead. The TOML
//! spelling is unchanged; what changes is that keys belonging to the unselected backend are
//! ignored rather than refused.
//!
//! `RouteTarget` is flat for the same reason, which is also the shape the design's own sample
//! TOML already uses.

mod alertmanager;
mod discord;
mod engine;
mod ingest;
mod links;
mod observability;
mod render;
mod routes;
mod storage;

pub use alertmanager::{Alertmanager, Retry};
pub use discord::{Capabilities, Discord};
pub use engine::{Engine, Retention, Storm};
pub use ingest::Ingest;
pub use links::{LinkButton, Links};
pub use observability::Observability;
pub use render::Render;
pub use routes::{
    ForumStateTags, GroupStrategy, Mentions, RouteConfig, RouteTarget, Severity, TargetKind,
    TargetPolicy, ThreadKind, ThreadTrigger,
};
pub use storage::{Backend, PostgresConfig, SqliteConfig, Storage};

use serde::Deserialize;
use terrace_config::Terrace;

/// Prefix every environment variable, secret file name and `_FILE` indirection derives from.
pub const ENV_PREFIX: &str = "DAM_";

/// Log format, read straight from the environment before the layers exist.
pub const LOG_FORMAT_VAR: &str = "DAM_LOG_FORMAT";

/// Log filter, read straight from the environment before the layers exist.
pub const LOG_LEVEL_VAR: &str = "DAM_LOG_LEVEL";

/// Builds the layer stack every load and every reload goes through.
///
/// Both reserved variables are read by the tracing subscriber, which is installed before the
/// configuration is loaded and is not rebuilt on reload. Reserving them turns an attempt to
/// supply either through a secrets file into an error instead of a silent no-op.
///
/// # Examples
///
/// ```no_run
/// # use dam_config::{Config, layers};
/// let config: Config = layers().load()?;
/// # Ok::<(), terrace_config::Error>(())
/// ```
#[must_use]
pub fn layers() -> Terrace {
    Terrace::new(ENV_PREFIX)
        .reserve(LOG_FORMAT_VAR)
        .reserve(LOG_LEVEL_VAR)
}

/// Everything the bot reads at boot.
#[derive(Debug, Clone, Default, Deserialize)]
#[cfg_attr(
    feature = "config-schema",
    derive(serde::Serialize, terrace_config::schema::Describe)
)]
#[serde(default, deny_unknown_fields)]
pub struct Config {
    /// Gateway credentials, command registration scope and the role-to-capability map.
    #[cfg_attr(feature = "config-schema", config(nested))]
    pub discord: Discord,

    /// Where Alertmanager is, how to authenticate to it, and how hard to retry.
    #[cfg_attr(feature = "config-schema", config(nested))]
    pub alertmanager: Alertmanager,

    /// Which database backend to use, and how to reach it.
    #[cfg_attr(feature = "config-schema", config(nested))]
    pub storage: Storage,

    /// The webhook listener that Alertmanager posts to.
    #[cfg_attr(feature = "config-schema", config(nested))]
    pub ingest: Ingest,

    /// Pipeline cadences, retention horizons and storm thresholds.
    #[cfg_attr(feature = "config-schema", config(nested))]
    pub engine: Engine,

    /// How an alert card is laid out and how often it may be edited.
    #[cfg_attr(feature = "config-schema", config(nested))]
    pub render: Render,

    /// Templates for the link buttons on a card, and the host allowlist they are checked against.
    #[cfg_attr(feature = "config-schema", config(nested))]
    pub links: Links,

    /// Metrics and the channel the deadman posts to.
    #[cfg_attr(feature = "config-schema", config(nested))]
    pub observability: Observability,

    /// Routes declared in the file, which cannot be edited or deleted from Discord.
    ///
    /// Declaring a route here is what makes a deployment reproducible from its manifests. A route
    /// that disappears from this list is disabled rather than deleted, so the notifications it
    /// created keep their history. `/route add` writes the other kind, which lives only in the
    /// database.
    pub routes: Vec<RouteConfig>,
}
