//! Which database backend to use, and how to reach each of them.

use std::path::PathBuf;

use secrecy::SecretString;
use serde::Deserialize;

/// The storage section.
///
/// Only the table named by `backend` is read. Keys under the other one are ignored, which is what
/// lets a deployment carry both and switch with a single key.
#[derive(Debug, Clone, Default, Deserialize)]
#[cfg_attr(
    feature = "config-schema",
    derive(serde::Serialize, terrace_config::schema::Describe)
)]
#[serde(default, deny_unknown_fields)]
pub struct Storage {
    /// Which backend the bot connects to.
    #[cfg_attr(feature = "config-schema", config(values))]
    pub backend: Backend,

    /// Connection settings used when `backend` is `sqlite`.
    #[cfg_attr(feature = "config-schema", config(nested))]
    pub sqlite: SqliteConfig,

    /// Connection settings used when `backend` is `postgres`.
    #[cfg_attr(feature = "config-schema", config(nested))]
    pub postgres: PostgresConfig,
}

/// The database engines the bot can run against.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[cfg_attr(
    feature = "config-schema",
    derive(serde::Serialize, terrace_config::schema::Describe)
)]
#[serde(rename_all = "lowercase")]
pub enum Backend {
    /// One file, one replica, no daemon to operate.
    #[default]
    Sqlite,

    /// Several dispatcher workers claiming disjoint rows, and more than one replica.
    Postgres,
}

/// `SQLite` connection settings.
///
/// A `SQLite` deployment is a single replica. The writer pool is effectively one connection, and
/// nothing here provides leader election, so running two of them against one file will corrupt
/// the outbox rather than share it.
#[derive(Debug, Clone, Deserialize)]
#[cfg_attr(
    feature = "config-schema",
    derive(serde::Serialize, terrace_config::schema::Describe)
)]
#[serde(default, deny_unknown_fields)]
pub struct SqliteConfig {
    /// Path to the database file, created on first start if it does not exist.
    pub path: PathBuf,

    /// Size of the read pool. The writer is always one connection.
    pub max_connections: u32,

    /// Seconds to wait for a connection from the pool before failing the operation.
    pub acquire_timeout_secs: u64,

    /// Run pending migrations during startup.
    ///
    /// Set this false where a separate job owns migrations, which is what a `GitOps` deployment
    /// usually wants: the bot then refuses to start against a schema it does not recognise
    /// instead of migrating underneath a running replica.
    pub migrate_on_start: bool,
}

impl Default for SqliteConfig {
    fn default() -> Self {
        Self {
            path: PathBuf::from("discord-alertmanager.db"),
            max_connections: 4,
            acquire_timeout_secs: 5,
            migrate_on_start: true,
        }
    }
}

/// `PostgreSQL` connection settings.
#[derive(Debug, Clone, Deserialize)]
#[cfg_attr(
    feature = "config-schema",
    derive(serde::Serialize, terrace_config::schema::Describe)
)]
#[serde(default, deny_unknown_fields)]
pub struct PostgresConfig {
    /// Connection URL. Supply it through `DAM_STORAGE__POSTGRES__URL_FILE` or the secrets
    /// directory, since it carries the password.
    #[cfg_attr(feature = "config-schema", config(secret))]
    #[serde(skip_serializing)]
    pub url: SecretString,

    /// Maximum pooled connections.
    pub max_connections: u32,

    /// Seconds to wait for a connection from the pool before failing the operation.
    pub acquire_timeout_secs: u64,

    /// Run pending migrations during startup.
    pub migrate_on_start: bool,
}

impl Default for PostgresConfig {
    fn default() -> Self {
        Self {
            url: SecretString::from(String::new()),
            max_connections: 16,
            acquire_timeout_secs: 5,
            migrate_on_start: true,
        }
    }
}
