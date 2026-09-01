//! `PostgreSQL` backend for [`dam_store::Store`].
//!
//! Work is claimed with `SELECT … FOR UPDATE SKIP LOCKED`, which is the reason this backend is
//! the supported path above a small deployment: several dispatcher workers claim disjoint rows
//! without blocking each other, and a lease plus a janitor covers the worker that dies holding
//! one.
//!
//! # Why this crate exists separately from the `SQLite` one
//!
//! Each backend owns one dialect, its own `migrations/` and its own row mapping. The two
//! migration directories share version numbers and filenames so a reviewer can diff them side by
//! side, and a test asserts the filename sets match — a migration added to one dialect and
//! forgotten in the other is otherwise found by whichever operator runs the other backend.
//!
//! # Where this dialect costs less than `SQLite`
//!
//! It has a timestamp type, a JSON type and a boolean, so three of the encodings the other
//! backend performs by hand are the driver's job here, and the `convert` module is correspondingly
//! shorter. Queries are issued through `sqlx::query`/`QueryBuilder` rather than the `query!`
//! macro, for the same reason both backends share one row mapper: the filtered read paths are
//! dynamic and cannot be expressed as a macro at all, and a checked query feeding a hand-written
//! mapper checks the half that was never in doubt. What the mapping actually has to satisfy is a
//! behavioural contract, and `dam_store::conformance` runs it against both engines.

mod convert;
mod store;

use std::time::Duration;

use secrecy::{ExposeSecret, SecretString};
use sqlx::postgres::PgPoolOptions;
use sqlx::{Pool, Postgres};

/// Embedded migrations for this dialect, run at startup behind `storage.postgres.migrate_on_start`.
static MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!("./migrations");

/// Errors [`PostgresStore::connect`] can fail with.
#[derive(Debug, thiserror::Error)]
pub enum ConnectError {
    /// The server refused the connection, or the pool could not be built.
    #[error("cannot connect to PostgreSQL: {0}")]
    Pool(#[source] sqlx::Error),

    /// A pending migration failed to apply.
    #[error("cannot migrate the PostgreSQL database: {0}")]
    Migrate(#[source] sqlx::migrate::MigrateError),
}

/// Connection settings [`PostgresStore::connect`] needs, independent of `dam_config`.
#[derive(Debug, Clone)]
pub struct Settings {
    /// Connection URL, which carries the password and is therefore never logged.
    pub url: SecretString,

    /// Maximum pooled connections.
    pub max_connections: u32,

    /// How long to wait for a connection from the pool before failing the operation.
    pub acquire_timeout: Duration,

    /// Whether to run pending migrations before returning.
    pub migrate_on_start: bool,

    /// Whether an accepted change also appends a row to `alert_events`.
    ///
    /// A store-level setting rather than a per-call one, because it describes the deployment
    /// rather than the delivery: a webhook and a reconciler pass have the same answer, and
    /// threading it through every batch would let them disagree.
    pub persist_events: bool,

    /// How long a resolved alert may stay quiet and still re-fire onto its existing card.
    ///
    /// Here for the same reason `persist_events` is: classifying an arriving alert is where the
    /// window is applied, that happens inside the transaction, and a window supplied per batch
    /// would let a webhook and a reconciler pass disagree about whether one re-fire was a flap.
    pub regroup_window: Duration,
}

/// The regroup window a store built from a bare pool uses.
///
/// Half an hour, which is the configuration's own default. A test that reaches for
/// [`PostgresStore::from_pool`] is testing something else, and the value only has to be a
/// plausible one rather than the deployment's.
const DEFAULT_REGROUP_WINDOW: Duration = Duration::from_mins(30);

/// `PostgreSQL` backend for [`dam_store::Store`].
pub struct PostgresStore {
    pool: Pool<Postgres>,
    persist_events: bool,
    regroup_window: chrono::Duration,
}

/// Converts a window into the units `classify` takes, clamping a value no clock could hold.
fn regroup_window(value: Duration) -> chrono::Duration {
    chrono::Duration::from_std(value).unwrap_or_else(|_| chrono::Duration::days(365))
}

impl PostgresStore {
    /// Connects, and optionally migrates.
    ///
    /// # Errors
    ///
    /// Returns [`ConnectError::Pool`] when the server is unreachable or refuses the credentials,
    /// and [`ConnectError::Migrate`] when a pending migration fails to apply.
    pub async fn connect(settings: &Settings) -> Result<Self, ConnectError> {
        let pool = PgPoolOptions::new()
            .max_connections(settings.max_connections.max(1))
            .acquire_timeout(settings.acquire_timeout)
            .connect(settings.url.expose_secret())
            .await
            .map_err(ConnectError::Pool)?;

        if settings.migrate_on_start {
            MIGRATOR.run(&pool).await.map_err(ConnectError::Migrate)?;
        }

        Ok(Self {
            pool,
            persist_events: settings.persist_events,
            regroup_window: regroup_window(settings.regroup_window),
        })
    }

    /// Wraps an already-open pool, for tests that build their own.
    ///
    /// History is kept, because a test that asserts on it is the reason to reach for this
    /// constructor and a test that does not is unaffected by the extra row.
    #[must_use]
    pub fn from_pool(pool: Pool<Postgres>) -> Self {
        Self {
            pool,
            persist_events: true,
            regroup_window: regroup_window(DEFAULT_REGROUP_WINDOW),
        }
    }

    /// Applies pending migrations to an already-open pool.
    ///
    /// # Errors
    ///
    /// Returns the migrator's error unchanged.
    pub async fn migrate(pool: &Pool<Postgres>) -> Result<(), sqlx::migrate::MigrateError> {
        MIGRATOR.run(pool).await
    }
}

#[cfg(test)]
mod tests {
    use dam_store::conformance;
    use testcontainers::runners::AsyncRunner;
    use testcontainers_modules::postgres::Postgres as PostgresImage;

    use super::*;

    #[tokio::test]
    async fn the_conformance_suite_passes() {
        // A container rather than a service container in the pipeline: a service container works
        // in continuous integration and nowhere else, so `cargo test` on a laptop would stop
        // exercising the backend that most deployments actually run.
        let container = PostgresImage::default()
            .start()
            .await
            .expect("a PostgreSQL container starts");
        let port = container
            .get_host_port_ipv4(5432)
            .await
            .expect("the container publishes its port");

        let store = PostgresStore::connect(&Settings {
            url: SecretString::from(format!(
                "postgres://postgres:postgres@127.0.0.1:{port}/postgres"
            )),
            max_connections: 4,
            acquire_timeout: Duration::from_secs(10),
            migrate_on_start: true,
            persist_events: true,
            regroup_window: DEFAULT_REGROUP_WINDOW,
        })
        .await
        .expect("the store connects and migrates");

        conformance::run(&store).await;
    }
}
