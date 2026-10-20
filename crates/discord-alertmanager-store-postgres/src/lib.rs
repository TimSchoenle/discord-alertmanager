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

        // A database of its own in the same container: the conformance run above has already
        // taken the default one past every migration.
        sqlx::query("CREATE DATABASE legacy_keys")
            .execute(&store.pool)
            .await
            .expect("a second database is created");

        let legacy = sqlx::postgres::PgPoolOptions::new()
            .max_connections(1)
            .connect(&format!(
                "postgres://postgres:postgres@127.0.0.1:{port}/legacy_keys"
            ))
            .await
            .expect("the second database connects");

        migrate_around(&legacy, 5, LEGACY).await;
        assert_legacy_keys_migrated(&legacy).await;
    }

    /// Cards written under the per-episode keys that migration 0005 retires.
    ///
    /// Channel 1 holds three episodes of `ff`, the last still firing, and one card for `ee` that
    /// never re-fired. Channel 2 holds its own `ff`. The orphaned row and the group key carry a
    /// `#` or an `a:` of their own and must come through untouched.
    const LEGACY: &str = "\
        INSERT INTO alerts (fingerprint, labels_hash, labels, annotations, starts_at, status, \
            am_state, severity, first_seen_at, last_seen_at, updated_at, episode) VALUES \
            ('ff', '0', '{\"alertname\":\"Legacy\"}', '{}', '2026-01-01T00:00:00.000000Z', \
             'firing', 'active', 'warning', '2026-01-01T00:00:00.000000Z', \
             '2026-01-01T00:00:00.000000Z', '2026-01-01T00:00:00.000000Z', 2); \
        INSERT INTO notifications (id, dedupe_key, fingerprint, route_id, guild_id, channel_id, \
            state, created_at, updated_at) VALUES \
            (1, 'a:ff', 'ff', 1, 1, 1, 'resolved', '2026-01-01T00:00:00.000000Z', \
             '2026-01-01T01:00:00.000000Z'), \
            (2, 'a:ff#1', 'ff', 1, 1, 1, 'resolved', '2026-01-02T00:00:00.000000Z', \
             '2026-01-02T01:00:00.000000Z'), \
            (3, 'a:ff#2', 'ff', 1, 1, 1, 'firing', '2026-01-03T00:00:00.000000Z', \
             '2026-01-03T00:00:00.000000Z'), \
            (4, 'a:ee', 'ee', 1, 1, 1, 'firing', '2026-01-03T00:00:00.000000Z', \
             '2026-01-03T00:00:00.000000Z'), \
            (5, 'a:ff', 'ff', 1, 1, 2, 'firing', '2026-01-03T00:00:00.000000Z', \
             '2026-01-03T00:00:00.000000Z'), \
            (6, 'orphaned:6:a:ff#1', 'ff', 1, 1, 1, 'orphaned', '2026-01-02T00:00:00.000000Z', \
             '2026-01-02T00:00:00.000000Z'), \
            (7, 'g:{}:{alertname=\"x#1\"}', 'ff', 1, 1, 1, 'firing', \
             '2026-01-03T00:00:00.000000Z', '2026-01-03T00:00:00.000000Z');";

    /// What every legacy card is keyed by once 0005 has run, by id.
    const MIGRATED: [(i64, &str); 7] = [
        (1, "superseded:1:a:ff"),
        (2, "superseded:2:a:ff#1"),
        (3, "a:ff"),
        (4, "a:ee"),
        (5, "a:ff"),
        (6, "orphaned:6:a:ff#1"),
        (7, "g:{}:{alertname=\"x#1\"}"),
    ];

    /// Applies every migration before `version`, then the fixture, then the rest.
    async fn migrate_around(pool: &Pool<Postgres>, version: i64, fixture: &'static str) {
        for migration in MIGRATOR
            .iter()
            .filter(|migration| migration.version < version)
        {
            sqlx::raw_sql(migration.sql.clone())
                .execute(pool)
                .await
                .expect("an earlier migration applies");
        }

        sqlx::raw_sql(fixture)
            .execute(pool)
            .await
            .expect("the fixture is written");

        for migration in MIGRATOR
            .iter()
            .filter(|migration| migration.version >= version)
        {
            sqlx::raw_sql(migration.sql.clone())
                .execute(pool)
                .await
                .expect("the migration under test applies");
        }
    }

    /// Asserts what 0005 made of the legacy cards.
    async fn assert_legacy_keys_migrated(pool: &Pool<Postgres>) {
        let keys: Vec<(i64, String)> =
            sqlx::query_as("SELECT id, dedupe_key FROM notifications ORDER BY id")
                .fetch_all(pool)
                .await
                .expect("the keys read back");
        let expected: Vec<(i64, String)> = MIGRATED
            .iter()
            .map(|(id, key)| (*id, (*key).to_owned()))
            .collect();

        assert_eq!(
            keys, expected,
            "the newest card per alert and channel holds the bare key; the older ones retire"
        );

        let resolved: Vec<i64> = sqlx::query_scalar(
            "SELECT id FROM notifications WHERE resolved_at IS NOT NULL ORDER BY id",
        )
        .fetch_all(pool)
        .await
        .expect("the resolution times read back");

        assert_eq!(
            resolved,
            vec![1, 2],
            "a card resolved before the migration takes its last write as its resolution time"
        );
    }
}
