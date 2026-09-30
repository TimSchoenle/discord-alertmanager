//! `SQLite` backend for [`dam_store::Store`].
//!
//! Work is claimed inside a `BEGIN IMMEDIATE` transaction, because `SQLite` has no
//! `FOR UPDATE SKIP LOCKED`. The writer pool is effectively one connection, which is why such a
//! deployment is a single replica: horizontal scaling and leader election are `PostgreSQL`-only,
//! and `docs/operations.md` says so where an operator will read it.
//!
//! Every connection sets `journal_mode=WAL`, `busy_timeout=5000`, `foreign_keys=ON` and
//! `synchronous=NORMAL`.
//!
//! # Where this dialect costs more than `PostgreSQL`
//!
//! Type inference is weaker, so queries are run at runtime through `sqlx::query`/`query_as`
//! rather than the `query!` macro, and every row is mapped by hand in the `convert` module. Timestamps are
//! `TEXT` in RFC 3339 with a fixed six-digit subsecond, so that lexicographic order matches
//! chronological order; anything shorter sorts `…:00.5Z` after `…:00.45Z`.
//!
//! The `PostgreSQL` backend was written first and this one ported from it. The inference
//! overrides are easier to reason about against a working reference than in parallel with one.

mod convert;
mod store;

use std::path::Path;
use std::str::FromStr;
use std::time::Duration;

use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
use sqlx::{Pool, Sqlite};

/// Embedded migrations for this dialect, run at startup behind `storage.sqlite.migrate_on_start`.
static MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!("./migrations");

/// Errors [`SqliteStore::connect`] can fail with.
#[derive(Debug, thiserror::Error)]
pub enum ConnectError {
    /// The pool could not be built or the file could not be opened.
    #[error("cannot open the SQLite database: {0}")]
    Pool(#[source] sqlx::Error),

    /// A pending migration failed to apply.
    #[error("cannot migrate the SQLite database: {0}")]
    Migrate(#[source] sqlx::migrate::MigrateError),
}

/// Connection settings [`SqliteStore::connect`] needs, independent of `dam_config`.
#[derive(Debug, Clone)]
pub struct Settings {
    /// Path to the database file, created on first start if it does not exist.
    pub path: std::path::PathBuf,
    /// Size of the read pool. The writer is always effectively one connection.
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
/// [`SqliteStore::from_pool`] is testing something else, and the value only has to be a plausible
/// one rather than the deployment's.
const DEFAULT_REGROUP_WINDOW: Duration = Duration::from_mins(30);

/// `SQLite` backend for [`dam_store::Store`].
pub struct SqliteStore {
    pool: Pool<Sqlite>,
    persist_events: bool,
    regroup_window: chrono::Duration,
}

/// Converts a window into the units `classify` takes, clamping a value no clock could hold.
fn regroup_window(value: Duration) -> chrono::Duration {
    chrono::Duration::from_std(value).unwrap_or_else(|_| chrono::Duration::days(365))
}

impl SqliteStore {
    /// Opens the database file, applies the pragmas every connection needs, and optionally
    /// migrates.
    ///
    /// # Errors
    ///
    /// Returns [`ConnectError::Pool`] when the file cannot be opened or the pool cannot be built,
    /// and [`ConnectError::Migrate`] when a pending migration fails to apply.
    pub async fn connect(settings: &Settings) -> Result<Self, ConnectError> {
        if let Some(parent) = settings.path.parent()
            && !parent.as_os_str().is_empty()
        {
            let _ = std::fs::create_dir_all(parent);
        }

        let options = connect_options(&settings.path)
            .journal_mode(sqlx::sqlite::SqliteJournalMode::Wal)
            .synchronous(sqlx::sqlite::SqliteSynchronous::Normal)
            .busy_timeout(Duration::from_secs(5))
            .foreign_keys(true)
            .create_if_missing(true);

        let pool = SqlitePoolOptions::new()
            .max_connections(settings.max_connections.max(1))
            .acquire_timeout(settings.acquire_timeout)
            .connect_with(options)
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
    pub fn from_pool(pool: Pool<Sqlite>) -> Self {
        Self {
            pool,
            persist_events: true,
            regroup_window: regroup_window(DEFAULT_REGROUP_WINDOW),
        }
    }
}

/// Builds connection options for a file path, tolerating the `Path` not being valid UTF-8 by
/// falling back to `Display`, which is the only case `SqliteConnectOptions::filename` cannot take
/// directly on every platform.
fn connect_options(path: &Path) -> SqliteConnectOptions {
    path.to_str()
        .and_then(|value| SqliteConnectOptions::from_str(&format!("sqlite:{value}")).ok())
        .unwrap_or_else(|| SqliteConnectOptions::new().filename(path))
}

#[cfg(test)]
mod tests {
    use dam_store::conformance;

    use super::*;

    async fn store() -> SqliteStore {
        let pool = sqlx::sqlite::SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .expect("in-memory SQLite connects");
        MIGRATOR.run(&pool).await.expect("migrations apply");
        SqliteStore::from_pool(pool)
    }

    #[tokio::test]
    async fn the_conformance_suite_passes() {
        conformance::run(&store().await).await;
    }

    #[tokio::test]
    async fn legacy_episode_keys_migrate_to_one_key_per_alert() {
        let pool = sqlx::sqlite::SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .expect("in-memory SQLite connects");

        migrate_around(&pool, 5, LEGACY).await;
        assert_legacy_keys_migrated(&pool).await;
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
    async fn migrate_around(pool: &Pool<Sqlite>, version: i64, fixture: &'static str) {
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
    async fn assert_legacy_keys_migrated(pool: &Pool<Sqlite>) {
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
