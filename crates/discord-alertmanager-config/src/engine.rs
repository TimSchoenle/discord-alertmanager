//! Pipeline cadences, retention horizons and storm thresholds.

use serde::Deserialize;

/// How often each background task runs, and how long anything is kept.
#[derive(Debug, Clone, Deserialize)]
#[cfg_attr(
    feature = "config-schema",
    derive(serde::Serialize, terrace_config::schema::Describe)
)]
#[serde(default, deny_unknown_fields)]
pub struct Engine {
    /// Outbox dispatcher workers.
    ///
    /// Work is hashed into one lane per worker by its dedupe key, so every effect for one alert
    /// lands on one worker. Raising this widens the fan-out across alerts and never splits an
    /// alert across two workers.
    pub dispatchers: u32,

    /// Seconds a claimed outbox row stays claimed before a janitor may reclaim it.
    ///
    /// The janitor reclaims at three times this, which is the margin for a worker that is slow
    /// rather than dead.
    pub outbox_lease_secs: u64,

    /// Outbox rows one worker claims per pass.
    pub outbox_batch_size: u32,

    /// Seconds between reconciler polls of the Alertmanager alert set.
    ///
    /// The reconciler is the authoritative path. Anything Alertmanager has that the database does
    /// not is injected as a synthetic event, and anything firing in the database that Alertmanager
    /// has not had for two consecutive polls is treated as resolved. That is what makes the bot
    /// converge after a restart or a partition.
    pub reconcile_interval_secs: u64,

    /// Seconds between silence syncs.
    pub silence_sync_interval_secs: u64,

    /// Seconds between escalation timer sweeps.
    pub escalation_interval_secs: u64,

    /// Seconds between retention sweeps.
    pub prune_interval_secs: u64,

    /// Seconds of webhook silence that, combined with an unreachable Alertmanager, trips the
    /// deadman.
    pub deadman_window_secs: u64,

    /// Seconds within which a re-fire reuses the existing card and thread.
    ///
    /// Inside the window the card is reused and its flap count goes up. Outside it, a new card is
    /// posted carrying a link to the previous one.
    pub regroup_window_secs: u64,

    /// Record a row in `alert_events` for every state transition.
    ///
    /// Turning this off keeps the current `alerts` row and drops the history. The four reasons
    /// the bot keeps its own alert table all rest on the current row, so nothing else changes.
    pub persist_events: bool,

    /// When a route switches to a single rolling digest card.
    #[cfg_attr(feature = "config-schema", config(nested))]
    pub storm: Storm,

    /// How long each kind of history is kept.
    #[cfg_attr(feature = "config-schema", config(nested))]
    pub retention: Retention,
}

impl Default for Engine {
    fn default() -> Self {
        Self {
            dispatchers: 4,
            outbox_lease_secs: 30,
            outbox_batch_size: 16,
            reconcile_interval_secs: 60,
            silence_sync_interval_secs: 30,
            escalation_interval_secs: 15,
            prune_interval_secs: 3600,
            deadman_window_secs: 1800,
            regroup_window_secs: 1800,
            persist_events: true,
            storm: Storm::default(),
            retention: Retention::default(),
        }
    }
}

/// The point at which a route stops posting one card per alert.
///
/// Past the threshold the route posts one rolling digest card with a thread, and a notice saying
/// why. Discord's per-channel limits are strict enough that an unthrottled storm produces nothing
/// but 429s, which is worse than a digest.
#[derive(Debug, Clone, Deserialize)]
#[cfg_attr(
    feature = "config-schema",
    derive(serde::Serialize, terrace_config::schema::Describe)
)]
#[serde(default, deny_unknown_fields)]
pub struct Storm {
    /// Alerts on one route inside the window that trigger digest mode.
    pub threshold: u32,

    /// Length of the window, in seconds.
    pub window_secs: u64,

    /// Threshold for forum routes, which is lower.
    ///
    /// Creating a forum post is heavier than sending a message and is rate-limited per channel,
    /// so a forum route reaches trouble sooner than a text one.
    pub forum_threshold: u32,
}

impl Default for Storm {
    fn default() -> Self {
        Self {
            threshold: 50,
            window_secs: 60,
            forum_threshold: 20,
        }
    }
}

/// How long each table is kept before the pruner deletes from it.
#[derive(Debug, Clone, Deserialize)]
#[cfg_attr(
    feature = "config-schema",
    derive(serde::Serialize, terrace_config::schema::Describe)
)]
#[serde(default, deny_unknown_fields)]
pub struct Retention {
    /// Days of `alert_events` history. This is the expensive table.
    pub events_days: u32,

    /// Days a resolved alert and its notification are kept.
    pub resolved_days: u32,

    /// Days of `audit_log`.
    ///
    /// Kept far longer than the rest because it is small and it is the record of what people
    /// decided. Check this horizon against what an incident review actually needs.
    pub audit_days: u32,
}

impl Default for Retention {
    fn default() -> Self {
        Self {
            events_days: 30,
            resolved_days: 30,
            audit_days: 365,
        }
    }
}
