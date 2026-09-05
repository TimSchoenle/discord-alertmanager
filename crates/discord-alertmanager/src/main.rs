//! The composition root: the one place that knows what everything else is made of.
//!
//! Every other crate is written against a trait. This binary chooses the implementations, wires
//! them together and owns the process lifecycle — configuration, logging, metrics, error
//! reporting, signals and shutdown. It is also the only crate permitted to use `anyhow`, because
//! it is the only one whose errors are read by a person looking at a log rather than matched on by
//! a caller.
//!
//! # What runs, and how it stops
//!
//! One listener, one gateway session, `engine.dispatchers` outbox workers and five periodic
//! tasks. All of them are children of one cancellation token, and the process only leaves [`run`]
//! once every one of them has stopped: a dispatcher killed mid-item would leave a claimed row
//! nobody owns until its lease expired, which is a delayed notification during exactly the
//! incident that produced it.
//!
//! # Order matters at startup
//!
//! The subscriber and the error reporter go up first, before anything that could fail in a way
//! worth reading about. The store opens before anything else that could accept work, the
//! configured routes are synchronised into it before the snapshot is built, and the snapshot is
//! published before the gateway connects. A webhook arriving one millisecond after the listener
//! binds has to find a routing table, not an empty one.

use std::process::ExitCode;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use chrono::Utc;
use dam_config::{Backend, Config};
use dam_discord::{Bot, LinkRenderer, Renderer, SerenitySink};
use dam_engine::{AlertmanagerApi, DecisionSettings, DiscordSink, RoutingSnapshot, SharedRouting};
use dam_store::{LaneAssignment, RetentionPolicy, RouteSource, Store};
use tokio::signal;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing::{error, info};

mod admin;
mod cards;
mod dispatch;
mod service;
mod tasks;
mod telemetry;

#[tokio::main]
async fn main() -> ExitCode {
    let code = match run().await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            // `{error:#}` rather than `{error}`, so the chain of contexts is printed and not just
            // the innermost failure, which on its own is rarely enough to act on.
            error!("{error:#}");
            ExitCode::FAILURE
        }
    };

    // After the report above rather than inside `run`, so the failure that stopped the process is
    // in the flush rather than one event behind it. A no-op when no DSN was configured.
    telemetry::flush_sentry();

    code
}

/// Loads the configuration, builds the process, and runs it until it is asked to stop.
#[expect(
    clippy::too_many_lines,
    reason = "the composition root is one linear list of what this process is made of; splitting \
              it into helpers that each take eight collaborators moves the list without shortening \
              it"
)]
async fn run() -> Result<()> {
    // The configuration is read before there is a subscriber, because the subscriber is one of
    // the things it describes. Nothing is lost: loading reads files and environment variables and
    // logs nothing. A configuration that will not load is the exception, and the bootstrap
    // subscriber exists to carry that one report.
    let config: Config = match dam_config::layers().load() {
        Ok(config) => config,
        Err(error) => {
            telemetry::install_bootstrap();
            return Err(error).context("loading configuration");
        }
    };

    telemetry::install(&config.telemetry).context("installing the log subscriber")?;

    info!(
        version = env!("CARGO_PKG_VERSION"),
        backend = ?config.storage.backend,
        "starting"
    );

    // Before anything is spawned. A Sentry client binds to the hub of the thread that binds it and
    // is inherited by threads that come after, so a worker that had already logged would keep a
    // hub with no client and report nothing for the life of the process.
    if telemetry::install_sentry(&config.telemetry.sentry).context("starting error reporting")? {
        info!(
            environment = config.telemetry.sentry.environment,
            traces_sample_rate = config.telemetry.sentry.traces_sample_rate,
            "reporting errors to Sentry"
        );
    }

    let metrics = install_metrics(&config)?;

    // Fails here rather than at the first query. A database that cannot be reached is the one
    // failure that makes everything downstream pointless, and saying so at boot is the difference
    // between a container that refuses to start and one that accepts webhooks and drops them.
    let store = open_store(&config).await.context("opening the database")?;

    let alertmanager: Arc<dyn AlertmanagerApi> = Arc::new(
        dam_am::AlertmanagerClient::new(&config.alertmanager)
            .context("building the Alertmanager client")?,
    );

    let routing = Arc::new(SharedRouting::new(
        sync_routes(store.as_ref(), &config)
            .await
            .context("loading the routing table")?,
    ));

    let admin = Arc::new(admin::AdminChannel::new(
        config.observability.admin_channel_id,
        Arc::clone(&store),
    ));

    // Nothing is storming until something has been counted, and the count is this process's own:
    // a restart forgets what the last minute looked like and relearns it inside one window.
    let storm_window = chrono::Duration::seconds(
        i64::try_from(config.engine.storm.window_secs.max(1)).unwrap_or(60),
    );
    let storm_state = Arc::new(dam_engine::SharedStorm::new(dam_engine::StormState::empty(
        config.engine.storm.threshold,
        config.engine.storm.forum_threshold,
        storm_window,
    )));

    let links = LinkRenderer::new(&config.links).context("compiling the link templates")?;
    let renderer = Arc::new(Renderer::new(config.render.clone(), links));

    let sink: Arc<dyn DiscordSink> = Arc::new(SerenitySink::from_token(
        &config.discord.token,
        Arc::clone(&renderer),
    ));

    let lease = Duration::from_secs(config.engine.outbox_lease_secs.max(1));
    let service = Arc::new(service::PipelineService::new(
        Arc::clone(&store),
        Arc::clone(&alertmanager),
        Arc::clone(&routing),
        Arc::clone(&storm_state),
        dam_engine::StormCounter::new(
            config.engine.storm.threshold,
            config.engine.storm.forum_threshold,
            storm_window,
        ),
        Arc::clone(&admin),
        DecisionSettings {
            debounce: chrono::Duration::seconds(
                i64::try_from(config.render.debounce_secs).unwrap_or(3),
            ),
            digest_window: storm_window,
            archive_after_minutes: config.render.thread_archive_after_minutes,
        },
        retention(&config),
        lease,
        chrono::Duration::seconds(i64::try_from(config.engine.deadman_window_secs).unwrap_or(1800)),
    ));

    let shutdown = CancellationToken::new();
    let mut background = JoinSet::new();

    let cards = Arc::new(cards::Cards::new(
        Arc::clone(&store),
        Arc::clone(&routing),
        Arc::clone(&storm_state),
        Arc::clone(&renderer),
    ));

    // Every worker owns one slice of the lane space, and a lane is a function of the dedupe key,
    // so every effect for one alert lands on one worker. Two workers can never edit one card.
    let workers = u16::try_from(config.engine.dispatchers.max(1)).unwrap_or(u16::MAX);
    for index in 0..workers {
        let dispatcher = dispatch::Dispatcher::new(
            Arc::clone(&store),
            Arc::clone(&sink),
            Arc::clone(&alertmanager),
            Arc::clone(&cards),
            Arc::clone(&renderer),
            Arc::clone(&routing),
            Arc::clone(&admin),
            service::PipelineService::worker_id(index),
            (workers > 1).then(|| LaneAssignment::new(index, workers)),
            lease,
            config.engine.outbox_batch_size.max(1),
        );

        let token = shutdown.clone();
        background.spawn(async move { dispatcher.run(token).await });
    }

    for (job, interval) in [
        (tasks::Job::Reconcile, config.engine.reconcile_interval_secs),
        (
            tasks::Job::SyncSilences,
            config.engine.silence_sync_interval_secs,
        ),
        (tasks::Job::ReclaimLeases, config.engine.outbox_lease_secs),
        (tasks::Job::Escalate, config.engine.escalation_interval_secs),
        (tasks::Job::Prune, config.engine.prune_interval_secs),
    ] {
        background.spawn(tasks::run(
            job,
            Arc::clone(&service),
            Duration::from_secs(interval),
            shutdown.clone(),
        ));
    }

    let gateway = Bot::new(
        &config.discord,
        Arc::clone(&store),
        Arc::clone(&alertmanager),
        Arc::clone(&sink),
        Arc::clone(&routing),
        Arc::clone(&renderer),
        route_defaults(&config),
        service.gateway_flag(),
    );

    let gateway_shutdown = shutdown.clone();
    background.spawn(async move {
        // A gateway that cannot connect is logged and does not take the webhook path with it: the
        // listener is what Alertmanager talks to, and dropping alerts because Discord is
        // unreachable would lose the alerts about Discord being unreachable.
        if let Err(error) = gateway.run(gateway_shutdown).await {
            error!(%error, "the Discord gateway stopped");
        }
    });

    // Not in the `JoinSet`: it is the one task that must be abandoned rather than awaited, since
    // it is waiting for a signal that has by then already arrived or never will.
    let signals = tokio::spawn(signal_watcher(shutdown.clone()));

    let state = dam_ingest::AppState::from_config(&config, Arc::clone(&service) as _, metrics);
    let served = dam_ingest::serve(state, shutdown.clone()).await;

    // Cancelled whether the listener stopped on request or on its own, so a bind failure does not
    // leave the process waiting for a signal nobody will send.
    shutdown.cancel();
    signals.abort();

    // Joined rather than aborted. A dispatcher holds a claimed row, and letting it finish the
    // item it is on is faster than waiting out the lease that reclaims it.
    while background.join_next().await.is_some() {}

    served.context("serving the webhook listener")?;

    info!("stopped");
    Ok(())
}

/// Opens the store the configuration selects.
///
/// Both backends are linked into every build, so which one a process runs is `storage.backend` and
/// nothing else. The pool opens here rather than lazily at the first query, because a container
/// that accepts webhooks and then drops them is worse than one that refuses to start.
async fn open_store(config: &Config) -> Result<Arc<dyn Store>> {
    match config.storage.backend {
        Backend::Sqlite => {
            let settings = dam_store_sqlite::Settings {
                path: config.storage.sqlite.path.clone(),
                max_connections: config.storage.sqlite.max_connections,
                acquire_timeout: Duration::from_secs(
                    config.storage.sqlite.acquire_timeout_secs.max(1),
                ),
                migrate_on_start: config.storage.sqlite.migrate_on_start,
                persist_events: config.engine.persist_events,
                regroup_window: Duration::from_secs(config.engine.regroup_window_secs),
            };

            Ok(Arc::new(
                dam_store_sqlite::SqliteStore::connect(&settings).await?,
            ))
        }

        Backend::Postgres => {
            let settings = dam_store_postgres::Settings {
                url: config.storage.postgres.url.clone(),
                max_connections: config.storage.postgres.max_connections,
                acquire_timeout: Duration::from_secs(
                    config.storage.postgres.acquire_timeout_secs.max(1),
                ),
                migrate_on_start: config.storage.postgres.migrate_on_start,
                persist_events: config.engine.persist_events,
                regroup_window: Duration::from_secs(config.engine.regroup_window_secs),
            };

            Ok(Arc::new(
                dam_store_postgres::PostgresStore::connect(&settings).await?,
            ))
        }
    }
}

/// Writes the configured routes into the database and returns the snapshot everything reads.
///
/// The file's routes go through the store rather than straight into the snapshot, because a
/// notification's foreign key points at a route row: a route that only ever existed in memory
/// would leave every card it produced pointing at nothing after a restart.
///
/// A route whose matchers do not compile stops the process here. A routing table that silently
/// matches nothing is worse than a refusal to start, because nobody notices it until the alert
/// that needed it is missed.
async fn sync_routes(store: &dyn Store, config: &Config) -> Result<RoutingSnapshot> {
    let now = Utc::now();
    let mut declared = Vec::with_capacity(config.routes.len());

    for route in &config.routes {
        let mut built = dam_engine::route_from_config(
            route,
            dam_store::RouteId::new(0),
            route_defaults(config),
            now,
        )
        .with_context(|| format!("route `{}`", route.name))?;
        built.source = RouteSource::Config;

        store
            .upsert_route(&built)
            .await
            .with_context(|| format!("storing route `{}`", route.name))?;

        declared.push(route.name.clone());
    }

    // Disabled rather than deleted, so a route removed from the file keeps the notifications it
    // created along with their history.
    let disabled = store
        .disable_missing_config_routes(&declared)
        .await
        .context("disabling routes that left the configuration")?;

    if disabled > 0 {
        info!(
            count = disabled,
            "disabled routes that are no longer in the configuration"
        );
    }

    dam_engine::load_snapshot(store, now)
        .await
        .context("building the routing snapshot")
}

/// What a route falls back to for the keys it does not set itself.
///
/// Read from sections a route knows nothing about, which is why it is resolved here and passed in
/// rather than looked up where a route is built: `/route add` has to land on the same values the
/// file does, and it has no configuration of its own.
fn route_defaults(config: &Config) -> dam_engine::RouteDefaults {
    dam_engine::RouteDefaults {
        archive_after_minutes: config.render.thread_archive_after_minutes,
    }
}

/// The retention horizons, in the units the store takes.
fn retention(config: &Config) -> RetentionPolicy {
    RetentionPolicy {
        events: chrono::Duration::days(i64::from(config.engine.retention.events_days)),
        resolved: chrono::Duration::days(i64::from(config.engine.retention.resolved_days)),
        audit: chrono::Duration::days(i64::from(config.engine.retention.audit_days)),
        ..RetentionPolicy::default()
    }
}

/// Installs the Prometheus recorder, when metrics are enabled.
///
/// The recorder is global state, so it is installed here and nowhere else: a library that
/// installed one would make itself unusable from a process that already had.
fn install_metrics(
    config: &Config,
) -> Result<Option<metrics_exporter_prometheus::PrometheusHandle>> {
    if !config.observability.metrics_enabled {
        return Ok(None);
    }

    let handle = metrics_exporter_prometheus::PrometheusBuilder::new()
        .install_recorder()
        .context("installing the Prometheus recorder")?;

    Ok(Some(handle))
}

/// Cancels the token on the first termination signal.
///
/// Both signals, because a container runtime sends `SIGTERM` and a terminal sends `SIGINT`, and a
/// process that only handles one of them is killed uncleanly by the other.
async fn signal_watcher(shutdown: CancellationToken) {
    #[cfg(unix)]
    {
        let mut term = match signal::unix::signal(signal::unix::SignalKind::terminate()) {
            Ok(stream) => stream,
            Err(error) => {
                error!(%error, "cannot listen for SIGTERM");
                return;
            }
        };

        tokio::select! {
            _ = signal::ctrl_c() => info!("interrupted"),
            _ = term.recv() => info!("terminated"),
        }
    }

    #[cfg(not(unix))]
    {
        if signal::ctrl_c().await.is_err() {
            error!("cannot listen for Ctrl-C");
            return;
        }
        info!("interrupted");
    }

    shutdown.cancel();
}
