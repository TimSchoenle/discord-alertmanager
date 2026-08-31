//! The composition root: the one place that knows what everything else is made of.
//!
//! Every other crate is written against a trait. This binary chooses the implementations, wires
//! them together and owns the process lifecycle — configuration, logging, metrics, signals and
//! shutdown. It is also the only crate permitted to use `anyhow`, because it is the only one
//! whose errors are read by a person looking at a log rather than matched on by a caller.

use std::process::ExitCode;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use dam_config::{Backend, Config};
use dam_engine::AlertmanagerApi;
use tokio::signal;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};
use tracing_subscriber::EnvFilter;
use tracing_subscriber::prelude::*;

mod service;

/// Environment variable naming the log format, read before the configuration exists.
const LOG_FORMAT_VAR: &str = dam_config::LOG_FORMAT_VAR;

/// Environment variable naming the log filter, read before the configuration exists.
const LOG_LEVEL_VAR: &str = dam_config::LOG_LEVEL_VAR;

#[tokio::main]
async fn main() -> ExitCode {
    // Logging is installed before anything else, and from the environment rather than from the
    // configuration: a configuration error is the failure most worth seeing, and it happens
    // before there is a configuration to describe how to report it.
    install_tracing();

    match run().await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            // `{error:#}` rather than `{error}`, so the chain of contexts is printed and not just
            // the innermost failure, which on its own is rarely enough to act on.
            error!("{error:#}");
            ExitCode::FAILURE
        }
    }
}

/// Loads the configuration, builds the process, and runs it until it is asked to stop.
async fn run() -> Result<()> {
    let config: Config = dam_config::layers()
        .load()
        .context("loading configuration")?;

    info!(
        version = env!("CARGO_PKG_VERSION"),
        backend = ?config.storage.backend,
        "starting"
    );

    let metrics = install_metrics(&config)?;
    let alertmanager: Arc<dyn AlertmanagerApi> = Arc::new(
        dam_am::AlertmanagerClient::new(&config.alertmanager)
            .context("building the Alertmanager client")?,
    );

    // Fails here rather than at the first query. A binary built without the backend its
    // configuration selects cannot do anything useful, and saying so at boot is the difference
    // between a container that refuses to start and one that accepts webhooks and drops them.
    let store = open_store(&config).context("opening the database")?;

    let service = Arc::new(service::PipelineService::new(
        Arc::clone(&store),
        Arc::clone(&alertmanager),
        configured_routing(&config)?,
    ));

    // Nothing reports the gateway as connected yet, so readiness is told the truth about it: this
    // build serves webhooks and has no session.
    service.gateway_flag().store(false, Ordering::Relaxed);

    let shutdown = CancellationToken::new();
    let signals = spawn_signal_watcher(shutdown.clone());
    let reconciler = spawn_reconciler(
        Arc::clone(&service),
        Duration::from_secs(config.engine.reconcile_interval_secs),
        shutdown.clone(),
    );

    let state = dam_ingest::AppState::from_config(&config, Arc::clone(&service) as _, metrics);
    let served = dam_ingest::serve(state, shutdown.clone()).await;

    // The background tasks are cancelled whether the listener stopped on request or on its own,
    // so a bind failure does not leave the process waiting for a signal nobody will send.
    shutdown.cancel();
    signals.abort();
    let _ = reconciler.await;

    served.context("serving the webhook listener")?;

    info!("stopped");
    Ok(())
}

/// Builds the store the configuration selects.
///
/// The two backends are compile-time features. A deployment that only ever runs one has no reason
/// to carry the other's driver, and a build that carries neither is refused here rather than
/// halfway through the first webhook.
fn open_store(config: &Config) -> Result<Arc<dyn dam_store::Store>> {
    match config.storage.backend {
        Backend::Sqlite => {
            bail!("this build has no SQLite backend compiled in; rebuild with `--features sqlite`")
        }
        Backend::Postgres => bail!(
            "this build has no PostgreSQL backend compiled in; rebuild with `--features postgres`"
        ),
    }
}

/// Builds the routing snapshot the configured routes describe.
///
/// Only the file's routes, because the database is where the ones created from Discord live and
/// this runs before a route synchronisation has happened. A route whose matchers do not compile
/// stops the process here: a routing table that silently matches nothing is worse than a refusal
/// to start, because nobody notices it until the alert that needed it is missed.
fn configured_routing(config: &Config) -> Result<dam_engine::RoutingSnapshot> {
    let now = chrono::Utc::now();
    let mut routes = Vec::with_capacity(config.routes.len());

    for (index, declared) in config.routes.iter().enumerate() {
        // A provisional key, replaced by the database's own when the routes are synchronised. It
        // only has to be unique within this snapshot, which is what orders equal priorities.
        let id = dam_store::RouteId::new(i64::try_from(index).unwrap_or(i64::MAX));

        routes.push(
            dam_engine::route_from_config(declared, id, now)
                .with_context(|| format!("route `{}`", declared.name))?,
        );
    }

    Ok(dam_engine::RoutingSnapshot::new(
        routes,
        Vec::new(),
        Vec::new(),
    ))
}

/// Installs the tracing subscriber.
///
/// JSON when asked for, because a log aggregator parses it and a person does not. The filter
/// falls back to `info` for this workspace and `warn` for everything else, so a dependency's
/// debug logging cannot bury the bot's own.
fn install_tracing() {
    let filter = EnvFilter::try_from_env(LOG_LEVEL_VAR)
        .unwrap_or_else(|_| EnvFilter::new("warn,discord_alertmanager=info,dam_=info"));

    let json =
        std::env::var(LOG_FORMAT_VAR).is_ok_and(|format| format.eq_ignore_ascii_case("json"));

    let registry = tracing_subscriber::registry().with(filter);

    if json {
        registry
            .with(tracing_subscriber::fmt::layer().json())
            .init();
    } else {
        registry.with(tracing_subscriber::fmt::layer()).init();
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

/// Polls Alertmanager on a fixed cadence until the token is cancelled.
///
/// The poll is what makes the push path survivable: a webhook can be lost to a restart, a
/// partition or a receiver that never sent `send_resolved`, and only a comparison against what
/// Alertmanager currently holds finds that out. It also feeds the freshness that readiness
/// reports, so a bot that has silently stopped hearing from Alertmanager stops claiming to be
/// ready.
fn spawn_reconciler(
    service: Arc<service::PipelineService>,
    interval: Duration,
    shutdown: CancellationToken,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        // `max(1)`: a zero interval in the configuration would otherwise spin the poll loop
        // against Alertmanager as fast as it can answer.
        let mut ticker = tokio::time::interval(interval.max(Duration::from_secs(1)));
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

        loop {
            tokio::select! {
                () = shutdown.cancelled() => return,
                _ = ticker.tick() => match service.poll_alertmanager().await {
                    Ok(count) => debug!(alerts = count, "polled Alertmanager"),
                    // A failed poll is logged and not retried early: the next tick is the retry,
                    // and hammering an Alertmanager that is already struggling helps nobody.
                    Err(error) => warn!(%error, "cannot poll Alertmanager"),
                },
            }
        }
    })
}

/// Cancels the token on the first termination signal.
///
/// Both signals, because a container runtime sends `SIGTERM` and a terminal sends `SIGINT`, and a
/// process that only handles one of them is killed uncleanly by the other.
fn spawn_signal_watcher(shutdown: CancellationToken) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
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
    })
}
