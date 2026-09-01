//! `/status` — Alertmanager's health, the bot's, and the effective configuration.
//!
//! Three subcommands rather than three commands, because they answer one question — "is this
//! working, and if not which half" — and an operator asking it during an incident should not have
//! to remember which noun the answer lives under.
//!
//! `config` shows the routing table and the non-secret settings. It never shows a token or a
//! connection URL: those arrive through a secrets file precisely so they do not appear in output
//! somebody can screenshot into a ticket.

use async_trait::async_trait;
use chrono::Utc;
use dam_store::RouteTarget;
use serenity::all::{
    CommandOptionType, CreateCommand, CreateCommandOption, CreateEmbed, CreateEmbedFooter,
};

use crate::capability::Capability;
use crate::commands::views;
use crate::commands::{CommandCtx, CommandError, Response, SlashCommand, hint};

/// The `/status` command.
pub(crate) struct Status;

#[async_trait]
impl SlashCommand for Status {
    fn name(&self) -> &'static str {
        "status"
    }

    fn capability(&self) -> Capability {
        Capability::View
    }

    fn definition(&self) -> CreateCommand {
        CreateCommand::new("status")
            .description("Whether this is working, and which half is not")
            .default_member_permissions(hint(Capability::View))
            .add_option(CreateCommandOption::new(
                CommandOptionType::SubCommand,
                "alertmanager",
                "Alertmanager's version, cluster and configuration hash",
            ))
            .add_option(CreateCommandOption::new(
                CommandOptionType::SubCommand,
                "bot",
                "The queue depth and what the bot can currently reach",
            ))
            .add_option(CreateCommandOption::new(
                CommandOptionType::SubCommand,
                "config",
                "The effective non-secret configuration and the route table",
            ))
    }

    async fn run(&self, ctx: &CommandCtx<'_>) -> Result<Response, CommandError> {
        match ctx.subcommand().map(|(name, _)| name) {
            Some("alertmanager") => alertmanager(ctx).await,
            Some("bot") => bot(ctx).await,
            Some("config") => config(ctx),
            Some(other) => Err(CommandError::BadRequest(format!(
                "`/status {other}` belongs to an older version of the bot"
            ))),
            None => Err(CommandError::BadRequest(
                "`/status` needs a subcommand".to_owned(),
            )),
        }
    }
}

/// What Alertmanager says about itself.
async fn alertmanager(ctx: &CommandCtx<'_>) -> Result<Response, CommandError> {
    let status = ctx
        .bot
        .alertmanager
        .status()
        .await
        .map_err(|error| CommandError::Failed(error.to_string()))?;

    let receivers = ctx
        .bot
        .alertmanager
        .receivers()
        .await
        .map(|receivers| receivers.len())
        .unwrap_or_default();

    let mut embed = CreateEmbed::new()
        .title("Alertmanager")
        .field("Version", format!("`{}`", status.version), true)
        .field(
            "Cluster",
            if status.cluster_ready {
                "settled"
            } else {
                "**not settled**"
            },
            true,
        )
        .field("Receivers", receivers.to_string(), true);

    if let Some(uptime) = status.uptime {
        embed = embed.field("Started", views::relative(uptime), true);
    }

    if !status.peers.is_empty() {
        embed = embed.field(
            "Peers",
            views::truncated(&status.peers.join(", "), 1024),
            false,
        );
    }

    // The hash is what tells two peers apart when one of them did not reload; without it a split
    // configuration looks exactly like a healthy cluster.
    if let Some(hash) = status.config_hash {
        embed = embed.footer(CreateEmbedFooter::new(format!("config {hash}")));
    }

    Ok(Response::embed(embed))
}

/// What the bot can currently reach, and how far behind it is.
async fn bot(ctx: &CommandCtx<'_>) -> Result<Response, CommandError> {
    let store_ok = ctx.bot.store.health().await.is_ok();
    let depths = ctx.bot.store.outbox_depth().await.unwrap_or_default();
    let queued: u64 = depths.iter().map(|(_, depth)| depth).sum();

    let snapshot = ctx.bot.routing.load();

    let breakdown = if depths.is_empty() {
        "empty".to_owned()
    } else {
        depths
            .iter()
            .map(|(kind, depth)| format!("{kind}: {depth}"))
            .collect::<Vec<_>>()
            .join(", ")
    };

    let embed = CreateEmbed::new()
        .title("Bot")
        .field(
            "Database",
            if store_ok { "reachable" } else { "**down**" },
            true,
        )
        .field("Routes", snapshot.routes().len().to_string(), true)
        .field("Queued effects", queued.to_string(), true)
        .field("Queue by kind", views::truncated(&breakdown, 1024), false)
        .footer(CreateEmbedFooter::new(format!(
            "as at {}",
            Utc::now().format("%H:%M UTC")
        )));

    Ok(Response::embed(embed))
}

/// The effective configuration, minus anything secret.
fn config(ctx: &CommandCtx<'_>) -> Result<Response, CommandError> {
    ctx.require(Capability::Admin)?;

    let snapshot = ctx.bot.routing.load();

    let routes = if snapshot.routes().is_empty() {
        "none".to_owned()
    } else {
        snapshot
            .routes()
            .iter()
            .take(15)
            .map(|route| {
                format!(
                    "`{}` {} → {}",
                    route.name,
                    views::truncated(&route.matcher_source, 60),
                    match &route.target {
                        RouteTarget::Text { channel, .. } => format!("<#{channel}>"),
                        RouteTarget::Forum { channel, .. } => format!("forum <#{channel}>"),
                        RouteTarget::Thread { thread } => format!("thread <#{thread}>"),
                        RouteTarget::Dm { user } => format!("DM <@{user}>"),
                    }
                )
            })
            .collect::<Vec<_>>()
            .join("\n")
    };

    let embed = CreateEmbed::new()
        .title("Effective configuration")
        .field("Routes", views::truncated(&routes, 1024), false)
        .field(
            "Capabilities",
            views::truncated(&ctx.bot.capabilities.describe(), 1024),
            false,
        )
        .footer(CreateEmbedFooter::new(
            "Secrets are never shown here. The full reference is in docs/config.md.",
        ));

    Ok(Response::embed(embed))
}
