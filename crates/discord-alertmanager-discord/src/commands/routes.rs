//! `/route` — where alerts are delivered, and why one route won.
//!
//! Routes declared in the configuration file are read-only from Discord. A deployment reproducible
//! from its manifests is the point of declaring them there, and a command that could edit one
//! would make the file a suggestion. `/route add` writes the other kind, which lives only in the
//! database.
//!
//! `/route test` is the command that earns this module its complexity. It answers "why did this
//! alert not reach my channel", which is otherwise a question you can only settle by causing an
//! alert.

use async_trait::async_trait;
use chrono::Utc;
use dam_config::{RouteConfig, TargetKind, TargetPolicy};
use dam_core::{LabelName, Labels, MatcherSet, Severity};
use dam_store::{RouteId, RouteSource, RouteTarget};
use serde_json::json;
use serenity::all::{
    CommandDataOption, CommandOptionType, CreateCommand, CreateCommandOption, CreateEmbed,
    CreateEmbedFooter,
};

use crate::capability::Capability;
use crate::commands::views;
use crate::commands::{
    CommandCtx, CommandError, Response, SlashCommand, channel_of, hint, integer_of, string_of,
};

/// Routes shown in one `/route list`.
const LIST_LIMIT: usize = 20;

/// The `/route` command.
pub(crate) struct Routes;

#[async_trait]
impl SlashCommand for Routes {
    fn name(&self) -> &'static str {
        "route"
    }

    fn capability(&self) -> Capability {
        Capability::Admin
    }

    fn definition(&self) -> CreateCommand {
        CreateCommand::new("route")
            .description("Where alerts are delivered")
            .default_member_permissions(hint(Capability::Admin))
            .add_option(
                CreateCommandOption::new(
                    CommandOptionType::SubCommand,
                    "add",
                    "Deliver matching alerts to a channel",
                )
                .add_sub_option(
                    CreateCommandOption::new(CommandOptionType::String, "name", "Route name")
                        .required(true),
                )
                .add_sub_option(
                    CreateCommandOption::new(
                        CommandOptionType::Channel,
                        "channel",
                        "Where cards go",
                    )
                    .required(true),
                )
                .add_sub_option(
                    CreateCommandOption::new(
                        CommandOptionType::String,
                        "matchers",
                        "Alertmanager matchers, such as `severity=critical`",
                    )
                    .required(true),
                )
                .add_sub_option(
                    CreateCommandOption::new(
                        CommandOptionType::String,
                        "kind",
                        "How the channel is used",
                    )
                    .add_string_choice("text", "text")
                    .add_string_choice("forum", "forum")
                    .add_string_choice("thread", "thread"),
                )
                .add_sub_option(
                    CreateCommandOption::new(
                        CommandOptionType::String,
                        "severity",
                        "Lowest severity this route accepts",
                    )
                    .add_string_choice("critical", "critical")
                    .add_string_choice("warning", "warning")
                    .add_string_choice("info", "info"),
                )
                .add_sub_option(CreateCommandOption::new(
                    CommandOptionType::Role,
                    "mention",
                    "Role mentioned when an alert first fires",
                ))
                .add_sub_option(CreateCommandOption::new(
                    CommandOptionType::Integer,
                    "priority",
                    "Evaluation order. Lower runs first",
                )),
            )
            .add_option(CreateCommandOption::new(
                CommandOptionType::SubCommand,
                "list",
                "Every route, and where it came from",
            ))
            .add_option(
                CreateCommandOption::new(
                    CommandOptionType::SubCommand,
                    "remove",
                    "Delete a route created from Discord",
                )
                .add_sub_option(
                    CreateCommandOption::new(CommandOptionType::String, "name", "Route name")
                        .required(true),
                ),
            )
            .add_option(
                CreateCommandOption::new(
                    CommandOptionType::SubCommand,
                    "test",
                    "Show which route a sample label set reaches, and why",
                )
                .add_sub_option(
                    CreateCommandOption::new(
                        CommandOptionType::String,
                        "sample",
                        "Labels, such as `alertname=Foo, severity=critical, namespace=prod`",
                    )
                    .required(true),
                ),
            )
    }

    async fn run(&self, ctx: &CommandCtx<'_>) -> Result<Response, CommandError> {
        let Some((name, options)) = ctx.subcommand() else {
            return Err(CommandError::BadRequest(
                "`/route` needs a subcommand".to_owned(),
            ));
        };

        match name {
            "add" => add(ctx, options).await,
            "list" => Ok(list(ctx)),
            "remove" => remove(ctx, options).await,
            "test" => test(ctx, options),
            other => Err(CommandError::BadRequest(format!(
                "`/route {other}` belongs to an older version of the bot"
            ))),
        }
    }
}

/// Creates a route that lives in the database.
async fn add(
    ctx: &CommandCtx<'_>,
    options: &[CommandDataOption],
) -> Result<Response, CommandError> {
    let guild = ctx.require_guild()?;
    let name = string_of(options, "name").unwrap_or_default();
    let matchers = string_of(options, "matchers").unwrap_or_default();

    let Some(channel) = channel_of(options, "channel") else {
        return Err(CommandError::BadRequest(
            "`/route add` needs a channel".to_owned(),
        ));
    };

    // Built through the same translation the configuration file goes through, so a route written
    // from Discord and one written in the file cannot end up with different defaults.
    let config = RouteConfig {
        name: name.to_owned(),
        guild_id: guild.get(),
        matchers: matchers.to_owned(),
        min_severity: string_of(options, "severity").and_then(severity_option),
        target: dam_config::RouteTarget {
            kind: match string_of(options, "kind") {
                Some("forum") => TargetKind::Forum,
                Some("thread") => TargetKind::Thread,
                _ => TargetKind::Text,
            },
            id: channel.get(),
            policy: TargetPolicy::default(),
        },
        mentions: dam_config::Mentions {
            roles: role_of(options, "mention").into_iter().collect(),
            ..dam_config::Mentions::default()
        },
        priority: integer_of(options, "priority")
            .and_then(|value| i32::try_from(value).ok())
            .unwrap_or(100),
        ..RouteConfig::default()
    };

    let mut route =
        dam_engine::route_from_config(&config, RouteId::new(0), ctx.bot.route_defaults, Utc::now())
            .map_err(|error| CommandError::BadRequest(error.to_string()))?;

    // The one field the translation cannot set: a route built from the file is owned by the file,
    // and this one has to stay editable from Discord.
    route.source = RouteSource::Discord;
    route.created_by = Some(ctx.actor());

    let id = ctx
        .bot
        .store
        .upsert_route(&route)
        .await
        .map_err(|error| CommandError::BadRequest(error.to_string()))?;

    ctx.bot.refresh_routing().await;

    Ok(Response::text(format!(
        "Route `{name}` (id `{id}`) now delivers `{}` to <#{channel}>.",
        views::truncated(matchers, 200)
    ))
    .about(name)
    .detailed(json!({ "channel": channel.get(), "matchers": matchers })))
}

/// Every route, in evaluation order.
///
/// Read from the published snapshot rather than from the database, so what it shows is what the
/// pipeline is actually evaluating.
fn list(ctx: &CommandCtx<'_>) -> Response {
    let snapshot = ctx.bot.routing.load();
    let routes = snapshot.routes();

    if routes.is_empty() {
        return Response::text("No route is configured, so no alert reaches Discord.");
    }

    let lines: Vec<String> = routes
        .iter()
        .take(LIST_LIMIT)
        .map(|route| {
            format!(
                "`{}` **{}** — {} → {} · priority {} · {}{}",
                route.id,
                route.name,
                views::truncated(&route.matcher_source, 80),
                describe_target(&route.target),
                route.priority,
                route.source.as_str(),
                if route.enabled { "" } else { " · disabled" }
            )
        })
        .collect();

    let embed = CreateEmbed::new()
        .title("Routes")
        .description(views::truncated(&lines.join("\n"), 4096))
        .footer(CreateEmbedFooter::new(
            "Routes from the configuration file cannot be changed from Discord.",
        ));

    Response::embed(embed)
}

/// Deletes a route that Discord created.
async fn remove(
    ctx: &CommandCtx<'_>,
    options: &[CommandDataOption],
) -> Result<Response, CommandError> {
    let guild = ctx.require_guild()?;
    let name = string_of(options, "name").unwrap_or_default();
    let snapshot = ctx.bot.routing.load();

    let Some(route) = snapshot
        .routes()
        .iter()
        .find(|route| route.name == name && route.guild_id == guild)
    else {
        return Err(CommandError::BadRequest(format!(
            "no route called `{name}` in this server"
        )));
    };

    if !route.source.is_mutable_from_discord() {
        return Err(CommandError::BadRequest(format!(
            "`{name}` is declared in the configuration file. Remove it there, so the deployment \
             stays reproducible from its manifests."
        )));
    }

    let mut disabled = route.clone();
    disabled.enabled = false;

    ctx.bot
        .store
        .upsert_route(&disabled)
        .await
        .map_err(|error| CommandError::Failed(error.to_string()))?;

    ctx.bot.refresh_routing().await;

    // Disabled rather than deleted, so the cards it created keep their route and their history.
    Ok(Response::text(format!(
        "Route `{name}` is off. The cards it created keep their history."
    ))
    .about(name))
}

/// Explains what a sample label set would do.
fn test(ctx: &CommandCtx<'_>, options: &[CommandDataOption]) -> Result<Response, CommandError> {
    let sample = string_of(options, "sample").unwrap_or_default();
    let labels = parse_labels(sample)?;
    let severity = Severity::from_labels(&labels);
    let snapshot = ctx.bot.routing.load();
    let now = Utc::now();

    let winners = snapshot.resolve(&labels, severity);

    let mut lines = vec![format!(
        "Severity read as **{}** from the label set.",
        severity.as_str()
    )];

    if winners.is_empty() {
        lines.push(String::new());
        lines.push("**No route matches.** These alerts would reach no channel.".to_owned());

        for route in snapshot.routes() {
            lines.push(format!(
                "- `{}` — {}",
                route.name,
                why_not(route, &labels, severity)
            ));
        }
    } else {
        for route in &winners {
            lines.push(String::new());
            lines.push(format!(
                "**{}** → {} (priority {})",
                route.name,
                describe_target(&route.target),
                route.priority
            ));
            lines.push(format!(
                "- matched `{}`",
                views::truncated(&route.matcher_source, 100)
            ));

            if let Some(channel) = route.target.channel()
                && let Some(rule) = snapshot.ignore_for(route.guild_id, channel, &labels, now)
            {
                lines.push(format!(
                    "- **muted** by ignore rule `{}`: {}",
                    rule.id,
                    views::truncated(&rule.reason, 120)
                ));
            }

            if route.mentions_at(severity) {
                lines.push("- would mention the route's roles on a first firing".to_owned());
            }

            if let RouteTarget::Forum { channel, policy } = &route.target {
                lines.extend(forum_checks(&snapshot, *channel, policy));
            }
        }
    }

    let embed = CreateEmbed::new()
        .title("Route test")
        .description(views::truncated(&lines.join("\n"), 4096))
        .footer(CreateEmbedFooter::new(views::truncated(sample, 2048)));

    Ok(Response::embed(embed).about(sample))
}

/// What a forum route would and would not manage to do, from the cached tag list.
///
/// Read from the snapshot rather than from Discord: the cache is what the hot path applies tags
/// from, so a gap here is exactly the gap a notification would hit.
fn forum_checks(
    snapshot: &dam_engine::RoutingSnapshot,
    channel: dam_store::ChannelId,
    policy: &dam_store::ForumPolicy,
) -> Vec<String> {
    let mut lines = Vec::new();
    let known = snapshot.tag_count(channel);

    if known == 0 {
        lines.push(
            "- **no forum tags are cached** for this channel; either it is not a forum, or the \
             bot cannot read it"
                .to_owned(),
        );
        return lines;
    }

    let mut missing: Vec<&str> = [
        policy.state_tags.firing.as_str(),
        policy.state_tags.acked.as_str(),
        policy.state_tags.silenced.as_str(),
        policy.state_tags.resolved.as_str(),
    ]
    .into_iter()
    .filter(|name| snapshot.tag_id(channel, name).is_none())
    .collect();

    if let Some(default) = &policy.default_tag
        && snapshot.tag_id(channel, default).is_none()
    {
        missing.push(default.as_str());
    }

    if missing.is_empty() {
        lines.push(format!("- every state tag resolves ({known} tags cached)"));
    } else {
        lines.push(format!(
            "- **missing tags**: {}. Posts get the ones that resolve and no more.",
            missing.join(", ")
        ));
    }

    if policy.title_template.trim().is_empty() {
        lines.push(
            "- **the title template is empty**, and a forum post must have a title".to_owned(),
        );
    }

    lines
}

/// Why one route did not match, in the order the route itself checks.
fn why_not(route: &dam_store::Route, labels: &Labels, severity: Severity) -> String {
    if !route.enabled {
        return "disabled".to_owned();
    }

    if !route.matchers.matches(labels) {
        return format!(
            "matchers `{}` do not match",
            views::truncated(&route.matcher_source, 80)
        );
    }

    match route.min_severity {
        Some(floor) if severity < floor => {
            format!("needs at least `{}`", floor.as_str())
        }
        _ => "matches, but was not reached".to_owned(),
    }
}

/// A target in one readable phrase.
fn describe_target(target: &RouteTarget) -> String {
    match target {
        RouteTarget::Text { channel, .. } => format!("<#{channel}>"),
        RouteTarget::Forum { channel, .. } => format!("forum <#{channel}>"),
        RouteTarget::Thread { thread } => format!("thread <#{thread}>"),
        RouteTarget::Dm { user } => format!("a direct message to <@{user}>"),
    }
}

/// Reads a sample label set written in matcher syntax.
///
/// Only equality is meaningful for a sample, so anything else is refused rather than silently
/// producing a label whose value is a regex.
fn parse_labels(sample: &str) -> Result<Labels, CommandError> {
    let set =
        MatcherSet::parse(sample).map_err(|error| CommandError::BadRequest(error.to_string()))?;

    let mut labels = Labels::new();

    for matcher in set.as_slice() {
        if matcher.op() != dam_core::MatchOp::Equal {
            return Err(CommandError::BadRequest(format!(
                "a sample is a label set, so `{}` has to use `=`",
                matcher.name()
            )));
        }

        labels
            .insert(
                LabelName::new(matcher.name().as_str())
                    .map_err(|error| CommandError::BadRequest(error.to_string()))?,
                matcher.value().to_owned(),
            )
            .map_err(|error| CommandError::BadRequest(error.to_string()))?;
    }

    if labels.is_empty() {
        return Err(CommandError::BadRequest(
            "a sample needs at least one label, such as `alertname=Foo`".to_owned(),
        ));
    }

    Ok(labels)
}

/// The configuration severity one choice names.
fn severity_option(word: &str) -> Option<dam_config::Severity> {
    Some(match word {
        "critical" => dam_config::Severity::Critical,
        "warning" => dam_config::Severity::Warning,
        "info" => dam_config::Severity::Info,
        _ => return None,
    })
}

/// Reads a role option.
fn role_of(options: &[CommandDataOption], name: &str) -> Option<u64> {
    options
        .iter()
        .find(|option| option.name == name)
        .and_then(|option| match &option.value {
            serenity::all::CommandDataOptionValue::Role(value) => Some(value.get()),
            _ => None,
        })
}
