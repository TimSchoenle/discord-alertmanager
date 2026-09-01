//! `/ignore` — the bot-local mute.
//!
//! An ignore stops the Discord notification and nothing else. The alert still gets its row, still
//! answers `/alerts list`, and Alertmanager still notifies every other receiver it has. Every
//! answer here says so, because an operator who believes an ignore is a silence has muted a page
//! they think they stopped.

use async_trait::async_trait;
use chrono::Utc;
use dam_core::MatcherSet;
use dam_store::{ChannelId, IgnoreId};
use serde_json::json;
use serenity::all::{
    CommandDataOption, CommandOptionType, CreateCommand, CreateCommandOption, CreateEmbed,
    CreateEmbedFooter,
};

use crate::actions::{self, NewIgnore};
use crate::capability::Capability;
use crate::commands::views;
use crate::commands::{
    CommandCtx, CommandError, Response, SlashCommand, boolean_of, hint, integer_of, string_of,
};

/// Rules shown in one `/ignore list`.
const LIST_LIMIT: usize = 20;

/// The `/ignore` command.
pub(crate) struct Ignores;

#[async_trait]
impl SlashCommand for Ignores {
    fn name(&self) -> &'static str {
        "ignore"
    }

    fn capability(&self) -> Capability {
        // `list` is a read; `add` and `remove` ask for `Operate` inside the handler.
        Capability::View
    }

    fn definition(&self) -> CreateCommand {
        CreateCommand::new("ignore")
            .description("Mute an alert in Discord. Alertmanager keeps notifying everyone else")
            .default_member_permissions(hint(Capability::Operate))
            .add_option(
                CreateCommandOption::new(
                    CommandOptionType::SubCommand,
                    "add",
                    "Stop posting cards for alerts matching an expression",
                )
                .add_sub_option(
                    CreateCommandOption::new(
                        CommandOptionType::String,
                        "matchers",
                        "Alertmanager matchers, such as `alertname=Noisy, namespace=dev`",
                    )
                    .required(true),
                )
                .add_sub_option(
                    CreateCommandOption::new(
                        CommandOptionType::String,
                        "reason",
                        "Why. An unexplained mute outlives whoever set it",
                    )
                    .required(true),
                )
                .add_sub_option(CreateCommandOption::new(
                    CommandOptionType::String,
                    "duration",
                    "How long, such as `4h`. Permanent when left out",
                ))
                .add_sub_option(CreateCommandOption::new(
                    CommandOptionType::Boolean,
                    "here",
                    "Mute only this channel rather than the whole server",
                )),
            )
            .add_option(CreateCommandOption::new(
                CommandOptionType::SubCommand,
                "list",
                "The ignore rules in force here",
            ))
            .add_option(
                CreateCommandOption::new(
                    CommandOptionType::SubCommand,
                    "remove",
                    "Revoke an ignore rule",
                )
                .add_sub_option(
                    CreateCommandOption::new(
                        CommandOptionType::Integer,
                        "id",
                        "The rule id, as shown by `/ignore list`",
                    )
                    .required(true),
                ),
            )
    }

    async fn run(&self, ctx: &CommandCtx<'_>) -> Result<Response, CommandError> {
        let Some((name, options)) = ctx.subcommand() else {
            return Err(CommandError::BadRequest(
                "`/ignore` needs a subcommand".to_owned(),
            ));
        };

        match name {
            "add" => add(ctx, options).await,
            "list" => list(ctx).await,
            "remove" => remove(ctx, options).await,
            other => Err(CommandError::BadRequest(format!(
                "`/ignore {other}` belongs to an older version of the bot"
            ))),
        }
    }
}

/// Adds a rule.
async fn add(
    ctx: &CommandCtx<'_>,
    options: &[CommandDataOption],
) -> Result<Response, CommandError> {
    ctx.require(Capability::Operate)?;

    let guild = ctx.require_guild()?;
    let matchers = string_of(options, "matchers").unwrap_or_default();
    let reason = string_of(options, "reason").unwrap_or_default();

    // Compiled here as well as in the store, so a typo is answered by the person who made it
    // instead of becoming a rule that silently matches nothing.
    MatcherSet::parse(matchers).map_err(|error| CommandError::BadRequest(error.to_string()))?;

    let expires_at = match string_of(options, "duration") {
        Some(raw) => {
            Some(Utc::now() + views::parse_duration(raw).map_err(CommandError::BadRequest)?)
        }
        None => None,
    };

    let channel = boolean_of(options, "here")
        .unwrap_or(false)
        .then(|| ChannelId::new(ctx.interaction.channel_id.get()));

    let id = actions::add_ignore(
        ctx.bot,
        NewIgnore {
            guild,
            channel,
            matchers,
            reason,
            actor: ctx.actor(),
            expires_at,
        },
    )
    .await
    .map_err(|error| CommandError::Failed(error.to_string()))?;

    let until = expires_at.map_or_else(
        || "until it is revoked".to_owned(),
        |at| format!("until {}", views::relative(at)),
    );

    Ok(Response::text(format!(
        "Rule `{id}` added: Discord stays quiet about `{}` {until}. Alertmanager still notifies \
         every other receiver.",
        views::truncated(matchers, 200)
    ))
    .about(id.to_string())
    .detailed(json!({ "matchers": matchers, "reason": reason })))
}

/// The rules in force in this guild.
async fn list(ctx: &CommandCtx<'_>) -> Result<Response, CommandError> {
    let guild = ctx.require_guild()?;
    let now = Utc::now();

    let rules = ctx
        .bot
        .store
        .active_ignores(guild, now)
        .await
        .map_err(|error| CommandError::Failed(error.to_string()))?;

    if rules.is_empty() {
        return Ok(Response::text("Nothing is being ignored here."));
    }

    let lines: Vec<String> = rules
        .iter()
        .take(LIST_LIMIT)
        .map(|rule| {
            let scope = rule
                .channel_id
                .map_or_else(|| "server".to_owned(), |channel| format!("<#{channel}>"));
            let expiry = rule.expires_at.map_or_else(
                || "no expiry".to_owned(),
                |at| format!("expires {}", views::relative(at)),
            );

            format!(
                "`{}` — {} · {scope} · {expiry} · <@{}>\n> {}",
                rule.id,
                views::truncated(&rule.matcher_source, 100),
                rule.created_by,
                views::truncated(&rule.reason, 150)
            )
        })
        .collect();

    let embed = CreateEmbed::new()
        .title("Ignore rules")
        .description(views::truncated(&lines.join("\n"), 4096))
        .footer(CreateEmbedFooter::new(
            "An ignore mutes Discord only. Alertmanager keeps notifying every other receiver.",
        ));

    Ok(Response::embed(embed))
}

/// Revokes a rule.
async fn remove(
    ctx: &CommandCtx<'_>,
    options: &[CommandDataOption],
) -> Result<Response, CommandError> {
    ctx.require(Capability::Operate)?;

    let guild = ctx.require_guild()?;
    let Some(id) = integer_of(options, "id") else {
        return Err(CommandError::BadRequest(
            "`/ignore remove` needs a rule id".to_owned(),
        ));
    };

    ctx.bot
        .store
        .revoke_ignore(IgnoreId::new(id), guild, Utc::now())
        .await
        .map_err(|error| CommandError::BadRequest(error.to_string()))?;

    // The snapshot readers hold is what the pipeline evaluates, so the rule is not gone until it
    // has been republished.
    ctx.bot.refresh_routing().await;

    Ok(Response::text(format!("Rule `{id}` revoked.")).about(id.to_string()))
}
