//! `/subscribe` — personal direct-message delivery.
//!
//! A subscription is a route the person owns rather than the server: it resolves on the same pass
//! a channel route does, under a pseudo-guild no real server can collide with, and it is always
//! additive. Somebody subscribing to `severity=critical` gets a direct message *as well as*
//! whatever the channel routes already decided, never instead of it.
//!
//! # Only your own
//!
//! Every write names the caller, and the store puts the owner in the predicate rather than in a
//! check before it. Nobody can rewrite or remove somebody else's subscription by guessing an id,
//! and `list` shows the caller's rows and no one else's — a subscription is a statement about when
//! a person wants to be woken up.

use async_trait::async_trait;
use chrono::Utc;
use dam_core::MatcherSet;
use dam_store::{Subscription, SubscriptionId};
use serde_json::json;
use serenity::all::{
    CommandDataOption, CommandOptionType, CreateCommand, CreateCommandOption, CreateEmbed,
    CreateEmbedFooter,
};

use crate::capability::Capability;
use crate::commands::views;
use crate::commands::{
    CommandCtx, CommandError, Response, SlashCommand, hint, integer_of, string_of,
};

/// Subscriptions shown in one `/subscribe list`.
const LIST_LIMIT: usize = 20;

/// The `/subscribe` command.
pub(crate) struct Subscriptions;

#[async_trait]
impl SlashCommand for Subscriptions {
    fn name(&self) -> &'static str {
        "subscribe"
    }

    fn capability(&self) -> Capability {
        // Reading alerts is the whole of what a subscription does, and it does it only to the
        // person who asked for it. Nothing here reaches anybody else.
        Capability::View
    }

    fn definition(&self) -> CreateCommand {
        CreateCommand::new("subscribe")
            .description("Have matching alerts sent to you directly")
            .default_member_permissions(hint(Capability::View))
            .add_option(
                CreateCommandOption::new(
                    CommandOptionType::SubCommand,
                    "add",
                    "Send yourself a direct message for alerts matching an expression",
                )
                .add_sub_option(
                    CreateCommandOption::new(
                        CommandOptionType::String,
                        "matchers",
                        "Alertmanager matchers, such as `namespace=payments`",
                    )
                    .required(true),
                )
                .add_sub_option(
                    CreateCommandOption::new(
                        CommandOptionType::String,
                        "severity",
                        "Only alerts at or above this severity",
                    )
                    .add_string_choice("critical", "critical")
                    .add_string_choice("warning", "warning")
                    .add_string_choice("info", "info"),
                ),
            )
            .add_option(CreateCommandOption::new(
                CommandOptionType::SubCommand,
                "list",
                "Your subscriptions",
            ))
            .add_option(
                CreateCommandOption::new(
                    CommandOptionType::SubCommand,
                    "remove",
                    "Stop one of your subscriptions",
                )
                .add_sub_option(
                    CreateCommandOption::new(
                        CommandOptionType::Integer,
                        "id",
                        "The subscription id, as shown by `/subscribe list`",
                    )
                    .required(true),
                ),
            )
    }

    async fn run(&self, ctx: &CommandCtx<'_>) -> Result<Response, CommandError> {
        let Some((name, options)) = ctx.subcommand() else {
            return Err(CommandError::BadRequest(
                "`/subscribe` needs a subcommand".to_owned(),
            ));
        };

        match name {
            "add" => add(ctx, options).await,
            "list" => list(ctx).await,
            "remove" => remove(ctx, options).await,
            other => Err(CommandError::BadRequest(format!(
                "`/subscribe {other}` belongs to an older version of the bot"
            ))),
        }
    }
}

/// Creates a subscription for the caller.
async fn add(
    ctx: &CommandCtx<'_>,
    options: &[CommandDataOption],
) -> Result<Response, CommandError> {
    let expression = string_of(options, "matchers").unwrap_or_default();

    // Refused here rather than stored and skipped later. A subscription whose matchers do not
    // compile is one that silently never fires, which looks exactly like an alert that never
    // happened.
    let matchers = MatcherSet::parse(expression)
        .map_err(|error| CommandError::BadRequest(error.to_string()))?;

    if matchers.is_empty() {
        return Err(CommandError::BadRequest(
            "a subscription to everything would forward every alert in the deployment; narrow it \
             with at least one matcher"
                .to_owned(),
        ));
    }

    let severity = string_of(options, "severity").and_then(views::severity_from);

    let id = ctx
        .bot
        .store
        .upsert_subscription(&Subscription {
            // Assigned by the database. The value here is what the row is keyed by on the way in
            // and is replaced by the one it comes back with.
            id: SubscriptionId::new(0),
            user_id: ctx.actor(),
            matcher_source: expression.to_owned(),
            matchers,
            min_severity: severity,
            created_at: Utc::now(),
        })
        .await
        .map_err(|error| CommandError::Failed(error.to_string()))?;

    // The snapshot readers hold is what the pipeline evaluates, so the subscription is not live
    // until it has been republished.
    ctx.bot.refresh_routing().await;

    let floor = severity.map_or_else(String::new, |severity| {
        format!(" at `{}` or above", severity.as_str())
    });

    Ok(Response::text(format!(
        "Subscription `{id}`: you will get a direct message for `{}`{floor}. The bot has to be \
         able to message you, so allow direct messages from server members.",
        views::truncated(expression, 200)
    ))
    .about(id.to_string())
    .detailed(json!({ "matchers": expression })))
}

/// The caller's own subscriptions.
async fn list(ctx: &CommandCtx<'_>) -> Result<Response, CommandError> {
    let actor = ctx.actor();

    let mine: Vec<_> = ctx
        .bot
        .store
        .subscriptions()
        .await
        .map_err(|error| CommandError::Failed(error.to_string()))?
        .into_iter()
        .filter(|subscription| subscription.user_id == actor)
        .take(LIST_LIMIT)
        .collect();

    if mine.is_empty() {
        return Ok(Response::text(
            "You have no subscriptions. `/subscribe add` starts one.",
        ));
    }

    let lines: Vec<String> = mine
        .iter()
        .map(|subscription| {
            let floor = subscription.min_severity.map_or_else(
                || "any severity".to_owned(),
                |severity| format!("{} or above", severity.as_str()),
            );

            format!(
                "`{}` — {} · {floor} · since {}",
                subscription.id,
                views::truncated(&subscription.matcher_source, 100),
                views::relative(subscription.created_at)
            )
        })
        .collect();

    let embed = CreateEmbed::new()
        .title("Your subscriptions")
        .description(views::truncated(&lines.join("\n"), 4096))
        .footer(CreateEmbedFooter::new(
            "A subscription is additive: it never replaces what a channel route already delivers.",
        ));

    Ok(Response::embed(embed))
}

/// Removes one of the caller's subscriptions.
async fn remove(
    ctx: &CommandCtx<'_>,
    options: &[CommandDataOption],
) -> Result<Response, CommandError> {
    let Some(id) = integer_of(options, "id") else {
        return Err(CommandError::BadRequest(
            "`/subscribe remove` needs a subscription id".to_owned(),
        ));
    };

    ctx.bot
        .store
        .remove_subscription(SubscriptionId::new(id), ctx.actor())
        .await
        .map_err(|_| {
            // Deliberately the same answer whether the row is missing or belongs to somebody else.
            // Distinguishing them would turn the id into a way to find out who has subscribed to
            // what.
            CommandError::BadRequest(format!("you have no subscription `{id}`"))
        })?;

    ctx.bot.refresh_routing().await;

    Ok(Response::text(format!("Subscription `{id}` removed.")).about(id.to_string()))
}
