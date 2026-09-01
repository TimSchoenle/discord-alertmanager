//! `/alerts` — list, inspect, take and hand over alerts.
//!
//! The read paths answer from the local tables rather than from Alertmanager, and that is
//! deliberate: Alertmanager garbage-collects a resolved alert within a collection interval, and
//! "what happened an hour ago" is exactly the question asked during a review. It also means
//! `/alerts list` still answers while Alertmanager is the thing that is down.

use async_trait::async_trait;
use dam_core::{NotificationState, Severity};
use dam_store::AckKind;
use serde_json::json;
use serenity::all::{
    CommandDataOption, CommandOptionType, CreateCommand, CreateCommandOption, CreateEmbed,
    CreateEmbedFooter,
};

use crate::actions;
use crate::capability::Capability;
use crate::commands::views::{self, ListFilter, PAGE_SIZE};
use crate::commands::{
    CommandCtx, CommandError, Response, SlashCommand, hint, integer_of, string_of, user_of,
};

/// The `/alerts` command.
pub(crate) struct Alerts;

#[async_trait]
impl SlashCommand for Alerts {
    fn name(&self) -> &'static str {
        "alerts"
    }

    fn capability(&self) -> Capability {
        Capability::View
    }

    fn definition(&self) -> CreateCommand {
        CreateCommand::new("alerts")
            .description("Inspect and answer alerts")
            .default_member_permissions(hint(Capability::View))
            .add_option(
                CreateCommandOption::new(CommandOptionType::SubCommand, "list", "List alerts")
                    .add_sub_option(state_option())
                    .add_sub_option(severity_option())
                    .add_sub_option(matcher_option())
                    .add_sub_option(
                        CreateCommandOption::new(
                            CommandOptionType::Integer,
                            "limit",
                            "Alerts per page (1–25)",
                        )
                        .min_int_value(1)
                        .max_int_value(25),
                    ),
            )
            .add_option(
                CreateCommandOption::new(
                    CommandOptionType::SubCommand,
                    "show",
                    "Show one alert in full",
                )
                .add_sub_option(reference_option()),
            )
            .add_option(
                CreateCommandOption::new(
                    CommandOptionType::SubCommand,
                    "ack",
                    "Take an alert, so everyone else can see it is being handled",
                )
                .add_sub_option(reference_option())
                .add_sub_option(CreateCommandOption::new(
                    CommandOptionType::String,
                    "note",
                    "What you are doing about it",
                )),
            )
            .add_option(
                CreateCommandOption::new(
                    CommandOptionType::SubCommand,
                    "unack",
                    "Give an alert back",
                )
                .add_sub_option(reference_option()),
            )
            .add_option(
                CreateCommandOption::new(
                    CommandOptionType::SubCommand,
                    "assign",
                    "Hand an alert to somebody else",
                )
                .add_sub_option(reference_option())
                .add_sub_option(
                    CreateCommandOption::new(
                        CommandOptionType::User,
                        "user",
                        "Who is taking it over",
                    )
                    .required(true),
                ),
            )
    }

    async fn run(&self, ctx: &CommandCtx<'_>) -> Result<Response, CommandError> {
        let Some((name, options)) = ctx.subcommand() else {
            return Err(CommandError::BadRequest(
                "`/alerts` needs a subcommand".to_owned(),
            ));
        };

        match name {
            "list" => list(ctx, options).await,
            "show" => show(ctx, options).await,
            "ack" => ack(ctx, options, false).await,
            "unack" => ack(ctx, options, true).await,
            "assign" => assign(ctx, options).await,
            other => Err(CommandError::BadRequest(format!(
                "`/alerts {other}` belongs to an older version of the bot"
            ))),
        }
    }
}

/// One page of the alert table.
pub(crate) async fn list(
    ctx: &CommandCtx<'_>,
    options: &[CommandDataOption],
) -> Result<Response, CommandError> {
    let filter = ListFilter {
        state: string_of(options, "state").and_then(views::state_from),
        severity: string_of(options, "severity").and_then(views::severity_from),
        matchers: string_of(options, "matcher").unwrap_or_default().to_owned(),
        limit: integer_of(options, "limit")
            .and_then(|value| u32::try_from(value).ok())
            .unwrap_or(PAGE_SIZE),
    };

    page(ctx, &filter, 0).await
}

/// Renders one page of a filter, which is what both `/alerts list` and a page button produce.
pub(crate) async fn page(
    ctx: &CommandCtx<'_>,
    filter: &ListFilter,
    offset: u32,
) -> Result<Response, CommandError> {
    let query = filter
        .query(offset)
        .map_err(|error| CommandError::BadRequest(error.to_string()))?;

    let page = ctx
        .bot
        .store
        .query_alerts(&query)
        .await
        .map_err(|error| CommandError::Failed(error.to_string()))?;

    if page.total == 0 {
        return Ok(Response::text("No alert matches that filter."));
    }

    let body = page
        .items
        .iter()
        .map(views::list_line)
        .collect::<Vec<_>>()
        .join("\n");

    let embed = CreateEmbed::new()
        .title("Alerts")
        .description(views::truncated(&body, 4096))
        .footer(CreateEmbedFooter::new(format!(
            "{}–{} of {}",
            offset + 1,
            page.end(),
            page.total
        )));

    Ok(Response::embed(embed).with_row(views::page_row(filter, offset, page.total)))
}

/// The full detail view of one alert.
pub(crate) async fn show(
    ctx: &CommandCtx<'_>,
    options: &[CommandDataOption],
) -> Result<Response, CommandError> {
    let reference = string_of(options, "ref").unwrap_or_default();
    let record = views::resolve(ctx.bot.store.as_ref(), reference)
        .await
        .map_err(|error| CommandError::BadRequest(error.to_string()))?;

    let held = ctx
        .bot
        .store
        .acknowledgement(record.fingerprint())
        .await
        .map_err(|error| CommandError::Failed(error.to_string()))?;

    let embed = views::detail_embed(&record, held.as_ref(), None);
    let row = views::link_row(&ctx.bot.renderer, &record.alert);

    Ok(Response::embed(embed)
        .with_row(row)
        .about(record.fingerprint().as_str()))
}

/// Takes an alert, or gives it back.
async fn ack(
    ctx: &CommandCtx<'_>,
    options: &[CommandDataOption],
    revoke: bool,
) -> Result<Response, CommandError> {
    ctx.require(Capability::Operate)?;

    let reference = string_of(options, "ref").unwrap_or_default();
    let record = views::resolve(ctx.bot.store.as_ref(), reference)
        .await
        .map_err(|error| CommandError::BadRequest(error.to_string()))?;

    let outcome = actions::acknowledge(
        ctx.bot,
        record.fingerprint(),
        ctx.actor(),
        AckKind::Ack,
        string_of(options, "note").map(str::to_owned),
        revoke,
    )
    .await
    .map_err(|error| CommandError::Failed(error.to_string()))?;

    let text = match (revoke, outcome.changed, outcome.holder) {
        (true, true, _) => "Released. The card is back to firing.".to_owned(),
        (true, false, _) => "Nobody was holding that alert.".to_owned(),
        (false, true, _) => format!("Taken. {} card(s) updated.", outcome.cards.len()),
        // The loser of a race is told who won rather than being told it worked, because it did
        // not, and the difference matters when two people think they are handling it.
        (false, false, Some(holder)) => format!("<@{holder}> already has that one."),
        (false, false, None) => "That alert could not be taken.".to_owned(),
    };

    Ok(Response::text(text)
        .about(record.fingerprint().as_str())
        .detailed(json!({ "changed": outcome.changed, "revoke": revoke })))
}

/// Hands an alert to somebody else.
async fn assign(
    ctx: &CommandCtx<'_>,
    options: &[CommandDataOption],
) -> Result<Response, CommandError> {
    ctx.require(Capability::Operate)?;

    let reference = string_of(options, "ref").unwrap_or_default();
    let Some(assignee) = user_of(options, "user") else {
        return Err(CommandError::BadRequest(
            "`/alerts assign` needs somebody to assign it to".to_owned(),
        ));
    };

    let record = views::resolve(ctx.bot.store.as_ref(), reference)
        .await
        .map_err(|error| CommandError::BadRequest(error.to_string()))?;

    // Revoked first, so a handover from somebody who already holds it is one acknowledgement
    // ending and another starting rather than a write the unique index refuses.
    actions::acknowledge(
        ctx.bot,
        record.fingerprint(),
        ctx.actor(),
        AckKind::Handover,
        None,
        true,
    )
    .await
    .map_err(|error| CommandError::Failed(error.to_string()))?;

    let outcome = actions::acknowledge(
        ctx.bot,
        record.fingerprint(),
        assignee,
        AckKind::Handover,
        Some(format!("handed over by <@{}>", ctx.actor())),
        false,
    )
    .await
    .map_err(|error| CommandError::Failed(error.to_string()))?;

    if !outcome.changed {
        return Err(CommandError::Failed(
            "the handover did not take; try again".to_owned(),
        ));
    }

    Ok(
        Response::text(format!("<@{assignee}> now has `{}`.", record.alert.name()))
            .about(record.fingerprint().as_str())
            .detailed(json!({ "assignee": assignee.get() })),
    )
}

/// The `<ref>` every subcommand but `list` takes.
fn reference_option() -> CreateCommandOption {
    CreateCommandOption::new(
        CommandOptionType::String,
        "ref",
        "Short fingerprint from a card's footer, or the alert name",
    )
    .required(true)
}

/// The `state` filter, spelled out as choices so nobody has to guess the vocabulary.
fn state_option() -> CreateCommandOption {
    let mut option = CreateCommandOption::new(
        CommandOptionType::String,
        "state",
        "Only alerts whose card is in this state",
    );

    for state in [
        NotificationState::Firing,
        NotificationState::Acked,
        NotificationState::Silenced,
        NotificationState::Ignored,
        NotificationState::Resolved,
    ] {
        option = option.add_string_choice(state.as_str(), state.as_str());
    }

    option
}

/// The `severity` filter, which is a floor rather than an equality.
fn severity_option() -> CreateCommandOption {
    let mut option = CreateCommandOption::new(
        CommandOptionType::String,
        "severity",
        "Only alerts at or above this severity",
    );

    for severity in [Severity::Critical, Severity::Warning, Severity::Info] {
        option = option.add_string_choice(severity.as_str(), severity.as_str());
    }

    option
}

/// The `matcher` filter, in Alertmanager's own syntax.
fn matcher_option() -> CreateCommandOption {
    CreateCommandOption::new(
        CommandOptionType::String,
        "matcher",
        "Alertmanager matchers, such as `namespace=prod, severity=~crit.*`",
    )
}
