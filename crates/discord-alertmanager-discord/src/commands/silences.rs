//! `/silence` — create, list, extend and expire Alertmanager silences.
//!
//! A silence is not a bot-local mute. It changes Alertmanager, which stops every receiver
//! including whatever pages somebody at four in the morning, and that is why it has a capability
//! of its own rather than sharing `Operate` with the ignore rules.
//!
//! Every write is queued rather than sent inline. Alertmanager can take the whole fifteen minutes
//! an interaction is allowed, and an operator told a silence exists when it never reached the
//! server is the one outcome worth engineering against.

use async_trait::async_trait;
use chrono::Utc;
use dam_core::MatcherSet;
use serde_json::json;
use serenity::all::{
    CommandDataOption, CommandOptionType, CreateCommand, CreateCommandOption, CreateEmbed,
    CreateEmbedFooter,
};

use crate::actions::{self, SilenceRequest};
use crate::capability::Capability;
use crate::commands::views;
use crate::commands::{CommandCtx, CommandError, Response, SlashCommand, hint, string_of};

/// Silences shown in one `/silence list`.
const LIST_LIMIT: usize = 15;

/// The `/silence` command.
pub(crate) struct Silences;

#[async_trait]
impl SlashCommand for Silences {
    fn name(&self) -> &'static str {
        "silence"
    }

    fn capability(&self) -> Capability {
        // The floor, because `list` is a read. Everything that changes Alertmanager asks again.
        Capability::View
    }

    fn definition(&self) -> CreateCommand {
        CreateCommand::new("silence")
            .description("Stop Alertmanager notifying about something, everywhere")
            .default_member_permissions(hint(Capability::Silence))
            .add_option(
                CreateCommandOption::new(
                    CommandOptionType::SubCommand,
                    "create",
                    "Silence an alert or a matcher expression",
                )
                .add_sub_option(
                    CreateCommandOption::new(
                        CommandOptionType::String,
                        "target",
                        "An alert reference, or Alertmanager matchers such as `namespace=prod`",
                    )
                    .required(true),
                )
                .add_sub_option(
                    CreateCommandOption::new(
                        CommandOptionType::String,
                        "duration",
                        "How long, such as `2h` or `1d`",
                    )
                    .required(true),
                )
                .add_sub_option(
                    CreateCommandOption::new(
                        CommandOptionType::String,
                        "comment",
                        "Why. Alertmanager requires it and somebody will read it later",
                    )
                    .required(true),
                ),
            )
            .add_option(
                CreateCommandOption::new(
                    CommandOptionType::SubCommand,
                    "list",
                    "Silences Alertmanager currently holds",
                )
                .add_sub_option(CreateCommandOption::new(
                    CommandOptionType::String,
                    "matcher",
                    "Only silences whose expression contains this text",
                )),
            )
            .add_option(
                CreateCommandOption::new(
                    CommandOptionType::SubCommand,
                    "extend",
                    "Push a silence's expiry further out",
                )
                .add_sub_option(id_option())
                .add_sub_option(
                    CreateCommandOption::new(
                        CommandOptionType::String,
                        "duration",
                        "How much longer, from now",
                    )
                    .required(true),
                ),
            )
            .add_option(
                CreateCommandOption::new(
                    CommandOptionType::SubCommand,
                    "expire",
                    "End a silence now",
                )
                .add_sub_option(id_option()),
            )
    }

    async fn run(&self, ctx: &CommandCtx<'_>) -> Result<Response, CommandError> {
        let Some((name, options)) = ctx.subcommand() else {
            return Err(CommandError::BadRequest(
                "`/silence` needs a subcommand".to_owned(),
            ));
        };

        match name {
            "create" => create(ctx, options).await,
            "list" => list(ctx, options).await,
            "extend" => extend(ctx, options).await,
            "expire" => expire(ctx, options).await,
            other => Err(CommandError::BadRequest(format!(
                "`/silence {other}` belongs to an older version of the bot"
            ))),
        }
    }
}

/// Queues a new silence.
async fn create(
    ctx: &CommandCtx<'_>,
    options: &[CommandDataOption],
) -> Result<Response, CommandError> {
    ctx.require(Capability::Silence)?;

    let target = string_of(options, "target").unwrap_or_default();
    let duration = views::parse_duration(string_of(options, "duration").unwrap_or_default())
        .map_err(CommandError::BadRequest)?;
    let comment = string_of(options, "comment").unwrap_or_default();

    let (matchers, key, subject) = if looks_like_matchers(target) {
        MatcherSet::parse(target).map_err(|error| CommandError::BadRequest(error.to_string()))?;
        (
            target.to_owned(),
            actions::silence_key(None),
            target.to_owned(),
        )
    } else {
        let record = views::resolve(ctx.bot.store.as_ref(), target)
            .await
            .map_err(|error| CommandError::BadRequest(error.to_string()))?;

        (
            views::matchers_of(&record.alert),
            actions::silence_key(Some(&record)),
            record.fingerprint().as_str().to_owned(),
        )
    };

    let ends_at = actions::queue_silence(
        ctx.bot,
        SilenceRequest {
            matchers: &matchers,
            duration,
            created_by: &ctx.provenance(),
            comment,
            actor: ctx.actor(),
            origin: None,
            key,
        },
    )
    .await
    .map_err(|error| CommandError::Failed(error.to_string()))?;

    Ok(Response::text(format!(
        "Silencing `{}` until {}.",
        views::truncated(&matchers, 200),
        views::relative(ends_at)
    ))
    .about(subject)
    .detailed(json!({ "matchers": matchers, "until": ends_at.to_rfc3339() })))
}

/// The silences Alertmanager holds, annotated with what the bot knows about them.
async fn list(
    ctx: &CommandCtx<'_>,
    options: &[CommandDataOption],
) -> Result<Response, CommandError> {
    let filter = string_of(options, "matcher").unwrap_or_default();

    let silences = ctx
        .bot
        .alertmanager
        .list_silences(&[])
        .await
        .map_err(|error| CommandError::Failed(error.to_string()))?;

    // The bot's own rows, so a silence somebody created from a card can name them. Alertmanager
    // has nowhere to keep that, which is the whole reason the link table exists.
    let known = ctx.bot.store.silences(false).await.unwrap_or_default();

    let now = Utc::now();
    let mut lines = Vec::new();

    for silence in silences
        .iter()
        .filter(|silence| silence.state.is_in_force())
    {
        let expression = silence.matchers.to_string();
        if !filter.is_empty() && !expression.contains(filter) {
            continue;
        }

        let author = known
            .iter()
            .find(|link| link.am_id == silence.id)
            .and_then(|link| link.discord_user_id)
            .map_or_else(|| silence.created_by.clone(), |user| format!("<@{user}>"));

        lines.push(format!(
            "`{}` — {} · expires {} · {}",
            silence.id,
            views::truncated(&expression, 120),
            views::relative(silence.ends_at),
            author
        ));

        if lines.len() >= LIST_LIMIT {
            break;
        }
    }

    if lines.is_empty() {
        return Ok(Response::text("No silence is in force."));
    }

    let embed = CreateEmbed::new()
        .title("Active silences")
        .description(views::truncated(&lines.join("\n"), 4096))
        .footer(CreateEmbedFooter::new(format!(
            "{} shown · as at {}",
            lines.len(),
            now.format("%H:%M UTC")
        )));

    Ok(Response::embed(embed))
}

/// Pushes a silence's expiry out, by replacing it with itself.
///
/// Alertmanager updates a silence when the request carries its id, so this is one call rather than
/// an expire and a create — which would leave a window where the alert was not silenced at all.
async fn extend(
    ctx: &CommandCtx<'_>,
    options: &[CommandDataOption],
) -> Result<Response, CommandError> {
    ctx.require(Capability::Silence)?;

    let id = string_of(options, "id").unwrap_or_default();
    let duration = views::parse_duration(string_of(options, "duration").unwrap_or_default())
        .map_err(CommandError::BadRequest)?;

    let silences = ctx
        .bot
        .alertmanager
        .list_silences(&[])
        .await
        .map_err(|error| CommandError::Failed(error.to_string()))?;

    let Some(silence) = silences.into_iter().find(|silence| silence.id == id) else {
        return Err(CommandError::BadRequest(format!(
            "Alertmanager has no silence `{id}`"
        )));
    };

    let ends_at = Utc::now() + duration;

    ctx.bot
        .alertmanager
        .upsert_silence(&dam_engine::SilenceRequest {
            id: Some(silence.id.clone()),
            matchers: silence.matchers.clone(),
            starts_at: silence.starts_at,
            ends_at,
            created_by: ctx.provenance(),
            comment: format!("{} (extended from Discord)", silence.comment),
        })
        .await
        .map_err(|error| CommandError::Failed(error.to_string()))?;

    Ok(
        Response::text(format!("`{id}` now expires {}.", views::relative(ends_at)))
            .about(id)
            .detailed(json!({ "until": ends_at.to_rfc3339() })),
    )
}

/// Ends a silence now.
async fn expire(
    ctx: &CommandCtx<'_>,
    options: &[CommandDataOption],
) -> Result<Response, CommandError> {
    ctx.require(Capability::Silence)?;

    let id = string_of(options, "id").unwrap_or_default();
    if id.is_empty() {
        return Err(CommandError::BadRequest(
            "`/silence expire` needs a silence id".to_owned(),
        ));
    }

    ctx.bot
        .enqueue(&[dam_store::NewOutboxItem::now(
            dam_store::Effect::ExpireSilence {
                am_id: id.to_owned(),
            },
            actions::silence_key(None),
            Utc::now(),
        )])
        .await
        .map_err(|error| CommandError::Failed(error.to_string()))?;

    Ok(Response::text(format!("Expiring `{id}`.")).about(id))
}

/// Whether the operator wrote a matcher expression rather than an alert reference.
///
/// An expression is anything carrying one of Alertmanager's four operators. A fingerprint and an
/// alert name carry none, so the two cases never overlap.
fn looks_like_matchers(target: &str) -> bool {
    target.contains('=') || target.contains("!~")
}

/// The silence id `extend` and `expire` take.
fn id_option() -> CreateCommandOption {
    CreateCommandOption::new(
        CommandOptionType::String,
        "id",
        "The silence id, as shown by `/silence list`",
    )
    .required(true)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_reference_is_not_mistaken_for_an_expression() {
        assert!(looks_like_matchers("severity=critical"));
        assert!(looks_like_matchers("namespace=~prod-.*"));
        assert!(looks_like_matchers("instance!=db-1"));
        assert!(!looks_like_matchers("a1b2c3d4e5f6"));
        assert!(!looks_like_matchers("KubePodCrashLooping"));
    }
}
