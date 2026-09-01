//! The buttons and selects on a card.
//!
//! A control carries a `custom_id` and nothing else, so this module is the other half of the codec
//! in [`crate::custom_id`]: it decodes what the control means, checks the capability the meaning
//! needs, and calls the same function the equivalent slash command calls.
//!
//! # An old control has to say so
//!
//! Cards outlive deploys. A button posted before an encoding change decodes to a version this
//! build does not know, and the answer is a sentence pointing at `/alerts show` — never a panic,
//! and never a different action that happens to parse.
//!
//! # Every control is ephemeral
//!
//! The card is shared; the answer to pressing something on it is not. Answering in the channel
//! would put a line of chatter under every alert during exactly the incident the channel is for,
//! and the card itself already shows the outcome once the dispatcher re-renders it.

use std::sync::Arc;

use chrono::Utc;
use dam_store::{AckKind, AuditEntry, AuditResult, ChannelId, GuildId, RoleId, UserId};
use serde_json::json;
use serenity::all::{
    ComponentInteraction, ComponentInteractionDataKind, Context, CreateActionRow,
    CreateInteractionResponse, CreateInteractionResponseMessage, CreateSelectMenu,
    CreateSelectMenuKind, CreateSelectMenuOption, EditInteractionResponse,
};
use tracing::warn;

use crate::actions::{self, NewIgnore, SilenceRequest};
use crate::bot::BotContext;
use crate::capability::Capability;
use crate::commands::views;
use crate::commands::{CommandError, Response, edit};
use crate::custom_id::{Action, CustomId, CustomIdError};

/// The durations a picker offers.
///
/// Short enough at the bottom that a wrong answer costs an hour, long enough at the top that a
/// planned maintenance window does not need four presses.
const DURATIONS: [(&str, &str); 6] = [
    ("30m", "30 minutes"),
    ("1h", "1 hour"),
    ("4h", "4 hours"),
    ("12h", "12 hours"),
    ("1d", "1 day"),
    ("7d", "7 days"),
];

/// Runs one component interaction: defers, decodes, checks, acts, audits, answers.
pub(crate) async fn dispatch(
    bot: &Arc<BotContext>,
    ctx: &Context,
    interaction: &ComponentInteraction,
) {
    let deferred = interaction
        .create_response(
            &ctx.http,
            CreateInteractionResponse::Defer(
                CreateInteractionResponseMessage::new().ephemeral(true),
            ),
        )
        .await;

    if let Err(error) = deferred {
        warn!(%error, "cannot acknowledge a component interaction");
        return;
    }

    let control = match CustomId::decode(&interaction.data.custom_id) {
        Ok(control) => control,
        Err(error) => {
            answer(ctx, interaction, Response::text(explain(&error))).await;
            return;
        }
    };

    let roles: Vec<RoleId> = interaction
        .member
        .as_ref()
        .map(|member| {
            member
                .roles
                .iter()
                .map(|role| RoleId::new(role.get()))
                .collect()
        })
        .unwrap_or_default();

    let names = role_names(ctx, interaction);
    let actor = UserId::new(interaction.user.id.get());

    let outcome = if bot
        .capabilities
        .allows(needed(control.action), &roles, &names)
    {
        run(bot, interaction, &control).await
    } else {
        Err(CommandError::Denied(needed(control.action)))
    };

    let guild = interaction.guild_id.map(|guild| GuildId::new(guild.get()));
    let action = format!("component.{}", control.action.as_str());

    match outcome {
        Ok(response) => {
            bot.audit(&AuditEntry {
                actor: Some(actor),
                guild_id: guild,
                action,
                subject: response.subject.clone(),
                detail: response.detail.clone(),
                result: AuditResult::Ok,
                at: Utc::now(),
            })
            .await;

            answer(ctx, interaction, response).await;
        }
        Err(error) => {
            bot.audit(&AuditEntry {
                actor: Some(actor),
                guild_id: guild,
                action,
                subject: Some(control.entity.to_string()),
                detail: json!({ "error": error.to_string() }),
                result: error.result(),
                at: Utc::now(),
            })
            .await;

            answer(ctx, interaction, Response::text(error.to_string())).await;
        }
    }
}

/// Carries out one decoded control.
async fn run(
    bot: &Arc<BotContext>,
    interaction: &ComponentInteraction,
    control: &CustomId,
) -> Result<Response, CommandError> {
    match control.action {
        Action::Ack => acknowledge(bot, interaction, control, false).await,
        Action::Unack => acknowledge(bot, interaction, control, true).await,
        Action::SilenceMenu => Ok(picker(
            control,
            Action::SilenceFor,
            "How long should Alertmanager stay quiet about this? Every receiver stops, not just \
             Discord.",
        )),
        Action::IgnoreMenu => Ok(picker(
            control,
            Action::IgnoreFor,
            "How long should Discord stay quiet about this? Alertmanager keeps notifying every \
             other receiver.",
        )),
        Action::SilenceFor => silence(bot, interaction, control).await,
        Action::IgnoreFor => ignore(bot, interaction, control).await,
        Action::Details => details(bot, control).await,
        Action::Page => page(bot, control).await,
    }
}

/// Takes an alert, or gives it back.
async fn acknowledge(
    bot: &Arc<BotContext>,
    interaction: &ComponentInteraction,
    control: &CustomId,
    revoke: bool,
) -> Result<Response, CommandError> {
    let card = card(bot, control).await?;

    let outcome = actions::acknowledge(
        bot,
        &card.fingerprint,
        UserId::new(interaction.user.id.get()),
        AckKind::Ack,
        None,
        revoke,
    )
    .await
    .map_err(|error| CommandError::Failed(error.to_string()))?;

    let text = match (revoke, outcome.changed, outcome.holder) {
        (true, true, _) => "Released. The card is back to firing.".to_owned(),
        (true, false, _) => "Nobody was holding that alert.".to_owned(),
        (false, true, _) => "Taken. The card is updating.".to_owned(),
        (false, false, Some(holder)) => format!("<@{holder}> got there first."),
        (false, false, None) => "That alert could not be taken.".to_owned(),
    };

    Ok(Response::text(text)
        .about(card.fingerprint.as_str())
        .detailed(json!({ "changed": outcome.changed, "revoke": revoke })))
}

/// The duration picker a `…` button opens.
fn picker(control: &CustomId, action: Action, prompt: &str) -> Response {
    let Ok(id) = CustomId::new(action, control.entity).encode() else {
        return Response::text("That control is from an older version of the bot.");
    };

    let options = DURATIONS
        .iter()
        .map(|(value, label)| CreateSelectMenuOption::new(*label, *value))
        .collect();

    Response::text(prompt).with_rows(vec![CreateActionRow::SelectMenu(
        CreateSelectMenu::new(id, CreateSelectMenuKind::String { options })
            .placeholder("Pick a duration"),
    )])
}

/// Queues the silence a picker chose.
async fn silence(
    bot: &Arc<BotContext>,
    interaction: &ComponentInteraction,
    control: &CustomId,
) -> Result<Response, CommandError> {
    let card = card(bot, control).await?;
    let duration =
        views::parse_duration(&chosen(interaction, control)?).map_err(CommandError::BadRequest)?;

    let record = bot
        .store
        .alert(&card.fingerprint)
        .await
        .map_err(|error| CommandError::Failed(error.to_string()))?
        .ok_or_else(|| {
            CommandError::BadRequest("that alert is no longer in the database".to_owned())
        })?;

    let matchers = views::matchers_of(&record.alert);
    let ends_at = actions::queue_silence(
        bot,
        SilenceRequest {
            matchers: &matchers,
            duration,
            created_by: &format!(
                "discord:{} ({})",
                interaction.user.name, interaction.user.id
            ),
            comment: &format!(
                "silenced from the card for {} by {}",
                record.alert.name(),
                interaction.user.name
            ),
            actor: UserId::new(interaction.user.id.get()),
            origin: Some(&interaction.message.link()),
            key: actions::silence_key(Some(&record)),
        },
    )
    .await
    .map_err(|error| CommandError::Failed(error.to_string()))?;

    Ok(Response::text(format!(
        "Alertmanager will stay quiet about this until {}. Every receiver is affected, not just \
         Discord.",
        views::relative(ends_at)
    ))
    .about(card.fingerprint.as_str())
    .detailed(json!({ "until": ends_at.to_rfc3339() })))
}

/// Adds the bot-local ignore a picker chose.
async fn ignore(
    bot: &Arc<BotContext>,
    interaction: &ComponentInteraction,
    control: &CustomId,
) -> Result<Response, CommandError> {
    let card = card(bot, control).await?;
    let duration =
        views::parse_duration(&chosen(interaction, control)?).map_err(CommandError::BadRequest)?;

    let record = bot
        .store
        .alert(&card.fingerprint)
        .await
        .map_err(|error| CommandError::Failed(error.to_string()))?
        .ok_or_else(|| {
            CommandError::BadRequest("that alert is no longer in the database".to_owned())
        })?;

    let matchers = views::matchers_of(&record.alert);
    let expires_at = Utc::now() + duration;

    let id = actions::add_ignore(
        bot,
        NewIgnore {
            guild: card.guild_id,
            // Scoped to the channel the card is in. A person muting a card is muting it where
            // they are looking, and widening that to the whole guild silently mutes channels they
            // cannot see.
            channel: Some(ChannelId::new(interaction.channel_id.get())),
            matchers: &matchers,
            reason: &format!("muted from the card by {}", interaction.user.name),
            actor: UserId::new(interaction.user.id.get()),
            expires_at: Some(expires_at),
        },
    )
    .await
    .map_err(|error| CommandError::Failed(error.to_string()))?;

    Ok(Response::text(format!(
        "Rule `{id}`: this channel stays quiet about it until {}. Alertmanager keeps notifying \
         every other receiver.",
        views::relative(expires_at)
    ))
    .about(card.fingerprint.as_str())
    .detailed(json!({ "ignore": id.get() })))
}

/// The full detail view behind the `Details` button.
async fn details(bot: &Arc<BotContext>, control: &CustomId) -> Result<Response, CommandError> {
    let card = card(bot, control).await?;

    let record = bot
        .store
        .alert(&card.fingerprint)
        .await
        .map_err(|error| CommandError::Failed(error.to_string()))?
        .ok_or_else(|| {
            CommandError::BadRequest("that alert is no longer in the database".to_owned())
        })?;

    let held = bot
        .store
        .acknowledgement(&card.fingerprint)
        .await
        .unwrap_or_default();

    let embed = views::detail_embed(&record, held.as_ref(), Some(card.state));
    let row = views::link_row(&bot.renderer, &record.alert);

    Ok(Response::embed(embed)
        .with_row(row)
        .about(card.fingerprint.as_str()))
}

/// Another page of a list the same person is already looking at.
async fn page(bot: &Arc<BotContext>, control: &CustomId) -> Result<Response, CommandError> {
    let argument = control.argument.as_deref().unwrap_or_default();

    let Some((filter, offset)) = views::ListFilter::unpack(argument) else {
        return Err(CommandError::BadRequest(
            "That control is from an older version of the bot. Run `/alerts list` again."
                .to_owned(),
        ));
    };

    let query = filter
        .query(offset)
        .map_err(|error| CommandError::BadRequest(error.to_string()))?;

    let page = bot
        .store
        .query_alerts(&query)
        .await
        .map_err(|error| CommandError::Failed(error.to_string()))?;

    if page.items.is_empty() {
        return Ok(Response::text("That page is empty now."));
    }

    let body = page
        .items
        .iter()
        .map(views::list_line)
        .collect::<Vec<_>>()
        .join("\n");

    let embed = serenity::all::CreateEmbed::new()
        .title("Alerts")
        .description(views::truncated(&body, 4096))
        .footer(serenity::all::CreateEmbedFooter::new(format!(
            "{}–{} of {}",
            offset + 1,
            page.end(),
            page.total
        )));

    Ok(Response::embed(embed).with_row(views::page_row(&filter, offset, page.total)))
}

/// The card a control acts on.
async fn card(
    bot: &Arc<BotContext>,
    control: &CustomId,
) -> Result<dam_store::Notification, CommandError> {
    bot.store
        .notification(control.entity)
        .await
        .map_err(|error| CommandError::Failed(error.to_string()))?
        .ok_or_else(|| {
            CommandError::BadRequest(
                "that card is no longer in the database. Use `/alerts show`.".to_owned(),
            )
        })
}

/// The duration the person chose, from the select or from the identifier.
///
/// Both, because the picker is a select and a card could carry a pre-set duration button; reading
/// only one of them would make the other silently do nothing.
fn chosen(interaction: &ComponentInteraction, control: &CustomId) -> Result<String, CommandError> {
    if let ComponentInteractionDataKind::StringSelect { values } = &interaction.data.kind
        && let Some(value) = values.first()
    {
        return Ok(value.clone());
    }

    control
        .argument
        .clone()
        .ok_or_else(|| CommandError::BadRequest("that control carried no duration".to_owned()))
}

/// The capability one control needs.
fn needed(action: Action) -> Capability {
    match action {
        Action::Details | Action::Page => Capability::View,
        Action::Ack | Action::Unack | Action::IgnoreMenu | Action::IgnoreFor => Capability::Operate,
        // A silence stops every receiver, including whatever pages somebody at four in the
        // morning. It is not the same permission as muting a channel.
        Action::SilenceMenu | Action::SilenceFor => Capability::Silence,
    }
}

/// What to tell somebody who pressed a control this build cannot read.
fn explain(error: &CustomIdError) -> String {
    match error {
        CustomIdError::Version { .. } => {
            "That control is from an older version of the bot. Use `/alerts show` instead."
                .to_owned()
        }
        other => format!("That control could not be read: {other}"),
    }
}

/// The names of the roles the caller holds, from the cache.
fn role_names(ctx: &Context, interaction: &ComponentInteraction) -> Vec<String> {
    let Some(member) = interaction.member.as_ref() else {
        return Vec::new();
    };

    interaction
        .guild_id
        .and_then(|guild| {
            ctx.cache.guild(guild).map(|guild| {
                member
                    .roles
                    .iter()
                    .filter_map(|role| guild.roles.get(role).map(|role| role.name.clone()))
                    .collect::<Vec<_>>()
            })
        })
        .unwrap_or_default()
}

/// Sends the answer to a deferred component interaction.
async fn answer(ctx: &Context, interaction: &ComponentInteraction, response: Response) {
    let edit: EditInteractionResponse = edit(response);

    if let Err(error) = interaction.edit_response(&ctx.http, edit).await {
        warn!(%error, "cannot answer a component interaction");
    }
}
