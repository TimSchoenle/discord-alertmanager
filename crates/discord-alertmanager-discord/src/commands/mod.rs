//! The command registry, and the one place authorisation, deferral and auditing happen.
//!
//! Plain `serenity` with a registry of about two hundred lines, rather than `poise`. What the
//! registry buys is a single seam every command passes through: the capability check runs before
//! any handler body, every mutating command and every refusal writes an audit row, and every
//! handler defers before it touches I/O. The cost is argument extraction, which is the helpers at
//! the bottom of this file, and that is an honest trade rather than a free one — a project that
//! valued delivery speed over dependency count could take `poise` and delete this module without
//! touching anything else, because commands only ever call into `dam_store` and `dam_engine`.
//!
//! # Deferral is not optional
//!
//! Discord gives an interaction three seconds to be acknowledged and fifteen minutes to be
//! answered. Every handler here reads a database and most of them reach Alertmanager, so every one
//! of them defers first. A handler that did the work before deferring would show "this interaction
//! failed" during exactly the Alertmanager slowness it was called to investigate.

mod alerts;
mod ignores;
mod routes;
mod silences;
mod status;
mod subscriptions;

pub(crate) mod views;

use std::sync::Arc;

use async_trait::async_trait;
use chrono::Utc;
use dam_store::{AuditEntry, AuditResult, ChannelId, GuildId, RoleId, UserId};
use serde_json::{Value, json};
use serenity::all::{
    CommandDataOption, CommandDataOptionValue, CommandInteraction, Context, CreateActionRow,
    CreateCommand, CreateEmbed, CreateInteractionResponse, CreateInteractionResponseMessage,
    EditInteractionResponse, Permissions,
};
use tracing::warn;

use crate::bot::BotContext;
use crate::capability::Capability;

/// What a handler produces.
///
/// One shape rather than a variant per layout, because the three parts are independent: a list is
/// an embed with controls, a confirmation is a line of text, and a duration picker is a line of
/// text with controls and no embed.
#[derive(Default)]
pub(crate) struct Response {
    /// The message body, if there is one.
    pub(crate) text: Option<String>,

    /// The embed, if there is one.
    pub(crate) embed: Option<CreateEmbed>,

    /// The controls under it.
    pub(crate) rows: Vec<CreateActionRow>,

    /// What the action acted on, for the audit row.
    ///
    /// Carried on the response rather than passed out of band, because only the handler knows
    /// which of its arguments is the subject, and an audit row that names the command without
    /// naming its target answers half the question an incident review asks.
    pub(crate) subject: Option<String>,

    /// Anything else worth keeping in the audit row.
    pub(crate) detail: Value,
}

impl Response {
    /// A line of text.
    pub(crate) fn text(text: impl Into<String>) -> Self {
        Self {
            text: Some(text.into()),
            ..Self::default()
        }
    }

    /// An embed.
    pub(crate) fn embed(embed: CreateEmbed) -> Self {
        Self {
            embed: Some(embed),
            ..Self::default()
        }
    }

    /// Adds controls under the answer.
    #[must_use]
    pub(crate) fn with_rows(mut self, rows: Vec<CreateActionRow>) -> Self {
        self.rows = rows;
        self
    }

    /// Adds one row of controls, when there is one to add.
    #[must_use]
    pub(crate) fn with_row(mut self, row: Option<CreateActionRow>) -> Self {
        self.rows.extend(row);
        self
    }

    /// Names what the action acted on, for the audit row.
    #[must_use]
    pub(crate) fn about(mut self, subject: impl Into<String>) -> Self {
        self.subject = Some(subject.into());
        self
    }

    /// Adds detail to the audit row.
    #[must_use]
    pub(crate) fn detailed(mut self, detail: Value) -> Self {
        self.detail = detail;
        self
    }
}

/// Why a command did not do what it was asked.
#[derive(Debug, thiserror::Error)]
pub(crate) enum CommandError {
    /// The caller lacks the capability.
    #[error("you need the `{0}` capability to do that")]
    Denied(Capability),

    /// The command was given something it cannot use.
    #[error("{0}")]
    BadRequest(String),

    /// Something the caller cannot do anything about failed.
    #[error("{0}")]
    Failed(String),
}

impl CommandError {
    /// How the audit row records this outcome.
    pub(crate) fn result(&self) -> AuditResult {
        match self {
            Self::Denied(_) => AuditResult::Denied,
            Self::BadRequest(_) | Self::Failed(_) => AuditResult::Error,
        }
    }
}

/// One slash command.
#[async_trait]
pub(crate) trait SlashCommand: Send + Sync {
    /// The name Discord registers it under.
    fn name(&self) -> &'static str;

    /// The definition Discord is given.
    fn definition(&self) -> CreateCommand;

    /// The least capability any of its subcommands needs.
    ///
    /// A subcommand needing more checks again inside the handler. This one exists so that a
    /// command nobody may run at all is refused before it is parsed.
    fn capability(&self) -> Capability;

    /// Runs it.
    async fn run(&self, ctx: &CommandCtx<'_>) -> Result<Response, CommandError>;
}

/// Everything a handler is given.
pub(crate) struct CommandCtx<'a> {
    /// The bot's collaborators.
    pub(crate) bot: &'a Arc<BotContext>,

    /// The interaction, for its options and its author.
    pub(crate) interaction: &'a CommandInteraction,

    /// The roles the caller holds, by id.
    pub(crate) roles: Vec<RoleId>,

    /// The names of those roles, so a configuration may name either.
    pub(crate) role_names: Vec<String>,
}

impl CommandCtx<'_> {
    /// Refuses unless the caller holds `capability`.
    ///
    /// # Errors
    ///
    /// Returns [`CommandError::Denied`], which the dispatcher turns into an audit row and a
    /// sentence naming the capability rather than the role.
    pub(crate) fn require(&self, capability: Capability) -> Result<(), CommandError> {
        if self
            .bot
            .capabilities
            .allows(capability, &self.roles, &self.role_names)
        {
            Ok(())
        } else {
            Err(CommandError::Denied(capability))
        }
    }

    /// Who is running the command.
    pub(crate) fn actor(&self) -> UserId {
        UserId::new(self.interaction.user.id.get())
    }

    /// Where they are running it.
    pub(crate) fn guild(&self) -> Option<GuildId> {
        self.interaction
            .guild_id
            .map(|guild| GuildId::new(guild.get()))
    }

    /// The guild this command must have been run in.
    ///
    /// # Errors
    ///
    /// Returns [`CommandError::BadRequest`] in a direct message, where there is no guild to scope
    /// a route or an ignore rule to.
    pub(crate) fn require_guild(&self) -> Result<GuildId, CommandError> {
        self.guild().ok_or_else(|| {
            CommandError::BadRequest("that command only works inside a server".to_owned())
        })
    }

    /// How the caller should be recorded in Alertmanager.
    ///
    /// Both the name and the id, because `amtool` shows the string and only the id survives a
    /// rename.
    pub(crate) fn provenance(&self) -> String {
        format!(
            "discord:{} ({})",
            self.interaction.user.name, self.interaction.user.id
        )
    }

    /// The subcommand and its options.
    pub(crate) fn subcommand(&self) -> Option<(&str, &[CommandDataOption])> {
        let option = self.interaction.data.options.first()?;

        match &option.value {
            CommandDataOptionValue::SubCommand(options)
            | CommandDataOptionValue::SubCommandGroup(options) => {
                Some((option.name.as_str(), options.as_slice()))
            }
            _ => None,
        }
    }
}

/// Every command this build registers.
pub(crate) fn registry() -> Vec<Arc<dyn SlashCommand>> {
    vec![
        Arc::new(alerts::Alerts),
        Arc::new(silences::Silences),
        Arc::new(ignores::Ignores),
        Arc::new(routes::Routes),
        Arc::new(subscriptions::Subscriptions),
        Arc::new(status::Status),
    ]
}

/// Runs one interaction: defers, checks, dispatches, audits, answers.
pub(crate) async fn dispatch(
    bot: &Arc<BotContext>,
    ctx: &Context,
    interaction: &CommandInteraction,
) {
    // Before anything else. Three seconds is the budget, and a database read can spend it.
    let deferred = interaction
        .create_response(
            &ctx.http,
            CreateInteractionResponse::Defer(
                CreateInteractionResponseMessage::new().ephemeral(true),
            ),
        )
        .await;

    if let Err(error) = deferred {
        warn!(%error, command = interaction.data.name, "cannot acknowledge an interaction");
        return;
    }

    let Some(command) = bot
        .commands
        .iter()
        .find(|candidate| candidate.name() == interaction.data.name)
    else {
        answer(
            ctx,
            interaction,
            Response::text("That command belongs to an older version of the bot."),
        )
        .await;
        return;
    };

    let (roles, role_names) = member_roles(ctx, interaction);
    let context = CommandCtx {
        bot,
        interaction,
        roles,
        role_names,
    };

    let outcome = match context.require(command.capability()) {
        Ok(()) => command.run(&context).await,
        Err(denied) => Err(denied),
    };

    let action = format!(
        "{}.{}",
        command.name(),
        context.subcommand().map_or("", |(name, _)| name)
    );

    match outcome {
        Ok(response) => {
            bot.audit(&AuditEntry {
                actor: Some(context.actor()),
                guild_id: context.guild(),
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
            // A denial that leaves no trace is indistinguishable afterwards from a command nobody
            // ran, which is exactly the question an incident review asks.
            bot.audit(&AuditEntry {
                actor: Some(context.actor()),
                guild_id: context.guild(),
                action,
                subject: None,
                detail: json!({ "error": error.to_string() }),
                result: error.result(),
                at: Utc::now(),
            })
            .await;

            answer(ctx, interaction, Response::text(error.to_string())).await;
        }
    }
}

/// Sends the answer to a deferred interaction.
pub(crate) async fn answer(ctx: &Context, interaction: &CommandInteraction, response: Response) {
    if let Err(error) = interaction.edit_response(&ctx.http, edit(response)).await {
        warn!(%error, command = interaction.data.name, "cannot answer an interaction");
    }
}

/// Turns a response into the edit that delivers it.
///
/// A response with neither text nor embed would be a rejected request, so the empty case is given
/// a body that at least says the command finished.
pub(crate) fn edit(response: Response) -> EditInteractionResponse {
    let mut edit = EditInteractionResponse::new().components(response.rows);

    match (response.text, response.embed) {
        (text, Some(embed)) => {
            edit = edit.content(text.unwrap_or_default()).embed(embed);
        }
        (Some(text), None) => edit = edit.content(text),
        (None, None) => edit = edit.content("Done."),
    }

    edit
}

/// The roles the caller holds, by id and by name.
///
/// The names come from the cache, which is the reason the `cache` feature earns its place: without
/// it, resolving `role:oncall` would be a guild fetch on every command.
fn member_roles(ctx: &Context, interaction: &CommandInteraction) -> (Vec<RoleId>, Vec<String>) {
    let Some(member) = interaction.member.as_ref() else {
        return (Vec::new(), Vec::new());
    };

    let ids: Vec<RoleId> = member
        .roles
        .iter()
        .map(|role| RoleId::new(role.get()))
        .collect();

    let names = interaction
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
        .unwrap_or_default();

    (ids, names)
}

/// The permission bits a capability is hinted with.
///
/// A hint and nothing more: Discord uses it to hide commands a member cannot run, and the real
/// check happens in [`CommandCtx::require`]. Treating it as the authorisation would put the
/// decision in a place an administrator can change without anybody auditing it.
pub(crate) fn hint(capability: Capability) -> Permissions {
    match capability {
        Capability::View => Permissions::VIEW_CHANNEL,
        Capability::Operate | Capability::Silence => Permissions::MANAGE_MESSAGES,
        Capability::Admin => Permissions::MANAGE_GUILD,
    }
}

/// Reads a string option.
pub(crate) fn string_of<'a>(options: &'a [CommandDataOption], name: &str) -> Option<&'a str> {
    options
        .iter()
        .find(|option| option.name == name)
        .and_then(|option| match &option.value {
            CommandDataOptionValue::String(value) => Some(value.as_str()),
            _ => None,
        })
}

/// Reads an integer option.
pub(crate) fn integer_of(options: &[CommandDataOption], name: &str) -> Option<i64> {
    options
        .iter()
        .find(|option| option.name == name)
        .and_then(|option| match &option.value {
            CommandDataOptionValue::Integer(value) => Some(*value),
            _ => None,
        })
}

/// Reads a boolean option.
pub(crate) fn boolean_of(options: &[CommandDataOption], name: &str) -> Option<bool> {
    options
        .iter()
        .find(|option| option.name == name)
        .and_then(|option| match &option.value {
            CommandDataOptionValue::Boolean(value) => Some(*value),
            _ => None,
        })
}

/// Reads a user option.
pub(crate) fn user_of(options: &[CommandDataOption], name: &str) -> Option<UserId> {
    options
        .iter()
        .find(|option| option.name == name)
        .and_then(|option| match &option.value {
            CommandDataOptionValue::User(value) => Some(UserId::new(value.get())),
            _ => None,
        })
}

/// Reads a channel option.
pub(crate) fn channel_of(options: &[CommandDataOption], name: &str) -> Option<ChannelId> {
    options
        .iter()
        .find(|option| option.name == name)
        .and_then(|option| match &option.value {
            CommandDataOptionValue::Channel(value) => Some(ChannelId::new(value.get())),
            _ => None,
        })
}
