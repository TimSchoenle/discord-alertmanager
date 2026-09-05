//! The gateway client, the shared context every handler is given, and the events the bot reacts
//! to.
//!
//! Three events matter and nothing else is subscribed to. `Ready` is where commands are
//! registered and a forum channel's tags are read into the cache; `InteractionCreate` is every
//! command and every button; and `Message` is how a card notices that a human replied in its
//! thread.
//!
//! # The context is shared, not rebuilt
//!
//! [`BotContext`] holds the collaborators the composition root already owns, so a command handler
//! reaches the store and Alertmanager through the same instances the webhook path uses. A handler
//! that opened its own client would be a second connection pool, a second retry policy, and a
//! second place for the two to disagree.
//!
//! # Nothing here decides what a card looks like
//!
//! A command that changes something writes the change and enqueues an effect. The dispatcher
//! renders and posts it, exactly as it does for a webhook, which is what keeps a button press and
//! an alert transition producing the same card.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use async_trait::async_trait;
use chrono::Utc;
use dam_config::Discord as DiscordConfig;
use dam_core::Severity;
use dam_engine::{
    AlertmanagerApi, DiscordSink, RouteDefaults, SharedRouting, TagSpec, load_snapshot,
};
use dam_store::{
    AuditEntry, ChannelId, Effect, ForumPolicy, NewOutboxItem, Notification, RouteTarget, Store,
    StoreError, ThreadReply, UserId,
};
use secrecy::{ExposeSecret, SecretString};
use serenity::all::{
    Client, Command, Context, CreateCommand, EventHandler, GatewayIntents, GuildId, Interaction,
    Message, Ready, ResumedEvent, ShardStageUpdateEvent,
};
use serenity::gateway::ConnectionStage;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

use crate::capability::CapabilityMap;
use crate::commands::{self, SlashCommand};
use crate::components;
use crate::render::Renderer;

/// Everything a handler is allowed to reach.
///
/// Constructed once by the composition root and shared by every command, component and event.
pub struct BotContext {
    /// The database.
    pub(crate) store: Arc<dyn Store>,

    /// Alertmanager, for the commands that read or change a silence.
    pub(crate) alertmanager: Arc<dyn AlertmanagerApi>,

    /// Discord, for the tag cache that `Ready` fills.
    pub(crate) sink: Arc<dyn DiscordSink>,

    /// The routing snapshot every decision reads, republished when a route or ignore changes.
    pub(crate) routing: Arc<SharedRouting>,

    /// The card renderer, for the detail views a command answers with.
    pub(crate) renderer: Arc<Renderer>,

    /// What a route created from Discord falls back to for the keys it does not ask about.
    ///
    /// `/route add` takes a handful of options and the file takes dozens, so a route created here
    /// resolves its defaults from the same place a configured one does. Without this, the two
    /// kinds of route would archive their threads on different schedules for no reason anybody
    /// could see.
    pub(crate) route_defaults: RouteDefaults,

    /// Who may do what.
    pub(crate) capabilities: CapabilityMap,

    /// The commands this build registers.
    pub(crate) commands: Vec<Arc<dyn SlashCommand>>,

    /// Guild to register commands into, or global registration when absent.
    dev_guild: Option<GuildId>,

    /// Whether the gateway currently holds a session, which readiness reports.
    connected: Arc<AtomicBool>,
}

impl BotContext {
    /// Writes one audit entry, swallowing a failure to write it.
    ///
    /// An action that succeeded and an audit row that did not are better than an action refused
    /// because its audit row could not be written. The failure is logged, loudly.
    pub(crate) async fn audit(&self, entry: &AuditEntry) {
        if let Err(error) = self.store.append_audit(entry).await {
            warn!(%error, action = entry.action, "cannot write an audit entry");
        }
    }

    /// Queues effects that belong to no decision.
    ///
    /// # Errors
    ///
    /// Returns the store's error.
    pub(crate) async fn enqueue(&self, items: &[NewOutboxItem]) -> Result<(), StoreError> {
        if items.is_empty() {
            return Ok(());
        }

        self.store.enqueue_effects(items, Utc::now()).await
    }

    /// Queues a re-render of every card in `cards`.
    ///
    /// What a command does about a change it made: the write is already committed, and the card
    /// catches up through the same queue an alert transition uses.
    pub(crate) async fn re_render(&self, cards: &[Notification]) {
        let now = Utc::now();
        let items: Vec<NewOutboxItem> = cards
            .iter()
            .map(|card| {
                NewOutboxItem::now(
                    Effect::EditCard {
                        notification: card.id,
                    },
                    card.dedupe_key.clone(),
                    now,
                )
            })
            .collect();

        if let Err(error) = self.enqueue(&items).await {
            warn!(%error, "cannot queue a card re-render");
        }
    }

    /// Rebuilds and republishes the routing snapshot.
    ///
    /// Called after any write that changes what routing resolves to. Republishing the whole
    /// snapshot rather than patching one entry is what keeps the hot path lock-free: readers swap
    /// an `Arc` and never see a half-applied change.
    pub(crate) async fn refresh_routing(&self) {
        match load_snapshot(self.store.as_ref(), Utc::now()).await {
            Ok(snapshot) => self.routing.store(snapshot),
            Err(error) => warn!(%error, "cannot rebuild the routing snapshot"),
        }
    }
}

/// Why the gateway stopped.
#[derive(Debug, thiserror::Error)]
pub enum GatewayError {
    /// The client could not be built, or the session ended in a way it could not recover from.
    #[error("the Discord gateway failed: {0}")]
    Gateway(#[source] serenity::Error),
}

/// The gateway client, before it is started.
pub struct Bot {
    token: SecretString,
    intents: GatewayIntents,
    context: Arc<BotContext>,
}

impl Bot {
    /// Builds the client around the collaborators the composition root owns.
    ///
    /// `connected` is the flag readiness reads. It is shared rather than queried, because
    /// readiness is asked far more often than a session changes.
    #[must_use]
    #[expect(
        clippy::too_many_arguments,
        reason = "the composition root hands the gateway every collaborator it owns; folding \n                  them into a struct would move the same list one line up"
    )]
    pub fn new(
        config: &DiscordConfig,
        store: Arc<dyn Store>,
        alertmanager: Arc<dyn AlertmanagerApi>,
        sink: Arc<dyn DiscordSink>,
        routing: Arc<SharedRouting>,
        renderer: Arc<Renderer>,
        route_defaults: RouteDefaults,
        connected: Arc<AtomicBool>,
    ) -> Self {
        // `GUILDS` for the channel and role cache authorisation reads, `GUILD_MESSAGES` for the
        // thread replies that mark a card responded. `MESSAGE_CONTENT` is privileged and only the
        // text of a reply needs it, so it is asked for solely when the operator opted in.
        let mut intents = GatewayIntents::GUILDS | GatewayIntents::GUILD_MESSAGES;
        if config.capture_reply_text {
            intents |= GatewayIntents::MESSAGE_CONTENT;
        }

        Self {
            token: config.token.clone(),
            intents,
            context: Arc::new(BotContext {
                store,
                alertmanager,
                sink,
                routing,
                renderer,
                route_defaults,
                capabilities: CapabilityMap::new(&config.capabilities),
                commands: commands::registry(),
                dev_guild: config.dev_guild_id.map(GuildId::new),
                connected,
            }),
        }
    }

    /// Connects, and runs until the token is cancelled or the session fails.
    ///
    /// # Errors
    ///
    /// Returns [`GatewayError::Gateway`] when the client cannot be built or the session ends in a
    /// failure it cannot recover from. A cancelled token is not an error.
    pub async fn run(self, shutdown: CancellationToken) -> Result<(), GatewayError> {
        let context = Arc::clone(&self.context);

        let mut client = Client::builder(self.token.expose_secret(), self.intents)
            .event_handler(Handler { bot: context })
            .await
            .map_err(GatewayError::Gateway)?;

        // The shard manager rather than an abort: a dropped task leaves Discord holding a session
        // until it times out, and the next start then contends with the one this process left.
        let shards = Arc::clone(&client.shard_manager);
        let watcher = tokio::spawn(async move {
            shutdown.cancelled().await;
            shards.shutdown_all().await;
        });

        let outcome = client.start().await.map_err(GatewayError::Gateway);

        watcher.abort();
        self.context.connected.store(false, Ordering::Relaxed);

        outcome
    }
}

/// The serenity event handler.
struct Handler {
    bot: Arc<BotContext>,
}

#[async_trait]
impl EventHandler for Handler {
    async fn ready(&self, ctx: Context, ready: Ready) {
        info!(
            bot = %ready.user.name,
            guilds = ready.guilds.len(),
            "gateway session established"
        );
        self.bot.connected.store(true, Ordering::Relaxed);

        register_commands(&self.bot, &ctx).await;
        sync_forum_tags(&self.bot).await;
    }

    async fn resume(&self, _: Context, _: ResumedEvent) {
        self.bot.connected.store(true, Ordering::Relaxed);
    }

    async fn shard_stage_update(&self, _: Context, event: ShardStageUpdateEvent) {
        // Readiness has to report the session the bot actually holds. A shard that is
        // reconnecting is not one that can deliver a card, and saying otherwise would keep a
        // replica in service while its notifications went nowhere.
        let connected = matches!(event.new, ConnectionStage::Connected);
        self.bot.connected.store(connected, Ordering::Relaxed);
    }

    /// Runs one interaction inside a span of its own.
    ///
    /// The span is the root of everything the interaction causes, so a trace collector sees one
    /// unit of work per command or button press. The name of the command is a bounded set and is
    /// recorded; who ran it is not recorded here, because that belongs in the audit trail rather
    /// than in a telemetry export.
    #[tracing::instrument(
        name = "interaction",
        skip_all,
        fields(kind = tracing::field::Empty, name = tracing::field::Empty)
    )]
    async fn interaction_create(&self, ctx: Context, interaction: Interaction) {
        let span = tracing::Span::current();

        match interaction {
            Interaction::Command(command) => {
                span.record("kind", "command");
                span.record("name", command.data.name.as_str());
                commands::dispatch(&self.bot, &ctx, &command).await;
            }
            Interaction::Component(component) => {
                span.record("kind", "component");
                span.record("name", component.data.custom_id.as_str());
                components::dispatch(&self.bot, &ctx, &component).await;
            }
            _ => {}
        }
    }

    async fn message(&self, _: Context, message: Message) {
        // The bot's own cards and thread notes arrive here too, and counting them would mark
        // every card responded the moment it was posted.
        if message.author.bot {
            return;
        }

        let reply = ThreadReply {
            thread_id: ChannelId::new(message.channel_id.get()),
            author_id: UserId::new(message.author.id.get()),
            at: Utc::now(),
        };

        match self.bot.store.record_reply(&reply).await {
            // `None` is the ordinary case: the message was in a channel that is not an alert's
            // thread, which is most messages in most guilds.
            Ok(None) => {}
            Ok(Some(card)) => {
                debug!(notification = %card.id, "a human replied in an alert's thread");
                self.bot.re_render(std::slice::from_ref(&card)).await;
            }
            Err(error) => warn!(%error, "cannot record a thread reply"),
        }
    }
}

/// Registers the command set, skipping the call when Discord already has it.
///
/// Guild-scoped when a development guild is configured, because those appear immediately, and
/// global otherwise, which takes up to an hour to propagate and is what a production deployment
/// wants.
async fn register_commands(bot: &Arc<BotContext>, ctx: &Context) {
    let desired: Vec<CreateCommand> = bot
        .commands
        .iter()
        .map(|command| command.definition())
        .collect();

    let existing = match bot.dev_guild {
        Some(guild) => guild.get_commands(&ctx.http).await,
        None => Command::get_global_commands(&ctx.http).await,
    };

    match existing {
        Ok(existing) if unchanged(&existing, &desired) => {
            debug!(
                count = desired.len(),
                "the command set is already registered"
            );
            return;
        }
        Ok(_) => {}
        // Registering anyway. A command set Discord has and this build cannot read is a worse
        // outcome than one extra write at startup.
        Err(error) => warn!(%error, "cannot read the registered commands; registering anyway"),
    }

    let written = match bot.dev_guild {
        Some(guild) => guild.set_commands(&ctx.http, desired).await,
        None => Command::set_global_commands(&ctx.http, desired).await,
    };

    match written {
        Ok(commands) => info!(count = commands.len(), "registered the command set"),
        Err(error) => error!(%error, "cannot register the command set"),
    }
}

/// Whether Discord already holds exactly the command set this build would write.
///
/// Compared on the fields Discord echoes back unchanged — name, description, and the shape of
/// every option. Anything this comparison cannot see counts as a difference, so the failure mode
/// is one redundant write at startup rather than a stale command nobody can call.
fn unchanged(existing: &[Command], desired: &[CreateCommand]) -> bool {
    let mut have: Vec<String> = existing.iter().filter_map(shape_of).collect();
    let mut want: Vec<String> = desired.iter().filter_map(shape_of).collect();

    if have.len() != existing.len() || want.len() != desired.len() {
        return false;
    }

    have.sort_unstable();
    want.sort_unstable();

    have == want
}

/// The comparable shape of one command, as a canonical string.
fn shape_of<T: serde::Serialize>(command: &T) -> Option<String> {
    let value = serde_json::to_value(command).ok()?;

    Some(normalise(&value).to_string())
}

/// Keeps only the keys Discord round-trips, recursively, so an id or a version does not read as a
/// change.
fn normalise(value: &serde_json::Value) -> serde_json::Value {
    const KEPT: [&str; 7] = [
        "name",
        "description",
        "type",
        "required",
        "options",
        "choices",
        "value",
    ];

    match value {
        serde_json::Value::Object(map) => {
            let mut kept = serde_json::Map::new();
            for key in KEPT {
                if let Some(field) = map.get(key) {
                    kept.insert(key.to_owned(), normalise(field));
                }
            }
            serde_json::Value::Object(kept)
        }
        serde_json::Value::Array(items) => {
            serde_json::Value::Array(items.iter().map(normalise).collect())
        }
        other => other.clone(),
    }
}

/// Reads every forum route's tags into the cache, creating the ones its policy asks for.
///
/// Done once per session rather than per notification: the hot path applies tags by name and
/// Discord's API takes ids, so without the cache every state change would cost a channel fetch.
async fn sync_forum_tags(bot: &Arc<BotContext>) {
    let snapshot = bot.routing.load();
    let mut changed = false;

    for route in snapshot.routes() {
        let RouteTarget::Forum { channel, policy } = &route.target else {
            continue;
        };

        let want = wanted_tags(policy);

        match bot.sink.ensure_forum_tags(*channel, &want).await {
            Ok(tags) => match bot.store.sync_forum_tags(*channel, &tags).await {
                Ok(()) => changed = true,
                Err(error) => warn!(%error, channel = channel.get(), "cannot cache forum tags"),
            },
            // Never fatal. A route whose tags could not be read still posts cards; it just posts
            // them without the tags that failed to resolve.
            Err(error) => warn!(%error, channel = channel.get(), "cannot read a forum's tags"),
        }
    }

    if changed {
        bot.refresh_routing().await;
    }
}

/// The tags a forum policy needs before the first post.
///
/// The state and severity tags only. A label tag's name depends on a value nobody has seen yet,
/// so those are created when an alert first carries the label rather than guessed at startup.
fn wanted_tags(policy: &ForumPolicy) -> Vec<TagSpec> {
    let mut names = vec![
        policy.state_tags.firing.clone(),
        policy.state_tags.acked.clone(),
        policy.state_tags.silenced.clone(),
        policy.state_tags.resolved.clone(),
    ];

    if policy.severity_tags {
        names.extend(
            [Severity::Critical, Severity::Warning, Severity::Info]
                .iter()
                .map(|severity| severity.as_str().to_owned()),
        );
    }

    if let Some(default) = &policy.default_tag {
        names.push(default.clone());
    }

    names.retain(|name| !name.is_empty());
    names.sort_unstable();
    names.dedup();

    names
        .into_iter()
        .map(|name| TagSpec {
            name,
            // Non-moderated deliberately: a moderated tag can only be applied by a member holding
            // `MANAGE_THREADS`, while a non-moderated one can be set by the thread's owner, which
            // the bot is.
            moderated: false,
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use serenity::all::{CommandOptionType, CreateCommandOption};

    use super::*;

    fn definition() -> CreateCommand {
        CreateCommand::new("alerts")
            .description("Inspect and answer alerts")
            .add_option(
                CreateCommandOption::new(CommandOptionType::SubCommand, "list", "List alerts")
                    .add_sub_option(CreateCommandOption::new(
                        CommandOptionType::String,
                        "state",
                        "Filter by state",
                    )),
            )
    }

    #[test]
    fn an_identical_set_is_not_written_again() {
        let desired = definition();
        let shape = shape_of(&desired).expect("a definition serialises");

        // Discord's own answer carries an id and a version this build never sets, so the
        // comparison has to survive them being present on one side only.
        let mut echoed = serde_json::to_value(&desired).expect("a definition serialises");
        echoed["id"] = serde_json::json!("123");
        echoed["version"] = serde_json::json!("456");

        assert_eq!(normalise(&echoed).to_string(), shape);
    }

    #[test]
    fn a_changed_description_is_a_difference() {
        let one = definition();
        let two = CreateCommand::new("alerts").description("Something else");

        assert_ne!(shape_of(&one), shape_of(&two));
    }

    #[test]
    fn a_policy_asks_for_its_state_and_severity_tags() {
        let policy = ForumPolicy {
            title_template: "{{ labels.alertname }}".to_owned(),
            manage_tags: true,
            state_tags: dam_store::StateTags {
                firing: "firing".to_owned(),
                acked: "acked".to_owned(),
                silenced: "silenced".to_owned(),
                resolved: "resolved".to_owned(),
            },
            severity_tags: true,
            label_tags: Vec::new(),
            default_tag: Some("firing".to_owned()),
            auto_archive_minutes: 10_080,
            archive_on_resolve: true,
            lock_on_resolve: false,
            pin_min_severity: None,
            max_pinned: 5,
            bump_on_state_change: true,
        };

        let names: Vec<String> = wanted_tags(&policy)
            .into_iter()
            .map(|spec| spec.name)
            .collect();

        // `firing` is both a state tag and the default, and the channel has room for one of them.
        assert_eq!(names.iter().filter(|name| *name == "firing").count(), 1);
        assert!(names.contains(&"critical".to_owned()));
        assert!(wanted_tags(&policy).iter().all(|spec| !spec.moderated));
    }
}
