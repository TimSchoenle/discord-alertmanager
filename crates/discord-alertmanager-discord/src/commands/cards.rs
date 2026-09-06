//! `/cards resync` — the repair that puts a forum's posts back in step with the database.
//!
//! Every ordinary edit and tag change is guarded by a hash on the card's row, which is what keeps
//! an alert storm inside Discord's per-channel edit limits: an update that would change nothing a
//! viewer can see costs no request at all. That guard trusts the row to describe the post, and
//! there are ways for it to stop being true — a queue emptied by hand, a database restored from a
//! backup taken behind Discord, a forum somebody rebuilt. In every one of them the rows claim the
//! posts are current, so nothing is ever sent, and the cards stay wrong until each alert happens
//! to change state on its own.
//!
//! This is the command for that, and it is the only thing in the system that ignores those hashes.
//! It queues one [`Effect::ResyncCard`] per live forum card, and the dispatcher redraws and
//! re-tags each one through the same path a webhook takes.
//!
//! # What it deliberately leaves alone
//!
//! Resolved and orphaned cards. A resolved forum post is history whose thread is usually archived,
//! and the dispatcher reopens a thread to write to it and does not close it again — so resyncing
//! the archive would drag every settled incident back to the top of the forum to say nothing new.
//! An orphaned row names a message somebody deleted. Neither is what "out of sync" means.

use async_trait::async_trait;
use chrono::Utc;
use dam_store::{Effect, NewOutboxItem, Notification, Route, RouteId, RouteTarget};
use serde_json::json;
use serenity::all::{
    CommandDataOption, CommandOptionType, CreateCommand, CreateCommandOption, CreateEmbed,
    CreateEmbedFooter,
};

use crate::capability::Capability;
use crate::commands::views;
use crate::commands::{CommandCtx, CommandError, Response, SlashCommand, hint, string_of};

/// Cards one run will queue.
///
/// A ceiling rather than a page: the set is the live cards of one server's forum routes, which on
/// a deployment that is not itself on fire is tens, and a run that reaches this number is
/// reporting a problem larger than the one the command fixes. High enough to be "all of them" in
/// practice, and low enough that a mistake cannot queue an unbounded burst of Discord calls.
const RESYNC_LIMIT: u32 = 1_000;

/// Routes named in the answer before it stops listing them.
const ROUTES_SHOWN: usize = 10;

/// The `/cards` command.
pub(crate) struct Cards;

#[async_trait]
impl SlashCommand for Cards {
    fn name(&self) -> &'static str {
        "cards"
    }

    fn capability(&self) -> Capability {
        Capability::Admin
    }

    fn definition(&self) -> CreateCommand {
        CreateCommand::new("cards")
            .description("Repair the cards this server's forums are showing")
            .default_member_permissions(hint(Capability::Admin))
            .add_option(
                CreateCommandOption::new(
                    CommandOptionType::SubCommand,
                    "resync",
                    "Redraw and re-tag every live forum post, whatever the database believes",
                )
                .add_sub_option(CreateCommandOption::new(
                    CommandOptionType::String,
                    "route",
                    "Limit it to one forum route. Every one of them by default",
                )),
            )
    }

    async fn run(&self, ctx: &CommandCtx<'_>) -> Result<Response, CommandError> {
        let Some((name, options)) = ctx.subcommand() else {
            return Err(CommandError::BadRequest(
                "`/cards` needs a subcommand".to_owned(),
            ));
        };

        match name {
            "resync" => resync(ctx, options).await,
            other => Err(CommandError::BadRequest(format!(
                "`/cards {other}` belongs to an older version of the bot"
            ))),
        }
    }
}

/// Queues a redraw and a re-tag of every live forum card in this server.
async fn resync(
    ctx: &CommandCtx<'_>,
    options: &[CommandDataOption],
) -> Result<Response, CommandError> {
    // Scoped to the server the command was run in rather than to the routing table as a whole. An
    // administrator holds `admin` in their own server, and a bot serving several must not let that
    // reach into the others.
    let guild = ctx.require_guild()?;
    let named = string_of(options, "route");

    let snapshot = ctx.bot.routing.load();
    let targets: Vec<&Route> = snapshot
        .routes()
        .iter()
        .filter(|route| {
            route.guild_id == guild && matches!(route.target, RouteTarget::Forum { .. })
        })
        .filter(|route| named.is_none_or(|name| route.name == name))
        .collect();

    if targets.is_empty() {
        return Err(CommandError::BadRequest(match named {
            Some(name) => format!("no forum route called `{name}` in this server"),
            None => "no route in this server delivers to a forum, so there is nothing to resync"
                .to_owned(),
        }));
    }

    let ids: Vec<RouteId> = targets.iter().map(|route| route.id).collect();

    let cards = ctx
        .bot
        .store
        .live_cards(&ids, RESYNC_LIMIT)
        .await
        .map_err(|error| CommandError::Failed(error.to_string()))?;

    let subject = subject_of(&targets);

    if cards.is_empty() {
        return Ok(
            Response::text(format!("Nothing to resync: {subject} has no live cards."))
                .about(subject),
        );
    }

    // Queued rather than carried out here. A command has fifteen minutes to answer and a thousand
    // cards is two thousand Discord calls, so the work goes through the outbox that already paces
    // it, retries it and survives a restart — the same path an alert transition takes.
    let now = Utc::now();
    let items: Vec<NewOutboxItem> = cards
        .iter()
        .map(|card| {
            NewOutboxItem::now(
                Effect::ResyncCard {
                    notification: card.id,
                },
                card.dedupe_key.clone(),
                now,
            )
        })
        .collect();

    ctx.bot
        .enqueue(&items)
        .await
        .map_err(|error| CommandError::Failed(error.to_string()))?;

    let detail = json!({
        "routes": targets
            .iter()
            .map(|route| route.name.as_str())
            .collect::<Vec<_>>(),
        "cards": cards.len(),
        "truncated": hit_ceiling(&cards),
    });

    Ok(Response::embed(summary(&targets, &cards))
        .about(subject)
        .detailed(detail))
}

/// What the run queued, and what to expect of it.
fn summary(targets: &[&Route], cards: &[Notification]) -> CreateEmbed {
    let mut lines = vec![
        format!(
            "Queued a redraw and a re-tag of **{}** live {}.",
            cards.len(),
            if cards.len() == 1 { "card" } else { "cards" }
        ),
        String::new(),
        "Each one is sent whatever the database believes about it, so a post that drifted out of \
         line comes back into it. They go out through the queue an alert transition uses and are \
         paced by it; `/status bot` shows how far it has left to go."
            .to_owned(),
    ];

    if hit_ceiling(cards) {
        lines.push(String::new());
        lines.push(format!(
            "**This run stopped at its ceiling of {RESYNC_LIMIT} cards**, oldest first. Run it \
             again once the queue has drained to reach the rest."
        ));
    }

    CreateEmbed::new()
        .title("Forum resync")
        .description(views::truncated(&lines.join("\n"), 4096))
        .field(
            "Routes",
            views::truncated(&route_list(targets), 1024),
            false,
        )
        .footer(CreateEmbedFooter::new(
            "Resolved and orphaned cards are left alone: reopening a settled post says nothing \
             new.",
        ))
}

/// Whether the run stopped at its ceiling rather than at the end of the set.
///
/// Named apart from [`views::truncated`], which shortens a string for an embed field and has
/// nothing to do with this.
fn hit_ceiling(cards: &[Notification]) -> bool {
    cards.len() >= RESYNC_LIMIT as usize
}

/// The routes the run covered, in one field.
fn route_list(targets: &[&Route]) -> String {
    let names: Vec<String> = targets
        .iter()
        .take(ROUTES_SHOWN)
        .map(|route| format!("`{}`", route.name))
        .collect();

    if targets.len() > ROUTES_SHOWN {
        format!(
            "{} and {} more",
            names.join(", "),
            targets.len() - ROUTES_SHOWN
        )
    } else {
        names.join(", ")
    }
}

/// What the run acted on, for the audit row and the answer.
fn subject_of(targets: &[&Route]) -> String {
    match targets {
        [route] => route.name.clone(),
        routes => format!("{} forum routes", routes.len()),
    }
}

#[cfg(test)]
mod tests {
    use dam_core::MatcherSet;
    use dam_store::{
        ChannelId, GroupStrategy, GuildId, Mentions, RouteSource, ThreadPolicy, UserId,
    };

    use super::*;

    /// A route carrying only the fields the answer reads.
    fn route(name: &str) -> Route {
        Route {
            id: RouteId::new(1),
            guild_id: GuildId::new(1),
            name: name.to_owned(),
            matcher_source: "severity=critical".to_owned(),
            matchers: MatcherSet::parse("severity=critical").expect("the expression parses"),
            min_severity: None,
            target: RouteTarget::Text {
                channel: ChannelId::new(2),
                thread: ThreadPolicy::default(),
            },
            group_strategy: GroupStrategy::PerAlert,
            mentions: Mentions::default(),
            escalation: None,
            priority: 100,
            continue_to_next: false,
            source: RouteSource::Config,
            enabled: true,
            created_by: Some(UserId::new(7)),
            created_at: Utc::now(),
        }
    }

    #[test]
    fn one_route_is_named_and_several_are_counted() {
        let one = route("payments");
        let two = route("platform");

        assert_eq!(subject_of(&[&one]), "payments");
        assert_eq!(subject_of(&[&one, &two]), "2 forum routes");
    }

    #[test]
    fn the_route_list_stops_naming_and_starts_counting() {
        let routes: Vec<Route> = (0..ROUTES_SHOWN + 3)
            .map(|index| route(&format!("route-{index}")))
            .collect();
        let borrowed: Vec<&Route> = routes.iter().collect();

        assert_eq!(route_list(&borrowed[..2]), "`route-0`, `route-1`");
        assert!(
            route_list(&borrowed).ends_with("and 3 more"),
            "a server with more forum routes than the field holds is still told how many"
        );
    }
}
