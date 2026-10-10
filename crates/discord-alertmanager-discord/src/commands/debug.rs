//! `/debug merge` — why two posts the operator expected to be one are two.
//!
//! The pipeline decides which card an alert lands on from its route, its channel, its labels, the
//! identity policy and the windows, and none of that is visible on the card. An operator looking
//! at two forum posts for what reads as one incident has no way to tell from Discord which of
//! those kept them apart. This command reads both cards back and asks [`explain_separation`],
//! which applies the decision's own rules, and turns its answer into a sentence and a next step.
//!
//! Read-only, so it needs `view`: it says nothing `/alerts` would not already show the same
//! member. Both posts must be cards in the server the command runs in. A thread in another server
//! is reported exactly as a thread that is not a card at all, so the command cannot be used to
//! probe what another server's forums hold.

use async_trait::async_trait;
use chrono::Duration;
use dam_core::{Alert, Retirement};
use dam_engine::{CardFacts, LabelDifference, Separation, Strategy, explain_separation};
use dam_store::{ChannelId, GuildId, Notification, NotificationId, Route};
use serde_json::json;
use serenity::all::{
    ChannelType, CommandDataOption, CommandOptionType, CreateCommand, CreateCommandOption,
    CreateEmbed, CreateEmbedFooter,
};

use crate::capability::Capability;
use crate::commands::views;
use crate::commands::{CommandCtx, CommandError, Response, SlashCommand, channel_of, hint};

/// Labels listed in the answer before it stops listing them.
const LABELS_SHOWN: usize = 15;

/// The label naming the rule an alert came from.
const ALERTNAME: &str = "alertname";

/// The `/debug` command.
///
/// Not named `Debug`, which would shadow the derive macro for everything in this module.
pub(crate) struct DebugCommand;

#[async_trait]
impl SlashCommand for DebugCommand {
    fn name(&self) -> &'static str {
        "debug"
    }

    fn capability(&self) -> Capability {
        Capability::View
    }

    fn definition(&self) -> CreateCommand {
        let post = |name: &'static str, description: &'static str| {
            CreateCommandOption::new(CommandOptionType::Channel, name, description)
                .channel_types(vec![ChannelType::PublicThread, ChannelType::PrivateThread])
                .required(true)
        };

        CreateCommand::new("debug")
            .description("Explain what the bot did and why")
            .default_member_permissions(hint(Capability::View))
            .add_option(
                CreateCommandOption::new(
                    CommandOptionType::SubCommand,
                    "merge",
                    "Explain why two alert posts were not merged into one",
                )
                .add_sub_option(post("first", "One of the two posts"))
                .add_sub_option(post("second", "The other post")),
            )
    }

    async fn run(&self, ctx: &CommandCtx<'_>) -> Result<Response, CommandError> {
        let Some((name, options)) = ctx.subcommand() else {
            return Err(CommandError::BadRequest(
                "`/debug` needs a subcommand".to_owned(),
            ));
        };

        match name {
            "merge" => merge(ctx, options).await,
            other => Err(CommandError::BadRequest(format!(
                "`/debug {other}` belongs to an older version of the bot"
            ))),
        }
    }
}

/// One side of the comparison, as read from the store.
struct Side {
    /// The thread the operator picked.
    thread: ChannelId,

    /// The card behind it.
    card: Notification,

    /// The alert the card last showed, unless it has been pruned.
    alert: Option<Alert>,
}

/// Explains why the two posts are separate cards.
async fn merge(
    ctx: &CommandCtx<'_>,
    options: &[CommandDataOption],
) -> Result<Response, CommandError> {
    let guild = ctx.require_guild()?;

    let (Some(first), Some(second)) = (channel_of(options, "first"), channel_of(options, "second"))
    else {
        return Err(CommandError::BadRequest(
            "`/debug merge` needs two posts".to_owned(),
        ));
    };

    let first = side(ctx, guild, first).await?;
    let second = side(ctx, guild, second).await?;

    let snapshot = ctx.bot.routing.load();
    let route_of = |side: &Side| snapshot.route(side.card.route_id);

    let separation = explain_separation(
        &CardFacts {
            card: &first.card,
            alert: first.alert.as_ref(),
            route: route_of(&first),
        },
        &CardFacts {
            card: &second.card,
            alert: second.alert.as_ref(),
            route: route_of(&second),
        },
        &ctx.bot.decisions,
    );

    let explanation = Explanation {
        first: &first,
        second: &second,
        first_route: route_of(&first),
        second_route: route_of(&second),
        regroup_window: ctx.bot.decisions.regroup_window,
        digest_window: ctx.bot.decisions.digest_window,
    };

    let detail = json!({
        "first": first.card.id.get(),
        "second": second.card.id.get(),
        "reason": reason_code(&separation),
    });

    Ok(Response::embed(explanation.embed(&separation))
        .about(format!("{} / {}", first.thread, second.thread))
        .detailed(detail))
}

/// Reads the card behind `thread`, and the alert it last showed.
///
/// A card in another server answers exactly as no card does. Telling the two apart would let a
/// member of one server learn which thread ids another server's alerts live in.
async fn side(
    ctx: &CommandCtx<'_>,
    guild: GuildId,
    thread: ChannelId,
) -> Result<Side, CommandError> {
    let card = ctx
        .bot
        .store
        .notification_for_thread(thread)
        .await
        .map_err(|error| CommandError::Failed(error.to_string()))?
        .filter(|card| card.guild_id == guild)
        .ok_or_else(|| {
            CommandError::BadRequest(format!(
                "<#{thread}> is not an alert post this bot opened in this server"
            ))
        })?;

    let alert = ctx
        .bot
        .store
        .alert(&card.fingerprint)
        .await
        .map_err(|error| CommandError::Failed(error.to_string()))?
        .map(|record| record.alert);

    Ok(Side {
        thread,
        card,
        alert,
    })
}

/// Everything the answer is written from.
struct Explanation<'a> {
    first: &'a Side,
    second: &'a Side,
    first_route: Option<&'a Route>,
    second_route: Option<&'a Route>,
    regroup_window: Duration,
    digest_window: Duration,
}

impl Explanation<'_> {
    /// The answer: a verdict, the reason in plain words, what to do about it, and both posts.
    fn embed(&self, separation: &Separation) -> CreateEmbed {
        let Words {
            verdict,
            reason,
            fix,
        } = self.words(separation);

        let mut description = format!("**{verdict}**\n\n{reason}");
        if let Some(fix) = fix {
            description.push_str("\n\n**What to change:** ");
            description.push_str(&fix);
        }

        let mut embed = CreateEmbed::new()
            .title("Why these posts were not merged")
            .description(views::truncated(&description, 4096));

        if let Some(differences) = differences_of(separation) {
            embed = embed.field(
                "Labels that differ",
                views::truncated(&Self::label_table(differences), 1024),
                false,
            );
        }

        embed
            .field(
                "First post",
                Self::summary(self.first, self.first_route),
                true,
            )
            .field(
                "Second post",
                Self::summary(self.second, self.second_route),
                true,
            )
            .footer(CreateEmbedFooter::new(format!(
                "Card keys: {} | {}",
                self.first.card.dedupe_key, self.second.card.dedupe_key
            )))
    }

    /// What the answer says about `separation`.
    fn words(&self, separation: &Separation) -> Words {
        let (a, b) = (self.first.thread, self.second.thread);

        match separation {
            Separation::SameCard => Words::new(
                "These are the same post.",
                format!("<#{a}> and <#{b}> are one card, so there is nothing to merge."),
                None::<String>,
            ),
            Separation::DifferentChannels { first, second } => self.channels(*first, *second),
            Separation::Replaced { older, newer } => self.replaced(*older, *newer),
            Separation::Retired { card, why } => self.retired(*card, *why),
            Separation::DifferentStrategies { first, second } => self.strategies(*first, *second),
            Separation::DifferentIdentities { differences } => identities(differences),
            Separation::KeyedBeforePolicy => Words::new(
                "They were opened before the current settings.",
                "Under today's `engine.dedupe_ignore_labels` these two alerts would share one \
                 post, but both posts were opened before that setting covered the labels they \
                 differ in. An open post keeps the grouping it was opened with, so these two \
                 stay apart until they resolve.",
                Some("Nothing. The next time these alerts fire they will share one post."),
            ),
            Separation::DifferentGroups { .. } => Words::new(
                "Alertmanager put them in different groups.",
                format!(
                    "Route {} opens one post per Alertmanager group, and Alertmanager grouped \
                     these alerts separately. Which labels form a group is set by `group_by` in \
                     Alertmanager's own configuration, not by this bot.",
                    route_name(self.first_route)
                ),
                Some(
                    "Remove the labels that should not split the group from `group_by` on the \
                     matching Alertmanager route.",
                ),
            ),
            Separation::DifferentDigestRoutes => Words::new(
                "They are digests from different routes.",
                format!(
                    "<#{a}> collects alerts for route {} and <#{b}> for route {}. Each route \
                     keeps its own digest.",
                    route_name(self.first_route),
                    route_name(self.second_route)
                ),
                None::<String>,
            ),
            Separation::DifferentDigestWindows { first, second } => Words::new(
                "They are digests for different time windows.",
                format!(
                    "A digest post collects {} of alerts and the next window opens a new one. \
                     <#{a}> covers the window starting {} and <#{b}> the one starting {}.",
                    minutes(self.digest_window),
                    views::relative(*first),
                    views::relative(*second)
                ),
                None::<String>,
            ),
            Separation::AlertUnknown => Words::new(
                "The bot can no longer compare them.",
                "Each post follows one alert, and whether two alerts are the same is decided by \
                 their labels. The alert behind at least one of these posts has been removed \
                 from the database by retention, so its labels are gone.",
                None::<String>,
            ),
            Separation::Unexplained => Words::new(
                "Nothing explains this.",
                "None of the things the bot uses to choose a post separates these two. That \
                 should not happen; please report it with the card keys below.",
                None::<String>,
            ),
        }
    }

    /// The posts are in different forums.
    fn channels(&self, first: ChannelId, second: ChannelId) -> Words {
        let (a, b) = (self.first.thread, self.second.thread);

        Words::new(
            "They are in different forums.",
            format!(
                "<#{a}> is in <#{first}> and <#{b}> is in <#{second}>. The bot only looks for an \
                 existing post inside the forum an alert is being sent to, so alerts sent to two \
                 forums always get two posts."
            ),
            Some(format!(
                "If these alerts belong together, point routes {} and {} at the same forum.",
                route_name(self.first_route),
                route_name(self.second_route)
            )),
        )
    }

    /// One post replaced the other after the regroup window.
    fn replaced(&self, older: NotificationId, newer: NotificationId) -> Words {
        let window = minutes(self.regroup_window);

        Words::new(
            "The newer post replaced the older one.",
            format!(
                "The alert resolved, stayed resolved for longer than the regroup window \
                 ({window}), and then fired again. Past that window a returning alert is treated \
                 as a new incident: it gets a fresh post, {}, which links back to {}, instead of \
                 reopening the old one.",
                self.mention(newer),
                self.mention(older)
            ),
            Some(format!(
                "If an alert coming back after {window} should reopen its old post, raise \
                 `engine.regroup_window_secs`."
            )),
        )
    }

    /// One post gave its key up.
    fn retired(&self, card: NotificationId, why: Retirement) -> Words {
        let post = self.mention(card);

        match why {
            Retirement::Superseded => Words::new(
                "One post was replaced by a later one.",
                format!(
                    "{post} was closed out when its alert came back after the regroup window \
                     ({}). A newer post took over from it, and every later change goes there, so \
                     nothing can be merged into {post} any more.",
                    minutes(self.regroup_window)
                ),
                None::<String>,
            ),
            Retirement::Orphaned => Words::new(
                "The bot lost track of one post.",
                format!(
                    "The message that started {post} was deleted, so the bot stopped updating \
                     that post and opens a fresh one the next time the alert changes."
                ),
                Some("Archive alert posts rather than deleting their first message."),
            ),
        }
    }

    /// The posts were keyed by different strategies.
    fn strategies(&self, first: Strategy, second: Strategy) -> Words {
        let (a, b) = (self.first.thread, self.second.thread);
        let storm = first == Strategy::StormDigest || second == Strategy::StormDigest;

        Words::new(
            "They were grouped in different ways.",
            format!(
                "<#{a}> is {}, and <#{b}> is {}. Posts grouped in different ways are never merged \
                 with each other.",
                self.strategy(first, self.first_route),
                self.strategy(second, self.second_route)
            ),
            storm.then_some(
                "Nothing, unless storms are frequent: a storm digest ends on its own once the \
                 route is quiet again. If they are, raise `engine.storm.threshold` or \
                 `engine.storm.forum_threshold`.",
            ),
        )
    }

    /// How a strategy reads in a sentence.
    fn strategy(&self, strategy: Strategy, route: Option<&Route>) -> String {
        match strategy {
            Strategy::PerAlert => "one post per alert".to_owned(),
            Strategy::PerGroup => "one post per Alertmanager group".to_owned(),
            Strategy::Digest => format!(
                "a digest collecting {} of alerts",
                minutes(self.digest_window)
            ),
            Strategy::StormDigest => format!(
                "a storm digest: route {} was receiving more alerts than its storm threshold \
                 allows, so new alerts were collected into one post until it calmed down",
                route_name(route)
            ),
            Strategy::Unrecognised => "grouped in a way this version does not recognise".to_owned(),
        }
    }

    /// One post's route, state and alert, for its field.
    fn summary(side: &Side, route: Option<&Route>) -> String {
        let alert = side
            .alert
            .as_ref()
            .and_then(|alert| alert.labels.alertname())
            .map_or_else(|| "unknown".to_owned(), |name| format!("`{name}`"));

        views::truncated(
            &format!(
                "<#{}>\nRoute: {}\nAlert: {alert}\nState: {}\nOpened {}",
                side.thread,
                route_name(route),
                side.card.state,
                views::relative(side.card.created_at)
            ),
            1024,
        )
    }

    /// One line per differing label, with each post's value.
    fn label_table(differences: &[LabelDifference]) -> String {
        let value = |value: &Option<String>| {
            value
                .as_deref()
                .map_or_else(|| "*not set*".to_owned(), |value| format!("`{value}`"))
        };

        let mut lines: Vec<String> = differences
            .iter()
            .take(LABELS_SHOWN)
            .map(|difference| {
                format!(
                    "`{}`: {} vs {}",
                    difference.name,
                    value(&difference.first),
                    value(&difference.second)
                )
            })
            .collect();

        if differences.len() > LABELS_SHOWN {
            lines.push(format!("and {} more", differences.len() - LABELS_SHOWN));
        }

        lines.join("\n")
    }

    /// The thread mention for one of the two cards.
    fn mention(&self, card: NotificationId) -> String {
        let thread = if card == self.first.card.id {
            self.first.thread
        } else {
            self.second.thread
        };

        format!("<#{thread}>")
    }
}

/// What the answer says: a verdict, the reason in plain words, and what to change.
struct Words {
    /// One sentence, shown in bold.
    verdict: String,

    /// Why, without the vocabulary of the configuration.
    reason: String,

    /// What to change, when anything should.
    fix: Option<String>,
}

impl Words {
    /// Builds an answer from whatever string types the call site has to hand.
    fn new(
        verdict: impl Into<String>,
        reason: impl Into<String>,
        fix: Option<impl Into<String>>,
    ) -> Self {
        Self {
            verdict: verdict.into(),
            reason: reason.into(),
            fix: fix.map(Into::into),
        }
    }
}

/// The per-alert posts follow alerts that differ in counted labels.
fn identities(differences: &[LabelDifference]) -> Words {
    if differences.is_empty() {
        return Words::new(
            "They are different alerts.",
            "Each post follows one alert, and these two alerts have different identities, \
             although no label the bot counts differs between them.",
            None::<String>,
        );
    }

    // Ignoring `alertname` would put every alert on one card, so it is never offered as the fix.
    if differences
        .iter()
        .any(|difference| difference.name == ALERTNAME)
    {
        return Words::new(
            "They come from different alert rules.",
            "Each post follows one alert, and these two are raised by different rules: their \
             `alertname` differs. Alerts from different rules are never merged.",
            None::<String>,
        );
    }

    let names = name_list(differences);

    Words::new(
        "They are different alerts.",
        format!(
            "Each post follows one alert, and an alert is told apart from another by its labels. \
             These two differ in {names}, so the bot treats them as two separate alerts."
        ),
        Some(format!(
            "If {names} changes without the problem changing (a pod name after a restart, for \
             example), add it to `engine.dedupe_ignore_labels`. Alerts that differ only in \
             ignored labels share one post. Posts that are already open keep their current \
             grouping."
        )),
    )
}

/// The label differences a separation carries, if it carries any worth listing.
fn differences_of(separation: &Separation) -> Option<&[LabelDifference]> {
    match separation {
        Separation::DifferentIdentities { differences }
        | Separation::DifferentGroups { differences }
            if !differences.is_empty() =>
        {
            Some(differences)
        }
        _ => None,
    }
}

/// The differing labels' names, as a phrase.
fn name_list(differences: &[LabelDifference]) -> String {
    let names: Vec<String> = differences
        .iter()
        .map(|difference| format!("`{}`", difference.name))
        .collect();

    match names.as_slice() {
        [] => String::new(),
        [one] => one.clone(),
        [rest @ .., last] => format!("{} and {last}", rest.join(", ")),
    }
}

/// A route's name for a sentence, or a placeholder for one since deleted.
fn route_name(route: Option<&Route>) -> String {
    route.map_or_else(
        || "*(deleted route)*".to_owned(),
        |route| format!("`{}`", route.name),
    )
}

/// A window, in the unit an operator configures it in their head.
fn minutes(window: Duration) -> String {
    match window.num_minutes() {
        1 => "1 minute".to_owned(),
        count => format!("{count} minutes"),
    }
}

/// A stable name for the audit row.
fn reason_code(separation: &Separation) -> &'static str {
    match separation {
        Separation::SameCard => "same_card",
        Separation::DifferentChannels { .. } => "different_channels",
        Separation::Replaced { .. } => "replaced",
        Separation::Retired { .. } => "retired",
        Separation::DifferentStrategies { .. } => "different_strategies",
        Separation::DifferentIdentities { .. } => "different_identities",
        Separation::KeyedBeforePolicy => "keyed_before_policy",
        Separation::DifferentGroups { .. } => "different_groups",
        Separation::DifferentDigestRoutes => "different_digest_routes",
        Separation::DifferentDigestWindows { .. } => "different_digest_windows",
        Separation::AlertUnknown => "alert_unknown",
        Separation::Unexplained => "unexplained",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn difference(name: &str) -> LabelDifference {
        LabelDifference {
            name: name.to_owned(),
            first: Some("a".to_owned()),
            second: None,
        }
    }

    #[test]
    fn label_names_read_as_a_phrase() {
        assert_eq!(name_list(&[difference("pod")]), "`pod`");
        assert_eq!(
            name_list(&[difference("pod"), difference("instance")]),
            "`pod` and `instance`"
        );
        assert_eq!(
            name_list(&[difference("a"), difference("b"), difference("c")]),
            "`a`, `b` and `c`"
        );
    }

    #[test]
    fn ignoring_the_alert_name_is_never_suggested() {
        let words = identities(&[difference("alertname"), difference("pod")]);

        assert!(words.fix.is_none());
        assert!(words.reason.contains("different rules"));
    }

    #[test]
    fn a_churning_label_is_suggested_for_the_ignore_list() {
        let words = identities(&[difference("pod")]);

        assert!(
            words
                .fix
                .is_some_and(|fix| fix.contains("`pod`") && fix.contains("dedupe_ignore_labels"))
        );
    }

    #[test]
    fn windows_read_in_minutes() {
        assert_eq!(minutes(Duration::minutes(1)), "1 minute");
        assert_eq!(minutes(Duration::seconds(1800)), "30 minutes");
    }

    #[test]
    fn only_a_separation_with_differences_lists_them() {
        assert!(differences_of(&Separation::KeyedBeforePolicy).is_none());
        assert!(
            differences_of(&Separation::DifferentIdentities {
                differences: Vec::new()
            })
            .is_none()
        );
        assert_eq!(
            differences_of(&Separation::DifferentGroups {
                differences: vec![difference("pod")]
            })
            .map(<[LabelDifference]>::len),
            Some(1)
        );
    }
}
