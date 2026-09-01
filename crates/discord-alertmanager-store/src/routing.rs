//! Routes, ignore rules, subscriptions, and the forum tag cache.

use std::str::FromStr;

use chrono::{DateTime, Utc};
use dam_core::{CoreError, Labels, MatcherSet, Severity};
use serde::{Deserialize, Serialize};

use crate::ids::{ChannelId, GuildId, IgnoreId, RoleId, RouteId, SubscriptionId, TagId, UserId};

/// Where a route came from, and therefore who may change it.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum RouteSource {
    /// Declared in the configuration file and synced in at startup.
    ///
    /// Not editable or deletable from Discord: a deployment reproducible from its manifests is
    /// the whole point of declaring it there. One that disappears from the file is disabled
    /// rather than deleted, so the notifications it created keep their foreign key.
    #[default]
    Config,

    /// Created by `/route add`, and living only in the database.
    Discord,
}

impl RouteSource {
    /// The source as the lowercase word stored in the database.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Config => "config",
            Self::Discord => "discord",
        }
    }

    /// Whether a Discord command may edit or delete a route from this source.
    #[must_use]
    pub fn is_mutable_from_discord(self) -> bool {
        matches!(self, Self::Discord)
    }
}

impl FromStr for RouteSource {
    type Err = CoreError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "config" => Ok(Self::Config),
            "discord" => Ok(Self::Discord),
            other => Err(CoreError::UnknownVariant {
                kind: "route source",
                value: other.to_owned(),
            }),
        }
    }
}

/// One routing rule, as it is stored and as the pipeline evaluates it.
///
/// Holds a compiled [`MatcherSet`] rather than the expression alone, so evaluating a route against
/// an alert never compiles a regex. The expression is kept beside it for `/route list`, which has
/// to show what the operator wrote rather than what it compiled to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Route {
    /// Primary key.
    pub id: RouteId,

    /// Guild this route delivers in.
    pub guild_id: GuildId,

    /// Name shown in `/route list` and in a card's footer. Unique within a guild.
    pub name: String,

    /// The matcher expression as written.
    pub matcher_source: String,

    /// The compiled matchers.
    pub matchers: MatcherSet,

    /// Lowest severity this route accepts, applied after the matchers.
    pub min_severity: Option<Severity>,

    /// Where cards go.
    pub target: RouteTarget,

    /// Whether one card covers one alert, one group, or a digest window.
    pub group_strategy: GroupStrategy,

    /// Who is mentioned on a new firing alert.
    pub mentions: Mentions,

    /// When and to whom this route escalates a card nobody has taken.
    pub escalation: Option<Escalation>,

    /// Evaluation order. Lower runs first.
    pub priority: i32,

    /// Whether evaluation continues past this route once it matches.
    pub continue_to_next: bool,

    /// Where the route came from.
    pub source: RouteSource,

    /// Whether the route is live.
    pub enabled: bool,

    /// Who created it, for a route created from Discord.
    pub created_by: Option<UserId>,

    /// When it was created.
    pub created_at: DateTime<Utc>,
}

impl Route {
    /// Whether an alert with these labels and this severity belongs to this route.
    ///
    /// The severity gate is second and separate from the matchers on purpose: `min_severity` is
    /// a comparison against a parsed value, so `warning` accepts `critical`, while a matcher on
    /// the `severity` label is string equality and would not.
    #[must_use]
    pub fn accepts(&self, labels: &Labels, severity: Severity) -> bool {
        self.enabled
            && self.matchers.matches(labels)
            && self.min_severity.is_none_or(|floor| severity >= floor)
    }

    /// Whether an alert at this severity mentions anyone on this route.
    #[must_use]
    pub fn mentions_at(&self, severity: Severity) -> bool {
        !self.mentions.is_empty()
            && self
                .mentions
                .min_severity
                .is_none_or(|floor| severity >= floor)
    }
}

/// Whether one card covers one alert, one Alertmanager group, or a window.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GroupStrategy {
    /// One card per fingerprint, edited in place for its whole lifetime.
    #[default]
    PerAlert,

    /// One card per Alertmanager group key.
    PerGroup,

    /// One rolling card per window, replaced rather than accumulated.
    Digest,
}

impl GroupStrategy {
    /// The strategy as the word stored in the database.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::PerAlert => "per_alert",
            Self::PerGroup => "per_group",
            Self::Digest => "digest",
        }
    }
}

impl FromStr for GroupStrategy {
    type Err = CoreError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "per_alert" => Ok(Self::PerAlert),
            "per_group" => Ok(Self::PerGroup),
            "digest" => Ok(Self::Digest),
            other => Err(CoreError::UnknownVariant {
                kind: "group strategy",
                value: other.to_owned(),
            }),
        }
    }
}

/// Where a route delivers.
///
/// An enum here, where the configuration file has a flat table with a `kind` discriminant. The
/// flat shape exists because it is what a TOML section can express; this shape exists because
/// the four targets have genuinely different mechanics, and making that a compile-time
/// distinction is what keeps forum handling out of the text-channel path.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "lowercase")]
pub enum RouteTarget {
    /// A card in a text channel, with an optional thread hung off it.
    Text {
        /// The channel to post in.
        channel: ChannelId,
        /// When and how a thread is opened.
        thread: ThreadPolicy,
    },

    /// A card as a forum post, with its state carried by tags.
    Forum {
        /// The forum channel to post in.
        channel: ChannelId,
        /// How tags, pinning and archiving behave.
        policy: ForumPolicy,
    },

    /// Every card into one pre-existing thread.
    Thread {
        /// The thread to post in.
        thread: ChannelId,
    },

    /// A direct message to one user.
    Dm {
        /// Who to write to.
        user: UserId,
    },
}

impl RouteTarget {
    /// The channel a card is posted into, which is the user's DM channel for a direct message.
    #[must_use]
    pub fn channel(&self) -> Option<ChannelId> {
        match self {
            Self::Text { channel, .. } | Self::Forum { channel, .. } => Some(*channel),
            Self::Thread { thread } => Some(*thread),
            Self::Dm { .. } => None,
        }
    }

    /// Whether this target is a forum channel, and therefore carries tags and posts.
    #[must_use]
    pub fn is_forum(&self) -> bool {
        matches!(self, Self::Forum { .. })
    }

    /// The discriminant as the lowercase word stored in the database.
    #[must_use]
    pub fn kind(&self) -> &'static str {
        match self {
            Self::Text { .. } => "text",
            Self::Forum { .. } => "forum",
            Self::Thread { .. } => "thread",
            Self::Dm { .. } => "dm",
        }
    }
}

/// When a text-channel route opens a thread, and what sort.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ThreadPolicy {
    /// What opens the thread.
    pub when: ThreadTrigger,

    /// Whether it is visible to the channel.
    pub kind: ThreadKind,

    /// Minutes of inactivity before Discord archives it.
    pub archive_after_minutes: u32,
}

/// What opens a thread on a text-channel route.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ThreadTrigger {
    /// Nothing. The densest option, and the one that needs the fewest permissions.
    #[default]
    Never,

    /// Every card.
    OnCreate,

    /// An acknowledgement.
    OnAck,

    /// A human replying, which keeps quiet channels quiet.
    OnFirstReply,
}

/// Whether a thread is visible to the channel.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ThreadKind {
    /// Anchored to the card, so card and discussion stay linked.
    #[default]
    Public,

    /// Visible only to invited members, and not anchored to any message.
    Private,
}

/// How a forum route names, tags, pins and archives its posts.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[expect(
    clippy::struct_excessive_bools,
    reason = "each flag is an independent operator choice with its own configuration key, so \
              folding them into an enum would invent states the file cannot express"
)]
pub struct ForumPolicy {
    /// Template for the post's title, which Discord requires and caps at 100 characters.
    pub title_template: String,

    /// Whether the bot may create tags it needs but cannot find.
    pub manage_tags: bool,

    /// Names of the four mutually exclusive state tags, in state order.
    pub state_tags: StateTags,

    /// Whether a tag named after the severity is applied.
    pub severity_tags: bool,

    /// Labels whose values become tags, at most three.
    pub label_tags: Vec<String>,

    /// Tag applied when nothing else resolves, for a channel that requires one.
    pub default_tag: Option<String>,

    /// Minutes of inactivity before Discord archives a post.
    pub auto_archive_minutes: u32,

    /// Whether a resolved post is archived.
    pub archive_on_resolve: bool,

    /// Whether a resolved post is locked.
    pub lock_on_resolve: bool,

    /// Lowest severity that pins an unacknowledged post.
    pub pin_min_severity: Option<Severity>,

    /// Most posts pinned at once.
    pub max_pinned: u32,

    /// Whether a state change posts a thread note to resurface the post.
    pub bump_on_state_change: bool,
}

/// The four mutually exclusive tags carrying a post's state.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StateTags {
    /// Tag for a firing, unacknowledged alert.
    pub firing: String,

    /// Tag for an alert somebody has taken.
    pub acked: String,

    /// Tag for a silenced alert.
    pub silenced: String,

    /// Tag for a resolved alert.
    pub resolved: String,
}

/// Who is mentioned when an alert starts firing on a route.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Mentions {
    /// Roles to mention.
    pub roles: Vec<RoleId>,

    /// Users to mention.
    pub users: Vec<UserId>,

    /// Lowest severity that mentions anyone.
    pub min_severity: Option<Severity>,
}

impl Mentions {
    /// Whether there is anybody to mention at all.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.roles.is_empty() && self.users.is_empty()
    }
}

/// When a route escalates a card nobody has acknowledged, and who hears about it.
///
/// A firing alert nobody takes is the failure a chat notification is worst at: the message is
/// there, it scrolled past, and the channel is quiet precisely because everybody assumes somebody
/// else has it. The escalation is one mentioning note in the card's thread, sent once, and the
/// card records that it was sent.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Escalation {
    /// Seconds a card may stay firing and unacknowledged before it escalates.
    pub after_secs: u64,

    /// Roles the escalation mentions.
    pub roles: Vec<RoleId>,

    /// Users the escalation mentions.
    pub users: Vec<UserId>,
}

impl Escalation {
    /// How long a card may stay unanswered, as a duration.
    ///
    /// Floored at a second, because a policy of zero would escalate every alert at the moment it
    /// fired, which is the mention the card itself already carries.
    #[must_use]
    pub fn after(&self) -> chrono::Duration {
        chrono::Duration::seconds(i64::try_from(self.after_secs.max(1)).unwrap_or(i64::MAX))
    }

    /// Whether there is anybody for an escalation to reach.
    ///
    /// A policy naming nobody is a policy that would post an unaddressed line into a thread the
    /// people who need it are not reading.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.roles.is_empty() && self.users.is_empty()
    }
}

/// A bot-local mute.
///
/// An ignored alert still gets its row, still appears in `/alerts list`, and still fires
/// everywhere else Alertmanager sends it. Only the Discord notification stops. That is the whole
/// difference from a silence, and it is why an ignore needs no Alertmanager write access.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IgnoreRule {
    /// Primary key.
    pub id: IgnoreId,

    /// Whether the rule covers a guild or one channel in it.
    pub scope: IgnoreScope,

    /// Guild it applies to.
    pub guild_id: GuildId,

    /// Channel it applies to, for a channel-scoped rule.
    pub channel_id: Option<ChannelId>,

    /// The matcher expression as written.
    pub matcher_source: String,

    /// The compiled matchers.
    pub matchers: MatcherSet,

    /// Why the rule exists. Required, because an unexplained mute outlives whoever set it.
    pub reason: String,

    /// Who set it.
    pub created_by: UserId,

    /// When it was set.
    pub created_at: DateTime<Utc>,

    /// When it lapses, if it does.
    pub expires_at: Option<DateTime<Utc>>,

    /// When it was revoked, if it was.
    pub revoked_at: Option<DateTime<Utc>>,
}

impl IgnoreRule {
    /// Whether the rule is in force at `now`.
    #[must_use]
    pub fn is_active(&self, now: DateTime<Utc>) -> bool {
        self.revoked_at.is_none() && self.expires_at.is_none_or(|expiry| expiry > now)
    }

    /// Whether the rule mutes an alert with these labels in this channel.
    #[must_use]
    pub fn mutes(&self, labels: &Labels, channel: ChannelId, now: DateTime<Utc>) -> bool {
        if !self.is_active(now) || !self.matchers.matches(labels) {
            return false;
        }

        match self.scope {
            IgnoreScope::Guild => true,
            IgnoreScope::Channel => self.channel_id == Some(channel),
        }
    }
}

/// How wide an ignore rule reaches.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum IgnoreScope {
    /// Every channel in the guild.
    #[default]
    Guild,

    /// One channel.
    Channel,
}

impl IgnoreScope {
    /// The scope as the lowercase word stored in the database.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Guild => "guild",
            Self::Channel => "channel",
        }
    }
}

impl FromStr for IgnoreScope {
    type Err = CoreError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "guild" => Ok(Self::Guild),
            "channel" => Ok(Self::Channel),
            other => Err(CoreError::UnknownVariant {
                kind: "ignore scope",
                value: other.to_owned(),
            }),
        }
    }
}

/// A personal direct-message subscription.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Subscription {
    /// Primary key.
    pub id: SubscriptionId,

    /// Who subscribed.
    pub user_id: UserId,

    /// The matcher expression as written.
    pub matcher_source: String,

    /// The compiled matchers.
    pub matchers: MatcherSet,

    /// Lowest severity that reaches them.
    pub min_severity: Option<Severity>,

    /// When they subscribed.
    pub created_at: DateTime<Utc>,
}

/// One cached forum tag, mapping a name to the id Discord assigned it.
///
/// Cached because the hot path applies tags by name and Discord's API takes ids. Fetching the
/// channel's tag list per notification would add a round trip to every state change.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ForumTag {
    /// The forum channel the tag belongs to.
    pub channel_id: ChannelId,

    /// The tag's name, at most Discord's twenty characters.
    pub name: String,

    /// The id Discord assigned.
    pub id: TagId,

    /// Whether only a member holding `MANAGE_THREADS` may apply it.
    ///
    /// Tags the bot creates are non-moderated deliberately: a non-moderated tag can be set by the
    /// thread's owner, which the bot is, while a moderated one cannot.
    pub moderated: bool,

    /// When the cache last agreed with Discord.
    pub synced_at: DateTime<Utc>,
}

#[cfg(test)]
mod tests {
    use dam_core::LabelName;

    use super::*;

    fn labels(pairs: &[(&str, &str)]) -> Labels {
        pairs
            .iter()
            .map(|(name, value)| {
                (
                    LabelName::new(*name).expect("test label name is valid"),
                    (*value).to_owned(),
                )
            })
            .collect()
    }

    fn route(expression: &str, min_severity: Option<Severity>) -> Route {
        Route {
            id: RouteId::new(1),
            guild_id: GuildId::new(1),
            name: "test".to_owned(),
            matcher_source: expression.to_owned(),
            matchers: MatcherSet::parse(expression).expect("expression parses"),
            min_severity,
            target: RouteTarget::Text {
                channel: ChannelId::new(2),
                thread: ThreadPolicy::default(),
            },
            group_strategy: GroupStrategy::default(),
            mentions: Mentions::default(),
            escalation: None,
            priority: 100,
            continue_to_next: false,
            source: RouteSource::Config,
            enabled: true,
            created_by: None,
            created_at: Utc::now(),
        }
    }

    #[test]
    fn the_severity_gate_is_a_comparison_not_an_equality() {
        let route = route("namespace=prod", Some(Severity::Warning));
        let labels = labels(&[("namespace", "prod")]);

        assert!(route.accepts(&labels, Severity::Critical));
        assert!(route.accepts(&labels, Severity::Warning));
        assert!(!route.accepts(&labels, Severity::Info));
    }

    #[test]
    fn a_disabled_route_accepts_nothing() {
        let mut route = route("", None);
        route.enabled = false;

        assert!(!route.accepts(&Labels::new(), Severity::Critical));
    }

    #[test]
    fn a_channel_scoped_ignore_does_not_mute_other_channels() {
        let now = Utc::now();
        let rule = IgnoreRule {
            id: IgnoreId::new(1),
            scope: IgnoreScope::Channel,
            guild_id: GuildId::new(1),
            channel_id: Some(ChannelId::new(7)),
            matcher_source: "alertname=Noisy".to_owned(),
            matchers: MatcherSet::parse("alertname=Noisy").expect("expression parses"),
            reason: "known flapper".to_owned(),
            created_by: UserId::new(3),
            created_at: now,
            expires_at: None,
            revoked_at: None,
        };
        let labels = labels(&[("alertname", "Noisy")]);

        assert!(rule.mutes(&labels, ChannelId::new(7), now));
        assert!(!rule.mutes(&labels, ChannelId::new(8), now));
    }

    #[test]
    fn an_expired_ignore_stops_muting() {
        let now = Utc::now();
        let mut rule = IgnoreRule {
            id: IgnoreId::new(1),
            scope: IgnoreScope::Guild,
            guild_id: GuildId::new(1),
            channel_id: None,
            matcher_source: String::new(),
            matchers: MatcherSet::default(),
            reason: "maintenance window".to_owned(),
            created_by: UserId::new(3),
            created_at: now,
            expires_at: Some(now + chrono::Duration::hours(1)),
            revoked_at: None,
        };

        assert!(rule.mutes(&Labels::new(), ChannelId::new(1), now));

        rule.expires_at = Some(now - chrono::Duration::seconds(1));

        assert!(!rule.mutes(&Labels::new(), ChannelId::new(1), now));
    }
}
