//! Routes declared in the configuration file, and the four kinds of target one can deliver to.

use serde::Deserialize;

/// One routing rule.
///
/// Routes are evaluated by ascending `priority`, then by declaration order, and the first match
/// wins unless it sets `continue_to_next`. `/route test` takes a sample label set and prints which
/// route wins and why, which is the fastest way to answer why an alert went where it did.
#[derive(Debug, Clone, Deserialize)]
#[cfg_attr(
    feature = "config-schema",
    derive(serde::Serialize, terrace_config::schema::Describe)
)]
#[serde(default, deny_unknown_fields)]
pub struct RouteConfig {
    /// Name shown in `/route list` and in a card's footer. Unique within a guild.
    pub name: String,

    /// Guild this route belongs to.
    pub guild_id: u64,

    /// Alertmanager matcher expression, such as `severity=critical, namespace=~prod-.*`.
    ///
    /// The operators are Alertmanager's: `=`, `!=`, `=~` and `!~`. A regex is fully anchored and
    /// matches the empty string when the label is absent, which is what Alertmanager does. A
    /// pattern longer than 512 bytes is refused, and compiled patterns carry size limits, because
    /// these also arrive from `/route add`.
    pub matchers: String,

    /// Lowest severity this route accepts. Every severity passes when unset.
    #[cfg_attr(feature = "config-schema", config(values))]
    pub min_severity: Option<Severity>,

    /// Where cards for this route are posted.
    #[cfg_attr(feature = "config-schema", config(nested))]
    pub target: RouteTarget,

    /// Whether one card covers one alert or one Alertmanager group.
    #[cfg_attr(feature = "config-schema", config(values))]
    pub group_strategy: GroupStrategy,

    /// Who is mentioned, and at what severity.
    #[cfg_attr(feature = "config-schema", config(nested))]
    pub mentions: Mentions,

    /// When a card nobody has acknowledged is escalated, and to whom.
    #[cfg_attr(feature = "config-schema", config(nested))]
    pub escalation: Escalation,

    /// Evaluation order. Lower runs first.
    pub priority: i32,

    /// Keep evaluating later routes after this one matches.
    ///
    /// This is Alertmanager's own `continue`, spelled out because `continue` is a Rust keyword.
    pub continue_to_next: bool,

    /// Whether the route is live.
    ///
    /// A route removed from this file is disabled rather than deleted, so the notifications it
    /// created keep their foreign key and stay renderable.
    pub enabled: bool,
}

impl Default for RouteConfig {
    fn default() -> Self {
        Self {
            name: String::new(),
            guild_id: 0,
            matchers: String::new(),
            min_severity: None,
            target: RouteTarget::default(),
            group_strategy: GroupStrategy::default(),
            mentions: Mentions::default(),
            escalation: Escalation::default(),
            priority: 100,
            continue_to_next: false,
            enabled: true,
        }
    }
}

/// Alert severity, ordered from least to most urgent.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Deserialize)]
#[cfg_attr(
    feature = "config-schema",
    derive(serde::Serialize, terrace_config::schema::Describe)
)]
#[serde(rename_all = "lowercase")]
pub enum Severity {
    /// Informational. Never pages, never mentions.
    #[default]
    Info,

    /// Something to look at during working hours.
    Warning,

    /// Something to look at now.
    Critical,
}

/// Whether a card covers one alert or one Alertmanager group.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[cfg_attr(
    feature = "config-schema",
    derive(serde::Serialize, terrace_config::schema::Describe)
)]
#[serde(rename_all = "snake_case")]
pub enum GroupStrategy {
    /// One card per alert fingerprint, edited in place for its whole lifetime.
    #[default]
    PerAlert,

    /// One card per Alertmanager group key, listing the alerts in it.
    ///
    /// Worth choosing where a typical group is large enough that one card per alert would bury
    /// the channel.
    PerGroup,

    /// One rolling card per window, replaced rather than accumulated.
    Digest,
}

/// Who gets mentioned when an alert starts firing.
///
/// Mentions happen only on the transition into firing, and never on an edit. Re-mentioning on
/// every update is the fastest way to get the bot muted by everyone it was meant to reach.
#[derive(Debug, Clone, Deserialize)]
#[cfg_attr(
    feature = "config-schema",
    derive(serde::Serialize, terrace_config::schema::Describe)
)]
#[serde(default, deny_unknown_fields)]
pub struct Mentions {
    /// Role ids mentioned on a new firing alert.
    pub roles: Vec<u64>,

    /// User ids mentioned on a new firing alert.
    pub users: Vec<u64>,

    /// Lowest severity that mentions anyone.
    #[cfg_attr(feature = "config-schema", config(values))]
    pub min_severity: Option<Severity>,
}

impl Default for Mentions {
    fn default() -> Self {
        // Critical only. A warning that pings a role at three in the morning is how a team learns
        // to mute the channel.
        Self {
            roles: Vec::new(),
            users: Vec::new(),
            min_severity: Some(Severity::Critical),
        }
    }
}

/// When an unanswered card is escalated, and who hears about it.
///
/// A firing alert nobody acknowledges is the failure a chat notification is worst at: the message
/// arrived, it scrolled past, and the channel is quiet precisely because everybody assumes
/// somebody else has it. An escalation is one further mention in the card's thread, sent once per
/// card, which is the smallest thing that answers it without becoming noise of its own.
#[derive(Debug, Clone, Default, Deserialize)]
#[cfg_attr(
    feature = "config-schema",
    derive(serde::Serialize, terrace_config::schema::Describe)
)]
#[serde(default, deny_unknown_fields)]
pub struct Escalation {
    /// Seconds a card may stay firing and unacknowledged before it escalates.
    ///
    /// Unset disables escalation on this route, which is the default: a route that escalates
    /// without anybody having asked for it is a second mention nobody expected.
    pub after_secs: Option<u64>,

    /// Role ids the escalation mentions. Falls back to `mentions.roles` when empty.
    pub roles: Vec<u64>,

    /// User ids the escalation mentions. Falls back to `mentions.users` when empty.
    ///
    /// Naming a person here rather than a role is what makes an escalation reach somebody who is
    /// not watching the channel it fires in.
    pub users: Vec<u64>,
}

/// Where a route delivers.
#[derive(Debug, Clone, Default, Deserialize)]
#[cfg_attr(
    feature = "config-schema",
    derive(serde::Serialize, terrace_config::schema::Describe)
)]
#[serde(default, deny_unknown_fields)]
pub struct RouteTarget {
    /// What sort of channel `id` names.
    ///
    /// `/route test` checks the channel's actual type against this before the route is accepted,
    /// because posting a plain message to a forum channel fails at delivery time otherwise.
    #[cfg_attr(feature = "config-schema", config(values))]
    pub kind: TargetKind,

    /// Snowflake of the channel, thread or user.
    pub id: u64,

    /// Settings the target kind reads. Keys belonging to other kinds are ignored.
    #[cfg_attr(feature = "config-schema", config(nested))]
    pub policy: TargetPolicy,
}

/// The four kinds of place a card can be delivered to.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[cfg_attr(
    feature = "config-schema",
    derive(serde::Serialize, terrace_config::schema::Describe)
)]
#[serde(rename_all = "lowercase")]
pub enum TargetKind {
    /// A text channel, with an optional thread hung off each card.
    #[default]
    Text,

    /// A forum channel, where the post index is the alert queue and tags carry state.
    Forum,

    /// One pre-existing thread that every card is posted into.
    Thread,

    /// A direct message to one user.
    Dm,
}

/// Everything a target kind can be configured with.
///
/// One flat table rather than one per kind, because a `[routes.target.policy]` section in a file
/// looks the same either way. `/route test` reports any key set for a kind other than the one
/// selected.
#[derive(Debug, Clone, Deserialize)]
#[cfg_attr(
    feature = "config-schema",
    derive(serde::Serialize, terrace_config::schema::Describe)
)]
#[serde(default, deny_unknown_fields)]
#[expect(
    clippy::struct_excessive_bools,
    reason = "each flag is an independent key an operator sets in the file, so grouping them \
              into an enum would invent combinations the table cannot express"
)]
pub struct TargetPolicy {
    /// When a text-channel route opens a thread for a card.
    #[cfg_attr(feature = "config-schema", config(values))]
    pub thread_when: ThreadTrigger,

    /// Whether that thread is visible to the channel or only to invited members.
    ///
    /// A private thread is not anchored to the card, so the card has to carry a link to it and
    /// the bot has to invite the mentioned roles' members. Choose it for routes whose labels are
    /// sensitive, and accept both costs.
    #[cfg_attr(feature = "config-schema", config(values))]
    pub thread_kind: ThreadKind,

    /// Title template for a forum post, rendered with `minijinja`.
    ///
    /// A forum post title is mandatory and 1 to 100 characters, so this falls back to the alert
    /// name and then to the fingerprint. An empty title fails post creation outright.
    pub title_template: String,

    /// Create missing forum tags, which needs `MANAGE_CHANNELS`.
    ///
    /// Created tags are non-moderated on purpose: a moderated tag can only be applied by a member
    /// holding `MANAGE_THREADS`, while a non-moderated one can be set by the thread's owner,
    /// which the bot is. Without the permission the bot applies the tags that already resolve and
    /// reports the gap in `/route test`; a missing tag never fails a notification.
    pub manage_tags: bool,

    /// Names of the four mutually exclusive state tags.
    #[cfg_attr(feature = "config-schema", config(nested))]
    pub state_tags: ForumStateTags,

    /// Apply a tag named after the alert's severity.
    pub severity_tags: bool,

    /// Labels whose values become tags, at most three.
    ///
    /// Use low-cardinality labels only. A forum channel holds 20 tags and a post holds 5; the bot
    /// refuses to create a new tag once the channel reaches 18, so an unbounded label exhausts
    /// the budget in an afternoon and later posts lose their tags.
    pub label_tags: Vec<String>,

    /// Tag applied when nothing else resolves, for a channel with `REQUIRE_TAG` set.
    pub default_tag: String,

    /// Minutes of inactivity before a resolved post or thread archives.
    ///
    /// Discord accepts 60, 1440, 4320 and 10080. Unset takes
    /// `render.thread_archive_after_minutes`. A card that is still firing holds 10080 whatever
    /// this says, so that a long incident never archives underneath the people working it.
    pub auto_archive_minutes: Option<u32>,

    /// Archive a forum post when its alert resolves.
    pub archive_on_resolve: bool,

    /// Lock a forum post when its alert resolves.
    ///
    /// A locked thread needs `MANAGE_THREADS` to reopen, and a flap has to reopen one. Without
    /// that permission the notification is marked orphaned and a fresh card is posted, so leave
    /// this off unless the permission is held.
    pub lock_on_resolve: bool,

    /// Lowest severity that pins an unacknowledged post.
    ///
    /// Pinned posts are a free "needs attention" tray at the top of the index. The pin is dropped
    /// on acknowledgement or resolution, and a pin that fails is a warning that never blocks the
    /// notification.
    #[cfg_attr(feature = "config-schema", config(values))]
    pub pin_min_severity: Option<Severity>,

    /// Most posts pinned at once.
    pub max_pinned: u32,

    /// Post a one-line thread note on every state change.
    ///
    /// Editing a message does not bump a forum's activity sort; only a new message does. Without
    /// this, a state change that only edits the card leaves the post buried where it was. The
    /// note also gives the post the readable timeline that an edited-in-place card deliberately
    /// does not keep.
    pub bump_on_state_change: bool,
}

impl Default for TargetPolicy {
    fn default() -> Self {
        Self {
            thread_when: ThreadTrigger::default(),
            thread_kind: ThreadKind::default(),
            title_template: "{{ labels.alertname }}".to_owned(),
            manage_tags: false,
            state_tags: ForumStateTags::default(),
            severity_tags: true,
            label_tags: Vec::new(),
            default_tag: String::new(),
            auto_archive_minutes: None,
            archive_on_resolve: true,
            lock_on_resolve: false,
            pin_min_severity: Some(Severity::Critical),
            max_pinned: 5,
            bump_on_state_change: true,
        }
    }
}

/// When a text-channel route opens a thread.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[cfg_attr(
    feature = "config-schema",
    derive(serde::Serialize, terrace_config::schema::Describe)
)]
#[serde(rename_all = "snake_case")]
pub enum ThreadTrigger {
    /// No thread. The densest option, and the one that works with any permission set.
    #[default]
    Never,

    /// A thread on every card, whether or not anyone uses it.
    OnCreate,

    /// A thread once the alert is acknowledged.
    OnAck,

    /// A thread once a human replies.
    ///
    /// This keeps quiet channels quiet. The trade is discoverability: a responder has to know
    /// that replying is what opens the thread.
    OnFirstReply,
}

/// Whether a thread is visible to the channel.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[cfg_attr(
    feature = "config-schema",
    derive(serde::Serialize, terrace_config::schema::Describe)
)]
#[serde(rename_all = "lowercase")]
pub enum ThreadKind {
    /// Anchored to the card, so card and discussion stay visually linked.
    #[default]
    Public,

    /// Visible only to invited members, and not anchored to any message.
    Private,
}

/// Names of the mutually exclusive forum tags that carry an alert's state.
///
/// Exactly one is applied at a time. They occupy 4 of a channel's 20 tag slots.
#[derive(Debug, Clone, Deserialize)]
#[cfg_attr(
    feature = "config-schema",
    derive(serde::Serialize, terrace_config::schema::Describe)
)]
#[serde(default, deny_unknown_fields)]
pub struct ForumStateTags {
    /// Tag for an alert that is firing and unacknowledged.
    pub firing: String,

    /// Tag for an alert somebody has taken.
    pub acked: String,

    /// Tag for an alert an Alertmanager silence is suppressing.
    pub silenced: String,

    /// Tag for an alert that has resolved.
    pub resolved: String,
}

impl Default for ForumStateTags {
    fn default() -> Self {
        // Discord caps a tag name at 20 characters, which every one of these is well inside.
        Self {
            firing: "firing".to_owned(),
            acked: "acked".to_owned(),
            silenced: "silenced".to_owned(),
            resolved: "resolved".to_owned(),
        }
    }
}
