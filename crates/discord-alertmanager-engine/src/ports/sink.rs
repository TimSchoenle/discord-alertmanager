//! The Discord port: what the dispatcher asks of Discord, and what it does about each failure.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use dam_core::{Alert, NotificationState, Severity};
use dam_store::{
    ChannelId, ForumPolicy, ForumTag, GuildId, MessageId, NotificationId, RouteTarget, TagId,
    ThreadPolicy, UserId,
};
use thiserror::Error;

/// Everything needed to render one card.
///
/// Deliberately data rather than a rendered embed. Enforcing Discord's limits — 6000 characters,
/// 25 fields, 100 bytes of custom id — needs the builder that produces them, and that builder
/// lives in the crate that depends on `serenity`. Handing the sink a finished embed would put
/// half the limit checks here, where the types describing them do not exist.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CardData {
    /// The card this describes, and what a button on it will carry.
    pub notification: NotificationId,

    /// The alert as last known, from the local row rather than from Alertmanager.
    ///
    /// Alertmanager garbage-collects a resolved alert quickly, and a card outlives that by a long
    /// way: a late thread reply or a button press half an hour later still has to re-render.
    pub alert: Alert,

    /// What the card is showing.
    pub state: NotificationState,

    /// The route's name, shown in the footer so an operator can find the rule that sent it.
    pub route_name: String,

    /// Who acknowledged it, if anyone.
    pub acknowledged_by: Option<UserId>,

    /// When they did.
    pub acknowledged_at: Option<DateTime<Utc>>,

    /// Human replies counted in the card's thread.
    pub reply_count: u32,

    /// How many times this alert has re-fired after resolving.
    pub flap_count: u32,

    /// When the alert was first seen, which is older than `starts_at` for a flapping alert.
    pub first_seen_at: DateTime<Utc>,

    /// The silence suppressing it, if one is.
    pub silence: Option<SilenceSummary>,

    /// Why a bot-local ignore rule is muting it, if one is.
    ///
    /// Shown on the card so that "why is this quiet" has an answer without a command. An ignore
    /// stops the Discord notification and nothing else; Alertmanager keeps notifying everyone.
    pub ignore_reason: Option<String>,

    /// Who to mention, on a first post into firing only.
    pub mentions: Vec<Mention>,

    /// Why this card is a digest rather than one card for one alert, when it is.
    ///
    /// A digest is a worse card than the one it replaces, and an operator who is not told why is
    /// left believing the bot has started summarising for no reason.
    pub digest: Option<DigestNotice>,

    /// The card this one replaced, when a re-fire started a new episode.
    ///
    /// The link is the whole point: without it, an alert that comes back a week later produces a
    /// card with no history and the one carrying that history is buried.
    pub previous: Option<PreviousCard>,

    /// When the card was rendered.
    pub rendered_at: DateTime<Utc>,
}

impl CardData {
    /// The alert's severity, which drives colour, tags and pinning.
    #[must_use]
    pub fn severity(&self) -> Severity {
        self.alert.severity()
    }
}

/// Why a route is in digest mode, in the numbers that put it there.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DigestNotice {
    /// Cards the route posted inside the window.
    pub cards: u64,

    /// The threshold it passed.
    pub threshold: u32,

    /// How long the window is, in seconds.
    pub window_secs: i64,
}

/// Where the card a new episode replaced can be found.
///
/// Carries the guild as well as the channel, because a Discord message link needs all three parts
/// and a card knows the guild it lives in while a message reference does not.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PreviousCard {
    /// The guild the old card lives in, or zero for a direct message.
    pub guild: GuildId,

    /// The channel or thread holding it.
    pub channel: ChannelId,

    /// The message itself.
    pub message: MessageId,
}

/// The silence suppressing an alert, as shown on its card.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SilenceSummary {
    /// The id Alertmanager assigned, which is what `amtool` takes.
    pub am_id: String,

    /// When it expires.
    pub ends_at: DateTime<Utc>,

    /// Who created it.
    pub created_by: String,
}

/// Somebody to mention when a card is first posted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mention {
    /// A role.
    Role(dam_store::RoleId),

    /// A user.
    User(UserId),
}

/// Where a card is to be posted.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CardTarget {
    /// The guild.
    pub guild: GuildId,

    /// The channel, forum or thread, and its policy.
    pub target: RouteTarget,

    /// The title a forum post needs, already truncated to Discord's hundred characters.
    ///
    /// Always present, even for a text channel, because a thread opened off a card needs a name
    /// and the same fallback chain produces it.
    pub title: String,

    /// The tags a forum post should carry, in priority order and already capped at five.
    pub tags: Vec<TagId>,
}

impl CardTarget {
    /// The forum policy, when this target is a forum channel.
    #[must_use]
    pub fn forum_policy(&self) -> Option<&ForumPolicy> {
        match &self.target {
            RouteTarget::Forum { policy, .. } => Some(policy),
            _ => None,
        }
    }

    /// The thread policy, when this target is a text channel.
    #[must_use]
    pub fn thread_policy(&self) -> Option<&ThreadPolicy> {
        match &self.target {
            RouteTarget::Text { thread, .. } => Some(thread),
            _ => None,
        }
    }
}

/// A message the sink created.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PostedMessage {
    /// The message id.
    pub message: MessageId,

    /// The thread it opened, if any.
    pub thread: Option<ChannelId>,
}

impl PostedMessage {
    /// A message posted into a channel, with no thread yet.
    #[must_use]
    pub fn plain(message: MessageId) -> Self {
        Self {
            message,
            thread: None,
        }
    }

    /// A forum post, whose starter message id and thread id are the same value.
    ///
    /// A constructor rather than two fields a caller sets to the same number, because the
    /// equality is Discord's invariant and belongs where it cannot be got wrong.
    #[must_use]
    pub fn forum(post: ChannelId) -> Self {
        Self {
            message: MessageId::new(post.get()),
            thread: Some(post),
        }
    }
}

/// Where an existing message lives.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MessageRef {
    /// The channel or thread holding it.
    pub channel: ChannelId,

    /// The message.
    pub message: MessageId,
}

/// A one-line note posted into a thread.
///
/// Also what resurfaces a forum post: editing a message does not bump a forum's activity sort, so
/// a state change that only edits the card leaves the post exactly where it was.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Note {
    /// The line to post.
    pub text: String,
}

/// The archive, lock and auto-archive state of a thread or forum post.
///
/// One structure because Discord takes them in one request, and the resolve path changes all
/// three at once.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PostFlags {
    /// Whether the thread is archived.
    pub archived: bool,

    /// Whether it is locked.
    ///
    /// Reopening a locked thread needs `MANAGE_THREADS`, and a flapping alert has to reopen one,
    /// so locking a resolved post costs more than it looks.
    pub locked: bool,

    /// Minutes of inactivity before Discord archives it. Only 60, 1440, 4320 and 10080 are legal.
    pub auto_archive_minutes: u32,
}

/// A tag the bot wants a forum channel to have.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TagSpec {
    /// The tag's name, at most twenty characters.
    pub name: String,

    /// Whether it may only be applied by a member holding `MANAGE_THREADS`.
    ///
    /// Always false for tags the bot creates: a non-moderated tag can be applied by the thread's
    /// owner, which the bot is, while a moderated one cannot.
    pub moderated: bool,
}

/// What a call to Discord can fail with.
///
/// The variants are the cases the dispatcher reacts to differently. Mapping `serenity`'s error
/// codes onto them is the job of the crate that owns the client; an error code in this crate
/// would mean the pipeline knew what Discord was.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum SinkError {
    /// The message is gone, usually because somebody deleted the card.
    ///
    /// The card is recreated rather than retried: retrying an edit against a deleted message
    /// fails forever, and the notification it carried is the point.
    #[error("message no longer exists")]
    UnknownMessage,

    /// The bot cannot do this here.
    ///
    /// The route is marked unhealthy and an admin is told. Retrying costs requests and changes
    /// nothing, because a permission is not a transient condition.
    #[error("missing permission: {detail}")]
    MissingPermissions {
        /// What was refused.
        detail: String,
    },

    /// Discord asked for the request to be made later.
    #[error("rate limited, retry in {retry_after_ms}ms")]
    RateLimited {
        /// How long Discord asked to wait.
        retry_after_ms: u64,
    },

    /// The thread is archived and must be reopened before it accepts writes.
    #[error("thread is archived")]
    ThreadArchived,

    /// The thread is locked, which needs `MANAGE_THREADS` to undo.
    #[error("thread is locked")]
    ThreadLocked,

    /// A tag id no longer exists, usually because a human deleted it.
    ///
    /// The tag cache is resynchronised and the call retried once; a tag that still does not
    /// resolve is dropped, because a missing tag never justifies losing a notification.
    #[error("unknown forum tag")]
    UnknownTag,

    /// The forum channel has no room for another tag.
    #[error("forum tag budget exhausted")]
    TagBudgetExceeded,

    /// The channel is not the kind this route expects.
    #[error("channel is not a {expected} channel")]
    WrongChannelType {
        /// What the route required.
        expected: &'static str,
    },

    /// Anything else, including network failures and 5xx.
    #[error("discord request failed: {detail}")]
    Transient {
        /// What went wrong.
        detail: String,
    },
}

impl SinkError {
    /// Whether retrying the same call could plausibly succeed.
    #[must_use]
    pub fn is_retryable(&self) -> bool {
        matches!(
            self,
            Self::RateLimited { .. } | Self::Transient { .. } | Self::ThreadArchived
        )
    }

    /// How long to wait before the next attempt, when Discord said.
    #[must_use]
    pub fn retry_after_ms(&self) -> Option<u64> {
        match self {
            Self::RateLimited { retry_after_ms } => Some(*retry_after_ms),
            _ => None,
        }
    }
}

/// Everything the dispatcher asks of Discord.
///
/// One method per API call, and no method that means "work out what to do". The pipeline decides;
/// this trait is the hand that carries it out, which is what keeps the decisions testable without
/// a gateway connection.
#[async_trait]
pub trait DiscordSink: Send + Sync {
    /// Posts a card into a text channel, a thread, or a direct message.
    ///
    /// # Errors
    ///
    /// Returns [`SinkError`] for a permission problem, a rate limit, or a transport failure.
    async fn post_card(
        &self,
        target: &CardTarget,
        card: &CardData,
    ) -> Result<PostedMessage, SinkError>;

    /// Creates a forum post and its starter message in one call.
    ///
    /// Separate from [`DiscordSink::post_card`] because a forum channel accepts no plain
    /// messages: the post, its title and its first message are one request, and its tags have to
    /// be supplied while making it.
    ///
    /// # Errors
    ///
    /// As [`DiscordSink::post_card`], plus [`SinkError::WrongChannelType`] when the channel is
    /// not a forum and [`SinkError::UnknownTag`] when a tag has been deleted.
    async fn create_forum_post(
        &self,
        target: &CardTarget,
        card: &CardData,
    ) -> Result<PostedMessage, SinkError>;

    /// Re-renders an existing card.
    ///
    /// # Errors
    ///
    /// Returns [`SinkError::UnknownMessage`] when the card was deleted, or
    /// [`SinkError::ThreadArchived`] when it lives in an archived thread.
    async fn edit_card(&self, message: &MessageRef, card: &CardData) -> Result<(), SinkError>;

    /// Opens a thread on a card.
    ///
    /// # Errors
    ///
    /// As [`DiscordSink::post_card`].
    async fn open_thread(&self, message: &MessageRef, name: &str) -> Result<ChannelId, SinkError>;

    /// Posts a note into a thread, or into any other channel.
    ///
    /// Notifies nobody: a note is a timeline entry and a way to resurface a forum post, and a
    /// second ping for a change somebody has already been told about is how a bot gets muted.
    /// The administrative notices take this path too, which is why it takes a channel rather than
    /// insisting on a thread.
    ///
    /// # Errors
    ///
    /// As [`DiscordSink::edit_card`].
    async fn post_thread_note(&self, thread: ChannelId, note: &Note) -> Result<(), SinkError>;

    /// Posts a line that deliberately does notify the people it names.
    ///
    /// The one call in this trait whose purpose is to interrupt somebody. An escalation exists
    /// because a firing alert nobody acknowledged has already been scrolled past once, so it
    /// pings — and only the roles and users it was given, never `@everyone` and never whatever a
    /// label happened to contain.
    ///
    /// # Errors
    ///
    /// As [`DiscordSink::edit_card`].
    async fn post_escalation(
        &self,
        channel: ChannelId,
        mentions: &[Mention],
        text: &str,
    ) -> Result<(), SinkError>;

    /// Removes or disables a card's components.
    ///
    /// # Errors
    ///
    /// As [`DiscordSink::edit_card`].
    async fn disable_components(&self, message: &MessageRef) -> Result<(), SinkError>;

    /// Replaces the tags on a forum post.
    ///
    /// # Errors
    ///
    /// As [`DiscordSink::create_forum_post`].
    async fn set_post_tags(&self, thread: ChannelId, tags: &[TagId]) -> Result<(), SinkError>;

    /// Sets a post's archive, lock and auto-archive state in one request.
    ///
    /// # Errors
    ///
    /// Returns [`SinkError::ThreadLocked`] when reopening needs a permission the bot lacks.
    async fn set_post_flags(&self, thread: ChannelId, flags: PostFlags) -> Result<(), SinkError>;

    /// Pins or unpins a post.
    ///
    /// # Errors
    ///
    /// As [`DiscordSink::post_card`]. A failed pin is a warning and never blocks a notification.
    async fn set_post_pinned(&self, thread: ChannelId, pinned: bool) -> Result<(), SinkError>;

    /// The tags a forum channel currently has.
    ///
    /// # Errors
    ///
    /// As [`DiscordSink::post_card`].
    async fn forum_tags(&self, forum: ChannelId) -> Result<Vec<ForumTag>, SinkError>;

    /// Creates the missing tags among `want`, and returns the channel's tags afterwards.
    ///
    /// Degrades rather than failing: without `MANAGE_CHANNELS` the tags that already resolve are
    /// returned and the rest are omitted, because a missing tag is a worse reason to lose a
    /// notification than any tag is a reason to have one.
    ///
    /// # Errors
    ///
    /// Returns [`SinkError::TagBudgetExceeded`] when the channel is at Discord's limit.
    async fn ensure_forum_tags(
        &self,
        forum: ChannelId,
        want: &[TagSpec],
    ) -> Result<Vec<ForumTag>, SinkError>;
}
