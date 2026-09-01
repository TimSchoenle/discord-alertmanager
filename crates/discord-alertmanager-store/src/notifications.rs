//! The notification rows: which Discord message represents which alert, and who answered it.

use std::str::FromStr;

use chrono::{DateTime, Utc};
use dam_core::{CoreError, DedupeKey, Fingerprint, NotificationState};
use serde::{Deserialize, Serialize};

use crate::ids::{ChannelId, GuildId, MessageId, NotificationId, RouteId, TagId, UserId};

/// One card: the join between an alert and the Discord message showing it.
///
/// The only fact in this system that nothing else can reconstruct. Alertmanager holds the alerts
/// and Discord holds the messages; nobody but the bot knows which is which, which is why this
/// table is mandatory even in a deployment that keeps no alert history at all.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Notification {
    /// Primary key, and what a button's custom id carries.
    pub id: NotificationId,

    /// What this card covers: one alert, one group, or one digest window.
    pub dedupe_key: DedupeKey,

    /// The alert the card last showed.
    ///
    /// Equal to the card's own alert under the default per-alert strategy, and the most recent
    /// member of the group under the other two. Stored rather than derived from `dedupe_key`,
    /// because a group key names a group and a renderer needs an alert: without this column the
    /// only way to draw a group card is to reverse a key into a query, and a digest key cannot be
    /// reversed at all.
    pub fingerprint: Fingerprint,

    /// The route that produced it.
    pub route_id: RouteId,

    /// Guild the card lives in.
    pub guild_id: GuildId,

    /// Channel the card lives in. For a forum post this is the post's parent forum.
    pub channel_id: ChannelId,

    /// The message, once it has been posted.
    ///
    /// Null between the row being created and the first post succeeding, which is the window the
    /// outbox exists to make survivable.
    pub message_id: Option<MessageId>,

    /// The thread hanging off the card, or the forum post itself.
    ///
    /// For a forum post this equals the starter message id, because Discord makes them the same
    /// value. Reply detection then works identically for both without a special case.
    pub thread_id: Option<ChannelId>,

    /// What the card is showing.
    pub state: NotificationState,

    /// Hash of the rendered card as last posted.
    ///
    /// An edit whose freshly computed hash equals this one is skipped outright. Discord's
    /// per-channel edit limits are strict enough that an alert storm without this produces
    /// nothing but rate-limit responses.
    pub render_hash: Option<String>,

    /// Forum tags currently on the post.
    pub applied_tags: Vec<TagId>,

    /// Hash of the desired tag set, compared before issuing a tag update.
    pub tags_hash: Option<String>,

    /// Whether the post is pinned.
    pub pinned: bool,

    /// Whether the thread or forum post is archived.
    ///
    /// An archived thread rejects edits and tag changes, so unarchiving is an explicit step in
    /// the effect list rather than a retry after the failure.
    pub archived: bool,

    /// When a human first responded, by button, command or thread reply.
    pub responded_at: Option<DateTime<Utc>>,

    /// When the escalation sweep mentioned somebody about this card.
    ///
    /// Set once and never cleared. It is the claim that makes escalation idempotent: two sweeps
    /// racing over one card produce one mention, and a card that has escalated is invisible to
    /// every sweep after it.
    pub escalated_at: Option<DateTime<Utc>>,

    /// The card this one replaced, when a re-fire started a new episode.
    ///
    /// Nullable and unenforced: the row it names may be pruned long before this one is, and a
    /// missing predecessor means a card without a back-reference rather than one that cannot be
    /// drawn.
    pub supersedes: Option<NotificationId>,

    /// Non-bot messages seen in the thread.
    pub reply_count: u32,

    /// When the row was created.
    pub created_at: DateTime<Utc>,

    /// When the row was last written.
    pub updated_at: DateTime<Utc>,
}

impl Notification {
    /// Whether the card has been posted to Discord yet.
    #[must_use]
    pub fn is_posted(&self) -> bool {
        self.message_id.is_some()
    }

    /// Whether a card rendering to `hash` would differ from what was last posted.
    #[must_use]
    pub fn needs_edit(&self, hash: &str) -> bool {
        self.render_hash.as_deref() != Some(hash)
    }

    /// Whether the post's tags differ from a desired set hashing to `hash`.
    #[must_use]
    pub fn needs_tag_update(&self, hash: &str) -> bool {
        self.tags_hash.as_deref() != Some(hash)
    }
}

/// A card to create, before Discord has given it a message id.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NewNotification {
    /// What the card covers.
    pub dedupe_key: DedupeKey,

    /// The alert it is being created for.
    pub fingerprint: Fingerprint,

    /// The route producing it.
    pub route_id: RouteId,

    /// Guild it belongs to.
    pub guild_id: GuildId,

    /// Channel it will be posted in.
    pub channel_id: ChannelId,

    /// The state it starts in.
    pub state: NotificationState,

    /// The card this one replaces, when a re-fire started a new episode.
    pub supersedes: Option<NotificationId>,

    /// When it was created.
    pub created_at: DateTime<Utc>,
}

/// A request to acknowledge an alert.
///
/// Carries the alert rather than a card, because acknowledging in one channel answers the alert
/// everywhere it is shown. The store returns every card that has to be re-rendered.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AckCommand {
    /// The alert being acknowledged.
    pub fingerprint: Fingerprint,

    /// Who is acknowledging it.
    pub user_id: UserId,

    /// What kind of acknowledgement this is.
    pub kind: AckKind,

    /// An optional note, shown on the card.
    pub note: Option<String>,

    /// Whether this revokes the active acknowledgement instead of creating one.
    pub revoke: bool,

    /// When it happened.
    pub at: DateTime<Utc>,
}

/// What an acknowledgement claims.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum AckKind {
    /// Seen and owned.
    #[default]
    Ack,

    /// Being worked on right now.
    Investigating,

    /// Passed to somebody else.
    Handover,
}

impl AckKind {
    /// The kind as the lowercase word stored in the database.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Ack => "ack",
            Self::Investigating => "investigating",
            Self::Handover => "handover",
        }
    }
}

impl FromStr for AckKind {
    type Err = CoreError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "ack" => Ok(Self::Ack),
            "investigating" => Ok(Self::Investigating),
            "handover" => Ok(Self::Handover),
            other => Err(CoreError::UnknownVariant {
                kind: "acknowledgement kind",
                value: other.to_owned(),
            }),
        }
    }
}

/// What acknowledging produced.
///
/// Computed inside the same transaction as the write, so two people pressing the button at once
/// produce one acknowledgement and one set of edits. The loser gets `changed = false` and can say
/// so instead of posting a second identical card.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AckOutcome {
    /// Whether this call was the one that changed anything.
    pub changed: bool,

    /// Who holds the acknowledgement now, which may be somebody else after a race.
    pub holder: Option<UserId>,

    /// When it was taken.
    pub acknowledged_at: Option<DateTime<Utc>>,

    /// The cards that now need re-rendering.
    pub cards: Vec<Notification>,
}

/// Who holds an alert, as the card shows it.
///
/// Read separately from the cards rather than folded onto them, because the acknowledgement
/// belongs to the alert: taking it in one channel answers it in every channel the alert appears
/// in, and a per-card copy would be one copy per channel to keep in step.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Acknowledgement {
    /// Who took it.
    pub user_id: UserId,

    /// What they claimed.
    pub kind: AckKind,

    /// What they said, if anything.
    pub note: Option<String>,

    /// When they took it.
    pub at: DateTime<Utc>,
}

/// A thread reply attributed to a card.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ThreadReply {
    /// The thread the message arrived in.
    pub thread_id: ChannelId,

    /// Who wrote it. Bot authors are filtered out before this is built.
    pub author_id: UserId,

    /// When it arrived.
    pub at: DateTime<Utc>,
}
