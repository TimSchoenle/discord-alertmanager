//! The durable side-effect queue, and what completing one of its items records.

use chrono::{DateTime, Utc};
use dam_core::DedupeKey;
use serde::{Deserialize, Serialize};

use crate::ids::{ChannelId, MessageId, NotificationId, OutboxId, RoleId, TagId, UserId, WorkerId};

/// One pending effect on Discord or Alertmanager.
///
/// The queue is what makes those effects restart-safe. A crash between deciding to post a card
/// and posting it leaves a claimable row rather than a notification nobody ever sees, and a
/// Discord rate-limit stall becomes a delayed row rather than a webhook timing out.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OutboxItem {
    /// Primary key, and the claim order.
    pub id: OutboxId,

    /// The worker lane this item belongs to.
    ///
    /// Derived from the dedupe key, so every effect for one alert lands on one worker and two
    /// workers never edit one card at the same time. Coalescing then becomes a local operation
    /// rather than one needing a lock.
    pub lane: u16,

    /// What to do.
    pub effect: Effect,

    /// What the effect is about, for coalescing and for the lane.
    pub dedupe_key: DedupeKey,

    /// Earliest time the item may be claimed.
    ///
    /// Carries both the debounce on card edits and the backoff after a failure.
    pub not_before: DateTime<Utc>,

    /// How many times it has been attempted.
    pub attempts: u32,

    /// The worker currently holding it.
    pub claimed_by: Option<WorkerId>,

    /// When the current claim was taken.
    pub claimed_at: Option<DateTime<Utc>>,

    /// Why the last attempt failed.
    pub last_error: Option<String>,

    /// When the item was enqueued.
    pub created_at: DateTime<Utc>,
}

/// An effect to enqueue.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NewOutboxItem {
    /// What to do.
    pub effect: Effect,

    /// What the effect is about.
    pub dedupe_key: DedupeKey,

    /// Earliest time it may run.
    pub not_before: DateTime<Utc>,
}

impl NewOutboxItem {
    /// An effect that may run as soon as a worker picks it up.
    #[must_use]
    pub fn now(effect: Effect, dedupe_key: DedupeKey, at: DateTime<Utc>) -> Self {
        Self {
            effect,
            dedupe_key,
            not_before: at,
        }
    }
}

/// Everything the dispatcher can be asked to do.
///
/// Each variant is one API call. Unarchiving before an edit is its own variant rather than a
/// retry after the failure, because an archived thread rejecting an edit is the normal path for a
/// resolved alert that re-fires, not an exceptional one.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Effect {
    /// Post a card for a notification that has no message yet.
    PostCard {
        /// The card to post.
        notification: NotificationId,
        /// Whether this post mentions the route's roles and users.
        ///
        /// Only ever true for a transition into firing. Re-mentioning on every update is the
        /// fastest way to get the bot muted by the people it exists to reach.
        mention: bool,
    },

    /// Re-render an existing card.
    EditCard {
        /// The card to edit.
        notification: NotificationId,
    },

    /// Open the thread a route's policy asks for.
    OpenThread {
        /// The card the thread hangs off.
        notification: NotificationId,
        /// The thread's name.
        name: String,
    },

    /// Post a one-line note in a card's thread.
    ///
    /// Also what resurfaces a forum post: editing a message does not bump a forum's activity
    /// sort, so a state change that only edits the card leaves the post buried.
    ThreadNote {
        /// The card whose thread to write in.
        notification: NotificationId,
        /// The line to post.
        text: String,
    },

    /// Replace the tags on a forum post.
    SetTags {
        /// The post to change.
        notification: NotificationId,
        /// The desired tag set, already truncated to Discord's five.
        tags: Vec<TagId>,
    },

    /// Change a forum post's archive, lock and auto-archive flags.
    SetFlags {
        /// The post to change.
        notification: NotificationId,
        /// Whether it should be archived.
        archived: bool,
        /// Whether it should be locked.
        locked: bool,
        /// Minutes of inactivity before Discord archives it.
        auto_archive_minutes: u32,
    },

    /// Pin or unpin a post.
    SetPinned {
        /// The post to change.
        notification: NotificationId,
        /// Whether it should be pinned.
        pinned: bool,
    },

    /// Strip or disable a resolved card's components.
    DisableComponents {
        /// The card to change.
        notification: NotificationId,
    },

    /// Create or replace an Alertmanager silence.
    CreateSilence {
        /// The silence request, held as written so a retry re-sends the same thing.
        request: SilenceEffect,
    },

    /// Expire an Alertmanager silence.
    ExpireSilence {
        /// The silence to expire.
        am_id: String,
    },

    /// Mention a route's escalation targets about a card nobody has taken.
    ///
    /// Carries who to mention rather than the route to read them from, so the note that is sent
    /// is the policy as it stood when the sweep decided, not as it stands whenever a retry
    /// happens to run.
    Escalate {
        /// The card that has gone unanswered.
        notification: NotificationId,
        /// Roles to mention.
        roles: Vec<RoleId>,
        /// Users to mention.
        users: Vec<UserId>,
    },

    /// Post a line into the administrative channel.
    ///
    /// The one effect naming a channel instead of a card. The deadman and the route-health
    /// notices are about the bot rather than about any alert, and by the time either fires there
    /// may be no card to hang it off — a route the bot cannot post to being the ordinary case.
    AdminNotice {
        /// The channel to write in.
        channel: ChannelId,
        /// The line to post.
        text: String,
    },
}

impl Effect {
    /// The card this effect acts on, when it acts on one.
    #[must_use]
    pub fn notification(&self) -> Option<NotificationId> {
        match self {
            Self::PostCard { notification, .. }
            | Self::EditCard { notification }
            | Self::OpenThread { notification, .. }
            | Self::ThreadNote { notification, .. }
            | Self::SetTags { notification, .. }
            | Self::SetFlags { notification, .. }
            | Self::SetPinned { notification, .. }
            | Self::DisableComponents { notification }
            | Self::Escalate { notification, .. } => Some(*notification),
            Self::CreateSilence { .. } | Self::ExpireSilence { .. } | Self::AdminNotice { .. } => {
                None
            }
        }
    }

    /// The discriminant as the lowercase word stored in the database.
    #[must_use]
    pub fn kind(&self) -> &'static str {
        match self {
            Self::PostCard { .. } => "post_card",
            Self::EditCard { .. } => "edit_card",
            Self::OpenThread { .. } => "open_thread",
            Self::ThreadNote { .. } => "thread_note",
            Self::SetTags { .. } => "set_tags",
            Self::SetFlags { .. } => "set_flags",
            Self::SetPinned { .. } => "set_pinned",
            Self::DisableComponents { .. } => "disable_components",
            Self::CreateSilence { .. } => "create_silence",
            Self::ExpireSilence { .. } => "expire_silence",
            Self::Escalate { .. } => "escalate",
            Self::AdminNotice { .. } => "admin_notice",
        }
    }

    /// Whether a later item of this kind supersedes an earlier one *for the same card*.
    ///
    /// Two queued edits of one card are one edit of its current state; two queued notes are two
    /// different sentences. Coalescing the first pair is what keeps a storm inside Discord's edit
    /// limits, and coalescing the second would lose a line of the timeline.
    ///
    /// The card, and never the dedupe key, is the scope. One alert fans out to every route that
    /// matches it, and each of those cards is keyed under the same per-alert key in a channel of
    /// its own; folding on the key alone would let the edit for one card overwrite the queued
    /// edit for another and leave that card frozen at whatever it last rendered. Every
    /// coalescable variant therefore names a notification, and the store folds on that.
    #[must_use]
    pub fn is_coalescable(&self) -> bool {
        matches!(self, Self::EditCard { .. } | Self::SetTags { .. })
    }
}

/// A silence request as it sits in the queue.
///
/// Flattened to strings and timestamps rather than holding a compiled matcher set, because the
/// row has to survive a restart and a compiled regex does not serialise.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SilenceEffect {
    /// The silence to replace, set on a retry so Alertmanager updates rather than duplicates.
    pub am_id: Option<String>,

    /// The matcher expression to silence.
    pub matchers: String,

    /// When the silence starts.
    pub starts_at: DateTime<Utc>,

    /// When it expires.
    pub ends_at: DateTime<Utc>,

    /// Who asked for it, as it should appear in `amtool`.
    pub created_by: String,

    /// Why, including a permalink to the card it came from.
    pub comment: String,

    /// The Discord user behind it, kept so the completed effect can write the link row whole.
    pub discord_user_id: Option<UserId>,

    /// Permalink to the card the silence was created from.
    pub origin_message: Option<String>,
}

/// What a completed effect changed, to be written back in the same transaction that clears the
/// item.
///
/// The dispatcher never writes these fields directly. An effect that succeeded and a row that
/// records it have to move together, or a crash between them leaves a card the bot will post a
/// second time.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AppliedEffect {
    /// The message Discord created.
    pub message_id: Option<MessageId>,

    /// The thread or forum post Discord created.
    pub thread_id: Option<ChannelId>,

    /// Hash of what was rendered, so the next identical render is skipped.
    pub render_hash: Option<String>,

    /// The tags now on the post, and their hash.
    pub applied_tags: Option<Vec<TagId>>,

    /// Hash of the applied tag set.
    pub tags_hash: Option<String>,

    /// Whether the post is now pinned.
    pub pinned: Option<bool>,

    /// Whether the post is now archived.
    pub archived: Option<bool>,

    /// The silence Alertmanager created or replaced.
    pub am_silence_id: Option<String>,
}

/// How many lanes a dedupe key is hashed into.
///
/// Fixed rather than set to the worker count, so that changing the number of dispatchers
/// redistributes the lanes instead of stranding the rows written under the old count. A worker
/// takes every lane congruent to its index, and any divisor of the space covers all of it.
pub const OUTBOX_LANES: u16 = 1024;

/// Which slice of the lane space one worker claims from.
///
/// Every effect for one alert hashes to one lane, so a lane belongs to exactly one worker and two
/// workers never edit one card at the same time. Coalescing then happens inside a single worker
/// rather than across a lock.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct LaneAssignment {
    /// This worker's index, from zero.
    pub index: u16,

    /// How many workers are sharing the lane space.
    pub of: u16,
}

impl LaneAssignment {
    /// The assignment for worker `index` of `of`.
    ///
    /// # Panics
    ///
    /// Never. `of` is clamped to at least one, because a zero divisor would take the whole queue
    /// out of service rather than fail visibly.
    #[must_use]
    pub fn new(index: u16, of: u16) -> Self {
        let of = of.max(1);

        Self {
            index: index % of,
            of,
        }
    }
}

/// How long a worker holds a claim, and how many items it takes at once.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClaimRequest {
    /// The slice of the lane space this worker owns.
    ///
    /// `None` claims from any lane, which is what a single-worker deployment and the janitor
    /// both want.
    pub lane: Option<LaneAssignment>,

    /// How long the claim is good for before a janitor may reclaim it.
    pub lease_secs: u32,

    /// Most items to take in one call.
    pub limit: u32,
}
