//! The hand that carries out what the pipeline decided.
//!
//! Every method here is one Discord call and no judgement. The dispatcher already knows whether a
//! card is being created or edited, whether a thread has to be reopened first, and which tags the
//! post should end up with; this module turns each of those into a request and each failure into
//! one of the handful of outcomes the dispatcher knows how to answer.
//!
//! Two Discord facts are encoded rather than rediscovered. A forum channel accepts no plain
//! message, so a post, its title and its first message are one call. And a forum post's starter
//! message id *is* its thread id, which is why [`dam_engine::PostedMessage::forum`] exists rather
//! than two columns somebody later wonders about.

use std::sync::Arc;

use async_trait::async_trait;
use dam_engine::{
    CardData, CardTarget, DiscordSink, Mention, MessageRef, Note, PostFlags, PostedMessage,
    SinkError, TagSpec,
};
use dam_store::{ChannelId, ForumTag, MessageId, RouteTarget, TagId};
use secrecy::{ExposeSecret, SecretString};
use serenity::all::{
    AutoArchiveDuration, ChannelFlags, ChannelId as SerenityChannelId, CreateAllowedMentions,
    CreateForumPost, CreateForumTag, CreateMessage, CreateThread, EditChannel, EditMessage,
    EditThread, ForumTagId, Http, MessageId as SerenityMessageId, RoleId as SerenityRoleId,
    UserId as SerenityUserId,
};
use tracing::warn;

use crate::error::sink_error;
use crate::render::{Renderer, mention_text};

/// Discord's cap on the tags one forum channel may define.
const CHANNEL_TAG_LIMIT: usize = 20;

/// The point past which the bot stops creating tags of its own.
///
/// Two below the limit, so a channel that fills up leaves room for a person to add the tag they
/// actually wanted. A label with unbounded cardinality would otherwise exhaust the channel in an
/// afternoon and take every future post's tags with it.
const TAG_CREATION_CEILING: usize = 18;

/// Discord's cap on the tags one post may carry.
const POST_TAG_LIMIT: usize = 5;

/// The `DiscordSink` over a real gateway's HTTP client.
pub struct SerenitySink {
    http: Arc<Http>,
    renderer: Arc<Renderer>,
}

impl SerenitySink {
    /// Builds the sink around an HTTP client and the renderer that produces cards.
    #[must_use]
    pub fn new(http: Arc<Http>, renderer: Arc<Renderer>) -> Self {
        Self { http, renderer }
    }

    /// Builds the sink and its own HTTP client from a bot token.
    ///
    /// The composition root has the token and no reason to depend on `serenity`; keeping the one
    /// line that turns a token into a client here is what lets the binary's manifest stay free of
    /// a Discord dependency it would use exactly once.
    #[must_use]
    pub fn from_token(token: &SecretString, renderer: Arc<Renderer>) -> Self {
        Self::new(Arc::new(Http::new(token.expose_secret())), renderer)
    }

    /// Builds the message one card becomes.
    ///
    /// `mention` is what separates a first post from an edit. Discord does not notify anyone for a
    /// mention inside an embed, so the mentions live in the message body, and the allowed-mentions
    /// list is set from what the card actually carries — an empty one on an edit, which is what
    /// stops a re-render from paging the on-call again.
    fn message(&self, card: &CardData) -> CreateMessage {
        let rendered = self.renderer.render(card);
        let mut message = CreateMessage::new()
            .embed(rendered.embed)
            .components(rendered.components)
            .allowed_mentions(allowed(card));

        if !rendered.content.is_empty() {
            message = message.content(rendered.content);
        }

        message
    }
}

/// Which mentions Discord is permitted to resolve for this message.
///
/// Explicit rather than default: without it a label value containing `@everyone` would notify a
/// guild, and label values come from metric targets.
fn allowed(card: &CardData) -> CreateAllowedMentions {
    let mut roles = Vec::new();
    let mut users = Vec::new();

    for mention in &card.mentions {
        match mention {
            dam_engine::Mention::Role(role) => roles.push(serenity::all::RoleId::new(role.get())),
            dam_engine::Mention::User(user) => users.push(SerenityUserId::new(user.get())),
        }
    }

    CreateAllowedMentions::new()
        .everyone(false)
        .roles(roles)
        .users(users)
}

/// The channel a card is posted into.
fn channel(id: ChannelId) -> SerenityChannelId {
    SerenityChannelId::new(id.get())
}

/// Discord accepts four auto-archive durations and rejects everything else.
///
/// Rounded down to the nearest legal value rather than refused, because the number comes from a
/// configuration file and a rejected request would cost a notification.
fn auto_archive(minutes: u32) -> AutoArchiveDuration {
    match minutes {
        0..=60 => AutoArchiveDuration::OneHour,
        61..=1_440 => AutoArchiveDuration::OneDay,
        1_441..=4_320 => AutoArchiveDuration::ThreeDays,
        _ => AutoArchiveDuration::OneWeek,
    }
}

/// The tag ids a post may carry, deduplicated and capped.
///
/// Discord does not deduplicate applied tag ids and rejects a sixth, so both happen here rather
/// than in the caller that happens to notice first.
fn tag_ids(tags: &[TagId]) -> Vec<ForumTagId> {
    let mut seen = Vec::new();

    for tag in tags {
        let id = ForumTagId::new(tag.get());
        if !seen.contains(&id) {
            seen.push(id);
        }
        if seen.len() == POST_TAG_LIMIT {
            break;
        }
    }

    seen
}

#[async_trait]
impl DiscordSink for SerenitySink {
    async fn post_card(
        &self,
        target: &CardTarget,
        card: &CardData,
    ) -> Result<PostedMessage, SinkError> {
        let message = self.message(card);

        let posted = match &target.target {
            RouteTarget::Text { channel: id, .. } => channel(*id)
                .send_message(&self.http, message)
                .await
                .map_err(|error| sink_error(&error, "text"))?,
            RouteTarget::Thread { thread } => channel(*thread)
                .send_message(&self.http, message)
                .await
                .map_err(|error| sink_error(&error, "thread"))?,
            RouteTarget::Dm { user } => {
                let dm = SerenityUserId::new(user.get())
                    .create_dm_channel(&self.http)
                    .await
                    .map_err(|error| sink_error(&error, "direct message"))?;

                dm.id
                    .send_message(&self.http, message)
                    .await
                    .map_err(|error| sink_error(&error, "direct message"))?
            }
            RouteTarget::Forum { .. } => {
                return Err(SinkError::WrongChannelType { expected: "text" });
            }
        };

        Ok(PostedMessage::plain(MessageId::new(posted.id.get())))
    }

    async fn create_forum_post(
        &self,
        target: &CardTarget,
        card: &CardData,
    ) -> Result<PostedMessage, SinkError> {
        let RouteTarget::Forum {
            channel: forum,
            policy,
        } = &target.target
        else {
            return Err(SinkError::WrongChannelType { expected: "forum" });
        };

        let mut post = CreateForumPost::new(target.title.clone(), self.message(card))
            .auto_archive_duration(auto_archive(policy.auto_archive_minutes));

        for tag in tag_ids(&target.tags) {
            post = post.add_applied_tag(tag);
        }

        let created = channel(*forum)
            .create_forum_post(&self.http, post)
            .await
            .map_err(|error| sink_error(&error, "forum"))?;

        // The starter message and the thread are one id. Saying so through the constructor keeps
        // it from becoming two columns whose equality nobody explains later.
        Ok(PostedMessage::forum(ChannelId::new(created.id.get())))
    }

    async fn edit_card(&self, message: &MessageRef, card: &CardData) -> Result<(), SinkError> {
        let rendered = self.renderer.render(card);

        channel(message.channel)
            .edit_message(
                &self.http,
                SerenityMessageId::new(message.message.get()),
                EditMessage::new()
                    .embed(rendered.embed)
                    .components(rendered.components),
            )
            .await
            .map(|_| ())
            .map_err(|error| sink_error(&error, "text"))
    }

    async fn open_thread(&self, message: &MessageRef, name: &str) -> Result<ChannelId, SinkError> {
        let thread = channel(message.channel)
            .create_thread_from_message(
                &self.http,
                SerenityMessageId::new(message.message.get()),
                CreateThread::new(name),
            )
            .await
            .map_err(|error| sink_error(&error, "text"))?;

        Ok(ChannelId::new(thread.id.get()))
    }

    async fn post_thread_note(&self, thread: ChannelId, note: &Note) -> Result<(), SinkError> {
        channel(thread)
            .send_message(
                &self.http,
                CreateMessage::new()
                    .content(note.text.clone())
                    // A note is a timeline entry and a way to resurface a forum post, never a
                    // second page.
                    .allowed_mentions(CreateAllowedMentions::new().empty_roles().empty_users()),
            )
            .await
            .map(|_| ())
            .map_err(|error| sink_error(&error, "thread"))
    }

    async fn post_escalation(
        &self,
        target: ChannelId,
        mentions: &[Mention],
        text: &str,
    ) -> Result<(), SinkError> {
        let prefix = mention_text(mentions);
        let content = if prefix.is_empty() {
            text.to_owned()
        } else {
            format!("{prefix} {text}")
        };

        // The allowlist is built from the ids the escalation was given rather than left open.
        // Everything a card carries — labels, annotations, a silence comment — is text somebody
        // else wrote, and an open allowlist would let any of it ping a role it names.
        let mut allowed = CreateAllowedMentions::new().empty_roles().empty_users();
        for mention in mentions {
            allowed = match mention {
                Mention::Role(role) => allowed.roles(vec![SerenityRoleId::new(role.get())]),
                Mention::User(user) => allowed.users(vec![SerenityUserId::new(user.get())]),
            };
        }

        channel(target)
            .send_message(
                &self.http,
                CreateMessage::new()
                    .content(content)
                    .allowed_mentions(allowed),
            )
            .await
            .map(|_| ())
            .map_err(|error| sink_error(&error, "thread"))
    }

    async fn disable_components(&self, message: &MessageRef) -> Result<(), SinkError> {
        channel(message.channel)
            .edit_message(
                &self.http,
                SerenityMessageId::new(message.message.get()),
                EditMessage::new().components(Vec::new()),
            )
            .await
            .map(|_| ())
            .map_err(|error| sink_error(&error, "text"))
    }

    async fn set_post_tags(&self, thread: ChannelId, tags: &[TagId]) -> Result<(), SinkError> {
        channel(thread)
            .edit_thread(&self.http, EditThread::new().applied_tags(tag_ids(tags)))
            .await
            .map(|_| ())
            .map_err(|error| sink_error(&error, "forum"))
    }

    async fn set_post_flags(&self, thread: ChannelId, flags: PostFlags) -> Result<(), SinkError> {
        channel(thread)
            .edit_thread(
                &self.http,
                EditThread::new()
                    .archived(flags.archived)
                    .locked(flags.locked)
                    .auto_archive_duration(auto_archive(flags.auto_archive_minutes)),
            )
            .await
            .map(|_| ())
            .map_err(|error| sink_error(&error, "forum"))
    }

    async fn set_post_pinned(&self, thread: ChannelId, pinned: bool) -> Result<(), SinkError> {
        let flags = if pinned {
            ChannelFlags::PINNED
        } else {
            ChannelFlags::empty()
        };

        channel(thread)
            .edit(&self.http, EditChannel::new().flags(flags))
            .await
            .map(|_| ())
            .map_err(|error| sink_error(&error, "forum"))
    }

    async fn forum_tags(&self, forum: ChannelId) -> Result<Vec<ForumTag>, SinkError> {
        let read = channel(forum)
            .to_channel(&self.http)
            .await
            .map_err(|error| sink_error(&error, "forum"))?;

        let Some(guild) = read.guild() else {
            return Err(SinkError::WrongChannelType { expected: "forum" });
        };

        Ok(guild
            .available_tags
            .iter()
            .map(|tag| ForumTag {
                channel_id: forum,
                name: tag.name.clone(),
                id: TagId::new(tag.id.get()),
                moderated: tag.moderated,
                synced_at: chrono::Utc::now(),
            })
            .collect())
    }

    async fn ensure_forum_tags(
        &self,
        forum: ChannelId,
        want: &[TagSpec],
    ) -> Result<Vec<ForumTag>, SinkError> {
        let existing = self.forum_tags(forum).await?;
        let missing: Vec<&TagSpec> = want
            .iter()
            .filter(|spec| !existing.iter().any(|tag| tag.name == spec.name))
            .collect();

        if missing.is_empty() {
            return Ok(existing);
        }

        if existing.len() >= TAG_CREATION_CEILING {
            // Reported once, and never at the cost of a notification: the caller applies the tags
            // that do resolve and `/route test` is where the gap is meant to be noticed.
            warn!(
                forum = forum.get(),
                existing = existing.len(),
                "refusing to create more forum tags; the channel is near Discord's limit"
            );
            return Ok(existing);
        }

        let mut tags: Vec<CreateForumTag> = existing
            .iter()
            .map(|tag| CreateForumTag::new(tag.name.clone()).moderated(tag.moderated))
            .collect();

        for spec in missing {
            if tags.len() >= CHANNEL_TAG_LIMIT {
                break;
            }
            // Non-moderated deliberately: a moderated tag can only be applied by a member holding
            // MANAGE_THREADS, while a non-moderated one can be set by the thread's owner, which
            // the bot is.
            tags.push(CreateForumTag::new(spec.name.clone()).moderated(false));
        }

        match channel(forum)
            .edit(&self.http, EditChannel::new().available_tags(tags))
            .await
        {
            Ok(_) => self.forum_tags(forum).await,
            // Degrading rather than failing: a missing tag is a worse reason to lose a
            // notification than any tag is a reason to have one.
            Err(error) => {
                warn!(
                    forum = forum.get(),
                    %error,
                    "cannot create forum tags; applying the ones that already resolve"
                );
                Ok(existing)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn applied_tags_are_deduplicated_and_capped() {
        let tags: Vec<TagId> = [1, 1, 2, 3, 4, 5, 6, 7]
            .into_iter()
            .map(TagId::new)
            .collect();

        let ids = tag_ids(&tags);

        assert_eq!(
            ids.len(),
            POST_TAG_LIMIT,
            "Discord rejects a sixth tag and does not deduplicate the first five itself"
        );
        assert_eq!(ids[0], ForumTagId::new(1));
        assert_eq!(ids[1], ForumTagId::new(2));
    }

    #[test]
    fn an_illegal_archive_duration_is_rounded_rather_than_rejected() {
        assert_eq!(auto_archive(0), AutoArchiveDuration::OneHour);
        assert_eq!(auto_archive(90), AutoArchiveDuration::OneDay);
        assert_eq!(auto_archive(10_080), AutoArchiveDuration::OneWeek);
        assert_eq!(auto_archive(99_999), AutoArchiveDuration::OneWeek);
    }
}
