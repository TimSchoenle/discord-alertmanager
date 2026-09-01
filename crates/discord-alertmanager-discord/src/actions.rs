//! The three mutations a person can ask for, written once.
//!
//! A button and a slash command are two ways to say the same thing, and each of these is reachable
//! from both. Writing them twice is how the button ends up acknowledging without re-rendering, or
//! the command ends up silencing without an audit row.
//!
//! Nothing here talks to Discord or to Alertmanager. Each function writes the durable change and
//! queues the effect; the dispatcher makes the call, because that is what survives a restart
//! between the two.

use chrono::{DateTime, Duration, Utc};
use dam_core::{DedupeKey, Fingerprint, MatcherSet};
use dam_store::{
    AckCommand, AckKind, AckOutcome, AlertRecord, Effect, IgnoreId, IgnoreRule, IgnoreScope,
    NewOutboxItem, SilenceEffect, StoreError, UserId,
};

use crate::bot::BotContext;

/// Records an acknowledgement, or revokes one, and queues the cards it changed.
///
/// The state move happens inside the store's transaction, so two people pressing the button at the
/// same moment produce one acknowledgement. The loser is told who holds it rather than posting a
/// second identical card.
///
/// # Errors
///
/// Returns the store's error.
pub(crate) async fn acknowledge(
    bot: &BotContext,
    fingerprint: &Fingerprint,
    user: UserId,
    kind: AckKind,
    note: Option<String>,
    revoke: bool,
) -> Result<AckOutcome, StoreError> {
    let outcome = bot
        .store
        .acknowledge(&AckCommand {
            fingerprint: fingerprint.clone(),
            user_id: user,
            kind,
            note,
            revoke,
            at: Utc::now(),
        })
        .await?;

    // Only when something moved. Re-rendering on a lost race would spend an edit to redraw the
    // card exactly as the winner already drew it.
    if outcome.changed {
        bot.re_render(&outcome.cards).await;
    }

    Ok(outcome)
}

/// Queues an Alertmanager silence.
///
/// Queued rather than sent inline for the reason every other outbound call is: the command has
/// fifteen minutes to answer and Alertmanager may take all of them, and a silence an operator was
/// told about but which never reached Alertmanager is the worst of the available outcomes.
///
/// # Errors
///
/// Returns the store's error.
pub(crate) async fn queue_silence(
    bot: &BotContext,
    request: SilenceRequest<'_>,
) -> Result<DateTime<Utc>, StoreError> {
    let starts_at = Utc::now();
    let ends_at = starts_at + request.duration;

    let effect = Effect::CreateSilence {
        request: SilenceEffect {
            // Absent on the first attempt. A retry sets it from what Alertmanager returned, which
            // is what turns a repeat into an update rather than a second silence.
            am_id: None,
            matchers: request.matchers.to_owned(),
            starts_at,
            ends_at,
            created_by: request.created_by.to_owned(),
            comment: request.comment.to_owned(),
            discord_user_id: Some(request.actor),
            origin_message: request.origin.map(str::to_owned),
        },
    };

    bot.enqueue(&[NewOutboxItem::now(effect, request.key, starts_at)])
        .await?;

    Ok(ends_at)
}

/// Everything a queued silence needs.
///
/// A struct because six of its seven fields are strings, and six positional strings is a call
/// nobody can read at the site.
pub(crate) struct SilenceRequest<'a> {
    /// The matcher expression to silence.
    pub(crate) matchers: &'a str,

    /// How long it lasts.
    pub(crate) duration: Duration,

    /// Who asked, as it should appear in `amtool`.
    pub(crate) created_by: &'a str,

    /// Why, which Alertmanager requires and an operator later reads.
    pub(crate) comment: &'a str,

    /// The Discord user behind it, kept so the link row can be written whole.
    pub(crate) actor: UserId,

    /// Permalink to the card it was created from, when it came from one.
    pub(crate) origin: Option<&'a str>,

    /// The lane the effect belongs to.
    ///
    /// The alert's own key when the silence came from a card, so it is serialised behind that
    /// alert's other effects rather than racing them.
    pub(crate) key: DedupeKey,
}

/// Adds a bot-local ignore rule and republishes the routing snapshot.
///
/// An ignore stops the Discord notification and nothing else: the alert keeps its row, still
/// answers `/alerts list`, and still fires everywhere Alertmanager sends it. That is the whole
/// difference from a silence, and it is why this needs no Alertmanager write access.
///
/// # Errors
///
/// Returns the store's error.
pub(crate) async fn add_ignore(
    bot: &BotContext,
    rule: NewIgnore<'_>,
) -> Result<IgnoreId, StoreError> {
    let now = Utc::now();
    let matchers = MatcherSet::parse(rule.matchers).map_err(|error| StoreError::Decode {
        kind: "matcher expression",
        detail: error.to_string(),
    })?;

    let id = bot
        .store
        .upsert_ignore(&IgnoreRule {
            // Assigned by the database. The value here is what the row is keyed by on the way in
            // and is replaced by the one it comes back with.
            id: IgnoreId::new(0),
            scope: if rule.channel.is_some() {
                IgnoreScope::Channel
            } else {
                IgnoreScope::Guild
            },
            guild_id: rule.guild,
            channel_id: rule.channel,
            matcher_source: rule.matchers.to_owned(),
            matchers,
            reason: rule.reason.to_owned(),
            created_by: rule.actor,
            created_at: now,
            expires_at: rule.expires_at,
            revoked_at: None,
        })
        .await?;

    bot.refresh_routing().await;

    Ok(id)
}

/// Everything a new ignore rule needs.
pub(crate) struct NewIgnore<'a> {
    /// The guild it applies to.
    pub(crate) guild: dam_store::GuildId,

    /// The channel it applies to, or the whole guild when absent.
    pub(crate) channel: Option<dam_store::ChannelId>,

    /// The matcher expression, as written.
    pub(crate) matchers: &'a str,

    /// Why the rule exists. Required, because an unexplained mute outlives whoever set it.
    pub(crate) reason: &'a str,

    /// Who set it.
    pub(crate) actor: UserId,

    /// When it lapses, if it does.
    pub(crate) expires_at: Option<DateTime<Utc>>,
}

/// The lane a silence created from an alert belongs to.
///
/// The stored record rather than the alert, because the key carries the firing episode and only
/// the record knows which one the alert is in. A silence and the card edits it causes then run on
/// one worker instead of two racing over the same card.
pub(crate) fn silence_key(alert: Option<&AlertRecord>) -> DedupeKey {
    match alert {
        Some(record) => DedupeKey::per_alert(&record.alert.fingerprint, record.episode),
        // A silence written from an expression covers no single alert, so it gets a lane of its
        // own rather than borrowing one an alert is already serialised on.
        None => DedupeKey::from_stored("silence"),
    }
}
