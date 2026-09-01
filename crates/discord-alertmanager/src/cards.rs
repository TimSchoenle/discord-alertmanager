//! Reading a card back out of the database, whole, so the sink can draw it.
//!
//! The dispatcher claims an outbox row carrying a notification id and nothing else. Everything a
//! card shows — the alert, who took it, which silence is suppressing it, which ignore rule is
//! muting it, what the route is called — lives in four different places, and this is where they
//! are joined.
//!
//! The alert comes from the local table rather than from Alertmanager, and that is the point.
//! Alertmanager garbage-collects a resolved alert within half an hour; a card outlives that by a
//! long way, and an outbox item that resolved its own alert over the network would also mean an
//! Alertmanager outage silently stopped the notifications about the Alertmanager outage.

use std::sync::Arc;

use chrono::Utc;
use dam_discord::Renderer;
use dam_engine::{
    CardData, CardTarget, DigestNotice, Mention, PreviousCard, SharedRouting, SharedStorm,
    SilenceSummary, desired_tags,
};
use dam_store::{Notification, NotificationId, Route, RouteTarget, Store, StoreError};
use tracing::warn;

/// A card and where it goes.
pub(crate) struct Assembled {
    /// Where the card is posted, and with which tags.
    pub(crate) target: CardTarget,

    /// What it shows.
    pub(crate) data: CardData,

    /// The row it was built from.
    pub(crate) card: Notification,
}

/// Joins the tables a card is drawn from.
pub(crate) struct Cards {
    store: Arc<dyn Store>,
    routing: Arc<SharedRouting>,
    storm: Arc<SharedStorm>,
    renderer: Arc<Renderer>,
}

impl Cards {
    /// Builds an assembler over the store, the current routing snapshot and the renderer.
    ///
    /// The renderer is here for one reason: a forum post and a thread both need a name, the name
    /// comes from the same template and fallback chain the card's title does, and computing it in
    /// two places is how the two drift apart.
    pub(crate) fn new(
        store: Arc<dyn Store>,
        routing: Arc<SharedRouting>,
        storm_state: Arc<SharedStorm>,
        renderer: Arc<Renderer>,
    ) -> Self {
        Self {
            store,
            routing,
            storm: storm_state,
            renderer,
        }
    }

    /// Reads everything one card needs.
    ///
    /// `None` means the card cannot be drawn at all — the row is gone, its route was removed, or
    /// the alert it showed has been pruned. Each of those is a reason to drop the effect rather
    /// than to retry it, because none of them will be different in thirty seconds.
    pub(crate) async fn assemble(
        &self,
        id: NotificationId,
        mention: bool,
    ) -> Result<Option<Assembled>, StoreError> {
        let Some(card) = self.store.notification(id).await? else {
            return Ok(None);
        };

        let snapshot = self.routing.load();
        let Some(route) = snapshot.route(card.route_id) else {
            warn!(notification = %id, "the route that produced this card is gone");
            return Ok(None);
        };

        let Some(record) = self.store.alert(&card.fingerprint).await? else {
            warn!(notification = %id, fingerprint = %card.fingerprint, "the alert is gone");
            return Ok(None);
        };

        // Acknowledgement belongs to the alert rather than to one card: taking it in one
        // channel answers the alert in every channel it appears in, so it is read once here and
        // not copied onto each row.
        let held = self
            .store
            .acknowledgement(&card.fingerprint)
            .await
            .unwrap_or_else(|error| {
                warn!(%error, "cannot read the acknowledgement");
                None
            });

        let silence = self.silence_for(&record.alert.silenced_by).await;
        let ignore = snapshot
            .ignore_for(
                card.guild_id,
                card.channel_id,
                &record.alert.labels,
                Utc::now(),
            )
            .map(|rule| rule.reason.clone());

        let data = CardData {
            notification: id,
            state: card.state,
            route_name: route.name.clone(),
            acknowledged_by: held.as_ref().map(|held| held.user_id),
            acknowledged_at: held.as_ref().map(|held| held.at),
            reply_count: card.reply_count,
            flap_count: record.flap_count,
            first_seen_at: record.first_seen_at,
            silence,
            ignore_reason: ignore,
            // Only ever on the way into firing, and only when the caller says so: a re-render
            // that mentioned the on-call again is how a bot gets muted by the people it exists to
            // reach.
            mentions: if mention {
                mentions_for(route)
            } else {
                Vec::new()
            },
            digest: self.digest_notice(route),
            previous: self.previous_card(card.supersedes).await,
            rendered_at: Utc::now(),
            alert: record.alert,
        };

        let target = CardTarget {
            guild: card.guild_id,
            tags: match &route.target {
                RouteTarget::Forum { channel, policy } => desired_tags(
                    &snapshot,
                    *channel,
                    policy,
                    card.state,
                    data.severity(),
                    &data.alert.labels,
                ),
                _ => Vec::new(),
            },
            target: route.target.clone(),
            title: self.renderer.name(&data),
        };

        Ok(Some(Assembled { target, data, card }))
    }

    /// Why this card is a digest, when its route is over its threshold.
    ///
    /// Read at render time rather than stored on the row, because a route leaves digest mode as
    /// soon as its rate drops and a card that kept saying otherwise would be explaining a
    /// condition that had passed.
    fn digest_notice(&self, route: &Route) -> Option<DigestNotice> {
        let storm = self.storm.load();

        storm.is_storming(route).then(|| DigestNotice {
            cards: storm.count(route.id),
            threshold: storm.threshold_for(route),
            window_secs: storm.window().num_seconds(),
        })
    }

    /// Where the card this one replaced can be found, when it replaced one.
    ///
    /// A predecessor that was never posted, or has since been pruned, is no link rather than a
    /// failure: the point of the reference is the history behind it, and a card that cannot be
    /// drawn is not history anybody can read.
    async fn previous_card(&self, id: Option<NotificationId>) -> Option<PreviousCard> {
        let card = self.store.notification(id?).await.ok()??;

        Some(PreviousCard {
            guild: card.guild_id,
            channel: card.thread_id.unwrap_or(card.channel_id),
            message: card.message_id?,
        })
    }

    /// The silence a card should name, out of the ids Alertmanager reported against the alert.
    ///
    /// The first one the bot has a row for. An alert can be covered by several silences and the
    /// card has room for one; the one the bot created is the one an operator is most likely to be
    /// looking for, and it is the only one the bot has an expiry and an author for anyway.
    async fn silence_for(&self, ids: &[String]) -> Option<SilenceSummary> {
        if ids.is_empty() {
            return None;
        }

        let links = self.store.silences(false).await.ok()?;

        links
            .into_iter()
            .find(|link| ids.contains(&link.am_id))
            .map(|link| SilenceSummary {
                am_id: link.am_id,
                ends_at: link.ends_at,
                created_by: link.created_by,
            })
    }
}

/// Everyone a route mentions on a first firing post.
fn mentions_for(route: &Route) -> Vec<Mention> {
    route
        .mentions
        .roles
        .iter()
        .map(|role| Mention::Role(*role))
        .chain(route.mentions.users.iter().map(|user| Mention::User(*user)))
        .collect()
}
