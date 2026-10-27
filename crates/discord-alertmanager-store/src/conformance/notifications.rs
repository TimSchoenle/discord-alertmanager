//! Cases for cards: decisions, replacements, acknowledgements, replies, escalations and resync.

use dam_core::{AlertStatus, DedupeKey, Fingerprint, NotificationState};

use crate::ids::{ChannelId, GuildId, MessageId, UserId, WorkerId};
use crate::notifications::{AckCommand, AckKind, NewNotification, ThreadReply};
use crate::outbox::{AppliedEffect, ClaimRequest, Effect};
use crate::{CardUpdate, Decision, PlannedCard, Store, StoreError};

use super::{alert, at, card_for, drain, ingest, record_post, route_for};

/// A resync sees the live cards on the routes it was given and on no others.
pub(super) async fn a_resync_reads_the_live_cards_of_its_own_routes(store: &dyn Store) {
    let route = route_for(store, "resync", 130).await;
    let other = route_for(store, "resync-other", 131).await;

    let live = DedupeKey::from_stored("a:resync-live");
    let queued = DedupeKey::from_stored("a:resync-queued");
    let settled = DedupeKey::from_stored("a:resync-settled");
    let foreign = DedupeKey::from_stored("a:resync-foreign");

    let live_id = card_for(store, route, 130, &live).await;
    let queued_id = card_for(store, route, 130, &queued).await;
    let settled_id = card_for(store, route, 130, &settled).await;
    let foreign_id = card_for(store, other, 131, &foreign).await;

    record_post(store, "resync-setup", &live, MessageId::new(9_130)).await;
    record_post(store, "resync-setup", &settled, MessageId::new(9_131)).await;
    record_post(store, "resync-setup", &foreign, MessageId::new(9_132)).await;

    store
        .set_notification_state(settled_id, NotificationState::Resolved, at(10))
        .await
        .expect("the card resolves");

    let seen: Vec<crate::NotificationId> = store
        .live_cards(&[route], 10)
        .await
        .expect("the read succeeds")
        .iter()
        .map(|card| card.id)
        .collect();

    assert!(
        seen.contains(&live_id),
        "a posted card that is still open is what a resync exists to repair"
    );
    assert!(
        !seen.contains(&queued_id),
        "a card with no message is not out of step with anything yet"
    );
    assert!(
        !seen.contains(&settled_id),
        "a resolved post is history, and reopening it to redraw it would say nothing new"
    );
    assert!(
        !seen.contains(&foreign_id),
        "another route's cards are not this run's to repair"
    );

    assert_eq!(
        store
            .live_cards(&[route], 10)
            .await
            .expect("the read succeeds")
            .len(),
        1,
        "the answer is the live set and nothing beside it"
    );

    assert!(
        store
            .live_cards(&[], 10)
            .await
            .expect("the read succeeds")
            .is_empty(),
        "no route owns no card, rather than every card"
    );

    drain(store, "resync-cleanup").await;
}

/// A card that replaces another takes its key, and the replaced card keeps its row and history.
pub(super) async fn a_replacement_card_takes_the_key_of_the_card_it_replaced(store: &dyn Store) {
    let route = route_for(store, "supersedes", 120).await;
    let key = DedupeKey::from_stored("a:super");
    let first = card_for(store, route, 120, &key).await;

    store
        .set_notification_state(first, NotificationState::Resolved, at(5))
        .await
        .expect("the first card resolves");

    let planned = |supersedes| Decision {
        new_cards: vec![PlannedCard {
            card: NewNotification {
                dedupe_key: key.clone(),
                fingerprint: Fingerprint::new("aaaa0001").expect("the fingerprint is hexadecimal"),
                route_id: route,
                guild_id: GuildId::new(1),
                channel_id: ChannelId::new(120),
                state: NotificationState::Firing,
                supersedes,
                created_at: at(10),
            },
            mention: true,
            not_before: at(10),
        }],
        updates: Vec::new(),
        at: at(10),
    };

    let created = store
        .apply_decision(&planned(Some(first)))
        .await
        .expect("the key is handed over rather than refused");
    let second = *created.first().expect("one card was created");

    let current = store
        .notification_for(&key, ChannelId::new(120))
        .await
        .expect("the read succeeds")
        .expect("the key names a card");

    assert_eq!(current.id, second, "the key names the replacement now");
    assert_eq!(
        current.supersedes,
        Some(first),
        "without this the new card has no history and the card that holds it is buried"
    );

    let retired = store
        .notification(first)
        .await
        .expect("the read succeeds")
        .expect("the replaced card keeps its row");

    assert_ne!(retired.dedupe_key, key, "the replaced card gave its key up");
    assert_eq!(retired.state, NotificationState::Resolved);

    // A second replacement racing the first names a card that no longer holds the key. It must
    // meet the unique index rather than retire the winner.
    assert!(
        matches!(
            store.apply_decision(&planned(Some(first))).await,
            Err(StoreError::Conflict { .. })
        ),
        "the loser of a race is told so and re-reads"
    );

    drain(store, "supersedes").await;
}

/// Resolving a card records when, and re-arming it forgets.
pub(super) async fn a_resolved_card_remembers_when_until_it_re_arms(store: &dyn Store) {
    let route = route_for(store, "resolved-at", 122).await;
    let key = DedupeKey::from_stored("a:resolved-at");
    let id = card_for(store, route, 122, &key).await;

    let read = || async {
        store
            .notification(id)
            .await
            .expect("the read succeeds")
            .expect("the row exists")
    };

    assert_eq!(
        read().await.resolved_at,
        None,
        "a firing card has not resolved"
    );

    store
        .apply_decision(&Decision {
            new_cards: Vec::new(),
            updates: vec![CardUpdate {
                id,
                fingerprint: Fingerprint::new("aaaa0001").expect("the fingerprint is hexadecimal"),
                state: Some(NotificationState::Resolved),
                effects: Vec::new(),
            }],
            at: at(20),
        })
        .await
        .expect("the update applies");

    assert_eq!(read().await.resolved_at, Some(at(20)));

    store
        .set_notification_state(id, NotificationState::Firing, at(30))
        .await
        .expect("the card re-arms");

    assert_eq!(
        read().await.resolved_at,
        None,
        "a re-armed card is not resolved since anything"
    );

    drain(store, "resolved-at").await;
}

/// Acknowledging reaches a card by the alert it shows, whatever the card is keyed by.
///
/// A card whose identity merges fingerprints is keyed by none of them, so a lookup by key alone
/// would acknowledge the alert and leave its card saying nobody had.
pub(super) async fn an_acknowledgement_reaches_every_card_showing_the_alert(store: &dyn Store) {
    let fingerprint = Fingerprint::new("aaaa0050").expect("the fingerprint is hexadecimal");
    ingest(
        store,
        alert("aaaa0050", AlertStatus::Firing, "critical"),
        at(0),
    )
    .await;

    let route = route_for(store, "ack-identity", 123).await;
    let created = store
        .apply_decision(&Decision {
            new_cards: vec![PlannedCard {
                card: NewNotification {
                    dedupe_key: DedupeKey::from_stored("a:0123456789abcdef"),
                    fingerprint: fingerprint.clone(),
                    route_id: route,
                    guild_id: GuildId::new(1),
                    channel_id: ChannelId::new(123),
                    state: NotificationState::Firing,
                    supersedes: None,
                    created_at: at(0),
                },
                mention: false,
                not_before: at(0),
            }],
            updates: Vec::new(),
            at: at(0),
        })
        .await
        .expect("the decision applies");
    let id = *created.first().expect("one card was created");

    let outcome = store
        .acknowledge(&AckCommand {
            fingerprint,
            user_id: UserId::new(1),
            kind: AckKind::Ack,
            note: None,
            revoke: false,
            at: at(10),
        })
        .await
        .expect("the acknowledgement is recorded");

    let card = outcome
        .cards
        .iter()
        .find(|card| card.id == id)
        .expect("the card showing the alert is among the ones to re-render");

    assert_eq!(card.state, NotificationState::Acked);

    store
        .orphan_notification(id, at(20))
        .await
        .expect("the card is released");

    let revoked = store
        .acknowledge(&AckCommand {
            fingerprint: Fingerprint::new("aaaa0050").expect("the fingerprint is hexadecimal"),
            user_id: UserId::new(1),
            kind: AckKind::Ack,
            note: None,
            revoke: true,
            at: at(30),
        })
        .await
        .expect("the revocation is recorded");

    assert!(
        !revoked.cards.iter().any(|card| card.id == id),
        "an orphaned card names a message that is gone, and is not handed back to re-render"
    );

    drain(store, "ack-identity").await;
}

/// Escalation is claimed, not merely read: two sweeps over one card produce one mention.
pub(super) async fn an_escalation_is_claimed_once(store: &dyn Store) {
    let route = route_for(store, "escalation", 121).await;
    let key = DedupeKey::from_stored("a:escalate");
    let id = card_for(store, route, 121, &key).await;

    // A card with no message has been shown to nobody, so it is not a candidate until its post
    // has been recorded.
    assert!(
        !store
            .pending_escalations(at(3600), 10)
            .await
            .expect("the read succeeds")
            .iter()
            .any(|card| card.id == id),
        "a card that has not been posted is nobody's to chase"
    );

    record_post(store, "escalation-setup", &key, MessageId::new(9_120)).await;

    let pending = store
        .pending_escalations(at(3600), 10)
        .await
        .expect("the read succeeds");

    assert!(
        pending.iter().any(|card| card.id == id),
        "a posted, firing, unanswered card is a candidate"
    );

    assert!(
        store
            .mark_escalated(id, at(3600))
            .await
            .expect("the claim succeeds"),
        "the first sweep takes it"
    );
    assert!(
        !store
            .mark_escalated(id, at(3600))
            .await
            .expect("the claim succeeds"),
        "the second sweep is told it was already taken rather than sending a second mention"
    );

    assert!(
        !store
            .pending_escalations(at(3600), 10)
            .await
            .expect("the read succeeds")
            .iter()
            .any(|card| card.id == id),
        "a card that has escalated is invisible to every sweep after it"
    );
}

pub(super) async fn a_decision_writes_the_card_and_its_post_together(store: &dyn Store) {
    let route = route_for(store, "decision", 100).await;
    let key = DedupeKey::from_stored("a:decision");
    let id = card_for(store, route, 100, &key).await;

    let card = store
        .notification(id)
        .await
        .expect("the read succeeds")
        .expect("the card exists");

    assert_eq!(card.state, NotificationState::Firing);
    assert!(!card.is_posted(), "no message id until the post succeeds");

    let items = store
        .claim_outbox(
            &WorkerId::new("decision"),
            ClaimRequest {
                lane: None,
                lease_secs: 30,
                limit: 10,
            },
            at(1),
        )
        .await
        .expect("the claim succeeds");

    assert_eq!(
        items.len(),
        1,
        "the card's post was enqueued in the same transaction as the row"
    );
    assert_eq!(
        items[0].effect,
        Effect::PostCard {
            notification: id,
            mention: true,
        }
    );

    store
        .fail_outbox(&WorkerId::new("decision"), items[0].id, "test", None)
        .await
        .expect("the item is abandoned");
}

pub(super) async fn a_second_card_for_one_key_in_one_channel_conflicts(store: &dyn Store) {
    let route = route_for(store, "conflict", 101).await;
    let key = DedupeKey::from_stored("a:conflict");

    card_for(store, route, 101, &key).await;

    let again = store
        .apply_decision(&Decision {
            new_cards: vec![PlannedCard {
                card: NewNotification {
                    dedupe_key: key.clone(),
                    fingerprint: Fingerprint::new("aaaa0001")
                        .expect("the fingerprint is hexadecimal"),
                    route_id: route,
                    guild_id: GuildId::new(1),
                    channel_id: ChannelId::new(101),
                    state: NotificationState::Firing,
                    supersedes: None,
                    created_at: at(0),
                },
                mention: false,
                not_before: at(0),
            }],
            updates: Vec::new(),
            at: at(0),
        })
        .await;

    assert!(
        matches!(again, Err(StoreError::Conflict { .. })),
        "the unique index is what stops two workers posting two cards for one alert, and both \
         dialects have to report it the same way: {again:?}"
    );

    let found = store
        .notification_for(&key, ChannelId::new(101))
        .await
        .expect("the read succeeds");

    assert!(found.is_some(), "the loser re-reads and finds the winner");

    drain(store, "conflict").await;
}

pub(super) async fn two_acknowledgements_produce_one(store: &dyn Store) {
    let fingerprint = Fingerprint::new("aaaa0010").expect("the fingerprint is hexadecimal");
    ingest(
        store,
        alert("aaaa0010", AlertStatus::Firing, "critical"),
        at(0),
    )
    .await;

    let route = route_for(store, "ack", 108).await;
    let key = DedupeKey::per_alert(&fingerprint);
    let id = card_for(store, route, 108, &key).await;

    let command = |user: u64| AckCommand {
        fingerprint: fingerprint.clone(),
        user_id: UserId::new(user),
        kind: AckKind::Ack,
        note: None,
        revoke: false,
        at: at(10),
    };

    let first = store
        .acknowledge(&command(1))
        .await
        .expect("the acknowledgement is recorded");
    let second = store
        .acknowledge(&command(2))
        .await
        .expect("the second call succeeds");

    assert!(first.changed, "the first press takes the alert");
    assert!(
        !second.changed,
        "a double press produces one acknowledgement, whichever dialect enforces the partial \
         unique index"
    );
    assert_eq!(
        second.holder,
        Some(UserId::new(1)),
        "the loser is told who holds it, so it can say so rather than posting a second card"
    );
    assert_eq!(first.acknowledged_at, Some(at(10)));

    let card = store
        .notification(id)
        .await
        .expect("the read succeeds")
        .expect("the card exists");

    assert_eq!(
        card.state,
        NotificationState::Acked,
        "the card moves inside the same transaction as the acknowledgement"
    );

    let held = store
        .acknowledgement(&fingerprint)
        .await
        .expect("the read succeeds")
        .expect("somebody holds it");

    assert_eq!(held.user_id, UserId::new(1));
    assert_eq!(held.kind, AckKind::Ack);
    assert_eq!(held.at, at(10));

    let revoked = store
        .acknowledge(&AckCommand {
            revoke: true,
            ..command(1)
        })
        .await
        .expect("the revocation succeeds");

    assert!(revoked.changed);
    assert_eq!(revoked.holder, None);
    assert!(
        store
            .acknowledgement(&fingerprint)
            .await
            .expect("the read succeeds")
            .is_none(),
        "a revoked acknowledgement leaves nobody holding the alert"
    );
    assert_eq!(
        revoked.cards.first().expect("the card is returned").state,
        NotificationState::Firing,
        "revoking returns the card to firing"
    );

    drain(store, "ack").await;
}

pub(super) async fn only_the_first_reply_changes_the_card(store: &dyn Store) {
    let route = route_for(store, "replies", 109).await;
    let key = DedupeKey::from_stored("a:replies");
    let id = card_for(store, route, 109, &key).await;

    let worker = WorkerId::new("replies");
    let items = store
        .claim_outbox(
            &worker,
            ClaimRequest {
                lane: None,
                lease_secs: 30,
                limit: 10,
            },
            at(1),
        )
        .await
        .expect("the claim succeeds");

    store
        .complete_outbox(
            &worker,
            items[0].id,
            &AppliedEffect {
                message_id: Some(MessageId::new(900)),
                thread_id: Some(ChannelId::new(901)),
                ..AppliedEffect::default()
            },
        )
        .await
        .expect("the completion succeeds");

    let found = store
        .notification_for_thread(ChannelId::new(901))
        .await
        .expect("the read succeeds")
        .expect("the thread names the card");

    assert_eq!(found.id, id);
    assert_eq!(
        found.reply_count, 0,
        "looking a card up by its thread is not a reply"
    );
    assert!(
        store
            .notification_for_thread(ChannelId::new(999_998))
            .await
            .expect("an unknown thread is not an error")
            .is_none()
    );

    let reply = ThreadReply {
        thread_id: ChannelId::new(901),
        author_id: UserId::new(3),
        at: at(20),
    };

    let first = store
        .record_reply(&reply)
        .await
        .expect("the reply is recorded")
        .expect("the first reply changes the card");

    assert_eq!(first.responded_at, Some(at(20)));
    assert_eq!(first.reply_count, 1);

    let second = store
        .record_reply(&ThreadReply {
            at: at(25),
            ..reply
        })
        .await
        .expect("the reply is recorded");

    assert!(
        second.is_none(),
        "a busy thread moves a counter without re-rendering the card once per message"
    );

    let card = store
        .notification(id)
        .await
        .expect("the read succeeds")
        .expect("the card exists");

    assert_eq!(card.reply_count, 2, "the counter still moved");
    assert_eq!(
        card.responded_at,
        Some(at(20)),
        "the first reply is the one"
    );

    let unknown = store
        .record_reply(&ThreadReply {
            thread_id: ChannelId::new(999_999),
            ..reply
        })
        .await
        .expect("an unknown thread is not an error");

    assert!(unknown.is_none());
}

pub(super) async fn a_deleted_card_releases_its_key(store: &dyn Store) {
    let route = route_for(store, "orphan", 111).await;
    let key = DedupeKey::from_stored("a:orphan");
    let id = card_for(store, route, 111, &key).await;

    store
        .orphan_notification(id, at(30))
        .await
        .expect("the card is released");

    let card = store
        .notification(id)
        .await
        .expect("the read succeeds")
        .expect("the row is kept");

    assert_eq!(card.state, NotificationState::Orphaned);
    assert_eq!(card.message_id, None);

    assert!(
        store
            .notification_for(&key, ChannelId::new(111))
            .await
            .expect("the read succeeds")
            .is_none(),
        "the key is freed, or the next change would edit a message that is not there"
    );

    // Freed means genuinely reusable: the unique index has to accept a fresh card under the same
    // key in the same channel.
    let replacement = card_for(store, route, 111, &key).await;

    assert_ne!(
        replacement, id,
        "the replacement is a new row with its own history"
    );

    drain(store, "orphan").await;
}
