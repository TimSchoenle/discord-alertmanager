//! Cases for the outbox: claims, leases, completion, failure and coalescing.

use chrono::{DateTime, Utc};
use dam_core::{DedupeKey, Fingerprint, NotificationState};

use crate::ids::{ChannelId, MessageId, WorkerId};
use crate::outbox::{AppliedEffect, ClaimRequest, Effect, LaneAssignment, NewOutboxItem};
use crate::{CardUpdate, Decision, Store, StoreError};

use super::{at, card_for, drain, route_for};

pub(super) async fn one_item_is_claimed_by_exactly_one_worker(store: &dyn Store) {
    let route = route_for(store, "claim", 102).await;
    let key = DedupeKey::from_stored("a:claim");
    card_for(store, route, 102, &key).await;

    let request = ClaimRequest {
        lane: None,
        lease_secs: 30,
        limit: 10,
    };

    let first = store
        .claim_outbox(&WorkerId::new("first"), request, at(1))
        .await
        .expect("the first claim succeeds");
    let second = store
        .claim_outbox(&WorkerId::new("second"), request, at(1))
        .await
        .expect("the second claim succeeds");

    assert_eq!(first.len(), 1, "the first worker takes the item");
    assert!(
        second.is_empty(),
        "a claimed item is invisible to every other worker, whether the backend holds it with \
         SKIP LOCKED or with an immediate transaction"
    );
    assert_eq!(
        first[0].attempts, 1,
        "the attempt count moves on the claim, so an item that kills its worker still runs out"
    );

    store
        .fail_outbox(&WorkerId::new("first"), first[0].id, "test", None)
        .await
        .expect("the item is abandoned");
}

pub(super) async fn a_claim_respects_the_lane_and_the_delay(store: &dyn Store) {
    let route = route_for(store, "lanes", 103).await;
    let key = DedupeKey::from_stored("a:lanes");
    let id = card_for(store, route, 103, &key).await;

    store
        .apply_decision(&Decision {
            new_cards: Vec::new(),
            updates: vec![CardUpdate {
                id,
                fingerprint: Fingerprint::new("aaaa0001").expect("the fingerprint is hexadecimal"),
                state: None,
                effects: vec![NewOutboxItem {
                    effect: Effect::ThreadNote {
                        notification: id,
                        text: "later".to_owned(),
                    },
                    dedupe_key: key.clone(),
                    not_before: at(600),
                }],
            }],
            at: at(0),
        })
        .await
        .expect("the decision applies");

    let early = store
        .claim_outbox(
            &WorkerId::new("lanes"),
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
        early.len(),
        1,
        "the delayed note is not claimable yet; only the post is"
    );

    // Both effects share a dedupe key, so both are in one lane, and a worker owning any other
    // slice of the lane space must not see either.
    let lane = key.lane(crate::OUTBOX_LANES);
    let elsewhere = store
        .claim_outbox(
            &WorkerId::new("elsewhere"),
            ClaimRequest {
                lane: Some(LaneAssignment::new(
                    lane.wrapping_add(1),
                    crate::OUTBOX_LANES,
                )),
                lease_secs: 30,
                limit: 10,
            },
            at(3600),
        )
        .await
        .expect("the claim succeeds");

    assert!(
        elsewhere.is_empty(),
        "every effect for one alert belongs to one worker, which is what keeps two of them from \
         editing one card at the same moment"
    );

    store
        .fail_outbox(&WorkerId::new("lanes"), early[0].id, "test", None)
        .await
        .expect("the item is abandoned");
    drain(store, "lanes").await;
}

pub(super) async fn completing_writes_back_what_discord_returned(store: &dyn Store) {
    let route = route_for(store, "complete", 104).await;
    let key = DedupeKey::from_stored("a:complete");
    let id = card_for(store, route, 104, &key).await;

    let worker = WorkerId::new("complete");
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
                message_id: Some(MessageId::new(555)),
                thread_id: Some(ChannelId::new(556)),
                render_hash: Some("hash-1".to_owned()),
                ..AppliedEffect::default()
            },
        )
        .await
        .expect("the completion succeeds");

    let card = store
        .notification(id)
        .await
        .expect("the read succeeds")
        .expect("the card exists");

    assert_eq!(card.message_id, Some(MessageId::new(555)));
    assert_eq!(card.thread_id, Some(ChannelId::new(556)));
    assert!(!card.needs_edit("hash-1"), "an identical render is skipped");
    assert!(card.needs_edit("hash-2"));

    let depth = store.outbox_depth().await.expect("the depth is readable");

    assert!(
        !depth.iter().any(|(kind, _)| kind == "post_card"),
        "a completed item leaves the queue"
    );
}

pub(super) async fn a_lost_lease_cannot_complete(store: &dyn Store) {
    let route = route_for(store, "lease", 105).await;
    let key = DedupeKey::from_stored("a:lease");
    card_for(store, route, 105, &key).await;

    let holder = WorkerId::new("holder");
    let items = store
        .claim_outbox(
            &holder,
            ClaimRequest {
                lane: None,
                lease_secs: 30,
                limit: 10,
            },
            at(1),
        )
        .await
        .expect("the claim succeeds");

    let stolen = store
        .complete_outbox(
            &WorkerId::new("thief"),
            items[0].id,
            &AppliedEffect::default(),
        )
        .await;

    assert!(
        matches!(stolen, Err(StoreError::LeaseLost { .. })),
        "a worker whose lease expired mid-flight must not write the result of work somebody \
         else has since redone: {stolen:?}"
    );

    store
        .fail_outbox(&holder, items[0].id, "test", None)
        .await
        .expect("the item is abandoned");
}

pub(super) async fn a_failure_returns_the_item_or_abandons_it(store: &dyn Store) {
    let route = route_for(store, "retry", 106).await;
    let key = DedupeKey::from_stored("a:retry");
    card_for(store, route, 106, &key).await;

    let worker = WorkerId::new("retry");
    let request = ClaimRequest {
        lane: None,
        lease_secs: 30,
        limit: 10,
    };

    let items = store
        .claim_outbox(&worker, request, at(1))
        .await
        .expect("the claim succeeds");

    store
        .fail_outbox(&worker, items[0].id, "rate limited", Some(at(300)))
        .await
        .expect("the item is released");

    let too_early = store
        .claim_outbox(&worker, request, at(200))
        .await
        .expect("the claim succeeds");

    assert!(too_early.is_empty(), "the backoff is respected");

    let later = store
        .claim_outbox(&worker, request, at(400))
        .await
        .expect("the claim succeeds");

    assert_eq!(later.len(), 1, "the item comes back after the backoff");
    assert_eq!(
        later[0].attempts, 2,
        "the second claim is the second attempt"
    );
    assert_eq!(later[0].last_error.as_deref(), Some("rate limited"));

    store
        .fail_outbox(&worker, later[0].id, "giving up", None)
        .await
        .expect("the item is abandoned");

    let gone = store
        .claim_outbox(&worker, request, at(1000))
        .await
        .expect("the claim succeeds");

    assert!(
        gone.is_empty(),
        "an abandoned item leaves the queue rather than becoming depth that never drains"
    );
}

pub(super) async fn an_expired_lease_becomes_claimable(store: &dyn Store) {
    let route = route_for(store, "janitor", 107).await;
    let key = DedupeKey::from_stored("a:janitor");
    card_for(store, route, 107, &key).await;

    let dead = WorkerId::new("dead");
    let request = ClaimRequest {
        lane: None,
        lease_secs: 30,
        limit: 10,
    };

    store
        .claim_outbox(&dead, request, at(1))
        .await
        .expect("the claim succeeds");

    let reclaimed = store
        .reclaim_expired(at(600), at(600))
        .await
        .expect("the sweep succeeds");

    assert!(reclaimed >= 1, "the janitor releases an expired lease");

    let items = store
        .claim_outbox(&WorkerId::new("alive"), request, at(601))
        .await
        .expect("the claim succeeds");

    assert_eq!(items.len(), 1, "a reclaimed item is claimable again");

    store
        .fail_outbox(&WorkerId::new("alive"), items[0].id, "test", None)
        .await
        .expect("the item is abandoned");
}

pub(super) async fn an_edit_is_coalesced_and_a_note_is_not(store: &dyn Store) {
    let route = route_for(store, "coalesce", 110).await;
    let key = DedupeKey::from_stored("a:coalesce");
    let id = card_for(store, route, 110, &key).await;

    let update = |not_before: DateTime<Utc>, text: &str| Decision {
        new_cards: Vec::new(),
        updates: vec![CardUpdate {
            id,
            fingerprint: Fingerprint::new("aaaa0001").expect("the fingerprint is hexadecimal"),
            state: None,
            effects: vec![
                NewOutboxItem {
                    effect: Effect::EditCard { notification: id },
                    dedupe_key: key.clone(),
                    not_before,
                },
                NewOutboxItem {
                    effect: Effect::ThreadNote {
                        notification: id,
                        text: text.to_owned(),
                    },
                    dedupe_key: key.clone(),
                    not_before,
                },
            ],
        }],
        at: at(0),
    };

    store
        .apply_decision(&update(at(30), "first"))
        .await
        .expect("the decision applies");
    store
        .apply_decision(&update(at(60), "second"))
        .await
        .expect("the decision applies");

    let items = store
        .claim_outbox(
            &WorkerId::new("coalesce"),
            ClaimRequest {
                lane: None,
                lease_secs: 30,
                limit: 20,
            },
            at(120),
        )
        .await
        .expect("the claim succeeds");

    let edits = items
        .iter()
        .filter(|item| matches!(item.effect, Effect::EditCard { .. }))
        .count();
    let notes = items
        .iter()
        .filter(|item| matches!(item.effect, Effect::ThreadNote { .. }))
        .count();

    assert_eq!(
        edits, 1,
        "two queued edits of one card are one edit of its current state"
    );
    assert_eq!(
        notes, 2,
        "two queued notes are two different sentences, and folding them loses a line"
    );

    let edit = items
        .iter()
        .find(|item| matches!(item.effect, Effect::EditCard { .. }))
        .expect("the edit is there");

    assert_eq!(
        edit.not_before,
        at(30),
        "the fold keeps the earlier deadline, so a sustained stream still produces an edit one \
         debounce after the first change rather than never"
    );

    for item in &items {
        store
            .fail_outbox(&WorkerId::new("coalesce"), item.id, "test", None)
            .await
            .expect("the item is abandoned");
    }

    drain(store, "coalesce").await;
}

/// One alert reaching two routes leaves two cards, and neither swallows the other's effects.
///
/// The per-alert dedupe key belongs to the alert, not to the card: every route matching one alert
/// produces a card under the same key in a channel of its own. Coalescing on the key alone folded
/// those cards' edits and tag changes into a single row, so all but one card stopped being
/// updated and went on showing the state it was posted in.
pub(super) async fn two_cards_sharing_a_key_keep_their_own_edits(store: &dyn Store) {
    let key = DedupeKey::from_stored("a:fanout");

    let first_route = route_for(store, "fanout-one", 210).await;
    let second_route = route_for(store, "fanout-two", 211).await;
    let first = card_for(store, first_route, 210, &key).await;
    let second = card_for(store, second_route, 211, &key).await;

    let resolve = |id: crate::NotificationId, tag: u64| CardUpdate {
        id,
        fingerprint: Fingerprint::new("aaaa0001").expect("the fingerprint is hexadecimal"),
        state: Some(NotificationState::Resolved),
        effects: vec![
            NewOutboxItem {
                effect: Effect::EditCard { notification: id },
                dedupe_key: key.clone(),
                not_before: at(30),
            },
            NewOutboxItem {
                effect: Effect::SetTags {
                    notification: id,
                    tags: vec![crate::TagId::new(tag)],
                },
                dedupe_key: key.clone(),
                not_before: at(30),
            },
        ],
    };

    store
        .apply_decision(&Decision {
            new_cards: Vec::new(),
            updates: vec![resolve(first, 1), resolve(second, 2)],
            at: at(0),
        })
        .await
        .expect("the decision applies");

    let items = store
        .claim_outbox(
            &WorkerId::new("fanout"),
            ClaimRequest {
                lane: None,
                lease_secs: 30,
                limit: 20,
            },
            at(120),
        )
        .await
        .expect("the claim succeeds");

    let edited: Vec<crate::NotificationId> = items
        .iter()
        .filter_map(|item| match item.effect {
            Effect::EditCard { notification } => Some(notification),
            _ => None,
        })
        .collect();
    let tagged: Vec<crate::NotificationId> = items
        .iter()
        .filter_map(|item| match item.effect {
            Effect::SetTags { notification, .. } => Some(notification),
            _ => None,
        })
        .collect();

    assert!(
        edited.contains(&first) && edited.contains(&second),
        "each card keeps its own re-render; folding on the shared key loses one of them"
    );
    assert!(
        tagged.contains(&first) && tagged.contains(&second),
        "each post keeps its own tag change, so both stop saying they are firing"
    );

    // Folding within one card is unaffected, and `an_edit_is_coalesced_and_a_note_is_not` is
    // where that is asserted: the scope narrowed from the alert to the card, and no further.
    for item in &items {
        store
            .fail_outbox(&WorkerId::new("fanout"), item.id, "test", None)
            .await
            .expect("the item is abandoned");
    }

    drain(store, "fanout").await;
}
