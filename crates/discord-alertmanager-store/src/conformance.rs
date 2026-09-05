//! The suite every backend runs against its own [`crate::Store`].
//!
//! Each backend crate takes this crate with the `conformance` feature in its `dev-dependencies`
//! and calls [`run`] from a test, so one body of assertions runs twice against two dialects from a
//! single `cargo test`.
//!
//! What belongs here is behaviour two implementations can disagree about, and nothing else:
//! whether `FOR UPDATE SKIP LOCKED` and `BEGIN IMMEDIATE` hand one row to exactly one claimant,
//! how equal timestamps order, and whether both map a unique violation onto the same
//! `StoreError`. SQL that is merely valid is already checked at compile time and needs no test.
//!
//! Every case works under a fingerprint and a dedupe key of its own, so the suite runs against one
//! database without the cases having to be ordered or isolated by hand.

use chrono::{DateTime, Duration, TimeZone, Utc};
use dam_core::{
    Alert, AlertStatus, AmState, Annotations, DedupeKey, EventKind, EventSource, Fingerprint,
    LabelName, Labels, MatchOp, MatcherSet, NotificationState,
};

use crate::alerts::{AlertQuery, IngestBatch, QueryMatcher, SilenceLifecycle, SilenceState};
use crate::audit::{AuditEntry, RetentionPolicy};
use crate::ids::{
    ChannelId, GuildId, IgnoreId, MessageId, RoleId, RouteId, SubscriptionId, UserId, WorkerId,
};
use crate::logic::suppression_map;
use crate::notifications::{AckCommand, AckKind, NewNotification, ThreadReply};
use crate::outbox::{AppliedEffect, ClaimRequest, Effect, LaneAssignment, NewOutboxItem};
use crate::routing::{
    Escalation, ForumTag, GroupStrategy, IgnoreRule, IgnoreScope, Mentions, Route, RouteSource,
    RouteTarget, Subscription, ThreadPolicy,
};
use crate::{CardUpdate, Decision, PlannedCard, SilenceLink, Store, StoreError};

/// Runs every case against one open store.
///
/// # Panics
///
/// On the first assertion the backend fails, with the case that failed.
pub async fn run(store: &dyn Store) {
    store.health().await.expect("the database answers");

    ingest_records_what_changed(store).await;
    ingest_discards_a_redelivery(store).await;
    a_re_fire_reuses_the_row_and_counts_the_flap(store).await;
    a_re_fire_after_the_regroup_window_starts_an_episode(store).await;
    a_replacement_card_remembers_what_it_replaced(store).await;
    an_escalation_is_claimed_once(store).await;
    a_route_keeps_its_escalation_policy(store).await;
    a_decision_writes_the_card_and_its_post_together(store).await;
    a_second_card_for_one_key_in_one_channel_conflicts(store).await;
    one_item_is_claimed_by_exactly_one_worker(store).await;
    a_claim_respects_the_lane_and_the_delay(store).await;
    completing_writes_back_what_discord_returned(store).await;
    a_lost_lease_cannot_complete(store).await;
    a_failure_returns_the_item_or_abandons_it(store).await;
    an_expired_lease_becomes_claimable(store).await;
    two_acknowledgements_produce_one(store).await;
    only_the_first_reply_changes_the_card(store).await;
    a_deleted_card_releases_its_key(store).await;
    an_edit_is_coalesced_and_a_note_is_not(store).await;
    two_cards_sharing_a_key_keep_their_own_edits(store).await;
    ignore_rules_expire_and_revoke(store).await;
    a_config_route_syncs_and_a_discord_route_collides(store).await;
    silences_sync_into_deltas(store).await;
    forum_tags_are_replaced_wholesale(store).await;
    queries_filter_paginate_and_match(store).await;
    the_reconciler_finds_what_alertmanager_dropped(store).await;
    pruning_is_bounded_and_says_so(store).await;
    a_subscription_belongs_to_its_owner(store).await;
    the_audit_log_accepts_every_result(store).await;
}

/// A subscription belongs to the person who made it, in the predicate rather than in a check.
async fn a_subscription_belongs_to_its_owner(store: &dyn Store) {
    let owner = UserId::new(4_001);
    let stranger = UserId::new(4_002);

    let id = store
        .upsert_subscription(&Subscription {
            id: SubscriptionId::new(0),
            user_id: owner,
            matcher_source: "alertname=Subscribed".to_owned(),
            matchers: MatcherSet::parse("alertname=Subscribed").expect("the expression parses"),
            min_severity: Some(dam_core::Severity::Warning),
            created_at: at(0),
        })
        .await
        .expect("the subscription is written");

    let listed = store.subscriptions().await.expect("the read succeeds");
    let mine = listed
        .iter()
        .find(|subscription| subscription.id == id)
        .expect("the subscription is listed");

    assert_eq!(mine.user_id, owner);
    assert_eq!(mine.min_severity, Some(dam_core::Severity::Warning));
    assert!(
        mine.matchers
            .matches(&labels(&[("alertname", "Subscribed")])),
        "the matchers are compiled on the way back out, not left as text"
    );

    // A stranger holding the id can neither rewrite it nor delete it. Both are one statement, so
    // there is no window between checking the owner and acting on the row.
    let hijack = store
        .upsert_subscription(&Subscription {
            id,
            user_id: stranger,
            matcher_source: "alertname=Anything".to_owned(),
            matchers: MatcherSet::parse("alertname=Anything").expect("the expression parses"),
            min_severity: None,
            created_at: at(0),
        })
        .await;

    assert!(
        matches!(hijack, Err(StoreError::NotFound { .. })),
        "somebody else's id is not found, rather than found and rewritten"
    );

    assert!(
        matches!(
            store.remove_subscription(id, stranger).await,
            Err(StoreError::NotFound { .. })
        ),
        "a stranger cannot unsubscribe the owner"
    );

    store
        .upsert_subscription(&Subscription {
            id,
            user_id: owner,
            matcher_source: "alertname=Subscribed, severity=critical".to_owned(),
            matchers: MatcherSet::parse("alertname=Subscribed, severity=critical")
                .expect("the expression parses"),
            min_severity: None,
            created_at: at(0),
        })
        .await
        .expect("the owner may rewrite their own");

    store
        .remove_subscription(id, owner)
        .await
        .expect("the owner may remove their own");

    assert!(
        store
            .subscriptions()
            .await
            .expect("the read succeeds")
            .iter()
            .all(|subscription| subscription.id != id),
        "a removed subscription is gone"
    );
}

/// A fixed clock, so an assertion about ordering is about the backend rather than about timing.
fn at(seconds: i64) -> DateTime<Utc> {
    Utc.timestamp_opt(1_700_000_000 + seconds, 0)
        .single()
        .expect("the fixed timestamp is representable")
}

/// Builds a label set.
fn labels(pairs: &[(&str, &str)]) -> Labels {
    pairs
        .iter()
        .map(|(name, value)| {
            (
                LabelName::new(*name).expect("the test label name is valid"),
                (*value).to_owned(),
            )
        })
        .collect()
}

/// Builds an alert with a severity and a name.
fn alert(fingerprint: &str, status: AlertStatus, severity: &str) -> Alert {
    Alert {
        fingerprint: Fingerprint::new(fingerprint).expect("the test fingerprint is hexadecimal"),
        labels: labels(&[
            ("alertname", "TestAlert"),
            ("severity", severity),
            ("namespace", "payments"),
        ]),
        annotations: Annotations::new(),
        starts_at: at(0),
        ends_at: None,
        generator_url: Some("https://prometheus.example.net/graph".to_owned()),
        status,
        am_state: AmState::Active,
        silenced_by: Vec::new(),
        inhibited_by: Vec::new(),
        group_key: None,
    }
}

/// Ingests one alert and returns what the store made of it.
async fn ingest(store: &dyn Store, alert: Alert, at: DateTime<Utc>) -> crate::IngestOutcome {
    store
        .ingest_batch(&IngestBatch::new(EventSource::Webhook, vec![alert], at))
        .await
        .expect("the batch is accepted")
}

/// Creates a route to hang cards off, since a card names the route that produced it.
async fn route_for(store: &dyn Store, name: &str, channel: u64) -> RouteId {
    store
        .upsert_route(&Route {
            id: RouteId::new(0),
            guild_id: GuildId::new(1),
            name: name.to_owned(),
            matcher_source: "namespace=payments".to_owned(),
            matchers: MatcherSet::parse("namespace=payments").expect("the expression parses"),
            min_severity: None,
            target: RouteTarget::Text {
                channel: ChannelId::new(channel),
                thread: ThreadPolicy::default(),
            },
            group_strategy: GroupStrategy::PerAlert,
            mentions: Mentions::default(),
            escalation: None,
            priority: 100,
            continue_to_next: false,
            source: RouteSource::Discord,
            enabled: true,
            created_by: Some(UserId::new(7)),
            created_at: at(0),
        })
        .await
        .expect("the route is written")
}

/// Writes one card and returns its key.
async fn card_for(
    store: &dyn Store,
    route: RouteId,
    channel: u64,
    key: &DedupeKey,
) -> crate::NotificationId {
    let created = store
        .apply_decision(&Decision {
            new_cards: vec![PlannedCard {
                card: NewNotification {
                    dedupe_key: key.clone(),
                    fingerprint: Fingerprint::new("aaaa0001")
                        .expect("the fingerprint is hexadecimal"),
                    route_id: route,
                    guild_id: GuildId::new(1),
                    channel_id: ChannelId::new(channel),
                    state: NotificationState::Firing,
                    supersedes: None,
                    created_at: at(0),
                },
                mention: true,
                not_before: at(0),
            }],
            updates: Vec::new(),
            at: at(0),
        })
        .await
        .expect("the decision applies");

    *created.first().expect("one card was created")
}

async fn ingest_records_what_changed(store: &dyn Store) {
    let outcome = ingest(
        store,
        alert("aaaa0001", AlertStatus::Firing, "critical"),
        at(0),
    )
    .await;

    assert_eq!(outcome.duplicates, 0, "a first delivery is not a duplicate");
    assert_eq!(outcome.deltas.len(), 1, "one alert is one delta");
    assert_eq!(outcome.deltas[0].kind, EventKind::Fired);

    let fingerprint = Fingerprint::new("aaaa0001").expect("the fingerprint is hexadecimal");
    let record = store
        .alert(&fingerprint)
        .await
        .expect("the read succeeds")
        .expect("the row exists");

    assert_eq!(record.alert.labels.get("namespace"), Some("payments"));
    assert_eq!(
        record.labels_hash,
        record.alert.labels_hash(),
        "the stored hash is the one the label set produces, not a recomputation on read"
    );
    assert_eq!(record.first_seen_at, at(0));
    assert_eq!(record.flap_count, 0);
}

async fn ingest_discards_a_redelivery(store: &dyn Store) {
    let alert = alert("aaaa0002", AlertStatus::Firing, "warning");

    ingest(store, alert.clone(), at(0)).await;
    let second = ingest(store, alert, at(30)).await;

    assert_eq!(
        second.duplicates, 1,
        "the same payload twice is a duplicate"
    );
    assert!(
        second.deltas.is_empty(),
        "a duplicate produces no card edits"
    );

    let record = store
        .alert(&Fingerprint::new("aaaa0002").expect("the fingerprint is hexadecimal"))
        .await
        .expect("the read succeeds")
        .expect("the row exists");

    assert_eq!(
        record.last_seen_at,
        at(30),
        "a duplicate still says the alert is there, and the reconciler reads exactly this column"
    );
}

async fn a_re_fire_reuses_the_row_and_counts_the_flap(store: &dyn Store) {
    ingest(
        store,
        alert("aaaa0003", AlertStatus::Firing, "critical"),
        at(0),
    )
    .await;

    let mut resolved = alert("aaaa0003", AlertStatus::Resolved, "critical");
    resolved.ends_at = Some(at(60));
    let resolve = ingest(store, resolved, at(60)).await;

    assert_eq!(resolve.deltas[0].kind, EventKind::Resolved);

    let refire = ingest(
        store,
        alert("aaaa0003", AlertStatus::Firing, "critical"),
        at(120),
    )
    .await;

    assert_eq!(refire.deltas[0].kind, EventKind::Fired);
    assert_eq!(refire.deltas[0].flap_count, 1, "a re-fire is a flap");

    let record = store
        .alert(&Fingerprint::new("aaaa0003").expect("the fingerprint is hexadecimal"))
        .await
        .expect("the read succeeds")
        .expect("the row exists");

    assert_eq!(record.first_seen_at, at(0), "the first sighting is kept");
    assert_eq!(record.resolved_at, None, "a firing alert has not resolved");
}

/// A re-fire long after the resolution is a new episode, and the episode reaches the caller.
///
/// The window is the store's own setting rather than a per-call one, so this asserts the
/// behaviour a deployment sees rather than one a test could dial in: the conformance store takes
/// the configuration's default of half an hour, and these timestamps are a day apart.
async fn a_re_fire_after_the_regroup_window_starts_an_episode(store: &dyn Store) {
    const DAY: i64 = 86_400;

    ingest(
        store,
        alert("aaaa0020", AlertStatus::Firing, "critical"),
        at(0),
    )
    .await;

    let mut resolved = alert("aaaa0020", AlertStatus::Resolved, "critical");
    resolved.ends_at = Some(at(60));
    ingest(store, resolved, at(60)).await;

    let refire = ingest(
        store,
        alert("aaaa0020", AlertStatus::Firing, "critical"),
        at(DAY),
    )
    .await;

    assert_eq!(refire.deltas[0].kind, EventKind::Fired);
    assert_eq!(
        refire.deltas[0].episode, 1,
        "a day of quiet is a new card, not a flap on the old one"
    );
    assert_eq!(
        refire.deltas[0].flap_count, 0,
        "the flap count belongs to the episode it counts within"
    );

    let record = store
        .alert(&Fingerprint::new("aaaa0020").expect("the fingerprint is hexadecimal"))
        .await
        .expect("the read succeeds")
        .expect("the row exists");

    assert_eq!(record.episode, 1, "the episode is persisted, not derived");
    assert_eq!(
        refire.deltas[0].per_alert_key(),
        DedupeKey::per_alert(record.fingerprint(), 1),
        "the key carries the episode, which is what posts a second card"
    );
}

/// The card a new episode posts keeps a reference to the one it replaced.
async fn a_replacement_card_remembers_what_it_replaced(store: &dyn Store) {
    let route = route_for(store, "supersedes", 120).await;
    let first = card_for(store, route, 120, &DedupeKey::from_stored("a:super")).await;

    let created = store
        .apply_decision(&Decision {
            new_cards: vec![PlannedCard {
                card: NewNotification {
                    dedupe_key: DedupeKey::from_stored("a:super#1"),
                    fingerprint: Fingerprint::new("aaaa0001")
                        .expect("the fingerprint is hexadecimal"),
                    route_id: route,
                    guild_id: GuildId::new(1),
                    channel_id: ChannelId::new(120),
                    state: NotificationState::Firing,
                    supersedes: Some(first),
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

    let second = store
        .notification(*created.first().expect("one card was created"))
        .await
        .expect("the read succeeds")
        .expect("the row exists");

    assert_eq!(
        second.supersedes,
        Some(first),
        "without this the new card has no history and the card that holds it is buried"
    );
}

/// Escalation is claimed, not merely read: two sweeps over one card produce one mention.
async fn an_escalation_is_claimed_once(store: &dyn Store) {
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

/// A route's escalation policy survives the round trip, including having none.
async fn a_route_keeps_its_escalation_policy(store: &dyn Store) {
    let mut route = Route {
        id: RouteId::new(0),
        guild_id: GuildId::new(3),
        name: "escalating".to_owned(),
        matcher_source: "severity=critical".to_owned(),
        matchers: MatcherSet::parse("severity=critical").expect("the expression parses"),
        min_severity: None,
        target: RouteTarget::Text {
            channel: ChannelId::new(122),
            thread: ThreadPolicy::default(),
        },
        group_strategy: GroupStrategy::PerAlert,
        mentions: Mentions::default(),
        escalation: Some(Escalation {
            after_secs: 900,
            roles: vec![RoleId::new(77)],
            users: vec![UserId::new(88)],
        }),
        priority: 100,
        continue_to_next: false,
        source: RouteSource::Discord,
        enabled: true,
        created_by: None,
        created_at: at(0),
    };

    route.id = store
        .upsert_route(&route)
        .await
        .expect("the route is written");

    let stored = read_route(store, route.id).await;

    assert_eq!(
        stored.escalation, route.escalation,
        "a policy that did not survive storage is a route that silently stops escalating"
    );

    route.escalation = None;
    store
        .upsert_route(&route)
        .await
        .expect("the route is written");

    assert!(
        read_route(store, route.id).await.escalation.is_none(),
        "removing the policy has to clear the column, not leave the old one behind"
    );
}

/// Reads one route back out of the table.
async fn read_route(store: &dyn Store, id: RouteId) -> Route {
    store
        .routes()
        .await
        .expect("the read succeeds")
        .into_iter()
        .find(|route| route.id == id)
        .expect("the route is there")
}

/// The dedupe keys the regrouping and escalation fixtures queue work under.
///
/// Named, because the helper below has to tell its own rows from the ones other cases are relying
/// on being left alone.
const FIXTURE_KEYS: [&str; 2] = ["a:super", "a:escalate"];

/// Records the post of the card queued under `key`, and leaves the queue as it found it.
///
/// Every case in this suite shares one database and several of them count what is claimable, so a
/// fixture that leaves work claimed or adds work of its own breaks cases it never touched. Its own
/// rows are cleared; anything else goes straight back at the time it was already due.
///
/// # Panics
///
/// When the card has no post queued, which would mean [`Store::apply_decision`] wrote a row
/// without the effect that shows it.
async fn record_post(store: &dyn Store, worker: &str, key: &DedupeKey, message: MessageId) {
    let worker = WorkerId::new(worker);

    let claimed = store
        .claim_outbox(
            &worker,
            ClaimRequest {
                lane: None,
                lease_secs: 60,
                limit: 50,
            },
            at(0),
        )
        .await
        .expect("the claim succeeds");

    let mut recorded = false;

    for item in claimed {
        if !recorded && item.dedupe_key == *key {
            store
                .complete_outbox(
                    &worker,
                    item.id,
                    &AppliedEffect {
                        message_id: Some(message),
                        ..AppliedEffect::default()
                    },
                )
                .await
                .expect("the post is recorded");

            recorded = true;
        } else if FIXTURE_KEYS
            .iter()
            .any(|prefix| item.dedupe_key.as_str().starts_with(prefix))
        {
            store
                .fail_outbox(&worker, item.id, "cleared by a fixture", None)
                .await
                .expect("the fixture's own row is cleared");
        } else {
            store
                .fail_outbox(
                    &worker,
                    item.id,
                    "returned by a fixture",
                    Some(item.not_before),
                )
                .await
                .expect("another case's row goes back on the queue");
        }
    }

    assert!(recorded, "the card's post is queued");
}

async fn a_decision_writes_the_card_and_its_post_together(store: &dyn Store) {
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

async fn a_second_card_for_one_key_in_one_channel_conflicts(store: &dyn Store) {
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

async fn one_item_is_claimed_by_exactly_one_worker(store: &dyn Store) {
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

async fn a_claim_respects_the_lane_and_the_delay(store: &dyn Store) {
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

async fn completing_writes_back_what_discord_returned(store: &dyn Store) {
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

async fn a_lost_lease_cannot_complete(store: &dyn Store) {
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

async fn a_failure_returns_the_item_or_abandons_it(store: &dyn Store) {
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

async fn an_expired_lease_becomes_claimable(store: &dyn Store) {
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

async fn two_acknowledgements_produce_one(store: &dyn Store) {
    let fingerprint = Fingerprint::new("aaaa0010").expect("the fingerprint is hexadecimal");
    ingest(
        store,
        alert("aaaa0010", AlertStatus::Firing, "critical"),
        at(0),
    )
    .await;

    let route = route_for(store, "ack", 108).await;
    let key = DedupeKey::per_alert(&fingerprint, 0);
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

async fn only_the_first_reply_changes_the_card(store: &dyn Store) {
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

async fn a_deleted_card_releases_its_key(store: &dyn Store) {
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

async fn an_edit_is_coalesced_and_a_note_is_not(store: &dyn Store) {
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
async fn two_cards_sharing_a_key_keep_their_own_edits(store: &dyn Store) {
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

async fn ignore_rules_expire_and_revoke(store: &dyn Store) {
    let guild = GuildId::new(1);
    let rule = IgnoreRule {
        id: IgnoreId::new(0),
        scope: IgnoreScope::Guild,
        guild_id: guild,
        channel_id: None,
        matcher_source: "alertname=Noisy".to_owned(),
        matchers: MatcherSet::parse("alertname=Noisy").expect("the expression parses"),
        reason: "known flapper".to_owned(),
        created_by: UserId::new(3),
        created_at: at(0),
        expires_at: Some(at(600)),
        revoked_at: None,
    };

    let id = store
        .upsert_ignore(&rule)
        .await
        .expect("the rule is written");

    let active = store
        .active_ignores(guild, at(10))
        .await
        .expect("the read succeeds");

    let found = active
        .iter()
        .find(|found| found.id == id)
        .expect("the rule is in force");

    assert_eq!(found.reason, "known flapper");
    assert_eq!(
        found.matchers.as_slice().len(),
        1,
        "the expression is compiled on read, so the hot path never compiles a regex"
    );

    let lapsed = store
        .active_ignores(guild, at(700))
        .await
        .expect("the read succeeds");

    assert!(
        !lapsed.iter().any(|found| found.id == id),
        "an expired rule stops muting without anything having to revoke it"
    );

    store
        .revoke_ignore(id, guild, at(20))
        .await
        .expect("the rule is revoked");

    let missing = store.revoke_ignore(id, guild, at(30)).await;

    assert!(
        matches!(missing, Err(StoreError::NotFound { .. })),
        "revoking twice is a missing row, not a silent success: {missing:?}"
    );
}

async fn a_config_route_syncs_and_a_discord_route_collides(store: &dyn Store) {
    let declared = Route {
        id: RouteId::new(0),
        guild_id: GuildId::new(2),
        name: "critical".to_owned(),
        matcher_source: "severity=critical".to_owned(),
        matchers: MatcherSet::parse("severity=critical").expect("the expression parses"),
        min_severity: Some(dam_core::Severity::Warning),
        target: RouteTarget::Text {
            channel: ChannelId::new(200),
            thread: ThreadPolicy::default(),
        },
        group_strategy: GroupStrategy::PerAlert,
        mentions: Mentions::default(),
        escalation: None,
        priority: 10,
        continue_to_next: true,
        source: RouteSource::Config,
        enabled: true,
        created_by: None,
        created_at: at(0),
    };

    let first = store
        .upsert_route(&declared)
        .await
        .expect("the route is written");
    let second = store
        .upsert_route(&declared)
        .await
        .expect("a second start writes the same route again");

    assert_eq!(
        first, second,
        "a route declared in the file is synchronised by name, so restarting does not duplicate it"
    );

    let stored = store
        .routes()
        .await
        .expect("the read succeeds")
        .into_iter()
        .find(|route| route.id == first)
        .expect("the route is stored");

    assert_eq!(stored.min_severity, Some(dam_core::Severity::Warning));
    assert!(stored.continue_to_next);
    assert_eq!(stored.source, RouteSource::Config);
    assert!(
        stored
            .matchers
            .matches(&labels(&[("severity", "critical")])),
        "the matchers survive the round trip through the expression column"
    );

    let from_discord = Route {
        source: RouteSource::Discord,
        created_by: Some(UserId::new(9)),
        ..declared.clone()
    };
    let collision = store.upsert_route(&from_discord).await;

    assert!(
        matches!(collision, Err(StoreError::Conflict { .. })),
        "`/route add` under a name the guild already uses is a mistake the operator has to see: \
         {collision:?}"
    );

    let disabled = store
        .disable_missing_config_routes(&["something-else".to_owned()])
        .await
        .expect("the sweep succeeds");

    assert!(disabled >= 1, "a route that left the file is disabled");

    let after = store
        .routes()
        .await
        .expect("the read succeeds")
        .into_iter()
        .find(|route| route.id == first)
        .expect("the route is still there");

    assert!(
        !after.enabled,
        "disabled rather than deleted, so the notifications it created keep their history"
    );
}

async fn silences_sync_into_deltas(store: &dyn Store) {
    let fingerprint = Fingerprint::new("aaaa0020").expect("the fingerprint is hexadecimal");
    ingest(
        store,
        alert("aaaa0020", AlertStatus::Firing, "critical"),
        at(0),
    )
    .await;

    store
        .record_silence(&SilenceLink {
            am_id: "silence-1".to_owned(),
            matchers: "alertname=TestAlert".to_owned(),
            starts_at: at(0),
            ends_at: at(3600),
            created_by: "discord:someone (3)".to_owned(),
            discord_user_id: Some(UserId::new(3)),
            origin_message: Some("https://discord.com/channels/1/2/3".to_owned()),
            comment: "maintenance".to_owned(),
            state: SilenceLifecycle::Active,
            synced_at: at(0),
        })
        .await
        .expect("the silence is recorded");

    let snapshot = vec![SilenceState {
        am_id: "silence-1".to_owned(),
        suppresses: vec![fingerprint.clone()],
        state: SilenceLifecycle::Active,
        ends_at: at(3600),
        observed_at: at(10),
    }];

    assert_eq!(
        suppression_map(&snapshot).get(&fingerprint).map(Vec::len),
        Some(1),
        "the suppression set comes from Alertmanager rather than from evaluating matchers locally"
    );

    let silenced = store
        .sync_silences(&snapshot, at(10))
        .await
        .expect("the sync succeeds");

    assert_eq!(silenced.len(), 1, "the alert became suppressed");
    assert_eq!(silenced[0].kind, EventKind::Silenced);
    assert_eq!(silenced[0].alert.am_state, AmState::Suppressed);

    let again = store
        .sync_silences(&snapshot, at(20))
        .await
        .expect("the sync succeeds");

    assert!(
        again.is_empty(),
        "an unchanged snapshot changes no cards, or every poll would re-render every silenced one"
    );

    let expired = vec![SilenceState {
        state: SilenceLifecycle::Expired,
        ..snapshot[0].clone()
    }];
    let unsilenced = store
        .sync_silences(&expired, at(30))
        .await
        .expect("the sync succeeds");

    assert_eq!(unsilenced.len(), 1);
    assert_eq!(unsilenced[0].kind, EventKind::Unsilenced);
    assert_eq!(unsilenced[0].alert.am_state, AmState::Active);

    let links = store.silences(false).await.expect("the read succeeds");
    let link = links
        .iter()
        .find(|link| link.am_id == "silence-1")
        .expect("the link is stored");

    assert_eq!(link.state, SilenceLifecycle::Expired);
    assert_eq!(link.discord_user_id, Some(UserId::new(3)));
    assert!(
        link.origin_message.is_some(),
        "the permalink is the one thing Alertmanager has nowhere to keep"
    );
}

async fn forum_tags_are_replaced_wholesale(store: &dyn Store) {
    let channel = ChannelId::new(300);
    let tag = |name: &str, id: u64| ForumTag {
        channel_id: channel,
        name: name.to_owned(),
        id: crate::TagId::new(id),
        moderated: false,
        synced_at: at(0),
    };

    store
        .sync_forum_tags(channel, &[tag("firing", 1), tag("acked", 2)])
        .await
        .expect("the cache is written");

    store
        .sync_forum_tags(channel, &[tag("firing", 1)])
        .await
        .expect("the cache is rewritten");

    let cached = store.forum_tags(channel).await.expect("the read succeeds");

    assert_eq!(
        cached.len(),
        1,
        "a tag a human deleted has to leave the cache, or the next apply fails on an id that is \
         no longer there"
    );
    assert_eq!(cached[0].name, "firing");
}

async fn queries_filter_paginate_and_match(store: &dyn Store) {
    for (index, severity) in ["critical", "warning", "info"].iter().enumerate() {
        let mut alert = alert(&format!("bbbb000{index}"), AlertStatus::Firing, severity);
        alert.labels = labels(&[
            ("alertname", "QueryAlert"),
            ("severity", severity),
            ("namespace", "queries"),
            ("pod", &format!("api-{index}")),
        ]);
        ingest(store, alert, at(0)).await;
    }

    let by_severity = store
        .query_alerts(&AlertQuery {
            min_severity: Some(dam_core::Severity::Warning),
            matchers: vec![QueryMatcher {
                name: "namespace".to_owned(),
                op: MatchOp::Equal,
                value: "queries".to_owned(),
            }],
            limit: 10,
            ..AlertQuery::default()
        })
        .await
        .expect("the query succeeds");

    assert_eq!(
        by_severity.total, 2,
        "the severity floor is a comparison against a rank, not equality on a word"
    );

    let absent_label = store
        .query_alerts(&AlertQuery {
            matchers: vec![QueryMatcher {
                name: "team".to_owned(),
                op: MatchOp::NotEqual,
                value: String::new(),
            }],
            limit: 10,
            ..AlertQuery::default()
        })
        .await
        .expect("the query succeeds");

    assert_eq!(
        absent_label.total, 0,
        "an absent label matches the empty string, exactly as Alertmanager reads it"
    );

    let by_regex = store
        .query_alerts(&AlertQuery {
            matchers: vec![
                QueryMatcher {
                    name: "namespace".to_owned(),
                    op: MatchOp::Equal,
                    value: "queries".to_owned(),
                },
                QueryMatcher {
                    name: "pod".to_owned(),
                    op: MatchOp::RegexMatch,
                    value: "api-[01]".to_owned(),
                },
            ],
            limit: 1,
            ..AlertQuery::default()
        })
        .await
        .expect("the query succeeds");

    assert_eq!(
        by_regex.total, 2,
        "a regex matcher is anchored and evaluated with the domain's own semantics"
    );
    assert_eq!(
        by_regex.items.len(),
        1,
        "the page is the size that was asked for"
    );
    assert!(by_regex.has_more());

    let unparseable = store
        .query_alerts(&AlertQuery {
            matchers: vec![QueryMatcher {
                name: "not a label name".to_owned(),
                op: MatchOp::Equal,
                value: "x".to_owned(),
            }],
            limit: 10,
            ..AlertQuery::default()
        })
        .await;

    assert!(
        unparseable.is_err(),
        "a matcher that cannot be compiled is refused rather than dropped, because dropping it \
         widens the filter silently: {unparseable:?}"
    );
}

async fn the_reconciler_finds_what_alertmanager_dropped(store: &dyn Store) {
    let kept = Fingerprint::new("cccc0001").expect("the fingerprint is hexadecimal");
    let lost = Fingerprint::new("cccc0002").expect("the fingerprint is hexadecimal");

    ingest(
        store,
        alert("cccc0001", AlertStatus::Firing, "critical"),
        at(0),
    )
    .await;
    ingest(
        store,
        alert("cccc0002", AlertStatus::Firing, "critical"),
        at(0),
    )
    .await;

    // The second poll refreshes one of the two, which is what "last seen before the cutoff" is
    // measuring: Alertmanager still has it, and no longer has the other.
    ingest(
        store,
        alert("cccc0001", AlertStatus::Firing, "critical"),
        at(120),
    )
    .await;

    let missing = store
        .firing_not_in(std::slice::from_ref(&kept), at(60))
        .await
        .expect("the read succeeds");

    assert!(
        missing.iter().any(|record| *record.fingerprint() == lost),
        "an alert Alertmanager stopped reporting is how a lost resolve is found"
    );
    assert!(
        !missing.iter().any(|record| *record.fingerprint() == kept),
        "an alert Alertmanager still reports is not missing"
    );
}

async fn pruning_is_bounded_and_says_so(store: &dyn Store) {
    for index in 0..3 {
        store
            .append_audit(&AuditEntry::ok(
                "prune.fixture",
                Some(UserId::new(1)),
                at(-86_400 * 400 + index),
            ))
            .await
            .expect("the entry is written");
    }

    let report = store
        .prune(
            &RetentionPolicy {
                batch_limit: 2,
                ..RetentionPolicy::default()
            },
            at(0),
        )
        .await
        .expect("the sweep succeeds");

    assert_eq!(report.audit, 2, "the batch limit bounds one pass");
    assert!(
        report.more,
        "a pass that filled its batch has left rows behind, and the caller schedules another"
    );

    let rest = store
        .prune(
            &RetentionPolicy {
                batch_limit: 100,
                ..RetentionPolicy::default()
            },
            at(0),
        )
        .await
        .expect("the sweep succeeds");

    assert_eq!(rest.audit, 1);
    assert!(!rest.more);
}

async fn the_audit_log_accepts_every_result(store: &dyn Store) {
    let denied = AuditEntry::denied("silence.create", Some(UserId::new(4)), at(0));

    store
        .append_audit(&denied)
        .await
        .expect("a denial is recorded");

    let mut detailed = AuditEntry::ok("silence.create", Some(UserId::new(4)), at(1));
    detailed.guild_id = Some(GuildId::new(1));
    detailed.subject = Some("aaaa0001".to_owned());
    detailed.detail = serde_json::json!({ "am_id": "silence-1", "duration": "4h" });

    store
        .append_audit(&detailed)
        .await
        .expect("the resulting silence id is recorded with it");
}

/// Empties the queue of everything a case left behind, so the next one starts from nothing.
///
/// Cases share one database and the outbox is the only table they would otherwise see each
/// other's rows in, because a claim without a lane takes whatever is oldest.
async fn drain(store: &dyn Store, worker: &str) {
    let worker = WorkerId::new(worker);

    loop {
        let items = store
            .claim_outbox(
                &worker,
                ClaimRequest {
                    lane: None,
                    lease_secs: 3600,
                    limit: 100,
                },
                Utc::now() + Duration::days(3650),
            )
            .await
            .expect("the claim succeeds");

        if items.is_empty() {
            return;
        }

        for item in items {
            store
                .fail_outbox(&worker, item.id, "drained", None)
                .await
                .expect("the item is abandoned");
        }
    }
}
