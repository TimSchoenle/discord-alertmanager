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
//!
//! The cases live in one module per concern, the same split as this crate's own modules. [`run`]
//! fixes the order they run in, and the fixtures they share are defined below it.

mod alerts;
mod audit;
mod notifications;
mod outbox;
mod routing;

use chrono::{DateTime, Duration, TimeZone, Utc};
use dam_core::{
    Alert, AlertStatus, AmState, Annotations, DedupeKey, EventSource, Fingerprint, LabelName,
    Labels, MatcherSet, NotificationState,
};

use crate::alerts::IngestBatch;
use crate::ids::{ChannelId, GuildId, MessageId, RouteId, UserId, WorkerId};
use crate::notifications::NewNotification;
use crate::outbox::{AppliedEffect, ClaimRequest};
use crate::routing::{GroupStrategy, Mentions, Route, RouteSource, RouteTarget, ThreadPolicy};
use crate::{Decision, PlannedCard, Store};

/// Runs every case against one open store.
///
/// # Panics
///
/// On the first assertion the backend fails, with the case that failed.
pub async fn run(store: &dyn Store) {
    store.health().await.expect("the database answers");

    alerts::ingest_records_what_changed(store).await;
    alerts::ingest_discards_a_redelivery(store).await;
    alerts::a_re_fire_reuses_the_row_and_counts_the_flap(store).await;
    alerts::a_re_fire_after_the_regroup_window_restarts_the_flap_count(store).await;
    alerts::a_poll_keeps_the_group_key_a_webhook_delivered(store).await;
    alerts::firing_alerts_are_found_by_name(store).await;
    notifications::a_replacement_card_takes_the_key_of_the_card_it_replaced(store).await;
    notifications::a_resolved_card_remembers_when_until_it_re_arms(store).await;
    notifications::an_acknowledgement_reaches_every_card_showing_the_alert(store).await;
    notifications::an_escalation_is_claimed_once(store).await;
    routing::a_route_keeps_its_escalation_policy(store).await;
    notifications::a_decision_writes_the_card_and_its_post_together(store).await;
    notifications::a_second_card_for_one_key_in_one_channel_conflicts(store).await;
    outbox::one_item_is_claimed_by_exactly_one_worker(store).await;
    outbox::a_claim_respects_the_lane_and_the_delay(store).await;
    outbox::completing_writes_back_what_discord_returned(store).await;
    outbox::a_lost_lease_cannot_complete(store).await;
    outbox::a_failure_returns_the_item_or_abandons_it(store).await;
    outbox::an_expired_lease_becomes_claimable(store).await;
    notifications::two_acknowledgements_produce_one(store).await;
    notifications::only_the_first_reply_changes_the_card(store).await;
    notifications::a_deleted_card_releases_its_key(store).await;
    notifications::pinned_posts_are_counted_per_channel(store).await;
    outbox::an_edit_is_coalesced_and_a_note_is_not(store).await;
    outbox::two_cards_sharing_a_key_keep_their_own_edits(store).await;
    routing::ignore_rules_expire_and_revoke(store).await;
    routing::a_config_route_syncs_and_a_discord_route_collides(store).await;
    alerts::silences_sync_into_deltas(store).await;
    routing::forum_tags_are_replaced_wholesale(store).await;
    alerts::queries_filter_paginate_and_match(store).await;
    alerts::the_reconciler_finds_what_alertmanager_dropped(store).await;
    audit::pruning_is_bounded_and_says_so(store).await;
    routing::a_subscription_belongs_to_its_owner(store).await;
    audit::the_audit_log_accepts_every_result(store).await;

    // Last, because it finishes by emptying the queue: the case has to leave a card unposted to
    // prove the read skips one, and abandoning the queue is cheaper than reaching into it for the
    // single row that leaves behind.
    notifications::a_resync_reads_the_live_cards_of_its_own_routes(store).await;
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
    record_applied(
        store,
        worker,
        key,
        &AppliedEffect {
            message_id: Some(message),
            ..AppliedEffect::default()
        },
    )
    .await;
}

/// Records `applied` against the card queued under `key`, as [`record_post`] does a post.
///
/// # Panics
///
/// As [`record_post`].
async fn record_applied(store: &dyn Store, worker: &str, key: &DedupeKey, applied: &AppliedEffect) {
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
                .complete_outbox(&worker, item.id, applied)
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
