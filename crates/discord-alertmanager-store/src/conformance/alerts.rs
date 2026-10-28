//! Cases for ingest, alert queries and silence sync.

use dam_core::{
    AlertStatus, AmState, DedupeKey, EventKind, EventSource, Fingerprint, MatchOp,
    NotificationState,
};

use crate::alerts::{AlertQuery, IngestBatch, QueryMatcher, SilenceLifecycle, SilenceState};
use crate::ids::{MessageId, UserId};
use crate::logic::suppression_map;
use crate::{SilenceLink, Store};

use super::{alert, at, card_for, ingest, labels, record_post, route_for};

pub(super) async fn ingest_records_what_changed(store: &dyn Store) {
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

pub(super) async fn ingest_discards_a_redelivery(store: &dyn Store) {
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

pub(super) async fn a_re_fire_reuses_the_row_and_counts_the_flap(store: &dyn Store) {
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

/// A re-fire long after the resolution is a new occurrence, and its flap count starts again.
///
/// The window is the store's own setting rather than a per-call one, so this asserts the
/// behaviour a deployment sees rather than one a test could dial in: the conformance store takes
/// the configuration's default of half an hour, and these timestamps are a day apart.
pub(super) async fn a_re_fire_after_the_regroup_window_restarts_the_flap_count(store: &dyn Store) {
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
        refire.deltas[0].flap_count, 0,
        "a day of quiet is a new occurrence, not a flap on the last one"
    );

    let record = store
        .alert(&Fingerprint::new("aaaa0020").expect("the fingerprint is hexadecimal"))
        .await
        .expect("the read succeeds")
        .expect("the row exists");

    assert_eq!(record.flap_count, 0, "the count is persisted, not derived");
    assert_eq!(record.resolved_at, None, "a firing alert has not resolved");
}

/// A reconciler pass does not erase the group key the webhook delivered.
///
/// Alertmanager's alert list carries no group key. Reading its absence as a change would record
/// an update on every poll and move the alert off its group's card on a per-group route.
pub(super) async fn a_poll_keeps_the_group_key_a_webhook_delivered(store: &dyn Store) {
    let group = dam_core::GroupKey::new("{}:{alertname=\"TestAlert\"}");

    let mut delivered = IngestBatch::new(
        EventSource::Webhook,
        vec![alert("aaaa0030", AlertStatus::Firing, "critical")],
        at(0),
    );
    delivered.group_key = Some(group.clone());
    store
        .ingest_batch(&delivered)
        .await
        .expect("the webhook batch is accepted");

    let polled = store
        .ingest_batch(&IngestBatch::new(
            EventSource::Reconciler,
            vec![alert("aaaa0030", AlertStatus::Firing, "critical")],
            at(60),
        ))
        .await
        .expect("the reconciler batch is accepted");

    assert!(
        polled.deltas.is_empty(),
        "a poll that learns nothing new is a redelivery: {:?}",
        polled.deltas
    );
    assert_eq!(polled.duplicates, 1);

    let record = store
        .alert(&Fingerprint::new("aaaa0030").expect("the fingerprint is hexadecimal"))
        .await
        .expect("the read succeeds")
        .expect("the row exists");

    assert_eq!(record.alert.group_key, Some(group));
}

/// One read returns the firing alerts under one name, and nothing resolved or differently named.
pub(super) async fn firing_alerts_are_found_by_name(store: &dyn Store) {
    let named = |fingerprint: &str, name: Option<&str>, status: AlertStatus| {
        let mut alert = alert(fingerprint, status, "warning");
        let mut pairs = vec![("namespace", "payments"), ("pod", fingerprint)];
        if let Some(name) = name {
            pairs.push(("alertname", name));
        }
        alert.labels = labels(&pairs);
        alert
    };

    for alert in [
        named("aaaa0040", Some("SiblingAlert"), AlertStatus::Firing),
        named("aaaa0041", Some("SiblingAlert"), AlertStatus::Firing),
        named("aaaa0042", Some("SiblingAlert"), AlertStatus::Resolved),
        named("aaaa0043", Some("OtherAlert"), AlertStatus::Firing),
        named("aaaa0044", None, AlertStatus::Firing),
    ] {
        ingest(store, alert, at(0)).await;
    }

    let fingerprints = |records: Vec<crate::AlertRecord>| -> Vec<String> {
        records
            .iter()
            .map(|record| record.fingerprint().as_str().to_owned())
            .collect()
    };

    assert_eq!(
        fingerprints(
            store
                .firing_alerts_named(Some("SiblingAlert"))
                .await
                .expect("the read succeeds")
        ),
        vec!["aaaa0040".to_owned(), "aaaa0041".to_owned()],
        "firing, under this name, in fingerprint order"
    );

    assert!(
        fingerprints(
            store
                .firing_alerts_named(None)
                .await
                .expect("the read succeeds")
        )
        .contains(&"aaaa0044".to_owned()),
        "an alert with no name is found by having none"
    );
}

pub(super) async fn silences_sync_into_deltas(store: &dyn Store) {
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

pub(super) async fn queries_filter_paginate_and_match(store: &dyn Store) {
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

pub(super) async fn the_reconciler_finds_what_alertmanager_dropped(store: &dyn Store) {
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

/// Runs after [`queries_filter_paginate_and_match`], whose `bbbb000x` alerts it filters.
///
/// Only an alert whose per-alert card is in the state asked for passes, and an alert with no card
/// at all never does.
pub(super) async fn a_query_on_card_state_joins_the_cards(store: &dyn Store) {
    let fingerprint = Fingerprint::new("bbbb0000").expect("the fingerprint is hexadecimal");
    let key = DedupeKey::per_alert(&fingerprint);
    let route = route_for(store, "queries", 4_242).await;
    card_for(store, route, 4_242, &key).await;
    record_post(store, "query-worker", &key, MessageId::new(424_242)).await;

    for (state, expected) in [
        (NotificationState::Firing, 1),
        (NotificationState::Acked, 0),
    ] {
        let by_card = store
            .query_alerts(&AlertQuery {
                statuses: vec![AlertStatus::Firing],
                matchers: vec![QueryMatcher {
                    name: "namespace".to_owned(),
                    op: MatchOp::Equal,
                    value: "queries".to_owned(),
                }],
                notification_state: Some(state),
                limit: 10,
                ..AlertQuery::default()
            })
            .await
            .expect("the query succeeds");

        assert_eq!(
            by_card.total, expected,
            "only the alert whose card is {state} passes a filter on {state} cards"
        );
    }
}
