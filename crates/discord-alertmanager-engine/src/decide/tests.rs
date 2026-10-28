use chrono::TimeZone;
use dam_core::{
    Alert, AlertStatus, AmState, Annotations, EventSource, Fingerprint, LabelName, Labels,
};
use dam_core::{IdentityPolicy, MatcherSet};
use dam_store::{
    ChannelId, ForumTag, GuildId, Mentions, NotificationId, RouteId, RouteSource, RouteTarget,
    StateTags, TagId, ThreadPolicy,
};

use super::*;

fn now() -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 5, 1, 12, 0, 0).unwrap()
}

fn labels(pairs: &[(&str, &str)]) -> Labels {
    pairs
        .iter()
        .map(|(name, value)| {
            (
                LabelName::new(*name).expect("test label name is valid"),
                (*value).to_owned(),
            )
        })
        .collect()
}

fn delta(kind: EventKind, status: AlertStatus, am_state: AmState) -> AlertDelta {
    AlertDelta {
        kind,
        source: EventSource::Webhook,
        alert: Alert {
            fingerprint: Fingerprint::new("0123456789abcdef").expect("hex is a fingerprint"),
            labels: labels(&[("alertname", "PodDown"), ("severity", "critical")]),
            annotations: Annotations::new(),
            starts_at: now(),
            ends_at: None,
            generator_url: None,
            status,
            am_state,
            silenced_by: Vec::new(),
            inhibited_by: Vec::new(),
            group_key: None,
        },
        flap_count: 0,
        observed_at: now(),
    }
}

fn text_route() -> Route {
    Route {
        id: RouteId::new(1),
        guild_id: GuildId::new(1),
        name: "all".to_owned(),
        matcher_source: String::new(),
        matchers: MatcherSet::default(),
        min_severity: None,
        target: RouteTarget::Text {
            channel: ChannelId::new(100),
            thread: ThreadPolicy::default(),
        },
        group_strategy: GroupStrategy::PerAlert,
        mentions: Mentions {
            roles: vec![dam_store::RoleId::new(5)],
            users: Vec::new(),
            min_severity: Some(Severity::Critical),
        },
        escalation: None,
        priority: 100,
        continue_to_next: false,
        source: RouteSource::Config,
        enabled: true,
        created_by: None,
        created_at: now(),
    }
}

fn forum_route() -> Route {
    Route {
        target: RouteTarget::Forum {
            channel: ChannelId::new(200),
            policy: ForumPolicy {
                title_template: "{{ labels.alertname }}".to_owned(),
                manage_tags: true,
                state_tags: StateTags {
                    firing: "firing".to_owned(),
                    acked: "acked".to_owned(),
                    silenced: "silenced".to_owned(),
                    resolved: "resolved".to_owned(),
                },
                severity_tags: true,
                label_tags: Vec::new(),
                default_tag: None,
                auto_archive_minutes: 10_080,
                archive_on_resolve: true,
                lock_on_resolve: false,
                pin_min_severity: Some(Severity::Critical),
                max_pinned: 5,
                bump_on_state_change: true,
            },
        },
        ..text_route()
    }
}

fn forum_tags() -> Vec<ForumTag> {
    ["firing", "acked", "silenced", "resolved", "critical"]
        .iter()
        .enumerate()
        .map(|(index, name)| ForumTag {
            channel_id: ChannelId::new(200),
            name: (*name).to_owned(),
            id: TagId::new(index as u64 + 1),
            moderated: false,
            synced_at: now(),
        })
        .collect()
}

fn card(state: NotificationState, channel: ChannelId, key: &DedupeKey) -> Notification {
    Notification {
        id: NotificationId::new(7),
        dedupe_key: key.clone(),
        fingerprint: Fingerprint::new("deadbeef").expect("the fingerprint is hexadecimal"),
        route_id: RouteId::new(1),
        guild_id: GuildId::new(1),
        channel_id: channel,
        message_id: Some(dam_store::MessageId::new(11)),
        thread_id: None,
        state,
        render_hash: None,
        applied_tags: Vec::new(),
        tags_hash: None,
        pinned: false,
        archived: false,
        responded_at: None,
        escalated_at: None,
        supersedes: None,
        resolved_at: None,
        reply_count: 0,
        created_at: now(),
        updated_at: now(),
    }
}

/// A storm state in which no route is over its threshold.
///
/// What every test that is not about storms wants, and the reason it is a function rather
/// than a constant: the thresholds are the configuration's own, so a test that does care
/// reads the same numbers a deployment would.
fn quiet() -> StormState {
    StormState::empty(50, 20, Duration::seconds(60))
}

/// What the caller knows about an alert with no siblings.
fn known(acknowledged: bool) -> AlertContext<'static> {
    AlertContext {
        acknowledged,
        firing_siblings: &[],
    }
}

/// The key a per-alert route files this delta under, with no identity policy.
fn key_of(delta: &AlertDelta) -> DedupeKey {
    DedupeKey::per_alert(&delta.alert.fingerprint)
}

fn kinds(update: &CardUpdate) -> Vec<&'static str> {
    update
        .effects
        .iter()
        .map(|item| item.effect.kind())
        .collect()
}

/// A storm state in which the one route under test is well past its threshold.
fn storming() -> StormState {
    StormState::new(
        std::iter::once((RouteId::new(1), 500)).collect(),
        50,
        20,
        Duration::seconds(60),
    )
}

#[test]
fn a_storming_route_is_keyed_as_a_digest_whatever_it_was_configured_as() {
    let route = text_route();
    let delta = delta(EventKind::Fired, AlertStatus::Firing, AmState::Active);

    assert_eq!(
        dedupe_key(
            &delta,
            &route,
            &quiet(),
            &DecisionSettings::default(),
            now()
        ),
        key_of(&delta),
        "a quiet per-alert route keeps its own card per alert"
    );

    let key = dedupe_key(
        &delta,
        &route,
        &storming(),
        &DecisionSettings::default(),
        now(),
    );

    assert_ne!(key, key_of(&delta));
    assert!(
        key.as_str().starts_with("d:"),
        "past the threshold the route rolls one card per window: {key:?}"
    );
}

#[test]
fn a_card_that_already_exists_survives_its_route_digesting() {
    let route = text_route();
    let channel = ChannelId::new(100);
    let snapshot = RoutingSnapshot::new(vec![route], Vec::new(), Vec::new());

    let key =
        DedupeKey::per_alert(&Fingerprint::new("0123456789abcdef").expect("hex is a fingerprint"));
    let mut existing = ExistingCards::new();
    existing.insert(
        (channel, key.clone()),
        card(NotificationState::Firing, channel, &key),
    );

    let decision = decide(
        &delta(EventKind::Resolved, AlertStatus::Resolved, AmState::Active),
        &snapshot,
        &storming(),
        &existing,
        &known(false),
        &DecisionSettings::default(),
        now(),
    );

    assert!(
        decision.new_cards.is_empty(),
        "an alert with a card of its own is not folded into the digest"
    );
    assert_eq!(
        decision.updates.len(),
        1,
        "and its card still learns that the alert resolved, rather than freezing on `firing`"
    );
    assert_eq!(decision.updates[0].state, Some(NotificationState::Resolved));
}

#[test]
fn every_alert_in_one_window_lands_on_one_digest_card() {
    let route = text_route();
    let settings = DecisionSettings::default();
    let first = delta(EventKind::Fired, AlertStatus::Firing, AmState::Active);
    let mut second = first.clone();
    second.alert.fingerprint = Fingerprint::new("fedcba9876543210").expect("hex is a fingerprint");

    assert_eq!(
        dedupe_key(&first, &route, &storming(), &settings, now()),
        dedupe_key(&second, &route, &storming(), &settings, now()),
        "two alerts inside one window are one card, which is the whole point of a digest"
    );
}

#[test]
fn a_digest_window_rolls_over() {
    let route = text_route();
    let settings = DecisionSettings::default();
    let delta = delta(EventKind::Fired, AlertStatus::Firing, AmState::Active);

    assert_ne!(
        dedupe_key(&delta, &route, &storming(), &settings, now()),
        dedupe_key(
            &delta,
            &route,
            &storming(),
            &settings,
            now() + settings.digest_window,
        ),
        "the next window is the next card, not an ever-growing one"
    );
}

#[test]
fn a_re_fire_long_after_the_card_resolved_replaces_it() {
    let snapshot = RoutingSnapshot::new(vec![text_route()], Vec::new(), Vec::new());
    let channel = ChannelId::new(100);
    let settings = DecisionSettings::default();

    let delta = delta(EventKind::Fired, AlertStatus::Firing, AmState::Active);
    let key = key_of(&delta);
    let mut resolved = card(NotificationState::Resolved, channel, &key);
    resolved.resolved_at = Some(now() - settings.regroup_window - Duration::seconds(1));

    let mut existing = ExistingCards::new();
    existing.insert((channel, key.clone()), resolved);

    let decision = decide(
        &delta,
        &snapshot,
        &quiet(),
        &existing,
        &known(false),
        &settings,
        now(),
    );

    assert!(
        decision.updates.is_empty(),
        "a card resolved longer ago than the window is not turned red again"
    );
    assert_eq!(decision.new_cards.len(), 1);
    assert_eq!(
        decision.new_cards[0].card.supersedes,
        Some(NotificationId::new(7)),
        "the replacement carries the link to what it replaced"
    );
    assert_eq!(
        decision.new_cards[0].card.dedupe_key, key,
        "and takes the key, so the next change finds it with one lookup"
    );
    assert!(
        decision.new_cards[0].mention,
        "a new occurrence pages again"
    );
}

#[test]
fn a_re_fire_inside_the_window_re_arms_the_card() {
    let snapshot = RoutingSnapshot::new(vec![forum_route()], Vec::new(), forum_tags());
    let channel = ChannelId::new(200);
    let settings = DecisionSettings::default();

    let delta = delta(EventKind::Fired, AlertStatus::Firing, AmState::Active);
    let key = key_of(&delta);
    let mut resolved = card(NotificationState::Resolved, channel, &key);
    resolved.resolved_at = Some(now() - settings.regroup_window + Duration::seconds(1));
    resolved.archived = true;

    let mut existing = ExistingCards::new();
    existing.insert((channel, key), resolved);

    let decision = decide(
        &delta,
        &snapshot,
        &quiet(),
        &existing,
        &known(false),
        &settings,
        now(),
    );

    assert!(decision.new_cards.is_empty(), "no second card");
    let update = &decision.updates[0];
    assert_eq!(update.state, Some(NotificationState::Firing));
    assert_eq!(kinds(update)[0], "set_flags", "the post is reopened first");

    let note = update
        .effects
        .iter()
        .find_map(|item| match &item.effect {
            Effect::ThreadNote { text, .. } => Some(text.clone()),
            _ => None,
        })
        .expect("a re-armed post says so");
    assert_eq!(note, "Firing again.");
}

#[test]
fn a_card_re_armed_for_a_suppressed_alert_comes_back_silenced() {
    let snapshot = RoutingSnapshot::new(vec![text_route()], Vec::new(), Vec::new());
    let channel = ChannelId::new(100);
    let delta = delta(EventKind::Fired, AlertStatus::Firing, AmState::Suppressed);
    let key = key_of(&delta);

    let mut existing = ExistingCards::new();
    existing.insert(
        (channel, key.clone()),
        card(NotificationState::Resolved, channel, &key),
    );

    let decision = decide(
        &delta,
        &snapshot,
        &quiet(),
        &existing,
        &known(false),
        &DecisionSettings::default(),
        now(),
    );

    assert_eq!(decision.updates[0].state, Some(NotificationState::Silenced));
}

#[test]
fn a_resolved_card_with_no_recorded_resolution_is_re_armed_rather_than_replaced() {
    let snapshot = RoutingSnapshot::new(vec![text_route()], Vec::new(), Vec::new());
    let channel = ChannelId::new(100);
    let delta = delta(EventKind::Fired, AlertStatus::Firing, AmState::Active);
    let key = key_of(&delta);

    let mut existing = ExistingCards::new();
    existing.insert(
        (channel, key.clone()),
        card(NotificationState::Resolved, channel, &key),
    );

    let decision = decide(
        &delta,
        &snapshot,
        &quiet(),
        &existing,
        &known(false),
        &DecisionSettings::default(),
        now(),
    );

    assert_eq!(decision.new_cards, Vec::<PlannedCard>::new());
    assert_eq!(decision.updates[0].state, Some(NotificationState::Firing));
}

#[test]
fn a_firing_alert_re_arms_its_resolved_card_whatever_the_event_says() {
    // A reconciler pass sees the alert firing while its card says resolved: the resolve and
    // the re-fire both happened, and the webhook for the second was lost. An annotation
    // update is what arrives, and it still has to put the card right.
    let snapshot = RoutingSnapshot::new(vec![text_route()], Vec::new(), Vec::new());
    let channel = ChannelId::new(100);
    let delta = delta(EventKind::Updated, AlertStatus::Firing, AmState::Active);
    let key = key_of(&delta);

    let mut existing = ExistingCards::new();
    existing.insert(
        (channel, key.clone()),
        card(NotificationState::Resolved, channel, &key),
    );

    let decision = decide(
        &delta,
        &snapshot,
        &quiet(),
        &existing,
        &known(false),
        &DecisionSettings::default(),
        now(),
    );

    assert_eq!(decision.updates[0].state, Some(NotificationState::Firing));
}

#[test]
fn a_resolved_alert_resolves_its_open_card_whatever_the_event_says() {
    let snapshot = RoutingSnapshot::new(vec![text_route()], Vec::new(), Vec::new());
    let channel = ChannelId::new(100);
    let delta = delta(EventKind::Updated, AlertStatus::Resolved, AmState::Active);
    let key = key_of(&delta);

    let mut existing = ExistingCards::new();
    existing.insert(
        (channel, key.clone()),
        card(NotificationState::Firing, channel, &key),
    );

    let decision = decide(
        &delta,
        &snapshot,
        &quiet(),
        &existing,
        &known(false),
        &DecisionSettings::default(),
        now(),
    );

    assert_eq!(decision.updates[0].state, Some(NotificationState::Resolved));
}

#[test]
fn a_churned_label_lands_on_the_card_already_showing_the_alert() {
    let snapshot = RoutingSnapshot::new(vec![forum_route()], Vec::new(), forum_tags());
    let channel = ChannelId::new(200);
    let settings = DecisionSettings {
        identity: IdentityPolicy::new(["pod"]),
        ..DecisionSettings::default()
    };

    let mut first = delta(EventKind::Fired, AlertStatus::Firing, AmState::Active);
    first.alert.labels = labels(&[
        ("alertname", "PodDown"),
        ("severity", "critical"),
        ("pod", "web-1"),
    ]);
    let mut second = first.clone();
    second.alert.fingerprint = Fingerprint::new("fedcba9876543210").expect("hex");
    second.alert.labels = labels(&[
        ("alertname", "PodDown"),
        ("severity", "critical"),
        ("pod", "web-2"),
    ]);

    let key = dedupe_key(&first, &forum_route(), &quiet(), &settings, now());
    assert_eq!(
        key,
        dedupe_key(&second, &forum_route(), &quiet(), &settings, now()),
        "one identity, one key"
    );

    let mut existing = ExistingCards::new();
    existing.insert(
        (channel, key.clone()),
        card(NotificationState::Resolved, channel, &key),
    );

    let decision = decide(
        &second,
        &snapshot,
        &quiet(),
        &existing,
        &known(false),
        &settings,
        now(),
    );

    assert!(
        decision.new_cards.is_empty(),
        "the replacement pod re-arms the card rather than posting its own"
    );
    assert_eq!(decision.updates[0].state, Some(NotificationState::Firing));
    assert_eq!(
        decision.updates[0].fingerprint, second.alert.fingerprint,
        "and the card shows the alert that is firing now"
    );
}

#[test]
fn a_card_stays_firing_while_another_alert_behind_it_still_fires() {
    let snapshot = RoutingSnapshot::new(vec![forum_route()], Vec::new(), forum_tags());
    let channel = ChannelId::new(200);
    let settings = DecisionSettings {
        identity: IdentityPolicy::new(["pod"]),
        ..DecisionSettings::default()
    };

    let mut gone = delta(EventKind::Resolved, AlertStatus::Resolved, AmState::Active);
    gone.alert.labels = labels(&[
        ("alertname", "PodDown"),
        ("severity", "critical"),
        ("pod", "web-1"),
    ]);
    let mut live = gone.alert.clone();
    live.fingerprint = Fingerprint::new("fedcba9876543210").expect("hex");
    live.status = AlertStatus::Firing;
    live.labels = labels(&[
        ("alertname", "PodDown"),
        ("severity", "critical"),
        ("pod", "web-2"),
    ]);

    let key = dedupe_key(&gone, &forum_route(), &quiet(), &settings, now());
    let mut existing = ExistingCards::new();
    existing.insert(
        (channel, key.clone()),
        card(NotificationState::Firing, channel, &key),
    );

    let siblings = [live.clone()];
    let decision = decide(
        &gone,
        &snapshot,
        &quiet(),
        &existing,
        &AlertContext {
            acknowledged: false,
            firing_siblings: &siblings,
        },
        &settings,
        now(),
    );

    let update = &decision.updates[0];
    assert_eq!(update.state, None, "the card does not resolve");
    assert_eq!(
        update.fingerprint, live.fingerprint,
        "it shows the one still firing"
    );
    assert_eq!(
        kinds(update),
        vec!["edit_card", "set_tags"],
        "and takes that alert's tags, with no note or pin for a move that did not happen"
    );

    let alone = decide(
        &gone,
        &snapshot,
        &quiet(),
        &existing,
        &known(false),
        &settings,
        now(),
    );

    assert_eq!(
        alone.updates[0].state,
        Some(NotificationState::Resolved),
        "the last one to resolve resolves the card"
    );
}

#[test]
fn siblings_are_the_other_alerts_of_one_identity() {
    let policy = IdentityPolicy::new(["pod"]);
    let alert = |fingerprint: &str, pod: &str, namespace: &str| {
        let mut alert = delta(EventKind::Fired, AlertStatus::Firing, AmState::Active).alert;
        alert.fingerprint = Fingerprint::new(fingerprint).expect("hex");
        alert.labels = labels(&[
            ("alertname", "PodDown"),
            ("namespace", namespace),
            ("pod", pod),
        ]);
        alert
    };

    let this = alert("aa", "web-1", "prod");
    let siblings = identity_siblings(
        &policy,
        &this,
        [
            this.clone(),
            alert("bb", "web-2", "prod"),
            alert("cc", "web-3", "staging"),
        ],
    );

    let found: Vec<&str> = siblings
        .iter()
        .map(|alert| alert.fingerprint.as_str())
        .collect();
    assert_eq!(
        found,
        vec!["bb"],
        "itself and another namespace are not siblings"
    );
}

#[test]
fn a_first_card_supersedes_nothing() {
    let snapshot = RoutingSnapshot::new(vec![text_route()], Vec::new(), Vec::new());
    let delta = delta(EventKind::Fired, AlertStatus::Firing, AmState::Active);

    let decision = decide(
        &delta,
        &snapshot,
        &quiet(),
        &ExistingCards::new(),
        &known(false),
        &DecisionSettings::default(),
        now(),
    );

    assert_eq!(decision.new_cards[0].card.supersedes, None);
}

#[test]
fn a_resolved_forum_post_archives_on_the_configured_window() {
    let route = forum_route();
    let channel = ChannelId::new(200);
    let snapshot = RoutingSnapshot::new(vec![route], Vec::new(), forum_tags());

    let key =
        DedupeKey::per_alert(&Fingerprint::new("0123456789abcdef").expect("hex is a fingerprint"));
    let mut existing = ExistingCards::new();
    existing.insert(
        (channel, key.clone()),
        card(NotificationState::Firing, channel, &key),
    );

    let settings = DecisionSettings {
        archive_after_minutes: 4_320,
        ..DecisionSettings::default()
    };

    let decision = decide(
        &delta(EventKind::Resolved, AlertStatus::Resolved, AmState::Active),
        &snapshot,
        &quiet(),
        &existing,
        &known(false),
        &settings,
        now(),
    );

    let archive = decision.updates[0]
        .effects
        .iter()
        .find_map(|item| match item.effect {
            Effect::SetFlags {
                archived: true,
                auto_archive_minutes,
                ..
            } => Some(auto_archive_minutes),
            _ => None,
        })
        .expect("a resolved post on this route archives");

    assert_eq!(
        archive, 10_080,
        "the route's own window wins over the deployment's default"
    );
}

#[test]
fn a_resolved_post_left_open_drops_to_the_route_window_unlocked() {
    let mut route = forum_route();
    if let RouteTarget::Forum { policy, .. } = &mut route.target {
        policy.archive_on_resolve = false;
        policy.lock_on_resolve = true;
        policy.auto_archive_minutes = 60;
    }
    let channel = ChannelId::new(200);
    let snapshot = RoutingSnapshot::new(vec![route], Vec::new(), forum_tags());

    let key =
        DedupeKey::per_alert(&Fingerprint::new("0123456789abcdef").expect("hex is a fingerprint"));
    let mut existing = ExistingCards::new();
    existing.insert(
        (channel, key.clone()),
        card(NotificationState::Firing, channel, &key),
    );

    let decision = decide(
        &delta(EventKind::Resolved, AlertStatus::Resolved, AmState::Active),
        &snapshot,
        &quiet(),
        &existing,
        &known(false),
        &DecisionSettings::default(),
        now(),
    );

    let flags: Vec<_> = decision.updates[0]
        .effects
        .iter()
        .filter_map(|item| match item.effect {
            Effect::SetFlags {
                archived,
                locked,
                auto_archive_minutes,
                ..
            } => Some((archived, locked, auto_archive_minutes)),
            _ => None,
        })
        .collect();

    assert_eq!(
        flags,
        vec![(false, false, 60)],
        "the post stays open and editable, but no longer holds the week it held while firing"
    );
}

#[test]
fn a_re_fired_post_left_open_takes_back_the_maximum_window() {
    let mut route = forum_route();
    if let RouteTarget::Forum { policy, .. } = &mut route.target {
        policy.archive_on_resolve = false;
        policy.auto_archive_minutes = 60;
    }
    let channel = ChannelId::new(200);
    let snapshot = RoutingSnapshot::new(vec![route], Vec::new(), forum_tags());

    let key =
        DedupeKey::per_alert(&Fingerprint::new("0123456789abcdef").expect("hex is a fingerprint"));
    let mut existing = ExistingCards::new();
    existing.insert(
        (channel, key.clone()),
        card(NotificationState::Resolved, channel, &key),
    );

    let decision = decide(
        &delta(EventKind::Fired, AlertStatus::Firing, AmState::Active),
        &snapshot,
        &quiet(),
        &existing,
        &known(false),
        &DecisionSettings::default(),
        now(),
    );

    let flags: Vec<_> = decision.updates[0]
        .effects
        .iter()
        .filter_map(|item| match item.effect {
            Effect::SetFlags {
                archived,
                locked,
                auto_archive_minutes,
                ..
            } => Some((archived, locked, auto_archive_minutes)),
            _ => None,
        })
        .collect();

    assert_eq!(
        flags,
        vec![(false, false, 10_080)],
        "the post never archived, so only the window changes, back to Discord's maximum"
    );
}

#[test]
fn a_reopened_card_holds_the_maximum_archive_window() {
    let route = text_route();
    let channel = ChannelId::new(100);
    let snapshot = RoutingSnapshot::new(vec![route], Vec::new(), Vec::new());

    let key =
        DedupeKey::per_alert(&Fingerprint::new("0123456789abcdef").expect("hex is a fingerprint"));
    let mut archived = card(NotificationState::Resolved, channel, &key);
    archived.archived = true;

    let mut existing = ExistingCards::new();
    existing.insert((channel, key.clone()), archived);

    let decision = decide(
        &delta(EventKind::Fired, AlertStatus::Firing, AmState::Active),
        &snapshot,
        &quiet(),
        &existing,
        &known(false),
        &DecisionSettings::default(),
        now(),
    );

    let reopen = decision.updates[0]
        .effects
        .iter()
        .find_map(|item| match item.effect {
            Effect::SetFlags {
                archived: false,
                auto_archive_minutes,
                ..
            } => Some(auto_archive_minutes),
            _ => None,
        })
        .expect("an archived card is reopened before it is edited");

    assert_eq!(
        reopen, 10_080,
        "a card that is firing again holds Discord's maximum, whatever its route asks for"
    );
}

#[test]
fn a_new_firing_alert_produces_one_card_and_one_mention() {
    let snapshot = RoutingSnapshot::new(vec![text_route()], Vec::new(), Vec::new());
    let delta = delta(EventKind::Fired, AlertStatus::Firing, AmState::Active);

    let decision = decide(
        &delta,
        &snapshot,
        &quiet(),
        &ExistingCards::new(),
        &known(false),
        &DecisionSettings::default(),
        now(),
    );

    assert_eq!(decision.new_cards.len(), 1);
    assert_eq!(decision.updates, Vec::<CardUpdate>::new());
    assert!(decision.new_cards[0].mention);
    assert_eq!(decision.new_cards[0].card.state, NotificationState::Firing);
}

#[test]
fn an_alert_that_resolved_before_any_card_existed_posts_nothing() {
    let snapshot = RoutingSnapshot::new(vec![text_route()], Vec::new(), Vec::new());
    let delta = delta(EventKind::Resolved, AlertStatus::Resolved, AmState::Active);

    let decision = decide(
        &delta,
        &snapshot,
        &quiet(),
        &ExistingCards::new(),
        &known(false),
        &DecisionSettings::default(),
        now(),
    );

    assert!(decision.is_empty());
}

#[test]
fn an_ignored_alert_produces_no_card() {
    let rule = dam_store::IgnoreRule {
        id: dam_store::IgnoreId::new(1),
        scope: dam_store::IgnoreScope::Guild,
        guild_id: GuildId::new(1),
        channel_id: None,
        matcher_source: "alertname=PodDown".to_owned(),
        matchers: MatcherSet::parse("alertname=PodDown").expect("expression parses"),
        reason: "known flapper".to_owned(),
        created_by: dam_store::UserId::new(3),
        created_at: now(),
        expires_at: None,
        revoked_at: None,
    };
    let snapshot = RoutingSnapshot::new(vec![text_route()], vec![rule], Vec::new());
    let delta = delta(EventKind::Fired, AlertStatus::Firing, AmState::Active);

    let decision = decide(
        &delta,
        &snapshot,
        &quiet(),
        &ExistingCards::new(),
        &known(false),
        &DecisionSettings::default(),
        now(),
    );

    assert!(decision.is_empty());
}

#[test]
fn an_alert_silenced_before_it_arrives_posts_a_silenced_card_without_mentioning_anyone() {
    let snapshot = RoutingSnapshot::new(vec![text_route()], Vec::new(), Vec::new());
    let delta = delta(EventKind::Fired, AlertStatus::Firing, AmState::Suppressed);

    let decision = decide(
        &delta,
        &snapshot,
        &quiet(),
        &ExistingCards::new(),
        &known(false),
        &DecisionSettings::default(),
        now(),
    );

    assert_eq!(
        decision.new_cards[0].card.state,
        NotificationState::Silenced
    );
    assert!(!decision.new_cards[0].mention);
}

#[test]
fn an_annotation_change_edits_the_card_without_moving_it() {
    let route = text_route();
    let delta = delta(EventKind::Updated, AlertStatus::Firing, AmState::Active);
    let key = key_of(&delta);
    let mut existing = ExistingCards::new();
    existing.insert(
        (ChannelId::new(100), key.clone()),
        card(NotificationState::Firing, ChannelId::new(100), &key),
    );
    let snapshot = RoutingSnapshot::new(vec![route], Vec::new(), Vec::new());

    let decision = decide(
        &delta,
        &snapshot,
        &quiet(),
        &existing,
        &known(false),
        &DecisionSettings::default(),
        now(),
    );

    assert_eq!(decision.new_cards, Vec::<PlannedCard>::new());
    assert_eq!(decision.updates.len(), 1);
    assert_eq!(decision.updates[0].state, None);
    assert_eq!(kinds(&decision.updates[0]), vec!["edit_card"]);
}

#[test]
fn a_card_edit_waits_for_the_debounce() {
    let route = text_route();
    let delta = delta(EventKind::Updated, AlertStatus::Firing, AmState::Active);
    let key = key_of(&delta);
    let mut existing = ExistingCards::new();
    existing.insert(
        (ChannelId::new(100), key.clone()),
        card(NotificationState::Firing, ChannelId::new(100), &key),
    );
    let snapshot = RoutingSnapshot::new(vec![route], Vec::new(), Vec::new());
    let settings = DecisionSettings {
        debounce: Duration::seconds(5),
        ..DecisionSettings::default()
    };

    let decision = decide(
        &delta,
        &snapshot,
        &quiet(),
        &existing,
        &known(false),
        &settings,
        now(),
    );

    assert_eq!(
        decision.updates[0].effects[0].not_before,
        now() + Duration::seconds(5)
    );
}

#[test]
fn resolving_a_forum_post_retags_notes_unpins_disables_and_archives_it() {
    let route = forum_route();
    let delta = delta(EventKind::Resolved, AlertStatus::Resolved, AmState::Active);
    let key = key_of(&delta);
    let mut card = card(NotificationState::Firing, ChannelId::new(200), &key);
    card.pinned = true;
    let mut existing = ExistingCards::new();
    existing.insert((ChannelId::new(200), key.clone()), card);
    let snapshot = RoutingSnapshot::new(vec![route], Vec::new(), forum_tags());

    let decision = decide(
        &delta,
        &snapshot,
        &quiet(),
        &existing,
        &known(false),
        &DecisionSettings::default(),
        now(),
    );

    let update = &decision.updates[0];

    assert_eq!(update.state, Some(NotificationState::Resolved));
    assert_eq!(
        kinds(update),
        vec![
            "edit_card",
            "set_tags",
            "thread_note",
            "set_pinned",
            "disable_components",
            "set_flags"
        ]
    );
}

#[test]
fn a_resolved_forum_post_is_re_rendered_before_it_is_archived() {
    let route = forum_route();
    let delta = delta(EventKind::Resolved, AlertStatus::Resolved, AmState::Active);
    let key = key_of(&delta);
    let mut existing = ExistingCards::new();
    existing.insert(
        (ChannelId::new(200), key.clone()),
        card(NotificationState::Firing, ChannelId::new(200), &key),
    );
    let snapshot = RoutingSnapshot::new(vec![route], Vec::new(), forum_tags());

    let decision = decide(
        &delta,
        &snapshot,
        &quiet(),
        &existing,
        &known(false),
        &DecisionSettings::default(),
        now(),
    );

    let effects = &decision.updates[0].effects;
    let edit = effects
        .iter()
        .position(|item| matches!(item.effect, Effect::EditCard { .. }))
        .expect("the resolve re-renders the card");
    let archive = effects
        .iter()
        .position(|item| matches!(item.effect, Effect::SetFlags { archived: true, .. }))
        .expect("the policy archives on resolve");

    // The queue is claimed by `not_before` and then by insertion, so the archive being no
    // earlier and no sooner is what puts the re-render first. An archive that ran first would
    // leave the edit to reopen the post it had just closed, or — on a locked one — to be
    // dropped, and the card would go on saying "firing" after the alert had stopped.
    assert!(
        effects[archive].not_before >= effects[edit].not_before,
        "the archive does not come due before the re-render"
    );
    assert!(
        archive > edit,
        "the archive is queued behind the re-render, so equal deadlines still order it last"
    );
}

#[test]
fn an_update_that_changes_no_state_asks_for_no_pin() {
    let route = forum_route();
    let delta = delta(EventKind::Updated, AlertStatus::Firing, AmState::Active);
    let key = key_of(&delta);
    let mut shown = card(NotificationState::Firing, ChannelId::new(200), &key);
    shown.fingerprint = delta.alert.fingerprint.clone();
    let mut existing = ExistingCards::new();
    existing.insert((ChannelId::new(200), key.clone()), shown);
    let snapshot = RoutingSnapshot::new(vec![route], Vec::new(), forum_tags());

    let decision = decide(
        &delta,
        &snapshot,
        &quiet(),
        &existing,
        &known(false),
        &DecisionSettings::default(),
        now(),
    );

    let update = &decision.updates[0];

    // The card is critical, firing and unpinned, so the pin it wants and the pin it has
    // differ. Asking for it again on an update that moved nothing is how a channel Discord
    // will not give another pin to collects one refused request per poll, for as long as the
    // alert lasts.
    assert_eq!(update.state, None);
    assert_eq!(kinds(update), vec!["edit_card"]);
}

#[test]
fn a_card_switching_to_another_alert_is_retagged_without_moving() {
    let mut route = forum_route();
    if let RouteTarget::Forum { policy, .. } = &mut route.target {
        policy.label_tags = vec!["namespace".to_owned()];
    }

    let mut tags = forum_tags();
    for (id, name) in [(10, "billing"), (11, "payments")] {
        tags.push(ForumTag {
            channel_id: ChannelId::new(200),
            name: name.to_owned(),
            id: TagId::new(id),
            moderated: false,
            synced_at: now(),
        });
    }

    // The card was showing an alert from `billing`, which is the tag it carries; the alert now on
    // show is from `payments`.
    let mut delta = delta(EventKind::Updated, AlertStatus::Firing, AmState::Active);
    delta.alert.labels = labels(&[
        ("alertname", "PodDown"),
        ("severity", "critical"),
        ("namespace", "payments"),
    ]);
    let key = key_of(&delta);
    let mut shown = card(NotificationState::Firing, ChannelId::new(200), &key);
    shown.applied_tags = vec![TagId::new(1), TagId::new(5), TagId::new(10)];
    let mut existing = ExistingCards::new();
    existing.insert((ChannelId::new(200), key.clone()), shown);
    let snapshot = RoutingSnapshot::new(vec![route], Vec::new(), tags);

    let decision = decide(
        &delta,
        &snapshot,
        &quiet(),
        &existing,
        &known(false),
        &DecisionSettings::default(),
        now(),
    );

    let update = &decision.updates[0];

    // Nothing moves, so there is no note and no pin; only the tags follow the new alert.
    assert_eq!(update.state, None);
    assert_eq!(kinds(update), vec!["edit_card", "set_tags"]);

    let Effect::SetTags { tags, .. } = &update.effects[1].effect else {
        unreachable!("the second effect is the retag");
    };
    assert_eq!(
        tags,
        &vec![TagId::new(1), TagId::new(5), TagId::new(11)],
        "the state and severity tags stay, and the label tag is the new alert's"
    );
}

#[test]
fn an_archived_post_is_reopened_before_it_is_edited() {
    let route = forum_route();
    let delta = delta(EventKind::Fired, AlertStatus::Firing, AmState::Active);
    let key = key_of(&delta);
    let mut card = card(NotificationState::Resolved, ChannelId::new(200), &key);
    card.archived = true;
    let mut existing = ExistingCards::new();
    existing.insert((ChannelId::new(200), key.clone()), card);
    let snapshot = RoutingSnapshot::new(vec![route], Vec::new(), forum_tags());

    let decision = decide(
        &delta,
        &snapshot,
        &quiet(),
        &existing,
        &known(false),
        &DecisionSettings::default(),
        now(),
    );

    let update = &decision.updates[0];

    assert_eq!(update.state, Some(NotificationState::Firing));
    assert_eq!(kinds(update)[0], "set_flags");
    assert_eq!(kinds(update)[1], "edit_card");
}

#[test]
fn a_flap_keeps_an_acknowledgement_and_says_so_in_the_thread() {
    let route = forum_route();
    let mut delta = delta(EventKind::Fired, AlertStatus::Firing, AmState::Active);
    delta.flap_count = 2;
    let key = key_of(&delta);
    let mut existing = ExistingCards::new();
    existing.insert(
        (ChannelId::new(200), key.clone()),
        card(NotificationState::Resolved, ChannelId::new(200), &key),
    );
    let snapshot = RoutingSnapshot::new(vec![route], Vec::new(), forum_tags());

    let decision = decide(
        &delta,
        &snapshot,
        &quiet(),
        &existing,
        &known(true),
        &DecisionSettings::default(),
        now(),
    );

    let update = &decision.updates[0];

    assert_eq!(update.state, Some(NotificationState::Acked));
    let note = update
        .effects
        .iter()
        .find_map(|item| match &item.effect {
            Effect::ThreadNote { text, .. } => Some(text.clone()),
            _ => None,
        })
        .expect("a state change posts a note");
    assert_eq!(note, "Firing again. Still acknowledged.");
}

#[test]
fn an_orphaned_card_is_never_touched_again() {
    let route = text_route();
    let delta = delta(EventKind::Resolved, AlertStatus::Resolved, AmState::Active);
    let key = key_of(&delta);
    let mut existing = ExistingCards::new();
    existing.insert(
        (ChannelId::new(100), key.clone()),
        card(NotificationState::Orphaned, ChannelId::new(100), &key),
    );
    let snapshot = RoutingSnapshot::new(vec![route], Vec::new(), Vec::new());

    let decision = decide(
        &delta,
        &snapshot,
        &quiet(),
        &existing,
        &known(false),
        &DecisionSettings::default(),
        now(),
    );

    assert!(decision.is_empty());
}

#[test]
fn a_newly_matching_ignore_mutes_a_card_that_already_exists() {
    let rule = dam_store::IgnoreRule {
        id: dam_store::IgnoreId::new(1),
        scope: dam_store::IgnoreScope::Guild,
        guild_id: GuildId::new(1),
        channel_id: None,
        matcher_source: String::new(),
        matchers: MatcherSet::default(),
        reason: "maintenance window".to_owned(),
        created_by: dam_store::UserId::new(3),
        created_at: now(),
        expires_at: None,
        revoked_at: None,
    };
    let delta = delta(EventKind::Updated, AlertStatus::Firing, AmState::Active);
    let key = key_of(&delta);
    let mut existing = ExistingCards::new();
    existing.insert(
        (ChannelId::new(100), key.clone()),
        card(NotificationState::Firing, ChannelId::new(100), &key),
    );
    let snapshot = RoutingSnapshot::new(vec![text_route()], vec![rule], Vec::new());

    let decision = decide(
        &delta,
        &snapshot,
        &quiet(),
        &existing,
        &known(false),
        &DecisionSettings::default(),
        now(),
    );

    assert_eq!(decision.updates[0].state, Some(NotificationState::Ignored));
}

#[test]
fn label_tags_follow_the_policy_order_and_skip_absent_values() {
    let RouteTarget::Forum { mut policy, .. } = forum_route().target else {
        unreachable!("the fixture is a forum route");
    };
    policy.label_tags = vec![
        "team".to_owned(),
        "missing".to_owned(),
        "namespace".to_owned(),
    ];

    let names = label_tag_names(
        &policy,
        &labels(&[
            ("namespace", "payments api"),
            ("team", "a-very-long-team-name-indeed"),
            ("missing", ""),
        ]),
    );

    assert_eq!(
        names,
        vec!["a-very-long-team-nam".to_owned(), "payments-api".to_owned()],
        "values are cut to Discord's twenty characters, whitespace becomes a hyphen, and an \
         empty value names no tag"
    );
}
