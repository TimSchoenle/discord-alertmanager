//! The one function that decides what an alert change does to the cards showing it.
//!
//! Pure over its arguments and total: no I/O, no clock, no randomness. Everything the pipeline
//! does to Discord is a consequence of what this returns, which is why it is the piece worth
//! testing exhaustively and the piece everything downstream can be written against.

use std::collections::HashMap;

use chrono::{DateTime, Duration, Utc};
use dam_core::{
    AlertDelta, DedupeKey, EventKind, Labels, NotificationState, Severity, Trigger, initial_state,
    next_state,
};
use dam_store::{
    CardUpdate, ChannelId, Decision, Effect, ForumPolicy, GroupStrategy, NewNotification,
    NewOutboxItem, Notification, NotificationId, PlannedCard, Route, RouteTarget,
};

use crate::routing::RoutingSnapshot;
use crate::storm::StormState;

/// Discord's cap on tags applied to one forum post.
const MAX_APPLIED_TAGS: usize = 5;

/// The longest Discord will leave a thread unarchived: one week.
///
/// What a firing alert holds, whatever its route asks for. An incident that runs into a second
/// day should not have its thread close underneath the people working it, and the route's own
/// window is about the quiet that follows rather than the incident itself.
const MAX_AUTO_ARCHIVE: u32 = 10_080;

/// The knobs the decision reads, all of them from the engine's configuration section.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DecisionSettings {
    /// How long a card edit waits before it is sent.
    ///
    /// Discord's per-channel edit limits are strict enough that an alert storm without a debounce
    /// produces nothing but rate-limit responses. Waiting a few seconds also collapses a burst of
    /// updates to one alert into a single edit carrying the last state.
    pub debounce: Duration,

    /// How long one digest card covers.
    pub digest_window: Duration,

    /// Minutes of inactivity after which a thread that is no longer firing archives.
    ///
    /// Only ever applied to a card that has stopped firing. A live one holds Discord's maximum,
    /// because the thread is where the incident is being worked.
    pub archive_after_minutes: u32,
}

impl Default for DecisionSettings {
    fn default() -> Self {
        Self {
            debounce: Duration::seconds(3),
            digest_window: Duration::minutes(5),
            archive_after_minutes: 1440,
        }
    }
}

/// What the caller already knows about the cards this change might touch.
///
/// Read from the store before the decision, so the decision itself stays pure. The key is the
/// pair a card is unique under, which is also the pair the database's unique index enforces.
pub type ExistingCards = HashMap<(ChannelId, DedupeKey), Notification>;

/// Decides what one accepted alert change does.
///
/// The three questions, in order: which routes want this alert, is any of them muted by an ignore
/// rule, and does each route already have a card for it. Everything else — mentions, tags, pins,
/// thread notes, archiving — follows from the answers.
///
/// `storm` decides only one of them, and it decides it before the rest: a route over its
/// threshold posts a new alert onto the window's rolling card rather than one of its own.
///
/// # Panics
///
/// Never: [`dedupe_keys`] returns at least one key for every route.
#[must_use]
pub fn decide(
    delta: &AlertDelta,
    snapshot: &RoutingSnapshot,
    storm: &StormState,
    existing: &ExistingCards,
    acknowledged: bool,
    settings: &DecisionSettings,
    now: DateTime<Utc>,
) -> Decision {
    let mut decision = Decision {
        at: now,
        ..Decision::default()
    };

    let severity = delta.alert.severity();
    let labels = &delta.alert.labels;

    for route in snapshot.resolve(labels, severity) {
        let channel = delivery_channel(&route.target);
        let ignore = snapshot.ignore_for(route.guild_id, channel, labels, now);
        let mut keys = dedupe_keys(delta, route, storm, settings, now);

        let held = keys
            .iter()
            .find_map(|key| existing.get(&(channel, key.clone())));

        if let Some(card) = held {
            if let Some(update) = update_for(
                delta,
                route,
                snapshot,
                card,
                ignore.is_some(),
                acknowledged,
                settings,
                now,
            ) {
                decision.updates.push(update);
            }

            continue;
        }

        // The last key rather than the first: with nothing to edit, a storming route posts into
        // its digest and a quiet one posts a card of its own.
        let key = keys.pop().expect("every route has at least one key");

        if let Some(planned) = creation_for(
            delta,
            route,
            channel,
            key,
            ignore.is_some(),
            severity,
            superseded(delta, channel, existing),
            now,
        ) {
            decision.new_cards.push(planned);
        }
    }

    decision
}

/// The card a new episode's card replaces, when the caller read one.
///
/// Only ever the immediately preceding episode. An alert that has flapped in and out for a month
/// has a chain of cards behind it, and a card linking to the one before it walks that chain one
/// step at a time, which is what somebody following it actually wants.
fn superseded(
    delta: &AlertDelta,
    channel: ChannelId,
    existing: &ExistingCards,
) -> Option<NotificationId> {
    let previous = delta.episode.checked_sub(1)?;
    let key = DedupeKey::per_alert(&delta.alert.fingerprint, previous);

    existing.get(&(channel, key)).map(|card| card.id)
}

/// The card to create for a route that has none, if one is warranted.
#[expect(
    clippy::too_many_arguments,
    reason = "every argument is an independent input to a pure function; bundling them into a \
              context struct would only move the same list one line up"
)]
fn creation_for(
    delta: &AlertDelta,
    route: &Route,
    channel: ChannelId,
    key: DedupeKey,
    ignored: bool,
    severity: Severity,
    supersedes: Option<NotificationId>,
    now: DateTime<Utc>,
) -> Option<PlannedCard> {
    // A muted alert gets no card at all. It still has its row and still answers `/alerts list`;
    // what an ignore removes is the Discord notification, and nothing else.
    if ignored {
        return None;
    }

    // An alert that resolved before any card existed is history, not news. Posting a green card
    // for something nobody was ever told about is noise, and it is the common shape of a webhook
    // arriving after a restart.
    if !delta.alert.is_firing() {
        return None;
    }

    let state = initial_state(delta.alert.status, delta.alert.am_state, false);

    Some(PlannedCard {
        card: NewNotification {
            dedupe_key: key,
            fingerprint: delta.alert.fingerprint.clone(),
            route_id: route.id,
            guild_id: route.guild_id,
            channel_id: channel,
            state,
            supersedes,
            created_at: now,
        },
        // Mentions happen on the way into firing and never again. A silenced first sighting does
        // not ping anyone either: somebody already decided this alert should be quiet.
        mention: state == NotificationState::Firing
            && delta.kind == EventKind::Fired
            && route.mentions_at(severity),
        not_before: now,
    })
}

/// The change to a card that already exists, if this delta changes anything about it.
#[expect(
    clippy::too_many_arguments,
    reason = "every argument is an independent input to a pure function; bundling them into a \
              context struct would only move the same list one line up"
)]
fn update_for(
    delta: &AlertDelta,
    route: &Route,
    snapshot: &RoutingSnapshot,
    card: &Notification,
    ignored: bool,
    acknowledged: bool,
    settings: &DecisionSettings,
    now: DateTime<Utc>,
) -> Option<CardUpdate> {
    // An orphaned card describes nothing that still exists in Discord. Editing it would fail
    // forever, and its replacement is posted under a new row.
    if card.state == NotificationState::Orphaned {
        return None;
    }

    let trigger = if ignored && card.state != NotificationState::Ignored {
        Some(Trigger::Ignored)
    } else if !ignored && card.state == NotificationState::Ignored {
        Some(Trigger::Unignored)
    } else {
        Trigger::from_event(delta.kind)
    };

    let state = trigger.and_then(|trigger| next_state(card.state, trigger, acknowledged));

    // An update with no transition still re-renders: the annotations on the card changed, and
    // that is what the card is for. A redelivery that changes neither is dropped upstream, so
    // reaching here with nothing to say is not a case worth optimising for.
    let effective = state.unwrap_or(card.state);
    let mut effects = Vec::new();

    // An archived thread rejects edits and tag changes, so reopening is a step in the plan rather
    // than a retry after the failure. A resolved alert that re-fires takes this path routinely.
    if card.archived {
        effects.push(immediate(
            Effect::SetFlags {
                notification: card.id,
                archived: false,
                locked: false,
                auto_archive_minutes: auto_archive_minutes(route, effective, settings),
            },
            card,
            now,
        ));
    }

    effects.push(NewOutboxItem {
        effect: Effect::EditCard {
            notification: card.id,
        },
        dedupe_key: card.dedupe_key.clone(),
        not_before: now + settings.debounce,
    });

    if let RouteTarget::Forum { channel, policy } = &route.target {
        effects.extend(forum_effects(
            delta, snapshot, card, *channel, policy, effective, state, now,
        ));
    }

    if state == Some(NotificationState::Resolved) {
        // A disabled control answers "why is nothing happening" before anyone asks it.
        effects.push(immediate(
            Effect::DisableComponents {
                notification: card.id,
            },
            card,
            now,
        ));

        if let RouteTarget::Forum { policy, .. } = &route.target
            && policy.archive_on_resolve
        {
            effects.push(immediate(
                Effect::SetFlags {
                    notification: card.id,
                    archived: true,
                    locked: policy.lock_on_resolve,
                    // The route's own window rather than a week: the incident is over, and a
                    // resolved post that lingers at the top of the index buries the ones that
                    // are not.
                    auto_archive_minutes: auto_archive_minutes(
                        route,
                        NotificationState::Resolved,
                        settings,
                    ),
                },
                card,
                now,
            ));
        }
    }

    Some(CardUpdate {
        id: card.id,
        fingerprint: delta.alert.fingerprint.clone(),
        state,
        effects,
    })
}

/// The tag, note and pin changes a forum post needs, given where it has just moved to.
#[expect(
    clippy::too_many_arguments,
    reason = "split out of the update plan purely to keep each function short, so it takes the \
              same inputs that plan already holds"
)]
fn forum_effects(
    delta: &AlertDelta,
    snapshot: &RoutingSnapshot,
    card: &Notification,
    channel: ChannelId,
    policy: &ForumPolicy,
    effective: NotificationState,
    state: Option<NotificationState>,
    now: DateTime<Utc>,
) -> Vec<NewOutboxItem> {
    let mut effects = Vec::new();

    if state.is_some() {
        let tags = desired_tags(
            snapshot,
            channel,
            policy,
            effective,
            delta.alert.severity(),
            &delta.alert.labels,
        );

        effects.push(immediate(
            Effect::SetTags {
                notification: card.id,
                tags,
            },
            card,
            now,
        ));

        // Editing a message does not bump a forum's activity sort; only a new message does.
        // Without this note a state change leaves the post buried exactly where it was.
        if policy.bump_on_state_change {
            effects.push(immediate(
                Effect::ThreadNote {
                    notification: card.id,
                    text: state_note(effective, delta),
                },
                card,
                now,
            ));
        }

        // Inside the state gate with the tags and the note, because the pin is decoration on the
        // same transition and an update that changes no state changes no pin. Outside it, a pin
        // Discord will not grant — its forums hold one pinned post — is asked for again on every
        // poll and every repeat of an alert that is going nowhere, for as long as the alert lasts.
        let wants_pin = effective.wants_pin()
            && policy
                .pin_min_severity
                .is_some_and(|floor| delta.alert.severity() >= floor);
        if wants_pin != card.pinned {
            effects.push(immediate(
                Effect::SetPinned {
                    notification: card.id,
                    pinned: wants_pin,
                },
                card,
                now,
            ));
        }
    }

    effects
}

/// Wraps an effect that has no reason to wait.
fn immediate(effect: Effect, card: &Notification, now: DateTime<Utc>) -> NewOutboxItem {
    NewOutboxItem {
        effect,
        dedupe_key: card.dedupe_key.clone(),
        not_before: now,
    }
}

/// The channel a route's cards live in.
///
/// A direct message has no channel until one is opened, so the user's own id stands in. It is
/// unique, it is stable, and it keeps the card's uniqueness constraint working without a second
/// nullable column that means "or a user".
#[must_use]
pub fn delivery_channel(target: &RouteTarget) -> ChannelId {
    match target {
        RouteTarget::Text { channel, .. } | RouteTarget::Forum { channel, .. } => *channel,
        RouteTarget::Thread { thread } => *thread,
        RouteTarget::Dm { user } => ChannelId::new(user.get()),
    }
}

/// Every key a card for this alert on this route could be under, least preferred last.
///
/// One key ordinarily. Two while a route is over its storm threshold, because a card that already
/// exists keeps its own key and only an alert without one joins the digest. The strategy says how
/// an operator wants the route to read; the threshold is the point at which Discord stops letting
/// it read that way at all, and one rolling card is what is left.
#[must_use]
pub fn dedupe_keys(
    delta: &AlertDelta,
    route: &Route,
    storm: &StormState,
    settings: &DecisionSettings,
    now: DateTime<Utc>,
) -> Vec<DedupeKey> {
    let configured = match route.group_strategy {
        GroupStrategy::PerGroup => delta.per_group_key(),
        // The per-alert key carries the episode, so an alert that re-fired after a whole regroup
        // window of quiet resolves to a key no card holds yet and is posted afresh.
        GroupStrategy::PerAlert => delta.per_alert_key(),
        GroupStrategy::Digest => return vec![digest_key(route, settings, now)],
    };

    if !storm.is_storming(route) {
        return vec![configured];
    }

    // Two, in this order, and the order is the point. An alert that already has a card goes on
    // being edited on it even while the route digests: freezing a live card halfway through an
    // incident because its route got busy would leave it saying "firing" long after the alert had
    // stopped. Only an alert with no card yet is folded into the digest, which is exactly the load
    // the digest exists to shed.
    vec![configured, digest_key(route, settings, now)]
}

/// The key a card takes when this alert reaches this route with no card of its own.
///
/// The last of [`dedupe_keys`], which is the digest while a route is storming and its configured
/// key otherwise.
///
/// # Panics
///
/// Never: [`dedupe_keys`] returns at least one key for every route.
#[must_use]
pub fn dedupe_key(
    delta: &AlertDelta,
    route: &Route,
    storm: &StormState,
    settings: &DecisionSettings,
    now: DateTime<Utc>,
) -> DedupeKey {
    dedupe_keys(delta, route, storm, settings, now)
        .pop()
        .expect("every route has at least one key")
}

/// The rolling key one window on one route shares.
fn digest_key(route: &Route, settings: &DecisionSettings, now: DateTime<Utc>) -> DedupeKey {
    let window = settings.digest_window.num_seconds().max(1);
    let start = now.timestamp() - now.timestamp().rem_euclid(window);

    DedupeKey::digest(
        route.id.get(),
        DateTime::from_timestamp(start, 0).unwrap_or(now),
    )
}

/// How long a post on this route may sit idle before Discord archives it, given where it stands.
///
/// A card that is still live holds Discord's maximum whatever the route asks for: the window
/// belongs to the quiet after an incident, and a thread that archives during one takes the
/// discussion with it. Everything else takes the route's window, or the deployment's where the
/// route names none.
fn auto_archive_minutes(
    route: &Route,
    state: NotificationState,
    settings: &DecisionSettings,
) -> u32 {
    if state != NotificationState::Resolved && state != NotificationState::Orphaned {
        return MAX_AUTO_ARCHIVE;
    }

    match &route.target {
        RouteTarget::Forum { policy, .. } => policy.auto_archive_minutes,
        RouteTarget::Text { thread, .. } => thread.archive_after_minutes,
        RouteTarget::Thread { .. } | RouteTarget::Dm { .. } => settings.archive_after_minutes,
    }
}

/// The tags a forum post should carry, in priority order and inside Discord's budget.
///
/// State first, then severity, then label values: when the five slots run out, the tag dropped is
/// the one an operator scanning the index is least likely to be reading.
#[must_use]
pub fn desired_tags(
    snapshot: &RoutingSnapshot,
    channel: ChannelId,
    policy: &ForumPolicy,
    state: NotificationState,
    severity: Severity,
    labels: &Labels,
) -> Vec<dam_store::TagId> {
    let mut names = Vec::new();

    names.push(match state {
        NotificationState::Acked => policy.state_tags.acked.clone(),
        NotificationState::Silenced => policy.state_tags.silenced.clone(),
        NotificationState::Resolved => policy.state_tags.resolved.clone(),
        _ => policy.state_tags.firing.clone(),
    });

    if policy.severity_tags {
        names.push(severity.as_str().to_owned());
    }

    for label in &policy.label_tags {
        let value = labels.get_or_empty(label);
        if !value.is_empty() {
            names.push(slug(value));
        }
    }

    let mut tags = Vec::new();
    for name in names {
        // A tag that does not resolve is dropped rather than fatal. The bot may lack the
        // permission to create it, or a human may have deleted it; neither is a reason to lose
        // the notification it would have decorated.
        if let Some(id) = snapshot.tag_id(channel, &name)
            && !tags.contains(&id)
        {
            tags.push(id);
        }

        if tags.len() == MAX_APPLIED_TAGS {
            break;
        }
    }

    if tags.is_empty()
        && let Some(default) = policy
            .default_tag
            .as_deref()
            .and_then(|name| snapshot.tag_id(channel, name))
    {
        // A channel with the require-tag flag rejects a post carrying none, so the fallback is
        // what keeps such a route deliverable at all.
        tags.push(default);
    }

    tags
}

/// Truncates a label value to something Discord will accept as a tag name.
fn slug(value: &str) -> String {
    const MAX_TAG_NAME: usize = 20;

    value
        .chars()
        .take(MAX_TAG_NAME)
        .map(|ch| if ch.is_whitespace() { '-' } else { ch })
        .collect()
}

/// The one line a state change posts into the thread.
fn state_note(state: NotificationState, delta: &AlertDelta) -> String {
    match state {
        NotificationState::Acked => "Acknowledged.".to_owned(),
        NotificationState::Silenced => "Silenced in Alertmanager.".to_owned(),
        NotificationState::Ignored => {
            "Muted in Discord. Alertmanager is still notifying.".to_owned()
        }
        NotificationState::Resolved => "Resolved.".to_owned(),
        NotificationState::Orphaned => "Card lost; a replacement was posted.".to_owned(),
        NotificationState::Firing if delta.flap_count > 0 => {
            format!("Firing again, flap ×{}.", delta.flap_count)
        }
        NotificationState::Firing => "Firing.".to_owned(),
    }
}

#[cfg(test)]
mod tests {
    use chrono::TimeZone;
    use dam_core::MatcherSet;
    use dam_core::{
        Alert, AlertStatus, AmState, Annotations, EventSource, Fingerprint, LabelName, Labels,
    };
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
            episode: 0,
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
            delta.per_alert_key(),
            "a quiet per-alert route keeps its own card per alert"
        );

        let key = dedupe_key(
            &delta,
            &route,
            &storming(),
            &DecisionSettings::default(),
            now(),
        );

        assert_ne!(key, delta.per_alert_key());
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

        let key = DedupeKey::per_alert(
            &Fingerprint::new("0123456789abcdef").expect("hex is a fingerprint"),
            0,
        );
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
            false,
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
        second.alert.fingerprint =
            Fingerprint::new("fedcba9876543210").expect("hex is a fingerprint");

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
    fn a_new_episode_links_back_to_the_card_it_replaced() {
        let route = text_route();
        let channel = ChannelId::new(100);
        let snapshot = RoutingSnapshot::new(vec![route], Vec::new(), Vec::new());

        let mut delta = delta(EventKind::Fired, AlertStatus::Firing, AmState::Active);
        delta.episode = 1;

        let previous = DedupeKey::per_alert(&delta.alert.fingerprint, 0);
        let mut existing = ExistingCards::new();
        existing.insert(
            (channel, previous.clone()),
            card(NotificationState::Resolved, channel, &previous),
        );

        let decision = decide(
            &delta,
            &snapshot,
            &quiet(),
            &existing,
            false,
            &DecisionSettings::default(),
            now(),
        );

        assert_eq!(
            decision.new_cards.len(),
            1,
            "the resolved card of the last episode is not edited back to firing"
        );
        assert_eq!(
            decision.new_cards[0].card.supersedes,
            Some(NotificationId::new(7)),
            "the replacement carries the link to what it replaced"
        );
        assert_eq!(
            decision.new_cards[0].card.dedupe_key,
            DedupeKey::per_alert(&delta.alert.fingerprint, 1)
        );
    }

    #[test]
    fn a_first_episode_supersedes_nothing() {
        let snapshot = RoutingSnapshot::new(vec![text_route()], Vec::new(), Vec::new());
        let delta = delta(EventKind::Fired, AlertStatus::Firing, AmState::Active);

        let decision = decide(
            &delta,
            &snapshot,
            &quiet(),
            &ExistingCards::new(),
            false,
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

        let key = DedupeKey::per_alert(
            &Fingerprint::new("0123456789abcdef").expect("hex is a fingerprint"),
            0,
        );
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
            false,
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
    fn a_reopened_card_holds_the_maximum_archive_window() {
        let route = text_route();
        let channel = ChannelId::new(100);
        let snapshot = RoutingSnapshot::new(vec![route], Vec::new(), Vec::new());

        let key = DedupeKey::per_alert(
            &Fingerprint::new("0123456789abcdef").expect("hex is a fingerprint"),
            0,
        );
        let mut archived = card(NotificationState::Resolved, channel, &key);
        archived.archived = true;

        let mut existing = ExistingCards::new();
        existing.insert((channel, key.clone()), archived);

        let decision = decide(
            &delta(EventKind::Fired, AlertStatus::Firing, AmState::Active),
            &snapshot,
            &quiet(),
            &existing,
            false,
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
            false,
            &DecisionSettings::default(),
            now(),
        );

        assert_eq!(decision.new_cards.len(), 1);
        assert!(decision.updates.is_empty());
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
            false,
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
            false,
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
            false,
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
        let key = delta.per_alert_key();
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
            false,
            &DecisionSettings::default(),
            now(),
        );

        assert!(decision.new_cards.is_empty());
        assert_eq!(decision.updates.len(), 1);
        assert_eq!(decision.updates[0].state, None);
        assert_eq!(kinds(&decision.updates[0]), vec!["edit_card"]);
    }

    #[test]
    fn a_card_edit_waits_for_the_debounce() {
        let route = text_route();
        let delta = delta(EventKind::Updated, AlertStatus::Firing, AmState::Active);
        let key = delta.per_alert_key();
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
            false,
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
        let key = delta.per_alert_key();
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
            false,
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
    fn an_update_that_changes_no_state_asks_for_no_pin() {
        let route = forum_route();
        let delta = delta(EventKind::Updated, AlertStatus::Firing, AmState::Active);
        let key = delta.per_alert_key();
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
            false,
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
    fn an_archived_post_is_reopened_before_it_is_edited() {
        let route = forum_route();
        let delta = delta(EventKind::Fired, AlertStatus::Firing, AmState::Active);
        let key = delta.per_alert_key();
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
            false,
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
        let key = delta.per_alert_key();
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
            true,
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
        assert!(note.contains("Acknowledged"), "{note}");
    }

    #[test]
    fn an_orphaned_card_is_never_touched_again() {
        let route = text_route();
        let delta = delta(EventKind::Resolved, AlertStatus::Resolved, AmState::Active);
        let key = delta.per_alert_key();
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
            false,
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
        let key = delta.per_alert_key();
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
            false,
            &DecisionSettings::default(),
            now(),
        );

        assert_eq!(decision.updates[0].state, Some(NotificationState::Ignored));
    }
}
