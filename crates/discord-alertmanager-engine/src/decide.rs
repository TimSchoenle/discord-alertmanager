//! The one function that decides what an alert change does to the cards showing it.
//!
//! Pure over its arguments and total: no I/O, no clock, no randomness. Everything the pipeline
//! does to Discord is a consequence of what this returns, which is why it is the piece worth
//! testing exhaustively and the piece everything downstream can be written against.
//!
//! # One card per alert, found by identity and re-armed in place
//!
//! A per-alert card is keyed by the alert's identity rather than by its fingerprint, so an alert
//! whose churning label gives it a new fingerprint lands on the card already showing it. The key
//! names the card the alert is shown on now, and a lookup under it is the whole of duplicate
//! detection.
//!
//! What happens to that card is decided from the alert's status against the card's state, not
//! from which kind of event happened to arrive. A card showing resolved for an alert that is
//! firing is re-armed whichever path noticed; a card showing firing for an alert that has
//! resolved is resolved, unless another alert sharing its identity is still firing, in which case
//! the card shows that one instead. The only re-fire that does not re-arm is one arriving after
//! the card has been resolved for longer than the regroup window: that posts a new card, which
//! takes the key and links back to the old one.

use std::collections::HashMap;

use chrono::{DateTime, Duration, Utc};
use dam_core::{
    Alert, AlertDelta, AlertStatus, DedupeKey, EventKind, Fingerprint, IdentityPolicy, Labels,
    NotificationState, Severity, Trigger, initial_state, next_state,
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
#[derive(Debug, Clone, PartialEq, Eq)]
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

    /// How long a resolved card stays re-armable.
    ///
    /// Measured from when the card resolved. A re-fire inside it turns the card back to firing;
    /// one after it posts a replacement.
    pub regroup_window: Duration,

    /// Which alerts share a per-alert card.
    pub identity: IdentityPolicy,
}

impl Default for DecisionSettings {
    fn default() -> Self {
        Self {
            debounce: Duration::seconds(3),
            digest_window: Duration::minutes(5),
            archive_after_minutes: 1440,
            regroup_window: Duration::minutes(30),
            identity: IdentityPolicy::default(),
        }
    }
}

/// What the caller already knows about the cards this change might touch.
///
/// Read from the store before the decision, so the decision itself stays pure. The key is the
/// pair a card is unique under, which is also the pair the database's unique index enforces.
pub type ExistingCards = HashMap<(ChannelId, DedupeKey), Notification>;

/// What the caller read about the alert beyond its cards.
#[derive(Debug, Clone, Copy, Default)]
pub struct AlertContext<'a> {
    /// Whether anyone holds the alert.
    ///
    /// A property of the alert rather than of a card: a re-fire keeps it, or an alert somebody is
    /// already working flaps back to unclaimed and pages the channel again.
    pub acknowledged: bool,

    /// Other alerts sharing this one's identity that are still firing.
    ///
    /// Only read when this change is a resolution and the identity policy merges anything. A card
    /// that merges fingerprints resolves when the last of them does, not the first.
    pub firing_siblings: &'a [Alert],
}

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
    context: &AlertContext<'_>,
    settings: &DecisionSettings,
    now: DateTime<Utc>,
) -> Decision {
    let mut decision = Decision {
        at: now,
        ..Decision::default()
    };

    let severity = delta.alert.severity();
    let labels = &delta.alert.labels;
    let identity = settings.identity.identity(&delta.alert);

    for route in snapshot.resolve(labels, severity) {
        let channel = delivery_channel(&route.target);
        let ignore = snapshot.ignore_for(route.guild_id, channel, labels, now);
        let mut keys = dedupe_keys(delta, route, storm, settings, now);

        // The first card that can still carry this change, and failing that the card it
        // outlived. A resolved card past the regroup window is history: the change goes on a
        // replacement, which takes its key and links back to it.
        let mut outlived_card = None;
        let held = keys.iter().find_map(|key| {
            let card = existing.get(&(channel, key.clone()))?;
            if outlived(card, delta, settings, now) {
                outlived_card.get_or_insert(card);
                return None;
            }
            Some(card)
        });

        if let Some(card) = held {
            let siblings = if card.dedupe_key == DedupeKey::per_alert(&identity) {
                context.firing_siblings
            } else {
                // A group or digest card is not keyed by identity, so the identity's other
                // members say nothing about whether it is still firing.
                &[]
            };

            if let Some(update) = update_for(
                delta,
                route,
                snapshot,
                card,
                ignore.is_some(),
                context.acknowledged,
                siblings,
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
            outlived_card.map(|card| card.id),
            now,
        ) {
            decision.new_cards.push(planned);
        }
    }

    decision
}

/// Whether a card has been resolved for too long for this change to re-arm it.
///
/// Only a firing alert can outlive a card: anything else it says about a resolved card is an edit
/// to history, and belongs on that card. A resolved card with no recorded resolution time is
/// re-armed rather than replaced, because a replacement is the one outcome that cannot be undone.
fn outlived(
    card: &Notification,
    delta: &AlertDelta,
    settings: &DecisionSettings,
    now: DateTime<Utc>,
) -> bool {
    card.state == NotificationState::Resolved
        && delta.alert.is_firing()
        && card
            .resolved_at
            .is_some_and(|resolved| now - resolved > settings.regroup_window)
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

/// The transition this change asks of a card, read from where the alert stands.
///
/// The alert's status against the card's state comes first, and the event kind only after it.
/// Both sources of change can arrive out of order or be lost, so a card that disagrees with its
/// alert is corrected by whichever change reaches it next, rather than only by the one event that
/// was supposed to carry the news.
fn trigger_for(delta: &AlertDelta, card: &Notification, ignored: bool) -> Option<Trigger> {
    let open = card.state.is_open() || card.state == NotificationState::Ignored;

    match delta.alert.status {
        AlertStatus::Resolved if open => return Some(Trigger::Resolved),
        AlertStatus::Resolved => return Trigger::from_event(delta.kind),
        AlertStatus::Firing => {}
    }

    if ignored && card.state != NotificationState::Ignored {
        return Some(Trigger::Ignored);
    }

    if !ignored && card.state == NotificationState::Ignored {
        return Some(Trigger::Unignored);
    }

    if card.state == NotificationState::Resolved {
        return Some(Trigger::Fired);
    }

    Trigger::from_event(delta.kind)
}

/// The alert a card should show once a resolution has landed on it, if it should stay firing.
///
/// Another alert sharing the card's identity and still firing on this route keeps the card open.
/// The one the card already shows is preferred, so a card flipping between two live alerts on
/// every resolution of a third does not happen.
fn still_firing<'a>(
    route: &Route,
    card: &Notification,
    siblings: &'a [Alert],
) -> Option<&'a Alert> {
    let live = |alert: &&Alert| alert.is_firing() && route.accepts(&alert.labels, alert.severity());

    siblings
        .iter()
        .filter(live)
        .find(|alert| alert.fingerprint == card.fingerprint)
        .or_else(|| siblings.iter().find(live))
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
    siblings: &[Alert],
    settings: &DecisionSettings,
    now: DateTime<Utc>,
) -> Option<CardUpdate> {
    // An orphaned card describes nothing that still exists in Discord. Editing it would fail
    // forever, and its replacement is posted under a new row.
    if card.state == NotificationState::Orphaned {
        return None;
    }

    let mut trigger = trigger_for(delta, card, ignored);
    let mut shown = &delta.alert;

    if trigger == Some(Trigger::Resolved)
        && let Some(sibling) = still_firing(route, card, siblings)
    {
        // One of several alerts behind this card resolved and another has not. The card stays
        // where it is and shows the one still firing, which is what an operator looking at it
        // needs to know.
        trigger = None;
        shown = sibling;
    }

    let mut state = trigger.and_then(|trigger| next_state(card.state, trigger, acknowledged));

    // A card re-armed for an alert Alertmanager is already suppressing lands on silenced, which is
    // where a card created for it would have started. Coming back red would page a channel about
    // an alert somebody has already decided should be quiet.
    if card.state == NotificationState::Resolved
        && matches!(
            state,
            Some(NotificationState::Firing | NotificationState::Acked)
        )
        && shown.am_state.is_suppressed()
    {
        state = Some(NotificationState::Silenced);
    }

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

    if let RouteTarget::Forum { .. } = &route.target {
        effects.extend(reopened_post_window(route, card, effective, settings, now));
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
            delta, snapshot, card, *channel, policy, effective, state, shown, now,
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

        if let RouteTarget::Forum { policy, .. } = &route.target {
            effects.extend(resolved_post_flags(route, policy, card, settings, now));
        }
    }

    Some(CardUpdate {
        id: card.id,
        fingerprint: shown.fingerprint.clone(),
        state,
        effects,
    })
}

/// The archive, lock and window a forum post takes when its alert resolves.
///
/// A post archived on resolve is sealed with the route's window. One left open still drops from
/// the week it held while firing to that window, or it would sit at the top of the index for a
/// week; an archived card needs nothing, as reopening it above already set that window.
fn resolved_post_flags(
    route: &Route,
    policy: &ForumPolicy,
    card: &Notification,
    settings: &DecisionSettings,
    now: DateTime<Utc>,
) -> Option<NewOutboxItem> {
    if !policy.archive_on_resolve && card.archived {
        return None;
    }

    Some(NewOutboxItem {
        effect: Effect::SetFlags {
            notification: card.id,
            archived: policy.archive_on_resolve,
            // A lock comes only with the archive: a re-fire reopens an archived post, which is
            // what lifts the lock, while one left open would stay locked against its edits.
            locked: policy.archive_on_resolve && policy.lock_on_resolve,
            // The route's own window rather than a week: the incident is over, and a resolved
            // post that lingers at the top of the index buries the ones that are not.
            auto_archive_minutes: auto_archive_minutes(
                route,
                NotificationState::Resolved,
                settings,
            ),
        },
        dedupe_key: card.dedupe_key.clone(),
        // Behind the edit's debounce, and queued after it, so that the claim order — `not_before`
        // then insertion — re-renders the card before the post is sealed. Archiving first left
        // the edit fighting the archive it had just caused: a locked post refuses the edit
        // outright and the card keeps saying "firing" for good, while an unlocked one is reopened
        // to carry it and stops being archived at all.
        not_before: now + settings.debounce,
    })
}

/// The window a forum post left open on resolve takes back when its alert fires again.
///
/// [`resolved_post_flags`] drops such a post to the route's window, and nothing else restores the
/// week: the reopening above runs only for an archived post. Without this, a re-fired alert
/// keeps the short window and Discord archives its post mid-incident, taking it off the active
/// list until the next edit happens to reopen it.
fn reopened_post_window(
    route: &Route,
    card: &Notification,
    effective: NotificationState,
    settings: &DecisionSettings,
    now: DateTime<Utc>,
) -> Option<NewOutboxItem> {
    if card.archived || card.state != NotificationState::Resolved || !effective.holds_thread_open()
    {
        return None;
    }

    Some(NewOutboxItem {
        effect: Effect::SetFlags {
            notification: card.id,
            archived: false,
            locked: false,
            auto_archive_minutes: auto_archive_minutes(route, effective, settings),
        },
        dedupe_key: card.dedupe_key.clone(),
        // Behind the debounce, like the resolve's own flags: a re-fire inside that debounce
        // finds the resolve's seal still queued and unapplied, and must land after it to undo it.
        not_before: now + settings.debounce,
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
    shown: &Alert,
    now: DateTime<Utc>,
) -> Vec<NewOutboxItem> {
    let mut effects = Vec::new();

    // The tags follow the state and the labels of the alert on show. A card can switch to
    // another alert without moving, when a churned label or a second member of its identity takes
    // over, and that alert's labels can want different label tags. Asked for only on a state
    // change or a switch, so a repeat of the same alert queues nothing; the worker skips a set
    // the post already carries.
    if state.is_some() || shown.fingerprint != card.fingerprint {
        let tags = desired_tags(
            snapshot,
            channel,
            policy,
            effective,
            shown.severity(),
            &shown.labels,
        );

        effects.push(immediate(
            Effect::SetTags {
                notification: card.id,
                tags,
            },
            card,
            now,
        ));
    }

    if state.is_some() {
        // Editing a message does not bump a forum's activity sort; only a new message does.
        // Without this note a state change leaves the post buried exactly where it was.
        if policy.bump_on_state_change {
            effects.push(immediate(
                Effect::ThreadNote {
                    notification: card.id,
                    text: state_note(card.state, effective, delta.flap_count),
                },
                card,
                now,
            ));
        }

        // Inside the state gate with the tags and the note, because the pin is decoration on the
        // same transition and an update that changes no state changes no pin. Outside it, a pin
        // Discord will not grant (its forums hold one pinned post) is asked for again on every
        // poll and every repeat of an alert that is going nowhere, for as long as the alert lasts.
        let wants_pin = effective.wants_pin()
            && policy
                .pin_min_severity
                .is_some_and(|floor| shown.severity() >= floor);
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
    let per_alert = || DedupeKey::per_alert(&settings.identity.identity(&delta.alert));

    let configured = match route.group_strategy {
        // Falls back to the per-alert key when no group key has ever been delivered for the
        // alert, which is the case for one the reconciler discovered before any webhook did.
        GroupStrategy::PerGroup => delta
            .alert
            .group_key
            .as_ref()
            .map_or_else(per_alert, DedupeKey::per_group),
        GroupStrategy::PerAlert => per_alert(),
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

/// The alerts among `candidates` that share `alert`'s identity, other than `alert` itself.
///
/// What the caller narrows the store's firing set to before handing it to [`decide`]. Here rather
/// than in the caller so the notion of "the same card" is spelled once, beside the key built
/// from it.
#[must_use]
pub fn identity_siblings(
    policy: &IdentityPolicy,
    alert: &Alert,
    candidates: impl IntoIterator<Item = Alert>,
) -> Vec<Alert> {
    let identity: Fingerprint = policy.identity(alert);

    candidates
        .into_iter()
        .filter(|candidate| {
            candidate.fingerprint != alert.fingerprint && policy.identity(candidate) == identity
        })
        .collect()
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
    if state.holds_thread_open() {
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

    names.extend(label_tag_names(policy, labels));

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

/// The tag names a forum policy derives from an alert's labels, in the policy's order.
///
/// One per label in `label_tags` that the alert carries with a non-empty value, cut to the
/// twenty characters Discord allows a tag name, with whitespace replaced by hyphens.
#[must_use]
pub fn label_tag_names(policy: &ForumPolicy, labels: &Labels) -> Vec<String> {
    policy
        .label_tags
        .iter()
        .map(|label| labels.get_or_empty(label))
        .filter(|value| !value.is_empty())
        .map(slug)
        .collect()
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
///
/// A card coming back from resolved says so, whichever open state it lands in: "Firing." on a
/// thread whose last line was "Resolved." reads as a new incident rather than the old one
/// returning.
fn state_note(previous: NotificationState, state: NotificationState, flap_count: u32) -> String {
    let rearmed = previous == NotificationState::Resolved;

    match state {
        NotificationState::Acked if rearmed => "Firing again. Still acknowledged.".to_owned(),
        NotificationState::Acked => "Acknowledged.".to_owned(),
        NotificationState::Silenced => "Silenced in Alertmanager.".to_owned(),
        NotificationState::Ignored => {
            "Muted in Discord. Alertmanager is still notifying.".to_owned()
        }
        NotificationState::Resolved => "Resolved.".to_owned(),
        NotificationState::Orphaned => "Card lost; a replacement was posted.".to_owned(),
        NotificationState::Firing if flap_count > 0 => {
            format!("Firing again, flap ×{flap_count}.")
        }
        NotificationState::Firing if rearmed => "Firing again.".to_owned(),
        NotificationState::Firing => "Firing.".to_owned(),
    }
}

#[cfg(test)]
mod tests;
