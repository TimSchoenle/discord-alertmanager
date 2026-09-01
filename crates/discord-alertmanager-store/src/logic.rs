//! The parts of a store's behaviour that are not SQL.
//!
//! Deciding whether an arriving alert is a re-fire, a duplicate or an annotation change is
//! dialect-independent, and two backends deciding it separately is two chances to decide it
//! differently. The conformance suite would catch the divergence, but only after both had been
//! written; putting the rule here means there is one of it.
//!
//! What stays in the backends is what the dialects genuinely differ on: how a row is claimed, how
//! a timestamp is stored, and how a driver reports a constraint violation.

use std::collections::BTreeMap;

use chrono::{DateTime, Duration, Utc};
use dam_core::{Alert, EventKind, Fingerprint, Matcher, Severity};

use crate::alerts::{AlertQuery, AlertRecord, QueryMatcher, SilenceLifecycle, SilenceState};

/// What an arriving alert does to the row already stored for it.
///
/// Produced by [`classify`], consumed by both backends to write one `alerts` row and at most one
/// `alert_events` row.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Transition {
    /// What changed.
    pub kind: EventKind,

    /// Consecutive firing periods this fingerprint has now seen inside the current episode.
    pub flap_count: u32,

    /// Which firing episode the alert is now in.
    pub episode: u32,

    /// When the fingerprint was first seen, ever.
    pub first_seen_at: DateTime<Utc>,

    /// When it last resolved, if it has.
    pub resolved_at: Option<DateTime<Utc>>,
}

/// Decides what an arriving alert changes, or that it changes nothing.
///
/// `None` is the redelivery case, and it is decided by comparing the payload against the stored
/// row rather than by letting a unique index reject the event insert. The index cannot tell two
/// successive annotation edits apart from one edit delivered twice — both carry the same
/// fingerprint, kind and timings — so it makes a poor duplicate detector and a good backstop.
///
/// `regroup` is how long a resolved alert may stay quiet and still count as the same episode when
/// it fires again. A re-fire inside it reuses the card and counts a flap; one after it starts a
/// new episode, and the episode is what gives the replacement card a key of its own.
#[must_use]
pub fn classify(
    previous: Option<&AlertRecord>,
    incoming: &Alert,
    received_at: DateTime<Utc>,
    regroup: Duration,
) -> Option<Transition> {
    let Some(previous) = previous else {
        return Some(Transition {
            kind: if incoming.is_firing() {
                EventKind::Fired
            } else {
                EventKind::Resolved
            },
            flap_count: 0,
            episode: 0,
            first_seen_at: received_at,
            resolved_at: resolved_at(incoming, received_at),
        });
    };

    let base = Transition {
        kind: EventKind::Updated,
        flap_count: previous.flap_count,
        episode: previous.episode,
        first_seen_at: previous.first_seen_at,
        resolved_at: previous.resolved_at,
    };

    if previous.alert.status != incoming.status {
        return Some(if !incoming.is_firing() {
            Transition {
                kind: EventKind::Resolved,
                resolved_at: resolved_at(incoming, received_at),
                ..base
            }
        } else if regrouped(previous, received_at, regroup) {
            // Long enough after the last resolution that nobody is still watching that card. The
            // episode moves, which moves the dedupe key, so this posts a new card carrying a link
            // back to the old one rather than turning a week-old resolved card red again.
            Transition {
                kind: EventKind::Fired,
                flap_count: 0,
                episode: previous.episode.saturating_add(1),
                resolved_at: None,
                ..base
            }
        } else {
            Transition {
                kind: EventKind::Fired,
                // A fingerprint that resolved and fired again inside the window is the flap the
                // window exists for: the card is reused and the count is what it shows.
                flap_count: previous.flap_count.saturating_add(1),
                resolved_at: None,
                ..base
            }
        });
    }

    let was_suppressed = previous.alert.am_state.is_suppressed();
    let is_suppressed = incoming.am_state.is_suppressed();

    if was_suppressed != is_suppressed {
        return Some(Transition {
            kind: if is_suppressed {
                EventKind::Silenced
            } else {
                EventKind::Unsilenced
            },
            ..base
        });
    }

    if is_material_change(&previous.alert, incoming) {
        return Some(base);
    }

    None
}

/// Whether a re-fire is far enough from the last resolution to belong to a new episode.
///
/// An alert with no recorded resolution is one this bot never saw resolve — a row written before
/// the reconciler caught up, or one whose resolution was lost — and it stays in its episode.
/// Starting a new one on a resolution nobody observed would post a second card for an alert whose
/// first card is still live.
fn regrouped(previous: &AlertRecord, received_at: DateTime<Utc>, regroup: Duration) -> bool {
    previous
        .resolved_at
        .is_some_and(|resolved| received_at - resolved > regroup)
}

/// When an alert that is not firing stopped firing.
///
/// Alertmanager supplies `endsAt` and a hand-posted alert need not, so the receipt time is the
/// fallback. Taking `now` unconditionally would move the resolution time every time a resolved
/// alert was redelivered.
fn resolved_at(incoming: &Alert, received_at: DateTime<Utc>) -> Option<DateTime<Utc>> {
    if incoming.is_firing() {
        None
    } else {
        Some(incoming.ends_at.unwrap_or(received_at))
    }
}

/// Whether two views of one alert differ in anything a card would show.
///
/// `last_seen_at` deliberately is not in the comparison. Every delivery moves it, and treating
/// that as a change would make every redelivery an update and every update a card edit, which is
/// the load the debounce exists to avoid rather than one to create.
fn is_material_change(previous: &Alert, incoming: &Alert) -> bool {
    previous.labels != incoming.labels
        || previous.annotations != incoming.annotations
        || previous.starts_at != incoming.starts_at
        || previous.ends_at != incoming.ends_at
        || previous.generator_url != incoming.generator_url
        || previous.am_state != incoming.am_state
        || previous.silenced_by != incoming.silenced_by
        || previous.inhibited_by != incoming.inhibited_by
        || previous.group_key != incoming.group_key
}

/// The silence ids in force against each fingerprint, from Alertmanager's own snapshot.
///
/// The suppression set comes from Alertmanager rather than from evaluating the silences' matchers
/// locally. This bot's matcher implementation agreeing with Alertmanager's is worth a test; it is
/// not worth deciding from which cards to recolour.
#[must_use]
pub fn suppression_map(snapshot: &[SilenceState]) -> BTreeMap<Fingerprint, Vec<String>> {
    let mut map: BTreeMap<Fingerprint, Vec<String>> = BTreeMap::new();

    for silence in snapshot {
        if silence.state != SilenceLifecycle::Active {
            continue;
        }

        for fingerprint in &silence.suppresses {
            map.entry(fingerprint.clone())
                .or_default()
                .push(silence.am_id.clone());
        }
    }

    for ids in map.values_mut() {
        ids.sort_unstable();
        ids.dedup();
    }

    map
}

impl QueryMatcher {
    /// Compiles the matcher, so a filtered read evaluates the same semantics a route does.
    ///
    /// # Errors
    ///
    /// Returns [`dam_core::CoreError`] when the label name is not a label name, or when a regex
    /// is too long or does not compile inside the size limits.
    pub fn compile(&self) -> Result<Matcher, dam_core::CoreError> {
        Matcher::new(
            dam_core::LabelName::new(self.name.clone())?,
            self.op,
            self.value.clone(),
        )
    }

    /// Whether the backend can turn this matcher into a SQL predicate.
    ///
    /// Equality and inequality are a JSON extraction and a comparison in both dialects. The two
    /// regex operators are not: `SQLite` has no `REGEXP` implementation unless one is registered,
    /// and Alertmanager's anchoring is not what either engine's own regex would apply.
    #[must_use]
    pub fn is_sql_expressible(&self) -> bool {
        !self.op.is_regex()
    }
}

/// Whether a stored alert satisfies every part of a query that SQL did not already apply.
///
/// The regex matchers, and nothing else. Applying an equality matcher here as well would be
/// harmless and would also hide a backend that quietly stopped emitting the predicate.
#[must_use]
pub fn matches_regex_matchers(record: &AlertRecord, query: &AlertQuery) -> bool {
    query
        .matchers
        .iter()
        .filter(|matcher| !matcher.is_sql_expressible())
        .all(|matcher| {
            matcher
                .compile()
                .is_ok_and(|compiled| compiled.matches(&record.alert.labels))
        })
}

/// Whether a query needs rows filtered in memory before they can be counted or paginated.
#[must_use]
pub fn needs_in_memory_filter(query: &AlertQuery) -> bool {
    query
        .matchers
        .iter()
        .any(|matcher| !matcher.is_sql_expressible())
}

/// Most rows a backend reads to satisfy a query it cannot express entirely in SQL.
///
/// A regex matcher forces the filter into memory, and an unbounded read would then let one
/// `/alerts list` pull the whole table into the process. The cap is high enough that no realistic
/// alert set reaches it and low enough that reaching it costs nothing.
pub const IN_MEMORY_SCAN_LIMIT: u32 = 5_000;

/// The severities at or above a floor, for a backend turning `min_severity` into a SQL `IN`.
///
/// Severity is stored as a word rather than a rank, so the comparison the domain expresses as
/// `>=` becomes set membership in SQL. Deriving the set here keeps the two backends from writing
/// two different lists.
#[must_use]
pub fn severities_at_or_above(floor: Severity) -> Vec<&'static str> {
    [Severity::Info, Severity::Warning, Severity::Critical]
        .into_iter()
        .filter(|severity| *severity >= floor)
        .map(Severity::as_str)
        .collect()
}

#[cfg(test)]
mod tests {
    use chrono::TimeZone;
    use dam_core::{AlertStatus, AmState, Annotations, LabelName};

    use super::*;

    fn at(seconds: i64) -> DateTime<Utc> {
        Utc.timestamp_opt(1_700_000_000 + seconds, 0)
            .single()
            .expect("timestamp is representable")
    }

    fn alert(status: AlertStatus) -> Alert {
        Alert {
            fingerprint: Fingerprint::new("deadbeef").expect("fingerprint is hex"),
            labels: [(
                LabelName::new("alertname").expect("label name is valid"),
                "Down".to_owned(),
            )]
            .into_iter()
            .collect(),
            annotations: Annotations::new(),
            starts_at: at(0),
            ends_at: None,
            generator_url: None,
            status,
            am_state: AmState::Active,
            silenced_by: Vec::new(),
            inhibited_by: Vec::new(),
            group_key: None,
        }
    }

    fn record(alert: Alert, flap_count: u32) -> AlertRecord {
        // A stored alert that is not firing has resolved, and the regroup window is measured from
        // when: a fixture that left this null would exercise the path taken by an alert whose
        // resolution was never observed rather than the ordinary one.
        let resolved_at = (!alert.is_firing()).then(|| alert.ends_at.unwrap_or(at(0)));

        AlertRecord {
            labels_hash: alert.labels_hash(),
            alert,
            first_seen_at: at(0),
            last_seen_at: at(0),
            resolved_at,
            flap_count,
            episode: 0,
            updated_at: at(0),
        }
    }

    /// The window every test here uses unless it is testing the window itself.
    const REGROUP: Duration = Duration::minutes(30);

    #[test]
    fn an_unchanged_redelivery_is_not_a_change() {
        let stored = record(alert(AlertStatus::Firing), 0);

        assert_eq!(
            classify(Some(&stored), &stored.alert.clone(), at(60), REGROUP),
            None
        );
    }

    #[test]
    fn a_re_fire_counts_as_a_flap() {
        let mut previous = alert(AlertStatus::Resolved);
        previous.ends_at = Some(at(30));
        let stored = record(previous, 2);

        let transition = classify(Some(&stored), &alert(AlertStatus::Firing), at(60), REGROUP)
            .expect("status changed");

        assert_eq!(transition.kind, EventKind::Fired);
        assert_eq!(transition.flap_count, 3);
        assert_eq!(transition.resolved_at, None);
    }

    #[test]
    fn a_re_fire_after_the_window_starts_a_new_episode() {
        let mut previous = alert(AlertStatus::Resolved);
        previous.ends_at = Some(at(30));
        let stored = record(previous, 2);

        let transition = classify(
            Some(&stored),
            &alert(AlertStatus::Firing),
            at(30) + REGROUP + Duration::seconds(1),
            REGROUP,
        )
        .expect("status changed");

        assert_eq!(transition.kind, EventKind::Fired);
        assert_eq!(transition.episode, 1, "the card is a new one");
        assert_eq!(
            transition.flap_count, 0,
            "the flap count belongs to the episode"
        );
    }

    #[test]
    fn a_re_fire_whose_resolution_was_never_seen_stays_in_its_episode() {
        // No `resolved_at`, which is what a row written before the reconciler caught up looks
        // like. Starting an episode from it would post a second card while the first is live.
        let mut stored = record(alert(AlertStatus::Resolved), 0);
        stored.resolved_at = None;

        let transition = classify(
            Some(&stored),
            &alert(AlertStatus::Firing),
            at(0) + REGROUP * 10,
            REGROUP,
        )
        .expect("status changed");

        assert_eq!(transition.episode, 0);
        assert_eq!(transition.flap_count, 1);
    }

    #[test]
    fn suppression_is_read_as_a_state_change_not_an_update() {
        let stored = record(alert(AlertStatus::Firing), 0);
        let mut incoming = alert(AlertStatus::Firing);
        incoming.am_state = AmState::Suppressed;
        incoming.silenced_by = vec!["abc".to_owned()];

        let transition =
            classify(Some(&stored), &incoming, at(60), REGROUP).expect("suppression changed");

        assert_eq!(transition.kind, EventKind::Silenced);
    }

    #[test]
    fn an_annotation_edit_is_an_update() {
        let stored = record(alert(AlertStatus::Firing), 0);
        let mut incoming = alert(AlertStatus::Firing);
        incoming.annotations.insert("summary", "disk is full");

        let transition =
            classify(Some(&stored), &incoming, at(60), REGROUP).expect("annotations changed");

        assert_eq!(transition.kind, EventKind::Updated);
    }

    #[test]
    fn a_resolve_without_an_end_time_resolves_at_receipt() {
        let stored = record(alert(AlertStatus::Firing), 0);

        let transition = classify(
            Some(&stored),
            &alert(AlertStatus::Resolved),
            at(90),
            REGROUP,
        )
        .expect("status changed");

        assert_eq!(transition.resolved_at, Some(at(90)));
    }

    #[test]
    fn only_active_silences_suppress() {
        let fingerprint = Fingerprint::new("deadbeef").expect("fingerprint is hex");
        let snapshot = vec![
            SilenceState {
                am_id: "active".to_owned(),
                suppresses: vec![fingerprint.clone()],
                state: SilenceLifecycle::Active,
                ends_at: at(600),
                observed_at: at(0),
            },
            SilenceState {
                am_id: "expired".to_owned(),
                suppresses: vec![fingerprint.clone()],
                state: SilenceLifecycle::Expired,
                ends_at: at(10),
                observed_at: at(0),
            },
        ];

        let map = suppression_map(&snapshot);

        assert_eq!(
            map.get(&fingerprint).map(Vec::as_slice),
            Some(&["active".to_owned()][..])
        );
    }

    #[test]
    fn a_severity_floor_becomes_the_set_above_it() {
        assert_eq!(
            severities_at_or_above(Severity::Warning),
            vec!["warning", "critical"]
        );
    }
}
