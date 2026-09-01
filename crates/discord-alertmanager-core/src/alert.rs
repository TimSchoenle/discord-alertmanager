//! The alert itself: its annotations, its severity, its two statuses, and what changed about it.

use std::collections::BTreeMap;
use std::fmt;
use std::str::FromStr;

use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};

use crate::CoreError;
use crate::labels::{FNV_OFFSET, Fingerprint, GroupKey, Labels, LabelsHash, fnv1a};

/// Label whose value carries the severity.
pub const SEVERITY_LABEL: &str = "severity";

/// The free-text side of an alert.
///
/// Separate from [`Labels`] because the two are not interchangeable despite both being string
/// maps: labels are identity and are matched against, annotations are prose and are rendered.
/// Changing an annotation must not change a fingerprint, and the type system is the cheapest
/// place to make that impossible.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Annotations(BTreeMap<String, String>);

impl Annotations {
    /// An empty set.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Looks an annotation up by name.
    #[must_use]
    pub fn get(&self, name: &str) -> Option<&str> {
        self.0.get(name).map(String::as_str)
    }

    /// Inserts an annotation, replacing any previous value.
    pub fn insert(&mut self, name: impl Into<String>, value: impl Into<String>) {
        self.0.insert(name.into(), value.into());
    }

    /// Iterates the set in name order.
    pub fn iter(&self) -> impl Iterator<Item = (&str, &str)> {
        self.0
            .iter()
            .map(|(name, value)| (name.as_str(), value.as_str()))
    }

    /// Number of annotations.
    #[must_use]
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Whether there are no annotations.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// The one-line `summary`, which becomes the top of a card's description.
    #[must_use]
    pub fn summary(&self) -> Option<&str> {
        self.get("summary")
    }

    /// The longer `description`.
    #[must_use]
    pub fn description(&self) -> Option<&str> {
        self.get("description")
    }

    /// The `runbook_url`, which becomes a link button when it passes the host allowlist.
    #[must_use]
    pub fn runbook_url(&self) -> Option<&str> {
        self.get("runbook_url")
    }

    /// The `value` annotation Prometheus templates the firing sample into.
    #[must_use]
    pub fn value(&self) -> Option<&str> {
        self.get("value")
    }

    /// The annotations present here whose value differs from `previous`.
    ///
    /// An `updated` event stores this rather than a whole envelope: the label set already lives on
    /// the alert row, and copying it per event dominates storage while buying nothing.
    #[must_use]
    pub fn changed_from(&self, previous: &Self) -> Self {
        let mut changed = Self::new();
        for (name, value) in self.iter() {
            if previous.get(name) != Some(value) {
                changed.insert(name, value);
            }
        }
        changed
    }
}

impl FromIterator<(String, String)> for Annotations {
    fn from_iter<T: IntoIterator<Item = (String, String)>>(iter: T) -> Self {
        Self(iter.into_iter().collect())
    }
}

/// How urgent an alert is.
///
/// Ordered least to most urgent, so a route's minimum severity is a comparison rather than a
/// table of which severities imply which.
#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
#[serde(rename_all = "lowercase")]
pub enum Severity {
    /// Informational. Never mentions anyone.
    #[default]
    Info,

    /// Worth looking at during working hours.
    Warning,

    /// Worth looking at now.
    Critical,
}

impl Severity {
    /// Reads the severity out of a label set, defaulting to [`Severity::Info`].
    ///
    /// The default is deliberate rather than an error: an alert with a missing or unrecognised
    /// severity still has to be delivered, and treating it as informational means an unknown
    /// value cannot page anyone. The parse is generous about spelling because the label is
    /// written by whoever wrote the alerting rule, and `crit` and `page` are both common.
    #[must_use]
    pub fn from_labels(labels: &Labels) -> Self {
        labels
            .get(SEVERITY_LABEL)
            .and_then(|value| value.parse().ok())
            .unwrap_or_default()
    }

    /// The severity as the lowercase word used in tags, colours and command arguments.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Info => "info",
            Self::Warning => "warning",
            Self::Critical => "critical",
        }
    }
}

impl FromStr for Severity {
    type Err = CoreError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim().to_ascii_lowercase().as_str() {
            "critical" | "crit" | "fatal" | "emergency" | "page" | "p1" => Ok(Self::Critical),
            "warning" | "warn" | "error" | "p2" => Ok(Self::Warning),
            "info" | "informational" | "information" | "none" | "p3" => Ok(Self::Info),
            _ => Err(CoreError::UnknownSeverity {
                value: value.to_owned(),
            }),
        }
    }
}

impl fmt::Display for Severity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Whether an alert is currently firing.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum AlertStatus {
    /// The alerting rule is still true.
    Firing,

    /// The rule stopped being true, or Alertmanager stopped hearing about it.
    Resolved,
}

impl AlertStatus {
    /// The status as the lowercase word stored in the database.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Firing => "firing",
            Self::Resolved => "resolved",
        }
    }
}

impl fmt::Display for AlertStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for AlertStatus {
    type Err = CoreError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "firing" => Ok(Self::Firing),
            "resolved" => Ok(Self::Resolved),
            other => Err(CoreError::UnknownVariant {
                kind: "alert status",
                value: other.to_owned(),
            }),
        }
    }
}

/// What Alertmanager itself thinks the alert's processing state is.
///
/// Distinct from [`AlertStatus`], and the distinction is the whole reason silencing works: a
/// silenced alert is still firing. It is suppressed, which is a statement about Alertmanager's
/// notification pipeline, not about whether the condition holds.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum AmState {
    /// Alertmanager will notify about this alert.
    #[default]
    Active,

    /// A silence or an inhibition rule is suppressing it.
    Suppressed,

    /// Alertmanager has received it but not yet processed it.
    Unprocessed,
}

impl AmState {
    /// The state as the lowercase word Alertmanager uses.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Active => "active",
            Self::Suppressed => "suppressed",
            Self::Unprocessed => "unprocessed",
        }
    }

    /// Whether Alertmanager is suppressing notifications for this alert.
    #[must_use]
    pub fn is_suppressed(self) -> bool {
        matches!(self, Self::Suppressed)
    }
}

impl fmt::Display for AmState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for AmState {
    type Err = CoreError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "active" => Ok(Self::Active),
            "suppressed" => Ok(Self::Suppressed),
            "unprocessed" => Ok(Self::Unprocessed),
            other => Err(CoreError::UnknownVariant {
                kind: "alertmanager state",
                value: other.to_owned(),
            }),
        }
    }
}

/// One alert, as this bot understands it.
///
/// This is the shape both the webhook and the reconciler produce, so everything downstream of
/// ingest is written once rather than once per source.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Alert {
    /// Alertmanager's fingerprint, and the primary key.
    pub fingerprint: Fingerprint,

    /// The identity of the alert.
    pub labels: Labels,

    /// The prose of the alert.
    pub annotations: Annotations,

    /// When the condition started holding.
    pub starts_at: DateTime<Utc>,

    /// When it stopped, if it has.
    pub ends_at: Option<DateTime<Utc>>,

    /// Link back to the expression in Prometheus that produced it.
    pub generator_url: Option<String>,

    /// Whether the alert is firing or resolved.
    pub status: AlertStatus,

    /// What Alertmanager is doing with it.
    pub am_state: AmState,

    /// Silence ids currently suppressing it, read from Alertmanager at render time.
    pub silenced_by: Vec<String>,

    /// Fingerprints of the alerts inhibiting it, read from Alertmanager at render time.
    pub inhibited_by: Vec<String>,

    /// The group Alertmanager put it in, when the source knows it.
    pub group_key: Option<GroupKey>,
}

impl Alert {
    /// The alert's severity, read from the `severity` label.
    #[must_use]
    pub fn severity(&self) -> Severity {
        Severity::from_labels(&self.labels)
    }

    /// The alert's name, falling back to the short fingerprint when the label is absent.
    ///
    /// Every Prometheus alert carries `alertname`, and an alert posted straight to
    /// Alertmanager's API by hand does not have to. A card title cannot be empty, so the fallback
    /// is here rather than in each of the three places that build one.
    #[must_use]
    pub fn name(&self) -> &str {
        self.labels
            .alertname()
            .filter(|name| !name.is_empty())
            .unwrap_or_else(|| self.fingerprint.short())
    }

    /// The locally computed hash of the label set.
    #[must_use]
    pub fn labels_hash(&self) -> LabelsHash {
        self.labels.labels_hash()
    }

    /// Whether the alert is firing.
    #[must_use]
    pub fn is_firing(&self) -> bool {
        self.status == AlertStatus::Firing
    }

    /// How long the alert has been firing at `now`, or how long it fired for once resolved.
    #[must_use]
    pub fn duration(&self, now: DateTime<Utc>) -> Duration {
        self.ends_at.unwrap_or(now) - self.starts_at
    }
}

/// What kind of change an ingest produced.
///
/// One row per transition is stored; `Updated` is the one that is not a transition and is why the
/// event payload is trimmed to the annotations that actually changed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum EventKind {
    /// The alert was not firing here and now is.
    Fired,

    /// The alert was firing here and now is not.
    Resolved,

    /// Annotations or timings changed while the status did not.
    Updated,

    /// Alertmanager started suppressing it.
    Silenced,

    /// Alertmanager stopped suppressing it.
    Unsilenced,
}

impl EventKind {
    /// The kind as the lowercase word stored in the database.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Fired => "fired",
            Self::Resolved => "resolved",
            Self::Updated => "updated",
            Self::Silenced => "silenced",
            Self::Unsilenced => "unsilenced",
        }
    }

    /// Whether this kind changes state rather than merely restating it.
    ///
    /// An `Updated` still edits a card, because the description it carries is on that card. It
    /// does not, however, mention anyone or move a forum tag.
    #[must_use]
    pub fn is_transition(self) -> bool {
        !matches!(self, Self::Updated)
    }
}

impl fmt::Display for EventKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for EventKind {
    type Err = CoreError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "fired" => Ok(Self::Fired),
            "resolved" => Ok(Self::Resolved),
            "updated" => Ok(Self::Updated),
            "silenced" => Ok(Self::Silenced),
            "unsilenced" => Ok(Self::Unsilenced),
            other => Err(CoreError::UnknownVariant {
                kind: "event kind",
                value: other.to_owned(),
            }),
        }
    }
}

/// Where an event came from.
///
/// Kept on every event because the answer to "why did this card appear an hour late" is almost
/// always that the reconciler produced it after a webhook was lost.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum EventSource {
    /// Alertmanager posted it to the webhook.
    Webhook,

    /// The reconciler found a difference between Alertmanager and the local state.
    Reconciler,

    /// A person did something in Discord.
    User,
}

impl EventSource {
    /// The source as the lowercase word stored in the database.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Webhook => "webhook",
            Self::Reconciler => "reconciler",
            Self::User => "user",
        }
    }
}

impl fmt::Display for EventSource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for EventSource {
    type Err = CoreError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "webhook" => Ok(Self::Webhook),
            "reconciler" => Ok(Self::Reconciler),
            "user" => Ok(Self::User),
            other => Err(CoreError::UnknownVariant {
                kind: "event source",
                value: other.to_owned(),
            }),
        }
    }
}

/// One accepted change to one alert, and the unit the decision pipeline consumes.
///
/// A delta exists only for a change that survived deduplication. A redelivered webhook produces
/// no delta at all, which is what keeps a duplicate from re-rendering a card.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AlertDelta {
    /// What changed.
    pub kind: EventKind,

    /// Where the change came from.
    pub source: EventSource,

    /// The alert as it now stands, not as it was.
    pub alert: Alert,

    /// Consecutive firing periods seen for this fingerprint inside the current episode.
    ///
    /// Zero on a first firing. A resolved alert that re-fires inside the regroup window
    /// increments it and reuses its card rather than posting a second one.
    pub flap_count: u32,

    /// Which firing episode this change belongs to.
    ///
    /// Zero until the alert first re-fires after a whole regroup window of quiet, and one more
    /// on every such re-fire after that. The number is in the per-alert dedupe key, so a new
    /// episode posts a new card instead of reviving one nobody has looked at since it resolved.
    pub episode: u32,

    /// When this change was accepted.
    pub observed_at: DateTime<Utc>,
}

impl AlertDelta {
    /// The dedupe key this delta belongs under for a per-alert route.
    #[must_use]
    pub fn per_alert_key(&self) -> DedupeKey {
        DedupeKey::per_alert(&self.alert.fingerprint, self.episode)
    }

    /// The dedupe key this delta belongs under for a per-group route.
    ///
    /// Falls back to the per-alert key when Alertmanager supplied no group key, which is the case
    /// for an alert the reconciler discovered rather than one a webhook delivered. Grouping is a
    /// property of Alertmanager's routing tree, and the API's alert list does not carry it.
    #[must_use]
    pub fn per_group_key(&self) -> DedupeKey {
        self.alert
            .group_key
            .as_ref()
            .map_or_else(|| self.per_alert_key(), DedupeKey::per_group)
    }
}

/// What separates a fingerprint from its episode number in a per-alert dedupe key.
///
/// Deliberately not a hex digit, so no fingerprint can produce a key that another fingerprint's
/// episode prefix also matches.
const EPISODE_SEPARATOR: char = '#';

/// What a card is keyed by within a channel.
///
/// One card per key per channel, enforced by a unique index, so two dispatcher workers racing on
/// one alert cannot produce two cards: the loser sees the constraint violation and re-reads.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct DedupeKey(String);

impl DedupeKey {
    /// The key for a route that posts one card per alert, in one firing episode.
    ///
    /// The episode is elided at zero, which covers every alert that has never outlived a regroup
    /// window â€” nearly all of them â€” so the ordinary key stays the short one it has always been.
    #[must_use]
    pub fn per_alert(fingerprint: &Fingerprint, episode: u32) -> Self {
        if episode == 0 {
            Self(format!("a:{fingerprint}"))
        } else {
            Self(format!("a:{fingerprint}{EPISODE_SEPARATOR}{episode}"))
        }
    }

    /// Every per-alert key one fingerprint can have: the first episode's key, and the prefix the
    /// rest share.
    ///
    /// Acknowledging an alert answers it on every card showing it, and after a re-fire those
    /// cards are spread across episodes. A caller matches the exact key or anything starting with
    /// the prefix; the separator is not a hex digit, so the prefix cannot reach a longer
    /// fingerprint that happens to begin with this one.
    #[must_use]
    pub fn per_alert_episodes(fingerprint: &Fingerprint) -> (Self, String) {
        (
            Self::per_alert(fingerprint, 0),
            format!("a:{fingerprint}{EPISODE_SEPARATOR}"),
        )
    }

    /// The key for a route that posts one card per Alertmanager group.
    #[must_use]
    pub fn per_group(group: &GroupKey) -> Self {
        Self(format!("g:{group}"))
    }

    /// The key for a route in digest mode, which rolls one card per window.
    #[must_use]
    pub fn digest(route_id: i64, window: DateTime<Utc>) -> Self {
        Self(format!("d:{route_id}:{}", window.timestamp()))
    }

    /// Wraps a key read back out of the database.
    #[must_use]
    pub fn from_stored(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    /// The key as a string slice.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// The worker lane this key belongs to.
    ///
    /// Hashing the key into one of `lanes` lanes puts every piece of work for one alert on one
    /// worker. Two workers then never edit one card concurrently in the common case, so the
    /// ordering that would otherwise need a lock is a property of the queue instead.
    #[must_use]
    pub fn lane(&self, lanes: u16) -> u16 {
        if lanes <= 1 {
            return 0;
        }

        let hash = fnv1a(FNV_OFFSET, self.0.as_bytes());

        u16::try_from(hash % u64::from(lanes)).unwrap_or(0)
    }
}

impl fmt::Display for DedupeKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

#[cfg(test)]
mod tests {
    use chrono::TimeZone;

    use super::*;
    use crate::labels::LabelName;

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

    fn alert(labels: Labels) -> Alert {
        Alert {
            fingerprint: Fingerprint::new("0123456789abcdef").expect("hex is a fingerprint"),
            labels,
            annotations: Annotations::new(),
            starts_at: Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap(),
            ends_at: None,
            generator_url: None,
            status: AlertStatus::Firing,
            am_state: AmState::Active,
            silenced_by: Vec::new(),
            inhibited_by: Vec::new(),
            group_key: None,
        }
    }

    #[test]
    fn severity_falls_back_to_info_rather_than_failing() {
        assert_eq!(Severity::from_labels(&labels(&[])), Severity::Info);
        assert_eq!(
            Severity::from_labels(&labels(&[("severity", "nonsense")])),
            Severity::Info
        );
        assert_eq!(
            Severity::from_labels(&labels(&[("severity", "PAGE")])),
            Severity::Critical
        );
    }

    #[test]
    fn severities_order_from_least_to_most_urgent() {
        assert!(Severity::Critical > Severity::Warning);
        assert!(Severity::Warning > Severity::Info);
    }

    #[test]
    fn an_alert_without_an_alertname_still_has_a_title() {
        let alert = alert(labels(&[]));

        assert_eq!(alert.name(), "01234567");
    }

    #[test]
    fn changed_annotations_carry_only_the_difference() {
        let mut previous = Annotations::new();
        previous.insert("summary", "one");
        previous.insert("description", "unchanged");

        let mut current = Annotations::new();
        current.insert("summary", "two");
        current.insert("description", "unchanged");

        let changed = current.changed_from(&previous);

        assert_eq!(changed.len(), 1);
        assert_eq!(changed.get("summary"), Some("two"));
    }

    #[test]
    fn dedupe_keys_of_different_kinds_never_collide() {
        let fingerprint = Fingerprint::new("abcdef").expect("hex is a fingerprint");
        let group = GroupKey::new("abcdef");

        assert_ne!(
            DedupeKey::per_alert(&fingerprint, 0),
            DedupeKey::per_group(&group)
        );
    }

    #[test]
    fn a_later_episode_is_a_different_card() {
        let fingerprint = Fingerprint::new("abcdef").expect("hex is a fingerprint");

        assert_ne!(
            DedupeKey::per_alert(&fingerprint, 0),
            DedupeKey::per_alert(&fingerprint, 1)
        );
    }

    #[test]
    fn an_episode_prefix_cannot_reach_a_longer_fingerprint() {
        let short = Fingerprint::new("abcdef").expect("hex is a fingerprint");
        let longer = Fingerprint::new("abcdef01").expect("hex is a fingerprint");

        let (_, prefix) = DedupeKey::per_alert_episodes(&short);

        assert!(
            !DedupeKey::per_alert(&longer, 0)
                .as_str()
                .starts_with(&prefix)
        );
        assert!(
            !DedupeKey::per_alert(&longer, 3)
                .as_str()
                .starts_with(&prefix)
        );
        assert!(
            DedupeKey::per_alert(&short, 3)
                .as_str()
                .starts_with(&prefix)
        );
    }

    #[test]
    fn a_key_always_lands_in_the_same_lane() {
        let key = DedupeKey::per_alert(
            &Fingerprint::new("0123456789abcdef").expect("hex is a fingerprint"),
            0,
        );

        assert_eq!(key.lane(4), key.lane(4));
        assert!(key.lane(4) < 4);
        assert_eq!(key.lane(1), 0);
        assert_eq!(key.lane(0), 0);
    }
}
