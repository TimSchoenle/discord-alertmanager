//! Which alerts one card treats as the same alert.
//!
//! Alertmanager's fingerprint covers every label, so a label whose value churns gives the same
//! condition a new fingerprint each time it churns. A Kubernetes rollout is the everyday case: the
//! `pod` label changes with every replacement pod, and each replacement arrives as a new alert
//! that a per-alert route posts as a new card. An [`IdentityPolicy`] names the labels that do not
//! distinguish one alert from another, and the identity it computes is what a per-alert card is
//! keyed by.

use std::collections::BTreeSet;

use crate::alert::Alert;
use crate::labels::{Fingerprint, Labels};

/// What separates a name from its value, and a pair from the next, in the identity hash.
///
/// `0xff` cannot occur in UTF-8, so no pair of labels can be spelled as another pair.
const SEPARATOR: u8 = 0xff;

/// The labels that do not count towards which card an alert belongs on.
///
/// Empty by default, and empty makes the identity exactly the fingerprint: nothing is merged that
/// Alertmanager keeps apart.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct IdentityPolicy {
    ignored: BTreeSet<String>,
}

impl IdentityPolicy {
    /// Builds a policy that ignores the named labels.
    ///
    /// A name outside Prometheus's grammar is kept rather than refused. No label can carry it, so
    /// it never matches, which is the same outcome a refusal would have produced one step later.
    pub fn new<I, S>(ignored: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        Self {
            ignored: ignored.into_iter().map(Into::into).collect(),
        }
    }

    /// Whether the policy merges nothing, so every identity is a fingerprint.
    ///
    /// A caller uses this to skip work that only matters when two fingerprints can share a card.
    #[must_use]
    pub fn is_exact(&self) -> bool {
        self.ignored.is_empty()
    }

    /// The identity of one alert.
    #[must_use]
    pub fn identity(&self, alert: &Alert) -> Fingerprint {
        self.identity_of(&alert.fingerprint, &alert.labels)
    }

    /// The identity of an alert given its fingerprint and labels.
    ///
    /// The fingerprint itself when no ignored label is present, so an alert the policy does not
    /// touch keeps the card key it has always had. Otherwise a hash of the remaining labels, built
    /// the way Prometheus builds a label-set fingerprint: sorted names, FNV-1a, a `0xff` after
    /// every name and every value. Removing nothing from a set therefore produces the
    /// fingerprint Alertmanager already assigned to it, and an alert that never carried the
    /// ignored label shares a card with the ones that did.
    #[must_use]
    pub fn identity_of(&self, fingerprint: &Fingerprint, labels: &Labels) -> Fingerprint {
        if !labels
            .iter()
            .any(|(name, _)| self.ignored.contains(name.as_str()))
        {
            return fingerprint.clone();
        }

        let mut hash = crate::labels::FNV_OFFSET;
        for (name, value) in labels.iter() {
            if self.ignored.contains(name.as_str()) {
                continue;
            }

            hash = crate::labels::fnv1a(hash, name.as_str().as_bytes());
            hash = crate::labels::fnv1a(hash, &[SEPARATOR]);
            hash = crate::labels::fnv1a(hash, value.as_bytes());
            hash = crate::labels::fnv1a(hash, &[SEPARATOR]);
        }

        Fingerprint::from_hash(hash)
    }
}

#[cfg(test)]
mod tests {
    use chrono::{TimeZone, Utc};

    use super::*;
    use crate::alert::{AlertStatus, AmState, Annotations};
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

    fn alert(fingerprint: &str, pairs: &[(&str, &str)]) -> Alert {
        Alert {
            fingerprint: Fingerprint::new(fingerprint).expect("hex is a fingerprint"),
            labels: labels(pairs),
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
    fn the_hash_is_the_one_prometheus_computes_for_a_label_set() {
        // The vector from `prometheus/common`'s own fingerprint test. Matching it is what lets an
        // alert without the ignored label share a card with the ones that carry it: both
        // identities are then Alertmanager's fingerprint of the same reduced set.
        let policy = IdentityPolicy::new(["pod"]);
        let reduced = policy.identity_of(
            &Fingerprint::new("0").expect("hex is a fingerprint"),
            &labels(&[
                ("name", "garland, briggs"),
                ("fear", "love is not enough"),
                ("pod", "a"),
            ]),
        );

        assert_eq!(
            reduced.as_str(),
            format!("{:016x}", 5_799_056_148_416_392_346_u64)
        );
    }

    #[test]
    fn an_exact_policy_keeps_the_fingerprint() {
        let alert = alert("abc123", &[("alertname", "Down"), ("pod", "a")]);

        assert!(IdentityPolicy::default().is_exact());
        assert_eq!(
            IdentityPolicy::default().identity(&alert),
            alert.fingerprint
        );
    }

    #[test]
    fn an_alert_without_an_ignored_label_keeps_its_fingerprint() {
        let alert = alert("abc123", &[("alertname", "Down"), ("namespace", "prod")]);

        assert_eq!(
            IdentityPolicy::new(["pod"]).identity(&alert),
            alert.fingerprint
        );
    }

    #[test]
    fn alerts_differing_only_in_an_ignored_label_share_an_identity() {
        let policy = IdentityPolicy::new(["pod"]);
        let first = alert("aaaa", &[("alertname", "Down"), ("pod", "web-1")]);
        let second = alert("bbbb", &[("alertname", "Down"), ("pod", "web-2")]);

        assert_eq!(policy.identity(&first), policy.identity(&second));
    }

    #[test]
    fn a_label_that_is_not_ignored_still_separates_alerts() {
        let policy = IdentityPolicy::new(["pod"]);
        let first = alert(
            "aaaa",
            &[("alertname", "Down"), ("namespace", "a"), ("pod", "x")],
        );
        let second = alert(
            "bbbb",
            &[("alertname", "Down"), ("namespace", "b"), ("pod", "x")],
        );

        assert_ne!(policy.identity(&first), policy.identity(&second));
    }
}
