//! Why two cards are two cards.
//!
//! The reverse of [`crate::decide`]: given two cards an operator expected to be one, it names the
//! input that sent them apart. Pure for the same reason the decision is, and kept beside it so a
//! change to what "the same card" means has to be made in both places within one crate.
//!
//! The checks run in the order the decision itself would separate two alerts, so the reason given
//! is the first one that holds rather than the most interesting one. Two cards in different
//! channels may also differ in their labels, and the channel is still the answer: no label change
//! would have put them on one card.

use std::collections::BTreeSet;

use chrono::{DateTime, Utc};
use dam_core::{Alert, KeyScope, Retirement};
use dam_store::{ChannelId, GroupStrategy, Notification, NotificationId, Route};

use crate::decide::DecisionSettings;

/// One side of the comparison: a card, and what the caller could read about it.
#[derive(Debug, Clone, Copy)]
pub struct CardFacts<'a> {
    /// The card.
    pub card: &'a Notification,

    /// The alert the card last showed, when the store still holds it.
    ///
    /// Absent once retention has pruned it. The labels are then unknown, and so is any answer that
    /// depends on them.
    pub alert: Option<&'a Alert>,

    /// The route that posted the card, when it still exists.
    pub route: Option<&'a Route>,
}

/// How a card's key was chosen.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Strategy {
    /// One card per alert identity.
    PerAlert,

    /// One card per Alertmanager group.
    PerGroup,

    /// One rolling card per window, because the route is configured to digest.
    Digest,

    /// One rolling card per window, because the route was over its storm threshold.
    StormDigest,

    /// A key no strategy produces.
    Unrecognised,
}

/// One label on which the two alerts disagree.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LabelDifference {
    /// The label's name.
    pub name: String,

    /// Its value on the first card's alert, or `None` when that alert does not carry it.
    pub first: Option<String>,

    /// Its value on the second card's alert, or `None` when that alert does not carry it.
    pub second: Option<String>,
}

/// The reason two cards are separate.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Separation {
    /// Both sides are the same card.
    SameCard,

    /// The cards live in different channels, and a key is only unique within one.
    DifferentChannels {
        /// The first card's channel.
        first: ChannelId,
        /// The second card's channel.
        second: ChannelId,
    },

    /// The alert re-fired after `older` had been resolved for longer than the regroup window, so
    /// `newer` was posted in its place.
    Replaced {
        /// The card that was resolved for too long.
        older: NotificationId,
        /// The card posted in its place.
        newer: NotificationId,
    },

    /// One card gave its key up, so nothing new can land on it.
    Retired {
        /// The card that gave its key up.
        card: NotificationId,
        /// Why it did.
        why: Retirement,
    },

    /// The cards were keyed by different strategies.
    DifferentStrategies {
        /// How the first card was keyed.
        first: Strategy,
        /// How the second card was keyed.
        second: Strategy,
    },

    /// Per-alert cards whose alerts differ in a label the identity policy counts.
    DifferentIdentities {
        /// The labels that differ and are counted, by name.
        differences: Vec<LabelDifference>,
    },

    /// Per-alert cards whose alerts the identity policy now puts on one card.
    ///
    /// A card keeps the key it was created under, so cards posted before the policy learned to
    /// ignore a label stay apart until they are replaced.
    KeyedBeforePolicy,

    /// Per-group cards for different Alertmanager groups.
    DifferentGroups {
        /// Every label on which the alerts each card last showed differ.
        ///
        /// Context rather than cause: Alertmanager's `group_by` decides the group, and which of
        /// these labels it names is in Alertmanager's configuration, not this one.
        differences: Vec<LabelDifference>,
    },

    /// Digest cards from different routes.
    DifferentDigestRoutes,

    /// Digest cards from one route for different windows.
    DifferentDigestWindows {
        /// When the first card's window opened.
        first: DateTime<Utc>,
        /// When the second card's window opened.
        second: DateTime<Utc>,
    },

    /// The answer depends on labels, and at least one alert has been pruned.
    AlertUnknown,

    /// Nothing that decides a card separates these two.
    ///
    /// Not reachable through the decision as written. Reported rather than hidden, because it means
    /// the database holds something the decision would not have produced.
    Unexplained,
}

/// Names the first reason the decision would have kept `first` and `second` on separate cards.
#[must_use]
pub fn explain_separation(
    first: &CardFacts<'_>,
    second: &CardFacts<'_>,
    settings: &DecisionSettings,
) -> Separation {
    let (a, b) = (first.card, second.card);

    if a.id == b.id {
        return Separation::SameCard;
    }

    if a.channel_id != b.channel_id {
        return Separation::DifferentChannels {
            first: a.channel_id,
            second: b.channel_id,
        };
    }

    // Before the key comparison: a replacement takes its predecessor's key, so a replaced pair
    // would otherwise read as two cards that agree on everything.
    if b.supersedes == Some(a.id) {
        return Separation::Replaced {
            older: a.id,
            newer: b.id,
        };
    }

    if a.supersedes == Some(b.id) {
        return Separation::Replaced {
            older: b.id,
            newer: a.id,
        };
    }

    for card in [a, b] {
        if let Some(why) = card.dedupe_key.retirement() {
            return Separation::Retired { card: card.id, why };
        }
    }

    match (a.dedupe_key.scope(), b.dedupe_key.scope()) {
        (KeyScope::Alert(_), KeyScope::Alert(_)) => {
            let (Some(left), Some(right)) = (first.alert, second.alert) else {
                return Separation::AlertUnknown;
            };

            if settings.identity.identity(left) == settings.identity.identity(right) {
                return Separation::KeyedBeforePolicy;
            }

            let differences: Vec<LabelDifference> = label_differences(left, right)
                .into_iter()
                .filter(|difference| !settings.identity.ignores(&difference.name))
                .collect();

            Separation::DifferentIdentities { differences }
        }

        (KeyScope::Group(left), KeyScope::Group(right)) if left != right => {
            Separation::DifferentGroups {
                differences: match (first.alert, second.alert) {
                    (Some(left), Some(right)) => label_differences(left, right),
                    _ => Vec::new(),
                },
            }
        }

        (
            KeyScope::Digest {
                route_id: left_route,
                window: left_window,
            },
            KeyScope::Digest {
                route_id: right_route,
                window: right_window,
            },
        ) => {
            if left_route != right_route {
                Separation::DifferentDigestRoutes
            } else if left_window != right_window {
                Separation::DifferentDigestWindows {
                    first: left_window,
                    second: right_window,
                }
            } else {
                Separation::Unexplained
            }
        }

        (left, right) => {
            let (left, right) = (
                strategy_of(left, first.route),
                strategy_of(right, second.route),
            );

            if left == right {
                Separation::Unexplained
            } else {
                Separation::DifferentStrategies {
                    first: left,
                    second: right,
                }
            }
        }
    }
}

/// How a key in `scope` was chosen, given the route that chose it.
///
/// A digest key on a route not configured to digest can only have come from the storm fallback,
/// which is the one case worth telling apart: it is transient, and the next quiet window ends it.
fn strategy_of(scope: KeyScope<'_>, route: Option<&Route>) -> Strategy {
    match scope {
        KeyScope::Alert(_) => Strategy::PerAlert,
        KeyScope::Group(_) => Strategy::PerGroup,
        KeyScope::Digest { .. } => match route {
            Some(route) if route.group_strategy != GroupStrategy::Digest => Strategy::StormDigest,
            _ => Strategy::Digest,
        },
        KeyScope::Unrecognised => Strategy::Unrecognised,
    }
}

/// Every label on which `first` and `second` disagree, by name.
fn label_differences(first: &Alert, second: &Alert) -> Vec<LabelDifference> {
    let names: BTreeSet<&str> = first
        .labels
        .iter()
        .chain(second.labels.iter())
        .map(|(name, _)| name.as_str())
        .collect();

    names
        .into_iter()
        .filter_map(|name| {
            let (left, right) = (first.labels.get(name), second.labels.get(name));

            (left != right).then(|| LabelDifference {
                name: name.to_owned(),
                first: left.map(str::to_owned),
                second: right.map(str::to_owned),
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use chrono::TimeZone;
    use dam_core::{
        AlertStatus, AmState, Annotations, DedupeKey, Fingerprint, GroupKey, IdentityPolicy,
        LabelName, Labels, MatcherSet, NotificationState,
    };
    use dam_store::{GuildId, Mentions, RouteId, RouteSource, RouteTarget, ThreadPolicy};

    use super::*;

    fn at(minute: u32) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 1, 1, 0, minute, 0).unwrap()
    }

    fn alert(fingerprint: &str, pairs: &[(&str, &str)]) -> Alert {
        Alert {
            fingerprint: Fingerprint::new(fingerprint).expect("hex is a fingerprint"),
            labels: pairs
                .iter()
                .map(|(name, value)| {
                    (
                        LabelName::new(*name).expect("test label name is valid"),
                        (*value).to_owned(),
                    )
                })
                .collect::<Labels>(),
            annotations: Annotations::new(),
            starts_at: at(0),
            ends_at: None,
            generator_url: None,
            status: AlertStatus::Firing,
            am_state: AmState::Active,
            silenced_by: Vec::new(),
            inhibited_by: Vec::new(),
            group_key: None,
        }
    }

    fn card(id: i64, channel: u64, key: DedupeKey) -> Notification {
        Notification {
            id: NotificationId::new(id),
            dedupe_key: key,
            fingerprint: Fingerprint::new("aaaa").expect("hex is a fingerprint"),
            route_id: RouteId::new(1),
            guild_id: GuildId::new(1),
            channel_id: ChannelId::new(channel),
            message_id: None,
            thread_id: None,
            state: NotificationState::Firing,
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
            created_at: at(0),
            updated_at: at(0),
        }
    }

    fn route(strategy: GroupStrategy) -> Route {
        Route {
            id: RouteId::new(1),
            guild_id: GuildId::new(1),
            name: "payments".to_owned(),
            matcher_source: "severity=critical".to_owned(),
            matchers: MatcherSet::parse("severity=critical").expect("the expression parses"),
            min_severity: None,
            target: RouteTarget::Text {
                channel: ChannelId::new(10),
                thread: ThreadPolicy::default(),
            },
            group_strategy: strategy,
            mentions: Mentions::default(),
            escalation: None,
            priority: 100,
            continue_to_next: false,
            source: RouteSource::Config,
            enabled: true,
            created_by: None,
            created_at: at(0),
        }
    }

    fn per_alert(identity: &str) -> DedupeKey {
        DedupeKey::per_alert(&Fingerprint::new(identity).expect("hex is a fingerprint"))
    }

    fn facts<'a>(
        card: &'a Notification,
        alert: Option<&'a Alert>,
        route: Option<&'a Route>,
    ) -> CardFacts<'a> {
        CardFacts { card, alert, route }
    }

    fn settings(ignored: &[&str]) -> DecisionSettings {
        DecisionSettings {
            identity: IdentityPolicy::new(ignored.iter().copied()),
            ..DecisionSettings::default()
        }
    }

    #[test]
    fn one_card_twice_is_the_same_card() {
        let one = card(1, 10, per_alert("aaaa"));

        assert_eq!(
            explain_separation(
                &facts(&one, None, None),
                &facts(&one, None, None),
                &settings(&[])
            ),
            Separation::SameCard
        );
    }

    #[test]
    fn the_channel_is_the_answer_even_when_the_labels_also_differ() {
        let first = card(1, 10, per_alert("aaaa"));
        let second = card(2, 11, per_alert("bbbb"));
        let left = alert("aaaa", &[("alertname", "Down"), ("pod", "a")]);
        let right = alert("bbbb", &[("alertname", "Down"), ("pod", "b")]);

        assert_eq!(
            explain_separation(
                &facts(&first, Some(&left), None),
                &facts(&second, Some(&right), None),
                &settings(&[])
            ),
            Separation::DifferentChannels {
                first: ChannelId::new(10),
                second: ChannelId::new(11),
            }
        );
    }

    #[test]
    fn a_replacement_is_named_whichever_side_it_is_given_on() {
        let older = card(1, 10, DedupeKey::from_stored("superseded:1:a:aaaa"));
        let mut newer = card(2, 10, per_alert("aaaa"));
        newer.supersedes = Some(older.id);

        let expected = Separation::Replaced {
            older: older.id,
            newer: newer.id,
        };

        assert_eq!(
            explain_separation(
                &facts(&older, None, None),
                &facts(&newer, None, None),
                &settings(&[])
            ),
            expected
        );
        assert_eq!(
            explain_separation(
                &facts(&newer, None, None),
                &facts(&older, None, None),
                &settings(&[])
            ),
            expected
        );
    }

    #[test]
    fn a_deleted_card_is_retired() {
        let orphaned = card(1, 10, DedupeKey::from_stored("orphaned:1:a:aaaa"));
        let live = card(2, 10, per_alert("aaaa"));

        assert_eq!(
            explain_separation(
                &facts(&live, None, None),
                &facts(&orphaned, None, None),
                &settings(&[])
            ),
            Separation::Retired {
                card: orphaned.id,
                why: Retirement::Orphaned,
            }
        );
    }

    #[test]
    fn per_alert_cards_report_only_the_labels_the_policy_counts() {
        let first = card(1, 10, per_alert("aaaa"));
        let second = card(2, 10, per_alert("bbbb"));
        let left = alert(
            "aaaa",
            &[("alertname", "Down"), ("namespace", "a"), ("pod", "x")],
        );
        let right = alert(
            "bbbb",
            &[("alertname", "Down"), ("pod", "y"), ("zone", "eu")],
        );

        assert_eq!(
            explain_separation(
                &facts(&first, Some(&left), None),
                &facts(&second, Some(&right), None),
                &settings(&["pod"])
            ),
            Separation::DifferentIdentities {
                differences: vec![
                    LabelDifference {
                        name: "namespace".to_owned(),
                        first: Some("a".to_owned()),
                        second: None,
                    },
                    LabelDifference {
                        name: "zone".to_owned(),
                        first: None,
                        second: Some("eu".to_owned()),
                    },
                ],
            }
        );
    }

    #[test]
    fn alerts_the_policy_now_merges_were_keyed_before_it_did() {
        let first = card(1, 10, per_alert("aaaa"));
        let second = card(2, 10, per_alert("bbbb"));
        let left = alert("aaaa", &[("alertname", "Down"), ("pod", "x")]);
        let right = alert("bbbb", &[("alertname", "Down"), ("pod", "y")]);

        assert_eq!(
            explain_separation(
                &facts(&first, Some(&left), None),
                &facts(&second, Some(&right), None),
                &settings(&["pod"])
            ),
            Separation::KeyedBeforePolicy
        );
    }

    #[test]
    fn per_alert_cards_without_their_alerts_cannot_be_compared() {
        let first = card(1, 10, per_alert("aaaa"));
        let second = card(2, 10, per_alert("bbbb"));

        assert_eq!(
            explain_separation(
                &facts(&first, None, None),
                &facts(&second, None, None),
                &settings(&[])
            ),
            Separation::AlertUnknown
        );
    }

    #[test]
    fn group_cards_name_every_differing_label_as_context() {
        let first = card(1, 10, DedupeKey::per_group(&GroupKey::new("{}:{a=\"1\"}")));
        let second = card(2, 10, DedupeKey::per_group(&GroupKey::new("{}:{a=\"2\"}")));
        let left = alert("aaaa", &[("a", "1")]);
        let right = alert("bbbb", &[("a", "2")]);

        assert_eq!(
            explain_separation(
                &facts(&first, Some(&left), None),
                &facts(&second, Some(&right), None),
                &settings(&[])
            ),
            Separation::DifferentGroups {
                differences: vec![LabelDifference {
                    name: "a".to_owned(),
                    first: Some("1".to_owned()),
                    second: Some("2".to_owned()),
                }],
            }
        );
    }

    #[test]
    fn digest_cards_differ_by_route_before_window() {
        let window = |minute| DedupeKey::digest(1, at(minute));
        let first = card(1, 10, window(0));
        let later = card(2, 10, window(5));
        let elsewhere = card(3, 10, DedupeKey::digest(2, at(5)));

        assert_eq!(
            explain_separation(
                &facts(&first, None, None),
                &facts(&later, None, None),
                &settings(&[])
            ),
            Separation::DifferentDigestWindows {
                first: at(0),
                second: at(5),
            }
        );
        assert_eq!(
            explain_separation(
                &facts(&first, None, None),
                &facts(&elsewhere, None, None),
                &settings(&[])
            ),
            Separation::DifferentDigestRoutes
        );
    }

    #[test]
    fn a_digest_on_a_route_that_does_not_digest_is_a_storm() {
        let per_alert_route = route(GroupStrategy::PerAlert);
        let digest_route = route(GroupStrategy::Digest);
        let first = card(1, 10, per_alert("aaaa"));
        let second = card(2, 10, DedupeKey::digest(1, at(0)));

        let separation = |route| {
            explain_separation(
                &facts(&first, None, Some(&per_alert_route)),
                &facts(&second, None, Some(route)),
                &settings(&[]),
            )
        };

        assert_eq!(
            separation(&per_alert_route),
            Separation::DifferentStrategies {
                first: Strategy::PerAlert,
                second: Strategy::StormDigest,
            }
        );
        assert_eq!(
            separation(&digest_route),
            Separation::DifferentStrategies {
                first: Strategy::PerAlert,
                second: Strategy::Digest,
            }
        );
    }
}
