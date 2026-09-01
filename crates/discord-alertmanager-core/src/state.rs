//! The notification state machine: what a card is showing, and what may change it.

use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Serialize};

use crate::CoreError;
use crate::alert::{AlertStatus, AmState, EventKind};

/// What one card is currently showing.
///
/// The state belongs to the notification rather than to the alert, because two routes can hold
/// different views of one alert: acknowledged in the channel where someone took it, still firing
/// in the channel that only watches. Putting the state on the alert would make an acknowledgement
/// in one guild silently answer another guild's page.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum NotificationState {
    /// Firing and unacknowledged. The state that pins, mentions and colours red.
    Firing,

    /// Somebody has taken it. Still firing.
    Acked,

    /// An Alertmanager silence is suppressing it. Still firing, and nobody is being told.
    Silenced,

    /// A bot-local ignore rule is muting it. Alertmanager is still notifying everyone else.
    Ignored,

    /// The condition stopped holding.
    Resolved,

    /// The card is gone or unreachable, and this row no longer describes anything in Discord.
    Orphaned,
}

impl NotificationState {
    /// The state as the lowercase word stored in the database.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Firing => "firing",
            Self::Acked => "acked",
            Self::Silenced => "silenced",
            Self::Ignored => "ignored",
            Self::Resolved => "resolved",
            Self::Orphaned => "orphaned",
        }
    }

    /// Whether the alert behind this card is still firing.
    #[must_use]
    pub fn is_open(self) -> bool {
        matches!(self, Self::Firing | Self::Acked | Self::Silenced)
    }

    /// Whether a card in this state still carries working buttons.
    ///
    /// A resolved card keeps a single disabled control rather than a live set. An orphaned one
    /// has nothing to carry them.
    #[must_use]
    pub fn has_components(self) -> bool {
        self.is_open() || self == Self::Ignored
    }

    /// Whether an unacknowledged card in this state should be pinned.
    ///
    /// Pinning is the "needs attention" tray, so it survives neither an acknowledgement nor a
    /// silence: both mean somebody has already decided what to do about the alert.
    #[must_use]
    pub fn wants_pin(self) -> bool {
        self == Self::Firing
    }
}

impl fmt::Display for NotificationState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for NotificationState {
    type Err = CoreError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "firing" => Ok(Self::Firing),
            "acked" => Ok(Self::Acked),
            "silenced" => Ok(Self::Silenced),
            "ignored" => Ok(Self::Ignored),
            "resolved" => Ok(Self::Resolved),
            "orphaned" => Ok(Self::Orphaned),
            other => Err(CoreError::UnknownVariant {
                kind: "notification state",
                value: other.to_owned(),
            }),
        }
    }
}

/// Everything that can move a card from one state to another.
///
/// Named after what happened rather than after the state it produces, because the same input
/// produces different states from different starting points: an unsilence returns a card to
/// `Firing` or to `Acked` depending on whether anyone had taken it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Trigger {
    /// The alert started, or restarted after resolving.
    Fired,

    /// The alert resolved.
    Resolved,

    /// Somebody acknowledged it, by button or by command.
    Acknowledged,

    /// Somebody revoked the acknowledgement.
    AckRevoked,

    /// Alertmanager started suppressing it.
    Silenced,

    /// Alertmanager stopped suppressing it.
    Unsilenced,

    /// A bot-local ignore rule started matching it.
    Ignored,

    /// The ignore rule was removed or expired.
    Unignored,

    /// The Discord message backing this card is gone.
    MessageLost,
}

impl Trigger {
    /// The trigger an ingest event implies, if it implies one.
    ///
    /// `Updated` implies none: it re-renders a card without moving it, which is the distinction
    /// between an edit and a transition.
    #[must_use]
    pub fn from_event(kind: EventKind) -> Option<Self> {
        match kind {
            EventKind::Fired => Some(Self::Fired),
            EventKind::Resolved => Some(Self::Resolved),
            EventKind::Silenced => Some(Self::Silenced),
            EventKind::Unsilenced => Some(Self::Unsilenced),
            EventKind::Updated => None,
        }
    }
}

/// The state a card should be in, given where it is and what just happened.
///
/// Returns `None` when the trigger does not apply, which is not an error: a redelivered resolve
/// for an already-resolved card, or an unsilence for a card no silence ever touched, are both
/// normal. Returning `None` rather than the unchanged state is what lets the caller skip the edit
/// entirely instead of computing a render hash to discover it had nothing to do.
#[must_use]
pub fn next_state(
    current: NotificationState,
    trigger: Trigger,
    acknowledged: bool,
) -> Option<NotificationState> {
    use NotificationState as S;

    // An orphaned card is terminal. Its row is kept so the history survives, and a replacement is
    // posted under a new row rather than this one being revived.
    if current == S::Orphaned {
        return None;
    }

    // Coming back from resolved, silenced or ignored lands on whichever of the two open states
    // the card was in before, so an alert that flaps does not silently drop an acknowledgement
    // somebody is acting on.
    let resume = if acknowledged { S::Acked } else { S::Firing };

    let next = match trigger {
        Trigger::MessageLost => S::Orphaned,
        Trigger::Fired if current == S::Resolved => resume,
        Trigger::Resolved if current.is_open() || current == S::Ignored => S::Resolved,
        // An acknowledgement while silenced changes nothing: the silence is the stronger
        // statement about the alert, and it is the one an operator needs to see on the card.
        Trigger::Acknowledged if current == S::Firing => S::Acked,
        Trigger::AckRevoked if current == S::Acked => S::Firing,
        Trigger::Silenced if matches!(current, S::Firing | S::Acked) => S::Silenced,
        Trigger::Unsilenced if current == S::Silenced => resume,
        Trigger::Ignored if matches!(current, S::Firing | S::Acked) => S::Ignored,
        Trigger::Unignored if current == S::Ignored => resume,
        _ => return None,
    };

    (next != current).then_some(next)
}

/// The state a card starts in for a newly delivered alert.
///
/// A card is never born acknowledged, so the only question is whether Alertmanager is already
/// suppressing the alert when it first arrives — which happens whenever a silence predates the
/// alert it covers.
#[must_use]
pub fn initial_state(status: AlertStatus, am_state: AmState, ignored: bool) -> NotificationState {
    match (status, am_state, ignored) {
        (AlertStatus::Resolved, _, _) => NotificationState::Resolved,
        (_, _, true) => NotificationState::Ignored,
        (_, state, _) if state.is_suppressed() => NotificationState::Silenced,
        _ => NotificationState::Firing,
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::NotificationState as S;
    use super::*;

    #[rstest]
    #[case(S::Firing, Trigger::Acknowledged, false, Some(S::Acked))]
    #[case(S::Firing, Trigger::Silenced, false, Some(S::Silenced))]
    #[case(S::Firing, Trigger::Resolved, false, Some(S::Resolved))]
    #[case(S::Firing, Trigger::Ignored, false, Some(S::Ignored))]
    #[case(S::Firing, Trigger::MessageLost, false, Some(S::Orphaned))]
    #[case(S::Acked, Trigger::AckRevoked, true, Some(S::Firing))]
    #[case(S::Acked, Trigger::Silenced, true, Some(S::Silenced))]
    #[case(S::Acked, Trigger::Resolved, true, Some(S::Resolved))]
    #[case(S::Silenced, Trigger::Unsilenced, false, Some(S::Firing))]
    #[case(S::Silenced, Trigger::Unsilenced, true, Some(S::Acked))]
    #[case(S::Silenced, Trigger::Resolved, false, Some(S::Resolved))]
    #[case(S::Ignored, Trigger::Unignored, false, Some(S::Firing))]
    #[case(S::Ignored, Trigger::Unignored, true, Some(S::Acked))]
    #[case(S::Resolved, Trigger::Fired, false, Some(S::Firing))]
    #[case(S::Resolved, Trigger::Fired, true, Some(S::Acked))]
    fn the_transition_table_holds(
        #[case] current: S,
        #[case] trigger: Trigger,
        #[case] acknowledged: bool,
        #[case] expected: Option<S>,
    ) {
        assert_eq!(next_state(current, trigger, acknowledged), expected);
    }

    #[rstest]
    #[case(S::Resolved, Trigger::Resolved)]
    #[case(S::Firing, Trigger::Fired)]
    #[case(S::Firing, Trigger::AckRevoked)]
    #[case(S::Firing, Trigger::Unsilenced)]
    #[case(S::Acked, Trigger::Acknowledged)]
    #[case(S::Silenced, Trigger::Acknowledged)]
    #[case(S::Resolved, Trigger::Acknowledged)]
    fn a_trigger_that_changes_nothing_produces_no_transition(
        #[case] current: S,
        #[case] trigger: Trigger,
    ) {
        assert_eq!(next_state(current, trigger, false), None);
    }

    #[rstest]
    #[case(Trigger::Fired)]
    #[case(Trigger::Resolved)]
    #[case(Trigger::Acknowledged)]
    #[case(Trigger::AckRevoked)]
    #[case(Trigger::Silenced)]
    #[case(Trigger::Unsilenced)]
    #[case(Trigger::Ignored)]
    #[case(Trigger::Unignored)]
    #[case(Trigger::MessageLost)]
    fn an_orphaned_card_is_terminal(#[case] trigger: Trigger) {
        assert_eq!(next_state(S::Orphaned, trigger, false), None);
    }

    #[test]
    fn a_flap_keeps_an_acknowledgement_that_was_never_revoked() {
        assert_eq!(
            next_state(S::Resolved, Trigger::Fired, true),
            Some(S::Acked)
        );
    }

    #[test]
    fn a_silence_that_predates_its_alert_produces_a_silenced_card() {
        assert_eq!(
            initial_state(AlertStatus::Firing, AmState::Suppressed, false),
            S::Silenced
        );
        assert_eq!(
            initial_state(AlertStatus::Firing, AmState::Active, false),
            S::Firing
        );
        assert_eq!(
            initial_state(AlertStatus::Firing, AmState::Active, true),
            S::Ignored
        );
        assert_eq!(
            initial_state(AlertStatus::Resolved, AmState::Active, false),
            S::Resolved
        );
    }

    #[test]
    fn an_update_is_not_a_transition() {
        assert_eq!(Trigger::from_event(EventKind::Updated), None);
        assert_eq!(Trigger::from_event(EventKind::Fired), Some(Trigger::Fired));
    }
}
