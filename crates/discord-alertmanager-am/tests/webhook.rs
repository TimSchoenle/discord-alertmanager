//! The version-4 envelope, against a payload shaped like one a real receiver posts.
//!
//! The unit tests beside the type cover the edges. This one covers the whole document, escaped
//! group key and all, because the shape of a real payload is the thing most likely to have been
//! simplified into something that parses when the real one would not.

use chrono::{TimeZone, Utc};
use dam_am::{WEBHOOK_VERSION, WebhookPayload};
use dam_core::{AlertStatus, AmState, GroupKey};

const PAYLOAD: &str = include_str!("fixtures/webhook_v4.json");

#[test]
fn a_captured_payload_parses_into_the_alerts_it_carries() {
    let payload: WebhookPayload = serde_json::from_str(PAYLOAD).expect("the payload decodes");

    assert_eq!(payload.version, WEBHOOK_VERSION);
    assert_eq!(payload.receiver, "discord");
    assert_eq!(payload.status, AlertStatus::Firing);
    assert!(!payload.is_truncated());
    assert_eq!(payload.group_labels.len(), 2);
    assert_eq!(payload.common_labels.len(), 3);
    assert!(payload.common_annotations.is_empty());

    let group_key = payload.group_key.clone();
    let alerts = payload.into_alerts().expect("the alerts convert");

    assert_eq!(alerts.len(), 2);

    // The group key contains quotes, braces and a regex, and every alert has to carry it verbatim
    // for a per-group route to key on it.
    for alert in &alerts {
        assert_eq!(
            alert.group_key.as_ref().map(GroupKey::as_str),
            Some(group_key.as_str())
        );
        assert_eq!(alert.am_state, AmState::Active);
        assert!(alert.silenced_by.is_empty());
    }

    assert_eq!(alerts[0].name(), "PodDown");
    assert_eq!(alerts[0].severity().as_str(), "critical");
    assert_eq!(alerts[0].status, AlertStatus::Firing);
    assert_eq!(alerts[0].ends_at, None);
    assert_eq!(
        alerts[0].annotations.runbook_url(),
        Some("https://runbooks.example.com/PodDown")
    );

    assert_eq!(alerts[1].status, AlertStatus::Resolved);
    assert_eq!(
        alerts[1].ends_at,
        Utc.with_ymd_and_hms(2026, 3, 4, 9, 30, 0).single()
    );
}

#[test]
fn a_payload_from_a_version_this_crate_has_not_seen_is_refused() {
    let payload: WebhookPayload =
        serde_json::from_str(&PAYLOAD.replace(r#""version": "4""#, r#""version": "5""#))
            .expect("the payload decodes");

    assert!(payload.ensure_supported().is_err());
    assert!(payload.into_alerts().is_err());
}

#[test]
fn a_truncated_group_says_so_and_still_yields_what_arrived() {
    let payload: WebhookPayload = serde_json::from_str(
        &PAYLOAD.replace(r#""truncatedAlerts": 0"#, r#""truncatedAlerts": 42"#),
    )
    .expect("the payload decodes");

    assert!(payload.is_truncated());
    assert_eq!(payload.truncated_alerts, 42);
    assert_eq!(payload.into_alerts().expect("the alerts convert").len(), 2);
}
