//! The envelope Alertmanager's `webhook_config` receiver posts, version 4.
//!
//! One `POST` carries one notification group, so the payload is a group header plus the alerts in
//! it. The header is what makes the webhook worth having over the polling reconciler: the group
//! key and the receiver name are Alertmanager's routing decision, and the API's alert list does
//! not expose either.

use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use dam_core::{Alert, AlertStatus, AmState, Fingerprint, GroupKey};
use serde::Deserialize;

use crate::error::WireError;
use crate::model::{annotations_from_wire, labels_from_wire};

/// The only envelope version this crate reads.
pub const WEBHOOK_VERSION: &str = "4";

/// One `POST` from Alertmanager's webhook receiver.
///
/// Unknown fields are accepted and known ones are not: a field this crate has never heard of is
/// a newer Alertmanager adding to the envelope, which costs nothing to ignore, while a missing
/// `fingerprint` or an unparseable `startsAt` is a payload that cannot become an alert and is
/// better refused at the edge than half-stored.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WebhookPayload {
    /// The envelope version, which [`WebhookPayload::ensure_supported`] insists is `4`.
    pub version: String,

    /// Alertmanager's key for the group this notification covers.
    pub group_key: String,

    /// How many alerts were dropped because the group exceeded the receiver's size limit.
    ///
    /// Non-zero means this payload is an incomplete picture of the group. The alerts that did
    /// arrive are still real and are still ingested; what the caller must not do is conclude that
    /// an alert absent from the list has resolved. The reconciler is what closes that gap.
    #[serde(default)]
    pub truncated_alerts: u32,

    /// Whether the group as a whole is firing or resolved.
    pub status: AlertStatus,

    /// The receiver in `alertmanager.yml` that produced the notification.
    pub receiver: String,

    /// The labels Alertmanager grouped on.
    #[serde(default)]
    pub group_labels: BTreeMap<String, String>,

    /// The labels every alert in the group shares.
    #[serde(default)]
    pub common_labels: BTreeMap<String, String>,

    /// The annotations every alert in the group shares.
    #[serde(default)]
    pub common_annotations: BTreeMap<String, String>,

    /// The server's own external URL, as configured on it.
    #[serde(rename = "externalURL")]
    pub external_url: String,

    /// The alerts in the group.
    #[serde(default)]
    pub alerts: Vec<WebhookAlert>,
}

impl WebhookPayload {
    /// Checks the declared version before anything reads the rest.
    ///
    /// # Errors
    ///
    /// Returns [`WireError::UnsupportedWebhookVersion`] for any value other than
    /// [`WEBHOOK_VERSION`].
    pub fn ensure_supported(&self) -> Result<(), WireError> {
        if self.version == WEBHOOK_VERSION {
            Ok(())
        } else {
            Err(WireError::UnsupportedWebhookVersion {
                version: self.version.clone(),
            })
        }
    }

    /// Whether Alertmanager dropped alerts from this group before sending it.
    #[must_use]
    pub fn is_truncated(&self) -> bool {
        self.truncated_alerts > 0
    }

    /// Converts the envelope into the domain alerts it carries.
    ///
    /// The group key is copied onto every alert, because it is the only place it appears and a
    /// route that posts one card per group has nothing else to key on.
    ///
    /// # Errors
    ///
    /// Returns [`WireError::UnsupportedWebhookVersion`] when the version is not
    /// [`WEBHOOK_VERSION`], and [`WireError::Domain`] when an alert carries a fingerprint that is
    /// not hexadecimal or a label outside Prometheus's grammar. One bad alert refuses the whole
    /// payload: Alertmanager retries the delivery, and accepting a partial group would mean the
    /// retry is deduplicated against an incomplete record of what arrived.
    pub fn into_alerts(self) -> Result<Vec<Alert>, WireError> {
        self.ensure_supported()?;

        let group_key = GroupKey::new(self.group_key);
        let mut alerts = Vec::with_capacity(self.alerts.len());
        for alert in self.alerts {
            alerts.push(alert.into_core(&group_key)?);
        }

        Ok(alerts)
    }
}

/// One alert inside a webhook envelope.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WebhookAlert {
    /// Whether this alert is firing or resolved, which can differ from the group's status.
    pub status: AlertStatus,

    /// The identifying labels.
    #[serde(default)]
    pub labels: BTreeMap<String, String>,

    /// The rendered prose.
    #[serde(default)]
    pub annotations: BTreeMap<String, String>,

    /// When the condition started holding.
    pub starts_at: DateTime<Utc>,

    /// When it stopped, or Go's zero time while it has not.
    ///
    /// Alertmanager marshals an unset end time as `0001-01-01T00:00:00Z` rather than omitting the
    /// field or sending null, so the field is always present and the emptiness has to be read out
    /// of the value.
    pub ends_at: DateTime<Utc>,

    /// Link back to the expression in Prometheus that produced the alert.
    #[serde(rename = "generatorURL")]
    pub generator_url: String,

    /// Alertmanager's fingerprint, and this bot's primary key for the alert.
    pub fingerprint: String,
}

impl WebhookAlert {
    /// Converts the alert into the domain type, attaching the envelope's group key.
    ///
    /// # Errors
    ///
    /// Returns [`WireError::Domain`] when the fingerprint is not hexadecimal, a label name is
    /// outside Prometheus's grammar, or a label value is over the length the domain accepts.
    pub fn into_core(self, group_key: &GroupKey) -> Result<Alert, WireError> {
        Ok(Alert {
            fingerprint: Fingerprint::new(self.fingerprint)?,
            labels: labels_from_wire(self.labels)?,
            annotations: annotations_from_wire(self.annotations),
            starts_at: self.starts_at,
            ends_at: end_time(self.ends_at),
            generator_url: (!self.generator_url.is_empty()).then_some(self.generator_url),
            status: self.status,
            // The envelope says nothing about suppression, and it does not need to: Alertmanager
            // does not notify about a suppressed alert, so anything arriving here is one it is
            // notifying about. The reconciler is what discovers a silence taking hold later.
            am_state: AmState::Active,
            silenced_by: Vec::new(),
            inhibited_by: Vec::new(),
            group_key: Some(group_key.clone()),
        })
    }
}

/// Reads Go's zero time as the absence of an end time.
///
/// Anything at or before the Unix epoch is the zero value rather than a real timestamp; no alert
/// this bot will ever see ended in 1969.
fn end_time(ends_at: DateTime<Utc>) -> Option<DateTime<Utc>> {
    (ends_at > DateTime::UNIX_EPOCH).then_some(ends_at)
}

#[cfg(test)]
mod tests {
    use chrono::TimeZone;

    use super::*;

    const FIRING: &str = r#"{
        "version": "4",
        "groupKey": "{}:{alertname=\"PodDown\"}",
        "truncatedAlerts": 0,
        "status": "firing",
        "receiver": "discord",
        "groupLabels": {"alertname": "PodDown"},
        "commonLabels": {"alertname": "PodDown", "severity": "critical"},
        "commonAnnotations": {"summary": "pod is down"},
        "externalURL": "https://alertmanager.example.com",
        "alerts": [
            {
                "status": "firing",
                "labels": {"alertname": "PodDown", "namespace": "prod", "severity": "critical"},
                "annotations": {"summary": "pod is down", "runbook_url": "https://runbooks/pod"},
                "startsAt": "2026-03-04T09:00:00.000Z",
                "endsAt": "0001-01-01T00:00:00Z",
                "generatorURL": "https://prometheus.example.com/graph?g0.expr=up",
                "fingerprint": "9f8e7d6c5b4a3021"
            },
            {
                "status": "resolved",
                "labels": {"alertname": "PodDown", "namespace": "staging"},
                "annotations": {},
                "startsAt": "2026-03-04T08:00:00.000Z",
                "endsAt": "2026-03-04T08:30:00.000Z",
                "generatorURL": "",
                "fingerprint": "1122334455667788"
            }
        ]
    }"#;

    fn instant(hour: u32, minute: u32) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 3, 4, hour, minute, 0)
            .single()
            .expect("the timestamp is unambiguous")
    }

    #[test]
    fn a_version_four_envelope_becomes_domain_alerts() {
        let payload: WebhookPayload = serde_json::from_str(FIRING).expect("the payload decodes");

        assert_eq!(payload.receiver, "discord");
        assert_eq!(payload.status, AlertStatus::Firing);
        assert_eq!(payload.external_url, "https://alertmanager.example.com");
        assert_eq!(payload.common_labels.len(), 2);

        let alerts = payload.into_alerts().expect("the alerts convert");

        assert_eq!(alerts.len(), 2);
        assert_eq!(alerts[0].name(), "PodDown");
        assert_eq!(alerts[0].labels.get("namespace"), Some("prod"));
        assert_eq!(alerts[0].annotations.summary(), Some("pod is down"));
        assert_eq!(alerts[0].am_state, AmState::Active);
        assert_eq!(
            alerts[0].group_key.as_ref().map(GroupKey::as_str),
            Some(r#"{}:{alertname="PodDown"}"#)
        );
    }

    #[test]
    fn the_zero_end_time_is_read_as_no_end_time() {
        let alerts = serde_json::from_str::<WebhookPayload>(FIRING)
            .expect("the payload decodes")
            .into_alerts()
            .expect("the alerts convert");

        assert_eq!(alerts[0].ends_at, None);
        assert!(alerts[0].is_firing());
        assert_eq!(alerts[1].ends_at, Some(instant(8, 30)));
        assert_eq!(alerts[1].status, AlertStatus::Resolved);
    }

    #[test]
    fn an_empty_generator_url_is_absent_rather_than_empty() {
        let alerts = serde_json::from_str::<WebhookPayload>(FIRING)
            .expect("the payload decodes")
            .into_alerts()
            .expect("the alerts convert");

        assert!(alerts[0].generator_url.is_some());
        assert_eq!(alerts[1].generator_url, None);
    }

    #[test]
    fn a_start_time_survives_the_conversion_unchanged() {
        let alerts = serde_json::from_str::<WebhookPayload>(FIRING)
            .expect("the payload decodes")
            .into_alerts()
            .expect("the alerts convert");

        assert_eq!(alerts[0].starts_at, instant(9, 0));
    }

    #[test]
    fn any_version_but_four_is_refused() {
        let payload: WebhookPayload =
            serde_json::from_str(&FIRING.replace(r#""4""#, r#""3""#)).expect("the payload decodes");

        let error = payload
            .ensure_supported()
            .expect_err("version 3 is not understood");

        assert_eq!(
            error,
            WireError::UnsupportedWebhookVersion {
                version: "3".to_owned()
            }
        );
        assert!(payload.into_alerts().is_err());
    }

    #[test]
    fn a_truncated_group_is_flagged_and_still_yields_the_alerts_that_arrived() {
        let payload: WebhookPayload = serde_json::from_str(
            &FIRING.replace(r#""truncatedAlerts": 0"#, r#""truncatedAlerts": 7"#),
        )
        .expect("the payload decodes");

        assert!(payload.is_truncated());
        assert_eq!(payload.truncated_alerts, 7);
        assert_eq!(payload.into_alerts().expect("the alerts convert").len(), 2);
    }

    #[test]
    fn a_field_this_crate_does_not_know_is_ignored() {
        let payload: WebhookPayload = serde_json::from_str(&FIRING.replace(
            r#""version": "4","#,
            r#""version": "4", "somethingNew": {"a": 1},"#,
        ))
        .expect("the payload decodes");

        assert!(payload.into_alerts().is_ok());
    }

    #[test]
    fn an_alert_with_an_unusable_fingerprint_refuses_the_whole_payload() {
        let payload: WebhookPayload =
            serde_json::from_str(&FIRING.replace("9f8e7d6c5b4a3021", "not-hex"))
                .expect("the payload decodes");

        assert!(matches!(payload.into_alerts(), Err(WireError::Domain(_))));
    }
}
