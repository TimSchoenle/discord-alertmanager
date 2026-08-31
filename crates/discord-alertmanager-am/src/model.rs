//! The slice of Alertmanager's API v2 wire model this client actually speaks.
//!
//! Hand-written, and deliberately partial. Alertmanager's `openapi.yaml` describes far more than
//! six calls need, and a generated model would carry every field of it plus a dependency tree to
//! parse them. What is here is what is read; everything else in a response is ignored, which is
//! also what lets a newer server add a field without breaking this client.
//!
//! Types whose obvious name is already taken by a domain type carry a `Wire` prefix. The
//! distinction is worth the four characters: `WireMatcher` is two booleans and two strings that
//! came off a socket, and [`Matcher`] is a compiled, anchored, validated comparison. Confusing
//! them is how an unvalidated label name reaches the renderer.

use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use dam_core::{
    Alert, AlertStatus, AmState, Annotations, Fingerprint, LabelName, Labels, MatchOp, Matcher,
    MatcherSet,
};
use dam_engine::{AmStatus, Receiver, SilenceRecord, SilenceRequest};
use dam_store::SilenceLifecycle;
use serde::{Deserialize, Serialize};

use crate::error::WireError;

/// One alert as `GET /api/v2/alerts` returns it.
///
/// Unknown fields are ignored on purpose. A response carries `receivers` and a `status` block
/// with more in it than is read here, and pinning the struct to today's exact field list would
/// turn every Alertmanager upgrade into a decode failure during the poll that discovered it.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GettableAlert {
    /// Alertmanager's own fingerprint for the alert, and this bot's primary key for it.
    pub fingerprint: String,

    /// The identifying labels.
    #[serde(default)]
    pub labels: BTreeMap<String, String>,

    /// The rendered prose.
    #[serde(default)]
    pub annotations: BTreeMap<String, String>,

    /// When the condition started holding.
    pub starts_at: DateTime<Utc>,

    /// When it stops, which for an active alert is a time in the future.
    ///
    /// Alertmanager fills this in for every alert, resolved or not: an active alert's `endsAt` is
    /// its start plus the resolve timeout, and it moves forward each time Prometheus re-sends.
    /// Reading it as "this alert has ended" without comparing it to the clock marks every firing
    /// alert resolved, so the comparison lives in [`GettableAlert::into_core`].
    #[serde(default)]
    pub ends_at: Option<DateTime<Utc>>,

    /// Link back to the expression that produced the alert.
    #[serde(rename = "generatorURL", default)]
    pub generator_url: Option<String>,

    /// What Alertmanager is doing with the alert.
    pub status: WireAlertStatus,
}

impl GettableAlert {
    /// Converts the alert into the domain type, resolving `endsAt` against `now`.
    ///
    /// The clock is a parameter rather than read here so the firing-versus-resolved decision is
    /// reproducible in a test rather than a function of when the test ran.
    ///
    /// # Errors
    ///
    /// Returns [`WireError::Domain`] when the fingerprint is not hexadecimal, a label name is
    /// outside Prometheus's grammar, or a label value is over the length the domain accepts.
    pub fn into_core(self, now: DateTime<Utc>) -> Result<Alert, WireError> {
        // Ended in the past is the only thing that means resolved. An `endsAt` in the future is
        // the resolve timeout Alertmanager will forget the alert at if Prometheus stops sending.
        let ended = self.ends_at.filter(|ends_at| *ends_at <= now);

        Ok(Alert {
            fingerprint: Fingerprint::new(self.fingerprint)?,
            labels: labels_from_wire(self.labels)?,
            annotations: annotations_from_wire(self.annotations),
            starts_at: self.starts_at,
            ends_at: ended,
            generator_url: self.generator_url.filter(|url| !url.is_empty()),
            status: if ended.is_some() {
                AlertStatus::Resolved
            } else {
                AlertStatus::Firing
            },
            am_state: self.status.state,
            silenced_by: self.status.silenced_by,
            inhibited_by: self.status.inhibited_by,
            // The alert list carries no group key. Grouping is a property of Alertmanager's
            // routing tree, and only the webhook, which is invoked per group, knows it.
            group_key: None,
        })
    }
}

/// The `status` block of a gettable alert.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WireAlertStatus {
    /// Whether Alertmanager is notifying about the alert or suppressing it.
    pub state: AmState,

    /// Ids of the silences suppressing it.
    #[serde(default)]
    pub silenced_by: Vec<String>,

    /// Fingerprints of the alerts inhibiting it.
    #[serde(default)]
    pub inhibited_by: Vec<String>,
}

/// One silence as `GET /api/v2/silences` returns it.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GettableSilence {
    /// The id Alertmanager assigned.
    pub id: String,

    /// What the silence suppresses.
    #[serde(default)]
    pub matchers: Vec<WireMatcher>,

    /// When it starts.
    pub starts_at: DateTime<Utc>,

    /// When it expires.
    pub ends_at: DateTime<Utc>,

    /// When it was last written.
    pub updated_at: DateTime<Utc>,

    /// Who created it.
    #[serde(default)]
    pub created_by: String,

    /// Why it exists.
    #[serde(default)]
    pub comment: String,

    /// Where it is in its life.
    pub status: WireSilenceStatus,
}

impl GettableSilence {
    /// Converts the silence into the record the port hands back.
    ///
    /// # Errors
    ///
    /// Returns [`WireError::Domain`] when a matcher carries a label name outside Prometheus's
    /// grammar or a regex that does not compile inside the domain's limits.
    pub fn into_record(self) -> Result<SilenceRecord, WireError> {
        let mut matchers = Vec::with_capacity(self.matchers.len());
        for matcher in &self.matchers {
            matchers.push(matcher.to_matcher()?);
        }

        Ok(SilenceRecord {
            id: self.id,
            matchers: MatcherSet::new(matchers),
            starts_at: self.starts_at,
            ends_at: self.ends_at,
            updated_at: self.updated_at,
            created_by: self.created_by,
            comment: self.comment,
            state: self.status.state,
        })
    }
}

/// The `status` block of a gettable silence.
#[derive(Debug, Clone, Deserialize)]
pub struct WireSilenceStatus {
    /// Pending, active or expired, as Alertmanager computed it against its own clock.
    ///
    /// Taken from the server rather than derived from `startsAt` and `endsAt` here. The two agree
    /// almost always, and when they do not it is because the clocks disagree, in which case the
    /// server's answer is the one the suppression actually follows.
    pub state: SilenceLifecycle,
}

/// A silence on its way to `POST /api/v2/silences`.
///
/// The same document creates and updates. An `id` present means replace; absent means create, and
/// creating is not idempotent, so a caller retrying a create it is unsure about has to supply the
/// id of whatever the first attempt produced.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PostableSilence {
    /// The silence to replace, omitted entirely when this is a create.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,

    /// What the silence suppresses.
    pub matchers: Vec<WireMatcher>,

    /// When it starts.
    pub starts_at: DateTime<Utc>,

    /// When it expires.
    pub ends_at: DateTime<Utc>,

    /// Who created it, as it will appear in `amtool`.
    pub created_by: String,

    /// Why it exists.
    pub comment: String,
}

impl PostableSilence {
    /// Builds the document from the port's request.
    #[must_use]
    pub fn from_request(request: &SilenceRequest) -> Self {
        let mut matchers = Vec::with_capacity(request.matchers.len());
        for matcher in request.matchers.as_slice() {
            matchers.push(WireMatcher::from_matcher(matcher));
        }

        Self {
            id: request.id.clone(),
            matchers,
            starts_at: request.starts_at,
            ends_at: request.ends_at,
            created_by: request.created_by.clone(),
            comment: request.comment.clone(),
        }
    }
}

/// What `POST /api/v2/silences` answers with.
#[derive(Debug, Clone, Deserialize)]
pub struct SilenceCreated {
    /// The id of the silence that was created or replaced.
    ///
    /// Spelled `silenceID` on the wire, which is the one field on this API whose casing does not
    /// follow from the `camelCase` rule the rest of it obeys.
    #[serde(rename = "silenceID")]
    pub silence_id: String,
}

/// One label comparison in Alertmanager's own encoding.
///
/// Two booleans rather than an operator: `isRegex` says whether the value is a pattern, `isEqual`
/// says whether matching it satisfies the comparison, and the four combinations are exactly the
/// four operators. Silences are always sent this way and never as an expression, because
/// Alertmanager 0.27 shipped a second matcher parser whose edge cases differ from the classic
/// one, and handing the server a string means choosing which of the two to be wrong about.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WireMatcher {
    /// The label being compared.
    pub name: String,

    /// The literal or the pattern, as written.
    pub value: String,

    /// Whether `value` is a regular expression.
    pub is_regex: bool,

    /// Whether a match satisfies the comparison, rather than a non-match.
    ///
    /// Absent from silences written by Alertmanager before 0.22, which had no negative matchers
    /// at all. Defaulting it to true reads those as the equality they were.
    #[serde(default = "matches_by_default")]
    pub is_equal: bool,
}

impl WireMatcher {
    /// Encodes a compiled matcher for the wire.
    #[must_use]
    pub fn from_matcher(matcher: &Matcher) -> Self {
        Self {
            name: matcher.name().to_string(),
            value: matcher.value().to_owned(),
            is_regex: matcher.op().is_regex(),
            is_equal: matcher.op().is_equal(),
        }
    }

    /// Compiles the matcher into the domain type.
    ///
    /// # Errors
    ///
    /// Returns [`WireError::Domain`] when the name is outside Prometheus's grammar, or when a
    /// pattern is over the length cap or does not compile inside the size limits.
    pub fn to_matcher(&self) -> Result<Matcher, WireError> {
        let op = match (self.is_regex, self.is_equal) {
            (false, true) => MatchOp::Equal,
            (false, false) => MatchOp::NotEqual,
            (true, true) => MatchOp::RegexMatch,
            (true, false) => MatchOp::RegexNotMatch,
        };

        Ok(Matcher::new(
            LabelName::new(self.name.as_str())?,
            op,
            self.value.as_str(),
        )?)
    }
}

/// What `GET /api/v2/status` returns.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AlertmanagerStatus {
    /// The gossip cluster this server belongs to.
    pub cluster: ClusterStatus,

    /// Which build is running.
    pub version_info: VersionInfo,

    /// The configuration the server currently holds.
    #[serde(default)]
    pub config: Option<ServerConfig>,

    /// When the process started.
    #[serde(default)]
    pub uptime: Option<DateTime<Utc>>,
}

impl AlertmanagerStatus {
    /// Reduces the status to what the deadman and `/am status` need.
    #[must_use]
    pub fn into_status(self) -> AmStatus {
        let mut peers = Vec::with_capacity(self.cluster.peers.len());
        for peer in &self.cluster.peers {
            peers.push(peer.name.clone());
        }

        AmStatus {
            version: self.version_info.version,
            uptime: self.uptime,
            peers,
            // A single-node Alertmanager reports `disabled` rather than `ready`, and it is as
            // ready as it will ever be. Only `settling` means the cluster is not usable yet.
            cluster_ready: self.cluster.status != "settling",
            config_hash: self.config.map(|config| digest(&config.original)),
        }
    }
}

/// The gossip cluster's view of itself.
#[derive(Debug, Clone, Deserialize)]
pub struct ClusterStatus {
    /// `ready`, `settling`, or `disabled` on a server started without clustering.
    #[serde(default)]
    pub status: String,

    /// The peers this server can see, itself included.
    #[serde(default)]
    pub peers: Vec<PeerStatus>,
}

/// One peer in the gossip cluster.
#[derive(Debug, Clone, Deserialize)]
pub struct PeerStatus {
    /// The peer's gossip name.
    pub name: String,
}

/// The build the server is running.
#[derive(Debug, Clone, Deserialize)]
pub struct VersionInfo {
    /// The release, such as `0.27.0`.
    pub version: String,
}

/// The configuration the server has loaded.
///
/// Named for the server rather than for Alertmanager, because `dam_config::Alertmanager` is the
/// bot's own configuration for reaching it and confusing the two is a coin flip every reader
/// would have to make.
#[derive(Debug, Clone, Deserialize)]
pub struct ServerConfig {
    /// The `alertmanager.yml` text, exactly as the server parsed it.
    pub original: String,
}

/// One receiver as `GET /api/v2/receivers` returns it.
#[derive(Debug, Clone, Deserialize)]
pub struct WireReceiver {
    /// The receiver's name in `alertmanager.yml`.
    pub name: String,
}

impl From<WireReceiver> for Receiver {
    fn from(receiver: WireReceiver) -> Self {
        Self {
            name: receiver.name,
        }
    }
}

/// Validates a wire label map into a domain label set.
///
/// # Errors
///
/// Returns [`WireError::Domain`] on the first name outside Prometheus's grammar or value over the
/// domain's length cap. Refusing the whole set rather than dropping the offending pair is
/// deliberate: an alert missing a label is an alert with a different identity, and it would be
/// routed and deduplicated as one.
pub(crate) fn labels_from_wire(raw: BTreeMap<String, String>) -> Result<Labels, WireError> {
    let mut labels = Labels::new();
    for (name, value) in raw {
        labels.insert(LabelName::new(name)?, value)?;
    }
    Ok(labels)
}

/// Wraps a wire annotation map. Annotations are prose and have no grammar to violate.
pub(crate) fn annotations_from_wire(raw: BTreeMap<String, String>) -> Annotations {
    raw.into_iter().collect()
}

/// The default for a missing `isEqual`.
fn matches_by_default() -> bool {
    true
}

/// A short, stable digest of the loaded configuration.
///
/// Alertmanager exposes the configuration text but no hash of it, and comparing two multi-kilobyte
/// YAML documents across a peer set to answer "did they all reload" is a lot of bytes to move for
/// one bit of information. FNV-1a over the text is enough: nothing here is defending against a
/// constructed collision, it only has to differ when the configuration differs.
fn digest(text: &str) -> String {
    const OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
    const PRIME: u64 = 0x0000_0100_0000_01b3;

    let mut hash = OFFSET;
    for byte in text.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(PRIME);
    }

    format!("{hash:016x}")
}

#[cfg(test)]
mod tests {
    use chrono::TimeZone;

    use super::*;

    fn instant(hour: u32) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 3, 4, hour, 0, 0)
            .single()
            .expect("the timestamp is unambiguous")
    }

    #[test]
    fn the_two_booleans_round_trip_all_four_operators() {
        let cases = [
            (MatchOp::Equal, false, true),
            (MatchOp::NotEqual, false, false),
            (MatchOp::RegexMatch, true, true),
            (MatchOp::RegexNotMatch, true, false),
        ];

        for (op, is_regex, is_equal) in cases {
            let matcher = Matcher::new(
                LabelName::new("severity").expect("the label name is valid"),
                op,
                "crit.*",
            )
            .expect("the pattern compiles");

            let wire = WireMatcher::from_matcher(&matcher);

            assert_eq!(wire.is_regex, is_regex, "{op}");
            assert_eq!(wire.is_equal, is_equal, "{op}");
            assert_eq!(
                wire.to_matcher().expect("the matcher compiles back"),
                matcher,
                "{op}"
            );
        }
    }

    #[test]
    fn a_matcher_without_is_equal_reads_as_an_equality() {
        let wire: WireMatcher =
            serde_json::from_str(r#"{"name":"severity","value":"critical","isRegex":false}"#)
                .expect("the matcher decodes");

        assert!(wire.is_equal);
        assert_eq!(
            wire.to_matcher().expect("the matcher compiles").op(),
            MatchOp::Equal
        );
    }

    #[test]
    fn a_matcher_serialises_with_alertmanagers_own_field_names() {
        let matcher = Matcher::new(
            LabelName::new("namespace").expect("the label name is valid"),
            MatchOp::RegexNotMatch,
            "prod-.*",
        )
        .expect("the pattern compiles");

        let json = serde_json::to_string(&WireMatcher::from_matcher(&matcher))
            .expect("the matcher serialises");

        assert!(json.contains(r#""isRegex":true"#), "{json}");
        assert!(json.contains(r#""isEqual":false"#), "{json}");
    }

    #[test]
    fn an_end_time_in_the_future_is_a_resolve_timeout_and_not_a_resolution() {
        let json = r#"{
            "fingerprint": "0123456789abcdef",
            "labels": {"alertname": "PodDown"},
            "annotations": {},
            "startsAt": "2026-03-04T09:00:00.000Z",
            "endsAt": "2026-03-04T15:00:00.000Z",
            "generatorURL": "",
            "status": {"state": "active", "silencedBy": [], "inhibitedBy": []}
        }"#;

        let alert = serde_json::from_str::<GettableAlert>(json)
            .expect("the alert decodes")
            .into_core(instant(12))
            .expect("the alert converts");

        assert_eq!(alert.status, AlertStatus::Firing);
        assert_eq!(alert.ends_at, None);
        assert_eq!(alert.generator_url, None);
        assert_eq!(alert.am_state, AmState::Active);
    }

    #[test]
    fn an_end_time_in_the_past_resolves_the_alert() {
        let json = r#"{
            "fingerprint": "0123456789abcdef",
            "labels": {"alertname": "PodDown"},
            "annotations": {},
            "startsAt": "2026-03-04T09:00:00.000Z",
            "endsAt": "2026-03-04T11:00:00.000Z",
            "status": {"state": "suppressed", "silencedBy": ["abc"], "inhibitedBy": []}
        }"#;

        let alert = serde_json::from_str::<GettableAlert>(json)
            .expect("the alert decodes")
            .into_core(instant(12))
            .expect("the alert converts");

        assert_eq!(alert.status, AlertStatus::Resolved);
        assert_eq!(alert.ends_at, Some(instant(11)));
        assert_eq!(alert.am_state, AmState::Suppressed);
        assert_eq!(alert.silenced_by, vec!["abc".to_owned()]);
    }

    #[test]
    fn an_unknown_field_in_a_response_is_ignored() {
        let json = r#"{
            "fingerprint": "0123456789abcdef",
            "labels": {},
            "annotations": {},
            "startsAt": "2026-03-04T09:00:00.000Z",
            "endsAt": "2026-03-04T15:00:00.000Z",
            "receivers": [{"name": "discord"}],
            "status": {"state": "active", "silencedBy": [], "inhibitedBy": [], "mutedBy": []}
        }"#;

        assert!(serde_json::from_str::<GettableAlert>(json).is_ok());
    }

    #[test]
    fn a_label_name_outside_the_grammar_refuses_the_whole_alert() {
        let json = r#"{
            "fingerprint": "0123456789abcdef",
            "labels": {"not a name": "x"},
            "annotations": {},
            "startsAt": "2026-03-04T09:00:00.000Z",
            "status": {"state": "active"}
        }"#;

        let error = serde_json::from_str::<GettableAlert>(json)
            .expect("the alert decodes")
            .into_core(instant(12))
            .expect_err("the label name is refused");

        assert!(matches!(error, WireError::Domain(_)));
    }

    #[test]
    fn a_create_omits_the_id_and_an_update_carries_it() {
        let request = SilenceRequest {
            id: None,
            matchers: MatcherSet::parse("alertname=PodDown").expect("the expression parses"),
            starts_at: instant(9),
            ends_at: instant(11),
            created_by: "dam".to_owned(),
            comment: "deploying".to_owned(),
        };

        let create = serde_json::to_string(&PostableSilence::from_request(&request))
            .expect("the silence serialises");
        assert!(!create.contains("\"id\""), "{create}");

        let update = serde_json::to_string(&PostableSilence::from_request(&SilenceRequest {
            id: Some("abc".to_owned()),
            ..request
        }))
        .expect("the silence serialises");
        assert!(update.contains(r#""id":"abc""#), "{update}");
    }

    #[test]
    fn a_single_node_server_counts_as_ready() {
        let json = r#"{
            "cluster": {"status": "disabled", "peers": []},
            "versionInfo": {"version": "0.27.0", "revision": "abcdef"},
            "config": {"original": "route:\n  receiver: discord\n"},
            "uptime": "2026-03-04T09:00:00.000Z"
        }"#;

        let status = serde_json::from_str::<AlertmanagerStatus>(json)
            .expect("the status decodes")
            .into_status();

        assert_eq!(status.version, "0.27.0");
        assert!(status.cluster_ready);
        assert!(status.peers.is_empty());
        assert_eq!(status.uptime, Some(instant(9)));
        assert!(status.config_hash.is_some());
    }

    #[test]
    fn a_settling_cluster_is_not_ready_and_its_peers_are_named() {
        let json = r#"{
            "cluster": {
                "status": "settling",
                "peers": [{"name": "01F", "address": "10.0.0.1:9094"}]
            },
            "versionInfo": {"version": "0.27.0"}
        }"#;

        let status = serde_json::from_str::<AlertmanagerStatus>(json)
            .expect("the status decodes")
            .into_status();

        assert!(!status.cluster_ready);
        assert_eq!(status.peers, vec!["01F".to_owned()]);
        assert_eq!(status.config_hash, None);
    }

    #[test]
    fn the_configuration_digest_changes_with_the_configuration() {
        assert_eq!(digest("route:\n"), digest("route:\n"));
        assert_ne!(digest("route:\n"), digest("route: \n"));
        assert_eq!(digest("route:\n").len(), 16);
    }

    #[test]
    fn a_silence_keeps_the_servers_own_lifecycle() {
        let json = r#"{
            "id": "9d1a",
            "matchers": [{"name": "alertname", "value": "PodDown", "isRegex": false}],
            "startsAt": "2026-03-04T09:00:00.000Z",
            "endsAt": "2026-03-04T11:00:00.000Z",
            "updatedAt": "2026-03-04T09:00:00.000Z",
            "createdBy": "dam",
            "comment": "deploying",
            "status": {"state": "expired"}
        }"#;

        let record = serde_json::from_str::<GettableSilence>(json)
            .expect("the silence decodes")
            .into_record()
            .expect("the silence converts");

        assert_eq!(record.id, "9d1a");
        assert_eq!(record.state, SilenceLifecycle::Expired);
        assert_eq!(record.matchers.to_string(), "alertname=PodDown");
    }
}
