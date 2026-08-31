//! Domain types for alerts, matchers, routing decisions and the notification state machine.
//!
//! Nothing here performs I/O. The manifest lists four dependencies — `chrono`, `regex`, `serde`
//! and `thiserror` — and the `architecture` job in CI fails when a fifth appears. That constraint
//! is what makes the matcher semantics and the state machine testable in milliseconds, and it is
//! the mechanism enforcing that the domain does not know about Discord. A card, a custom id and a
//! snowflake belong to `dam_discord`; an outbox row belongs to `dam_store`.
//!
//! # What this crate owes Alertmanager
//!
//! Matcher semantics mirror Alertmanager's exactly, including the two that are easy to get wrong:
//! a regex is fully anchored, and an absent label matches against the empty string. Operators
//! already hold one model of what `severity=~crit.*` does, and a second one that disagrees at the
//! edges is worse than no matcher support at all.
//!
//! Alert identity is Alertmanager's own `fingerprint`. A locally computed `labels_hash` sits
//! beside it, so a change in Alertmanager's hashing across versions is detectable rather than
//! silent.

pub mod alert;
pub mod labels;
pub mod matcher;
pub mod state;

mod error;

pub use alert::{
    Alert, AlertDelta, AlertStatus, AmState, Annotations, DedupeKey, EventKind, EventSource,
    SEVERITY_LABEL, Severity,
};
pub use error::CoreError;
pub use labels::{Fingerprint, GroupKey, LabelName, Labels, LabelsHash, MAX_LABEL_LEN};
pub use matcher::{MAX_REGEX_LEN, MatchOp, Matcher, MatcherSet};
pub use state::{NotificationState, Trigger, initial_state, next_state};
