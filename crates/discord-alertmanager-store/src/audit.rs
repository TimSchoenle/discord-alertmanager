//! The audit log and the retention policy that keeps it while pruning everything else.

use std::str::FromStr;

use chrono::{DateTime, Duration, Utc};
use dam_core::CoreError;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::ids::{GuildId, UserId};

/// One entry in the record of human decisions.
///
/// Every mutating command writes one, and so does every refusal. A denial that leaves no trace is
/// indistinguishable afterwards from a command nobody ran, which is exactly the question an
/// incident review asks.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuditEntry {
    /// Who acted, when a person did.
    pub actor: Option<UserId>,

    /// Where they acted.
    pub guild_id: Option<GuildId>,

    /// What they tried to do, such as `silence.create` or `route.remove`.
    pub action: String,

    /// What they did it to: a fingerprint, a silence id, a route name.
    pub subject: Option<String>,

    /// Everything else worth keeping, including the resulting Alertmanager silence id.
    pub detail: Value,

    /// How it ended.
    pub result: AuditResult,

    /// When it happened.
    pub at: DateTime<Utc>,
}

impl AuditEntry {
    /// An entry for an action that succeeded.
    #[must_use]
    pub fn ok(action: impl Into<String>, actor: Option<UserId>, at: DateTime<Utc>) -> Self {
        Self {
            actor,
            guild_id: None,
            action: action.into(),
            subject: None,
            detail: Value::Null,
            result: AuditResult::Ok,
            at,
        }
    }

    /// An entry for an action refused by the capability check.
    #[must_use]
    pub fn denied(action: impl Into<String>, actor: Option<UserId>, at: DateTime<Utc>) -> Self {
        Self {
            result: AuditResult::Denied,
            ..Self::ok(action, actor, at)
        }
    }
}

/// How an audited action ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum AuditResult {
    /// It was carried out.
    Ok,

    /// The actor lacked the capability.
    Denied,

    /// It was attempted and failed.
    Error,
}

impl AuditResult {
    /// The result as the lowercase word stored in the database.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Ok => "ok",
            Self::Denied => "denied",
            Self::Error => "error",
        }
    }
}

impl FromStr for AuditResult {
    type Err = CoreError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "ok" => Ok(Self::Ok),
            "denied" => Ok(Self::Denied),
            "error" => Ok(Self::Error),
            other => Err(CoreError::UnknownVariant {
                kind: "audit result",
                value: other.to_owned(),
            }),
        }
    }
}

/// How long each kind of row is kept.
///
/// The audit log outlives everything else by an order of magnitude because it is small and it is
/// the record of what people decided. Alert events are the opposite on both counts.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct RetentionPolicy {
    /// How long alert transition events are kept.
    pub events: Duration,

    /// How long resolved alerts and their notifications are kept.
    pub resolved: Duration,

    /// How long audit entries are kept.
    pub audit: Duration,

    /// Most rows deleted in one pass.
    ///
    /// Pruning in bounded batches keeps a first run against a year of history from holding one
    /// long-running delete open across the whole table.
    pub batch_limit: u32,
}

impl Default for RetentionPolicy {
    fn default() -> Self {
        Self {
            events: Duration::days(30),
            resolved: Duration::days(30),
            audit: Duration::days(365),
            batch_limit: 5_000,
        }
    }
}

/// What one pruning pass deleted.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PruneReport {
    /// Alert events deleted.
    pub events: u64,

    /// Alerts deleted.
    pub alerts: u64,

    /// Notifications deleted.
    pub notifications: u64,

    /// Audit entries deleted.
    pub audit: u64,

    /// Whether the batch limit was reached, so another pass has work to do.
    pub more: bool,
}
