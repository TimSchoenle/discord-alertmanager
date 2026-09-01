//! The error vocabulary every backend maps its driver's failures into.

use thiserror::Error;

use crate::ids::{NotificationId, OutboxId, RouteId};

/// What a [`crate::Store`] call can fail with.
///
/// The variants are chosen by what the caller does differently, not by what the driver reports.
/// A dispatcher retries a `Backend` and gives up on a `Conflict`; the webhook returns 503 for the
/// first and 409 for the second. A single opaque error would force every caller to parse a
/// message string to decide, which is how a retry loop ends up retrying a constraint violation
/// forever.
#[derive(Debug, Error)]
pub enum StoreError {
    /// A unique index rejected the write.
    ///
    /// Not a bug and usually not even a problem: it is how two workers racing to create one card
    /// are resolved. The loser re-reads and edits the winner's row.
    #[error("unique constraint `{constraint}` rejected the write")]
    Conflict {
        /// Name of the index or constraint, as the backend reports it.
        constraint: String,
    },

    /// A row that must exist does not.
    #[error("no {kind} matching {key}")]
    NotFound {
        /// What was being looked for.
        kind: &'static str,
        /// How it was identified.
        key: String,
    },

    /// An outbox row was claimed by somebody else, or its lease had already expired.
    ///
    /// Distinct from `NotFound` because it is the expected outcome of a race rather than a
    /// missing row: the worker drops the item and claims the next one.
    #[error("outbox item {id} is no longer claimed by this worker")]
    LeaseLost {
        /// The item that could not be completed.
        id: OutboxId,
    },

    /// A stored JSON document no longer deserialises into its type.
    #[error("cannot decode stored {kind}: {detail}")]
    Decode {
        /// The type the document was being read into.
        kind: &'static str,
        /// What the decoder complained about.
        detail: String,
    },

    /// The database is unreachable, out of connections, or failed for a reason of its own.
    ///
    /// The one variant worth retrying, and the only one that should ever make `/readyz` fail.
    #[error("database error: {detail}")]
    Backend {
        /// The driver's message, already stripped of any connection string.
        detail: String,
    },

    /// Migrations could not be applied.
    #[error("migration failed: {detail}")]
    Migration {
        /// What the migrator complained about.
        detail: String,
    },
}

impl StoreError {
    /// Whether retrying the same call could plausibly succeed.
    ///
    /// The dispatcher and the reconciler both need this answer, and neither should be deciding it
    /// by matching on variants of an error type they do not own.
    #[must_use]
    pub fn is_retryable(&self) -> bool {
        matches!(self, Self::Backend { .. })
    }

    /// A `NotFound` for a notification.
    #[must_use]
    pub fn no_such_notification(id: NotificationId) -> Self {
        Self::NotFound {
            kind: "notification",
            key: id.to_string(),
        }
    }

    /// A `NotFound` for a route.
    #[must_use]
    pub fn no_such_route(id: RouteId) -> Self {
        Self::NotFound {
            kind: "route",
            key: id.to_string(),
        }
    }
}
