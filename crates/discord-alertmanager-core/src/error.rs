//! The one error type this crate produces.

use thiserror::Error;

/// Everything that can go wrong constructing a domain value.
///
/// One enum rather than one per module: every variant here means the same thing to a caller —
/// input that cannot become a domain value — and the layer above turns any of them into a 400, a
/// rejected command, or a skipped route. Splitting it would multiply `From` impls without giving
/// any caller a decision it does not already have.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum CoreError {
    /// A label name outside Prometheus's `[a-zA-Z_][a-zA-Z0-9_]*`.
    #[error("`{name}` is not a valid label name")]
    InvalidLabelName {
        /// The name as it was supplied.
        name: String,
    },

    /// A label value past the length this crate accepts.
    #[error("value of label `{name}` is {len} bytes, over the limit")]
    LabelValueTooLong {
        /// The label carrying the oversized value.
        name: String,
        /// Length of the supplied value.
        len: usize,
    },

    /// A fingerprint that is not lowercase hexadecimal.
    #[error("`{value}` is not a valid alert fingerprint")]
    InvalidFingerprint {
        /// The value as it was supplied.
        value: String,
    },

    /// A matcher expression that could not be split into `name`, operator and value.
    #[error("cannot parse matcher `{expression}`: {detail}")]
    BadMatcherExpression {
        /// The fragment that failed to parse.
        expression: String,
        /// Why it failed.
        detail: String,
    },

    /// A regex source longer than the cap.
    ///
    /// Length is checked before compilation, so a pattern written to exhaust the compiler is
    /// refused without being handed to it.
    #[error("regex is {len} bytes, over the {max}-byte limit")]
    RegexTooLong {
        /// Length of the supplied pattern.
        len: usize,
        /// The cap it exceeded.
        max: usize,
    },

    /// A regex that does not compile, or does not compile inside the size limits.
    #[error("cannot compile regex `{pattern}`: {detail}")]
    BadRegex {
        /// The pattern as it was written, without the anchors this crate adds.
        pattern: String,
        /// The compiler's complaint.
        detail: String,
    },

    /// A severity this crate does not recognise.
    #[error("`{value}` is not a severity")]
    UnknownSeverity {
        /// The value as it was supplied.
        value: String,
    },

    /// A stored discriminant that no longer maps to a variant.
    ///
    /// Reachable in exactly one situation: a downgrade reading rows a newer version wrote. The
    /// variant carries which enum failed, because the row it came from is otherwise the only way
    /// to find out.
    #[error("`{value}` is not a known {kind}")]
    UnknownVariant {
        /// The enum the value was being read into.
        kind: &'static str,
        /// The value as it was stored.
        value: String,
    },
}
