//! What a payload from Alertmanager can be wrong about, before it reaches the port.

use dam_core::CoreError;
use dam_engine::AmError;
use thiserror::Error;

/// A payload that parsed as JSON but does not describe anything this crate can use.
///
/// Separate from [`AmError`] because the two answer different questions. [`AmError`] is what the
/// pipeline decides on — retry, fail the command, mark the server unavailable — and it has five
/// variants because that is how many decisions there are. This enum is what an operator reads in
/// the log line, and it says which field of which document was wrong. The conversion below folds
/// one into the other at the port boundary, so the caller still sees a single error type while
/// the detail survives into its message.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum WireError {
    /// A webhook envelope declaring a version this crate does not implement.
    ///
    /// Refused rather than parsed on a best-effort basis. Alertmanager's envelope has been at
    /// version 4 since 0.15, so a different number means a payload nobody has read the shape of,
    /// and guessing at it would silently drop or mangle alerts during an incident.
    #[error("webhook payload declares version `{version}`, and only version `4` is understood")]
    UnsupportedWebhookVersion {
        /// The version as the payload declared it.
        version: String,
    },

    /// A field that is well-formed JSON but not a valid domain value.
    ///
    /// A label name outside Prometheus's grammar, a fingerprint that is not hexadecimal, a regex
    /// in a silence matcher that does not compile. Carried through rather than flattened to a
    /// string, so a caller that wants to distinguish them still can.
    ///
    /// The status words are not here: [`dam_core::AlertStatus`], [`dam_core::AmState`] and
    /// `SilenceLifecycle` already spell their variants the way the wire does, so `serde` rejects
    /// an unknown one with a message naming the alternatives.
    #[error("{0}")]
    Domain(#[from] CoreError),
}

// Every wire failure is a decoding failure as far as the pipeline is concerned: the server
// answered, the answer was not usable, and repeating the request will produce the same answer.
// `AmError::Decode` is precisely the variant that is not retryable for that reason.
impl From<WireError> for AmError {
    fn from(error: WireError) -> Self {
        Self::Decode {
            detail: error.to_string(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_wire_failure_crosses_the_port_as_a_decode_failure() {
        let error = AmError::from(WireError::UnsupportedWebhookVersion {
            version: "3".to_owned(),
        });

        assert!(!error.is_retryable());
        assert!(matches!(error, AmError::Decode { detail } if detail.contains('3')));
    }

    #[test]
    fn a_domain_failure_keeps_the_domain_message() {
        let error = WireError::from(CoreError::InvalidFingerprint {
            value: "not-hex".to_owned(),
        });

        assert!(error.to_string().contains("not-hex"));
    }
}
