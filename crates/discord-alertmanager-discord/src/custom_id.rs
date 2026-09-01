//! The codec behind every button and select on a card.
//!
//! Discord gives a component 100 bytes of `custom_id` and hands them back verbatim when somebody
//! clicks. That is the whole state a control may carry, so what goes in it is the card's surrogate
//! key and nothing else: a fingerprint would spend sixteen of the hundred bytes and a label set
//! would not fit at all, and both would put the alert's identity into a string the client can
//! read.
//!
//! # The version prefix is not decoration
//!
//! A deployment that changes the encoding leaves live buttons in every channel encoded the old
//! way. Without a version, one of them decodes into something plausible and wrong. With one, it
//! decodes into a refusal the handler turns into "this control is from an older version of the
//! bot, use `/alerts show`", which is a sentence rather than an incident.

use std::fmt;

use dam_store::NotificationId;
use thiserror::Error;

/// Discord's hard limit on a component's identifier.
pub const MAX_CUSTOM_ID: usize = 100;

/// The encoding this build writes, and the only one it accepts.
const VERSION: &str = "v1";

/// Crockford's base-32 alphabet, which has no `I`, `L`, `O` or `U`.
///
/// Chosen over hexadecimal because it is a fifth shorter, and over base-64 because the alphabet
/// is unambiguous when a human reads an id out of a log line to somebody else.
const ALPHABET: &[u8; 32] = b"0123456789ABCDEFGHJKMNPQRSTVWXYZ";

/// What a control does when it is used.
///
/// One variant per handler. The wire form is the short word, because the budget is a hundred
/// bytes and a descriptive name spends a fifth of it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Action {
    /// Take the alert.
    Ack,

    /// Give it back.
    Unack,

    /// Open the duration picker for an Alertmanager silence.
    SilenceMenu,

    /// Create the silence the picker chose, with the duration in the argument.
    SilenceFor,

    /// Open the duration picker for a bot-local ignore.
    IgnoreMenu,

    /// Create the ignore the picker chose, with the duration in the argument.
    IgnoreFor,

    /// Show the whole alert, ephemerally.
    Details,

    /// Move to another page of a list, with the offset in the argument.
    Page,
}

impl Action {
    /// The word written into the identifier.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Ack => "ak",
            Self::Unack => "uk",
            Self::SilenceMenu => "sm",
            Self::SilenceFor => "sf",
            Self::IgnoreMenu => "im",
            Self::IgnoreFor => "if",
            Self::Details => "dt",
            Self::Page => "pg",
        }
    }

    /// Reads the word back.
    fn parse(value: &str) -> Option<Self> {
        Some(match value {
            "ak" => Self::Ack,
            "uk" => Self::Unack,
            "sm" => Self::SilenceMenu,
            "sf" => Self::SilenceFor,
            "im" => Self::IgnoreMenu,
            "if" => Self::IgnoreFor,
            "dt" => Self::Details,
            "pg" => Self::Page,
            _ => return None,
        })
    }
}

/// One control's identifier, decoded.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CustomId {
    /// What the control does.
    pub action: Action,

    /// The card it acts on.
    pub entity: NotificationId,

    /// Whatever the action needs beyond the card, such as a duration or a page offset.
    pub argument: Option<String>,
}

impl CustomId {
    /// An identifier with no argument.
    #[must_use]
    pub fn new(action: Action, entity: NotificationId) -> Self {
        Self {
            action,
            entity,
            argument: None,
        }
    }

    /// An identifier carrying an argument.
    #[must_use]
    pub fn with_argument(
        action: Action,
        entity: NotificationId,
        argument: impl Into<String>,
    ) -> Self {
        Self {
            action,
            entity,
            argument: Some(argument.into()),
        }
    }

    /// Renders the identifier, refusing one Discord would not accept.
    ///
    /// # Errors
    ///
    /// Returns [`CustomIdError::TooLong`] when the argument pushes it past a hundred bytes, and
    /// [`CustomIdError::Malformed`] when the argument contains the separator.
    pub fn encode(&self) -> Result<String, CustomIdError> {
        if let Some(argument) = &self.argument
            && argument.contains(':')
        {
            return Err(CustomIdError::Malformed {
                detail: "the argument contains the separator",
            });
        }

        let rendered = self.to_string();

        if rendered.len() > MAX_CUSTOM_ID {
            return Err(CustomIdError::TooLong {
                len: rendered.len(),
            });
        }

        Ok(rendered)
    }

    /// Reads an identifier Discord handed back.
    ///
    /// # Errors
    ///
    /// Returns [`CustomIdError::Version`] for an identifier written by another build, which is
    /// the case the version prefix exists for, and [`CustomIdError::Malformed`] for anything
    /// else.
    pub fn decode(raw: &str) -> Result<Self, CustomIdError> {
        let mut parts = raw.splitn(4, ':');

        let version = parts.next().unwrap_or_default();
        if version != VERSION {
            return Err(CustomIdError::Version {
                found: version.to_owned(),
            });
        }

        let action = parts
            .next()
            .and_then(Action::parse)
            .ok_or(CustomIdError::Malformed {
                detail: "the action is not one this build knows",
            })?;

        let entity = parts
            .next()
            .and_then(decode_base32)
            .ok_or(CustomIdError::Malformed {
                detail: "the entity is not a base-32 key",
            })?;

        Ok(Self {
            action,
            entity: NotificationId::new(entity.cast_signed()),
            argument: parts.next().map(str::to_owned),
        })
    }
}

impl fmt::Display for CustomId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{VERSION}:{}:{}",
            self.action.as_str(),
            encode_base32(self.entity.get().cast_unsigned())
        )?;

        if let Some(argument) = &self.argument {
            write!(f, ":{argument}")?;
        }

        Ok(())
    }
}

/// Why an identifier could not be used.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum CustomIdError {
    /// The identifier was written by a build using another encoding.
    ///
    /// The one error a handler answers with a sentence rather than a log line: the button is real,
    /// the person pressing it is not doing anything wrong, and the alert is still there under
    /// `/alerts show`.
    #[error("this control was made by an older version of the bot (`{found}`)")]
    Version {
        /// The prefix that was found instead.
        found: String,
    },

    /// The identifier does not decode.
    #[error("malformed control identifier: {detail}")]
    Malformed {
        /// Which part failed.
        detail: &'static str,
    },

    /// The identifier is longer than Discord accepts.
    #[error("control identifier is {len} bytes, over the {MAX_CUSTOM_ID}-byte limit")]
    TooLong {
        /// The length that was produced.
        len: usize,
    },
}

/// Renders a key in Crockford base 32, shortest form, without padding.
fn encode_base32(mut value: u64) -> String {
    if value == 0 {
        return "0".to_owned();
    }

    let mut digits = Vec::with_capacity(13);
    while value > 0 {
        digits.push(ALPHABET[usize::try_from(value % 32).unwrap_or(0)]);
        value /= 32;
    }
    digits.reverse();

    String::from_utf8(digits).unwrap_or_default()
}

/// Reads a key back, rejecting anything the alphabet does not contain.
fn decode_base32(raw: &str) -> Option<u64> {
    if raw.is_empty() || raw.len() > 13 {
        return None;
    }

    let mut value: u64 = 0;
    for byte in raw.bytes() {
        let digit = ALPHABET.iter().position(|candidate| *candidate == byte)?;
        value = value.checked_mul(32)?.checked_add(digit as u64)?;
    }

    Some(value)
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;

    #[test]
    fn a_control_round_trips() {
        let id = CustomId::with_argument(Action::SilenceFor, NotificationId::new(987_654), "4h");

        let encoded = id.encode().expect("the identifier fits");

        assert_eq!(CustomId::decode(&encoded), Ok(id));
    }

    #[test]
    fn an_identifier_from_another_encoding_is_refused_by_name() {
        let decoded = CustomId::decode("v0:ak:ZZZZ");

        assert!(
            matches!(decoded, Err(CustomIdError::Version { .. })),
            "an old button has to produce a sentence rather than a wrong action: {decoded:?}"
        );
    }

    #[test]
    fn an_unknown_action_is_malformed_rather_than_ignored() {
        assert!(matches!(
            CustomId::decode("v1:zz:1"),
            Err(CustomIdError::Malformed { .. })
        ));
    }

    #[test]
    fn a_separator_in_the_argument_is_refused_where_it_is_written() {
        let id = CustomId::with_argument(Action::Page, NotificationId::new(1), "a:b");

        assert!(matches!(id.encode(), Err(CustomIdError::Malformed { .. })));
    }

    proptest! {
        #[test]
        fn every_generated_identifier_fits_discords_budget(entity: i64, argument in "[a-z0-9]{0,16}") {
            let id = CustomId::with_argument(Action::SilenceFor, NotificationId::new(entity), argument);
            let encoded = id.encode().expect("a sixteen-byte argument leaves room");

            prop_assert!(encoded.len() <= MAX_CUSTOM_ID);
            prop_assert_eq!(CustomId::decode(&encoded), Ok(id));
        }
    }
}
