//! Snowflakes, surrogate keys, and the one place a `u64` becomes an `i64`.

use std::fmt;

use serde::{Deserialize, Serialize};

/// A Discord snowflake, stored as a signed 64-bit integer.
///
/// Discord ids are unsigned 64-bit and every SQL engine here offers a signed one. The two
/// alternatives are storing them as text, which gives up integer indexing and sorting for a
/// problem that does not exist, or scattering `as i64` casts across the backends, which is how
/// one of them eventually becomes a saturating cast nobody notices.
///
/// The reinterpretation is lossless in both directions, so an id past `i64::MAX` — none exist
/// today, and the type does not depend on that staying true — round-trips as a negative number
/// rather than being clamped.
#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
#[serde(transparent)]
pub struct Snowflake(u64);

impl Snowflake {
    /// Wraps a Discord id.
    #[must_use]
    pub fn new(value: u64) -> Self {
        Self(value)
    }

    /// The id as Discord states it.
    #[must_use]
    pub fn get(self) -> u64 {
        self.0
    }

    /// The id as it is stored.
    #[must_use]
    pub fn to_db(self) -> i64 {
        self.0.cast_signed()
    }

    /// Reads an id back out of a column.
    #[must_use]
    pub fn from_db(value: i64) -> Self {
        Self(value.cast_unsigned())
    }
}

impl fmt::Display for Snowflake {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<u64> for Snowflake {
    fn from(value: u64) -> Self {
        Self(value)
    }
}

/// Declares a newtype over [`Snowflake`], so a channel id cannot be passed where a guild id is
/// expected.
///
/// The three ids are all snowflakes and are all `i64` in the database, which makes them exactly
/// the kind of argument that gets swapped in a call with several of them. The wrapper costs
/// nothing at runtime and the swap becomes a compile error.
macro_rules! snowflake_id {
    ($(#[$meta:meta])* $name:ident) => {
        $(#[$meta])*
        #[derive(
            Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
        )]
        #[serde(transparent)]
        pub struct $name(Snowflake);

        impl $name {
            /// Wraps a Discord id.
            #[must_use]
            pub fn new(value: u64) -> Self {
                Self(Snowflake::new(value))
            }

            /// The id as Discord states it.
            #[must_use]
            pub fn get(self) -> u64 {
                self.0.get()
            }

            /// The id as it is stored.
            #[must_use]
            pub fn to_db(self) -> i64 {
                self.0.to_db()
            }

            /// Reads an id back out of a column.
            #[must_use]
            pub fn from_db(value: i64) -> Self {
                Self(Snowflake::from_db(value))
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                fmt::Display::fmt(&self.0, f)
            }
        }

        impl From<u64> for $name {
            fn from(value: u64) -> Self {
                Self::new(value)
            }
        }
    };
}

snowflake_id! {
    /// A Discord guild.
    GuildId
}

snowflake_id! {
    /// A Discord channel, which may be a text channel, a forum channel or a thread.
    ChannelId
}

snowflake_id! {
    /// A Discord message.
    MessageId
}

snowflake_id! {
    /// A Discord user.
    UserId
}

snowflake_id! {
    /// A Discord role.
    RoleId
}

snowflake_id! {
    /// A forum tag.
    TagId
}

/// Declares a newtype over a database surrogate key.
macro_rules! surrogate_id {
    ($(#[$meta:meta])* $name:ident) => {
        $(#[$meta])*
        #[derive(
            Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
        )]
        #[serde(transparent)]
        pub struct $name(i64);

        impl $name {
            /// Wraps a key read from the database.
            #[must_use]
            pub fn new(value: i64) -> Self {
                Self(value)
            }

            /// The key as it is stored.
            #[must_use]
            pub fn get(self) -> i64 {
                self.0
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, "{}", self.0)
            }
        }

        impl From<i64> for $name {
            fn from(value: i64) -> Self {
                Self(value)
            }
        }
    };
}

surrogate_id! {
    /// Primary key of a `notifications` row.
    ///
    /// This is what a card's custom id encodes. A fingerprint or a label set in a button would
    /// blow the 100-byte custom-id budget and would leak the label set into a client-visible
    /// string; a surrogate key does neither.
    NotificationId
}

surrogate_id! {
    /// Primary key of a `routes` row.
    RouteId
}

surrogate_id! {
    /// Primary key of an `outbox` row.
    OutboxId
}

surrogate_id! {
    /// Primary key of an `ignore_rules` row.
    IgnoreId
}

surrogate_id! {
    /// Primary key of an `acknowledgements` row.
    AckId
}

surrogate_id! {
    /// Primary key of a `subscriptions` row.
    SubscriptionId
}

/// Identifies one dispatcher worker for the lifetime of a process.
///
/// Written into `outbox.claimed_by`, so a lease that expires can be reported with the worker that
/// took it. A restart produces a new value on purpose: rows claimed by the previous process must
/// look abandoned to the janitor, not reclaimable by whoever happens to start with the same name.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct WorkerId(String);

impl WorkerId {
    /// Wraps a worker identifier.
    #[must_use]
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    /// The identifier as it is stored.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for WorkerId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_snowflake_survives_the_signed_round_trip() {
        for value in [0, 1, 175_928_847_299_117_063, u64::MAX, i64::MAX as u64 + 1] {
            let id = Snowflake::new(value);

            assert_eq!(Snowflake::from_db(id.to_db()).get(), value);
        }
    }

    #[test]
    fn ids_of_different_kinds_are_different_types() {
        // A compile-time property, asserted here only so the intent is written down: the two
        // lines below would not compile if either took a plain `i64`.
        let guild = GuildId::new(1);
        let channel = ChannelId::new(1);

        assert_eq!(guild.to_db(), channel.to_db());
    }
}
