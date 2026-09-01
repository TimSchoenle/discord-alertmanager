//! Who may do what, decided by the bot rather than by Discord.
//!
//! `default_member_permissions` is set on every command so the client hides the ones a person
//! cannot run, and that is all it is: a display hint. The check that matters happens here, before
//! any handler body runs, because Discord's permission bits describe what somebody may do to a
//! channel and this bot's capabilities describe what they may do to an incident. Silencing an
//! alert stops a page; there is no Discord permission that means that.
//!
//! Every refusal is auditable, which is why [`CapabilityMap::allows`] answers a question rather
//! than performing an action: the caller writes the audit row either way.

use std::fmt;

use dam_config::Capabilities;
use dam_store::RoleId;

/// What an action needs.
///
/// Ordered from least to most: a capability grants everything below it, so an administrator does
/// not have to be listed in four places to be able to press a button.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Capability {
    /// Read alerts, silences and routes.
    View,

    /// Acknowledge, assign, and add or remove bot-local ignores.
    Operate,

    /// Create, extend and expire Alertmanager silences, which stops every receiver.
    Silence,

    /// Manage routes and read the effective configuration.
    Admin,
}

impl Capability {
    /// The word this capability is audited and configured under.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::View => "view",
            Self::Operate => "operate",
            Self::Silence => "silence",
            Self::Admin => "admin",
        }
    }
}

impl fmt::Display for Capability {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// One configured grant.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Grant {
    /// Everybody in the guild.
    Everyone,

    /// A role, by id.
    Role(RoleId),

    /// A role, by name, resolved against the member's roles at check time.
    Named(String),
}

impl Grant {
    /// Reads one entry of a capability list.
    ///
    /// `@everyone`, `role:<id>` and `role:<name>` are the three spellings, and a bare word is
    /// treated as a role name so that a configuration missing the prefix still does what it
    /// obviously means.
    fn parse(entry: &str) -> Self {
        let entry = entry.trim();

        if entry.eq_ignore_ascii_case("@everyone") {
            return Self::Everyone;
        }

        let name = entry.strip_prefix("role:").unwrap_or(entry);

        name.parse::<u64>().map_or_else(
            |_| Self::Named(name.to_owned()),
            |id| Self::Role(RoleId::new(id)),
        )
    }
}

impl Grant {
    /// The grant as `/status config` shows it, mentioning a role so Discord renders its name.
    fn describe(&self) -> String {
        match self {
            Self::Everyone => "@everyone".to_owned(),
            Self::Role(id) => format!("<@&{id}>"),
            Self::Named(name) => format!("role `{name}`"),
        }
    }
}

/// The compiled role-to-capability map.
pub struct CapabilityMap {
    view: Vec<Grant>,
    operate: Vec<Grant>,
    silence: Vec<Grant>,
    admin: Vec<Grant>,
}

impl CapabilityMap {
    /// Compiles the configured lists.
    #[must_use]
    pub fn new(config: &Capabilities) -> Self {
        let compile =
            |entries: &[String]| entries.iter().map(|entry| Grant::parse(entry)).collect();

        Self {
            view: compile(&config.view),
            operate: compile(&config.operate),
            silence: compile(&config.silence),
            admin: compile(&config.admin),
        }
    }

    /// Whether somebody holding these roles may do something needing `capability`.
    ///
    /// A grant at a higher level implies the ones below it, so `admin` alone is enough to
    /// acknowledge an alert. `roles` carries both the ids and the names the member holds, because
    /// a configuration may name either and resolving a name to an id needs the guild.
    #[must_use]
    pub fn allows(&self, capability: Capability, roles: &[RoleId], names: &[String]) -> bool {
        [
            Capability::View,
            Capability::Operate,
            Capability::Silence,
            Capability::Admin,
        ]
        .into_iter()
        .filter(|level| *level >= capability)
        .any(|level| self.granted(level, roles, names))
    }

    /// The map as `/status config` shows it.
    ///
    /// The grants only, never who currently holds them: this answers "who may silence", which is
    /// a configuration question, and not "who is on call", which is not this bot's to answer.
    #[must_use]
    pub fn describe(&self) -> String {
        [
            (Capability::View, &self.view),
            (Capability::Operate, &self.operate),
            (Capability::Silence, &self.silence),
            (Capability::Admin, &self.admin),
        ]
        .into_iter()
        .map(|(capability, grants)| {
            let listed = if grants.is_empty() {
                "nobody".to_owned()
            } else {
                grants
                    .iter()
                    .map(Grant::describe)
                    .collect::<Vec<_>>()
                    .join(", ")
            };

            format!("`{capability}` — {listed}")
        })
        .collect::<Vec<_>>()
        .join(
            "
",
        )
    }

    /// Whether one level's list matches.
    fn granted(&self, capability: Capability, roles: &[RoleId], names: &[String]) -> bool {
        let grants = match capability {
            Capability::View => &self.view,
            Capability::Operate => &self.operate,
            Capability::Silence => &self.silence,
            Capability::Admin => &self.admin,
        };

        grants.iter().any(|grant| match grant {
            Grant::Everyone => true,
            Grant::Role(id) => roles.contains(id),
            Grant::Named(name) => names.iter().any(|held| held.eq_ignore_ascii_case(name)),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn map() -> CapabilityMap {
        CapabilityMap::new(&Capabilities {
            view: vec!["@everyone".to_owned()],
            operate: vec!["role:oncall".to_owned()],
            silence: vec!["role:12345".to_owned()],
            admin: vec!["role:platform-admin".to_owned()],
        })
    }

    #[test]
    fn a_higher_grant_implies_the_lower_ones() {
        let admin = vec!["platform-admin".to_owned()];

        assert!(map().allows(Capability::Operate, &[], &admin));
        assert!(map().allows(Capability::Silence, &[], &admin));
        assert!(
            map().allows(Capability::Admin, &[], &admin),
            "an administrator does not have to be listed in four places to press a button"
        );
    }

    #[test]
    fn silencing_is_not_granted_by_operating() {
        let oncall = vec!["oncall".to_owned()];

        assert!(map().allows(Capability::Operate, &[], &oncall));
        assert!(
            !map().allows(Capability::Silence, &[], &oncall),
            "a silence stops every receiver, including whatever pages somebody at four in the \
             morning; it is not implied by being on call"
        );
    }

    #[test]
    fn a_role_may_be_named_or_numbered() {
        assert!(map().allows(Capability::Silence, &[RoleId::new(12_345)], &[]));
        assert!(!map().allows(Capability::Silence, &[RoleId::new(54_321)], &[]));
    }

    #[test]
    fn the_default_map_reads_and_writes_nothing() {
        let empty = CapabilityMap::new(&Capabilities::default());

        assert!(empty.allows(Capability::View, &[], &[]));
        assert!(
            !empty.allows(Capability::Operate, &[], &[]),
            "a deployment that forgets to configure this can look at its alerts and cannot \
             silence a page"
        );
    }
}
