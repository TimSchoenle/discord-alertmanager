//! Gateway credentials, command registration scope, and the capability map.

use secrecy::SecretString;
use serde::Deserialize;

/// Everything the serenity client needs before it can connect.
#[derive(Debug, Clone, Default, Deserialize)]
#[cfg_attr(
    feature = "config-schema",
    derive(serde::Serialize, terrace_config::schema::Describe)
)]
#[serde(default, deny_unknown_fields)]
pub struct Discord {
    /// Bot token. Supply it through `DAM_DISCORD__TOKEN_FILE` or the secrets directory.
    ///
    /// Passing it as a plain environment variable puts it in the output of `docker inspect` and
    /// in any crash reporter that captures the environment.
    #[cfg_attr(feature = "config-schema", config(secret))]
    #[serde(skip_serializing)]
    pub token: SecretString,

    /// Guild to register slash commands into. Registration is global when unset.
    ///
    /// Guild-scoped commands appear immediately; global ones take up to an hour to propagate.
    /// Set this in development and leave it unset in production.
    pub dev_guild_id: Option<u64>,

    /// Capture the text of thread replies, which needs the privileged `MESSAGE_CONTENT` intent.
    ///
    /// A card marks itself responded without this: the author and channel of a reply arrive
    /// without the privileged intent, and that is all the detection needs. Only the text of the
    /// reply is gated, so leaving this off costs nothing except the reply preview.
    pub capture_reply_text: bool,

    /// Which Discord roles hold which capability.
    #[cfg_attr(feature = "config-schema", config(nested))]
    pub capabilities: Capabilities,
}

/// The role-to-capability map every command is checked against.
///
/// Each list holds `@everyone`, `role:<name>` or `role:<id>`. Authorisation is enforced here and
/// not by Discord's permission bits: `default_member_permissions` is set so the client hides
/// commands a user cannot run, and it is treated as a display hint. Every denial writes an audit
/// row.
#[derive(Debug, Clone, Deserialize)]
#[cfg_attr(
    feature = "config-schema",
    derive(serde::Serialize, terrace_config::schema::Describe)
)]
#[serde(default, deny_unknown_fields)]
pub struct Capabilities {
    /// Read alerts, silences and routes. Grants no mutation of any kind.
    pub view: Vec<String>,

    /// Acknowledge, assign, and add or remove bot-local ignores.
    pub operate: Vec<String>,

    /// Create, extend and expire Alertmanager silences, which affects every receiver.
    pub silence: Vec<String>,

    /// Manage routes and read the effective configuration.
    pub admin: Vec<String>,
}

impl Default for Capabilities {
    fn default() -> Self {
        // Read for everyone, write for nobody. A deployment that forgets to configure this can
        // look at its alerts and cannot silence a page.
        Self {
            view: vec!["@everyone".to_owned()],
            operate: Vec::new(),
            silence: Vec::new(),
            admin: Vec::new(),
        }
    }
}
