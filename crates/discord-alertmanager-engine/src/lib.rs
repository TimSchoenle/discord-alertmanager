//! The decision pipeline: ingest, route, decide, enqueue.
//!
//! This crate owns both outbound ports, `DiscordSink` and `AlertmanagerApi`, and implements
//! neither. `dam_discord` and `dam_am` supply the implementations, which is why `serenity` and
//! `reqwest` appear in neither this manifest nor this crate's tests. The whole pipeline runs
//! against in-memory fakes.
//!
//! # Where the behaviour lives
//!
//! One function decides everything. It is pure over `(delta, routes, ignores,
//! existing_notification, now)` and returns a `Vec<Effect>`. Route resolution, ignore rules, the
//! silenced-versus-firing distinction, whether a transition mentions anyone, and whether an
//! existing card is edited or a new one posted all happen inside it. Testing it exhaustively
//! costs less than testing anything downstream of it, so it is the one place worth over-testing.
//!
//! # Ignore and silence are not the same operation
//!
//! A silence mutates Alertmanager and stops every receiver, including whatever pages someone at
//! four in the morning. An ignore is bot-local: Discord goes quiet and the alert still fires
//! everywhere else. The pipeline keeps the two apart at every step, and an ignored alert still
//! gets its `alerts` row and still appears in `/alerts list`. It is muted, not hidden.
//!
//! # Push is fast, pull is authoritative
//!
//! The webhook path writes to the database and returns. Discord I/O inline would mean a Discord
//! rate-limit stall times the webhook out, Alertmanager retries, and the load multiplies during
//! exactly the incident the bot exists to report. The reconciler polls Alertmanager and diffs,
//! because webhooks are lost to restarts, partitions and missed `send_resolved`. The outbox is
//! what makes the effects restart-safe: a crash between deciding to post and posting leaves a
//! claimable row rather than a lost notification.

pub mod decide;
pub mod ports;
pub mod routing;

pub use decide::{DecisionSettings, ExistingCards, decide};
pub use ports::{
    AlertFilter, AlertmanagerApi, AmError, AmStatus, CardData, CardTarget, DiscordSink, MessageRef,
    Note, PostFlags, PostedMessage, Receiver, SilenceRecord, SilenceRequest, SinkError, TagSpec,
};
pub use routing::{RoutingSnapshot, SharedRouting, route_from_config};
