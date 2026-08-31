//! The two outbound ports, and the vocabulary the pipeline reacts to.
//!
//! Both traits are defined here and implemented elsewhere: `dam_discord` supplies the sink,
//! `dam_am` supplies the Alertmanager client. Neither `serenity` nor `reqwest` appears in this
//! crate's manifest as a result, and the whole pipeline runs against in-memory fakes.
//!
//! The types crossing these boundaries are domain types, never wire types. A port that spoke
//! Alertmanager's JSON model would make this crate depend on the client that owns it, and the
//! dependency would then point in both directions.

pub mod alertmanager;
pub mod sink;

pub use alertmanager::{
    AlertFilter, AlertmanagerApi, AmError, AmStatus, Receiver, SilenceRecord, SilenceRequest,
    suppressed_fingerprints,
};
pub use sink::{
    CardData, CardTarget, DiscordSink, Mention, MessageRef, Note, PostFlags, PostedMessage,
    SilenceSummary, SinkError, TagSpec,
};
