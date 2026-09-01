//! Alertmanager API v2 client and the version-4 webhook envelope.
//!
//! The model is hand-written against a pinned spec commit rather than generated from
//! `openapi.yaml`. The surface actually used is about ten structs and six calls; a generated
//! client pulls a large dependency tree, churns on every Alertmanager release, and produces a map
//! wrapper where `dam_core::Labels` belongs. A test deserialises fixtures captured from a real
//! 0.33 server, which is the part of a generated client's value worth keeping.
//!
//! # Matchers are constructed, never parsed
//!
//! Every silence matcher is built as `{ name, value, isRegex, isEqual }`. Alertmanager 0.27
//! introduced a UTF-8 matcher parser with backwards-incompatible edge cases against the classic
//! one, so handing it a string means choosing which parser to be wrong about. The structured form
//! sidesteps the question.
//!
//! # Creating a silence is not idempotent
//!
//! `POST /api/v2/silences` creates a new silence on every call. A retry therefore looks for an
//! existing `silences` row for the command id first and re-posts with its `id` set, which
//! Alertmanager treats as an update. Expiry is `DELETE /api/v2/silence/{id}`, singular, unlike
//! every other path on this API.

pub mod client;
pub mod error;
pub mod model;
pub mod webhook;

pub use client::AlertmanagerClient;
pub use error::WireError;
pub use model::{
    AlertmanagerStatus, ClusterStatus, GettableAlert, GettableSilence, PeerStatus, PostableSilence,
    ServerConfig, SilenceCreated, VersionInfo, WireAlertStatus, WireMatcher, WireReceiver,
    WireSilenceStatus,
};
pub use webhook::{WEBHOOK_VERSION, WebhookAlert, WebhookPayload};
