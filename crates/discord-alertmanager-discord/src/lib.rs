//! The serenity layer: the command registry, the component handlers, the card renderer and the
//! `DiscordSink` implementation.
//!
//! This crate is the only one that maps serenity's errors into the vocabulary the engine reacts
//! to. `HttpError::UnsuccessfulRequest` carrying code 10008 becomes `SinkError::UnknownMessage`,
//! so the engine recreates a card a user deleted; 50013 becomes `MissingPermissions`, so a route
//! is marked unhealthy instead of retrying forever. Leaving that mapping to the engine would put
//! a Discord error code in a crate that does not depend on Discord.
//!
//! # Plain serenity, not poise
//!
//! The registry is a `HashMap<&'static str, Arc<dyn SlashCommand>>` plus a component registry
//! keyed by the action segment of the custom id. It is roughly two hundred lines, and it buys one
//! place to hang authorisation, deferral, audit logging and error mapping, which is where the
//! risk in this layer sits. Argument extraction and autocomplete are the work being re-done, and
//! that is the honest cost of the choice. Commands only ever call into `dam_engine`, so swapping
//! the registry for poise would touch this crate and nothing else.
//!
//! # Intents
//!
//! `GUILDS` and `GUILD_MESSAGES`, and deliberately not `MESSAGE_CONTENT`. Author and channel
//! arrive without the privileged intent, which is everything needed to notice that a human
//! replied in an alert's thread. Only the text of that reply needs it, so capturing the text is a
//! config flag, documented as requiring the privileged intent, and off by default.
//!
//! # Authorisation is bot-side
//!
//! `default_member_permissions` is set so Discord hides commands a user cannot run, and it is
//! treated as a display hint. The capability check happens in `CommandCtx` before any handler
//! body runs, and every denial writes an audit row.

pub mod bot;
pub mod capability;
pub mod custom_id;
pub mod links;
pub mod render;
pub mod sink;
pub mod template;

mod actions;
mod commands;
mod components;
mod error;

pub use bot::{Bot, BotContext, GatewayError};
pub use capability::{Capability, CapabilityMap};
pub use custom_id::{Action, CustomId, CustomIdError, MAX_CUSTOM_ID};
pub use links::{LinkError, LinkRenderer, RenderedLink};
pub use render::{RenderedCard, Renderer};
pub use sink::SerenitySink;
