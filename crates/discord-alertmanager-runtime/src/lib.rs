//! The running pipeline: what turns a webhook into a stored decision, and a stored decision into a
//! Discord call.
//!
//! Three kinds of work live here. [`service::PipelineService`] is the webhook's implementation of
//! [`dam_ingest::WebhookService`] and the one place every alert change is decided, whether it came
//! from a webhook, a reconciler pass or a silence sync. [`dispatch::Dispatcher`] drains the outbox
//! that decision fills. [`tasks::run`] drives the periodic passes on their own clocks.
//! [`cards::Cards`] and [`admin::AdminChannel`] are what those three share.
//!
//! What does not belong here: choosing a backend, building the clients, installing the log
//! subscriber or the metrics recorder, and handling signals. Each of those is global state or a
//! choice about the environment, and the binary makes it. Every collaborator arrives as a trait
//! object, so the binary decides what this crate runs against and a test can hand it a fake.

pub mod admin;
pub mod cards;
pub mod dispatch;
pub mod service;
pub mod tasks;
