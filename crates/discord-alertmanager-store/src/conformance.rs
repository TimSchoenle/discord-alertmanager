//! The suite every backend runs against its own [`crate::Store`].
//!
//! Each backend crate takes this crate with the `conformance` feature in its `dev-dependencies`
//! and calls the entry point below from an integration test, so one body of assertions runs twice
//! against two dialects from a single `cargo test`.
//!
//! What belongs here is behaviour two implementations can disagree about, and nothing else:
//! whether `FOR UPDATE SKIP LOCKED` and `BEGIN IMMEDIATE` hand one row to exactly one claimant,
//! how equal timestamps order, and whether both map a unique violation onto the same
//! `StoreError`. SQL that is merely valid is already checked at compile time and needs no test.
