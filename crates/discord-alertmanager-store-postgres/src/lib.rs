//! `PostgreSQL` backend for [`dam_store::Store`].
//!
//! Work is claimed with `SELECT … FOR UPDATE SKIP LOCKED`, which is the reason this backend is
//! the supported path above a small deployment: several dispatcher workers claim disjoint rows
//! without blocking each other, and a lease plus a janitor covers the worker that dies holding
//! one.
//!
//! # Why this crate exists separately from the `SQLite` one
//!
//! `sqlx::query!` resolves its driver from a single `DATABASE_URL` per compilation unit, so one
//! crate cannot hold both dialects' checked queries. This crate owns its own `.env`,
//! `migrations/` and `.sqlx/`. Keeping the offline caches per crate rather than running
//! `cargo sqlx prepare --workspace` also removes a collision that is otherwise waiting to happen:
//! offline entries are keyed by a hash of the query text, and a parameterless query such as
//! `SELECT count(*) FROM alerts` is byte-identical in both dialects.
//!
//! Checked queries live in a thin `queries` module. Macro-heavy modules slow incremental builds
//! and `rust-analyzer`, so keeping them in one place leaves the rest of the crate fast to edit.
