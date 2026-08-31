//! `SQLite` backend for [`dam_store::Store`].
//!
//! Work is claimed inside a `BEGIN IMMEDIATE` transaction, because `SQLite` has no
//! `FOR UPDATE SKIP LOCKED`. The writer pool is effectively one connection, which is why such a
//! deployment is a single replica: horizontal scaling and leader election are `PostgreSQL`-only,
//! and `docs/operations.md` says so where an operator will read it.
//!
//! Every connection sets `journal_mode=WAL`, `busy_timeout=5000`, `foreign_keys=ON` and
//! `synchronous=NORMAL`.
//!
//! # Where this dialect costs more than `PostgreSQL`
//!
//! Type inference is weaker, so the checked queries carry more overrides — `SELECT id AS "id!:
//! i64"`, `labels AS "labels: Json<Labels>"` — than their `PostgreSQL` counterparts. Timestamps
//! are `TEXT` in RFC 3339 with a fixed six-digit subsecond, so that lexicographic order matches
//! chronological order; anything shorter sorts `…:00.5Z` after `…:00.45Z`.
//!
//! The `PostgreSQL` backend was written first and this one ported from it. The inference
//! overrides are easier to reason about against a working reference than in parallel with one.
