# Contributing

## Commit messages

[Conventional Commits](https://www.conventionalcommits.org). The type decides the changelog
section and the version bump: `feat` and a breaking change move the minor while the project is
pre-1.0, `fix` moves the patch, and `docs`, `chore`, `refactor` and `test` move nothing.

## Generated files

The files below are committed and generated from the sources in the table, so do not edit them by
hand: the next render replaces any change made to the output instead of its source.

| File | Written by | Source |
| --- | --- | --- |
| `README.md` | `.github/workflows/docs.yml` | `.github/templates/README.md.hbs` |
| `docs/config.md`, `docs/config.json`, `docs/config.contract.json`, `config.example.toml` | `cargo xtask config-docs` | the types in `crates/discord-alertmanager-config` |
| `crates/discord-alertmanager-store-postgres/.sqlx/`, `crates/discord-alertmanager-store-sqlite/.sqlx/` | `cargo xtask db-prepare` | each backend's queries and `migrations/` |
| `CHANGELOG.md`, the version in both manifests and in `Cargo.lock` | `.github/workflows/release-please.yaml` | the conventional-commit subjects since the last tag |

The README is rendered on every pull request and committed back to the branch, so a change to the
template arrives already rendered. A push to `main` whose `README.md` does not match its template
fails the `readme` job.

Adding a configuration key means adding it to the type, running `cargo xtask config-docs`, and
committing the four files that come out. Nothing else needs touching: the README's key count and
its table of required keys are lifted out of `docs/config.md` by
`.github/scripts/readme-variables.sh` when the README is rendered.

## Database queries

Every query in both backends goes through the `query!` family of macros (`query!`, `query_as!`,
`query_scalar!`), so its SQL, its parameters and the columns it returns are checked against the
migrated schema at compile time. Nothing assembles SQL at run time: a filter with optional parts
binds its lists as arrays in PostgreSQL and as JSON arrays in SQLite, and an empty list constrains
nothing.

An ordinary `cargo build` checks the queries against each backend's committed `.sqlx/` cache, so it
needs neither Docker nor a database. `.cargo/config.toml` pins `SQLX_OFFLINE=true` to keep it that
way. Changing a query or adding a migration means regenerating the caches and committing them with
the change:

```bash
cargo install sqlx-cli --version 0.9.0 --locked --no-default-features \
  --features postgres,sqlite,rustls,sqlx-toml
cargo xtask db-prepare           # rewrite both caches
cargo xtask db-prepare --check   # what CI runs: fail if either cache is stale
```

Both commands need a container runtime. They start a throwaway PostgreSQL container and a
temporary SQLite file, apply each backend's migrations, and recompile every query against them.
The `query-cache` job runs the check on every pull request.

To compile against a live database while writing a query, set the variable the crate's `sqlx.toml`
names and turn the pin off for that shell:

```bash
export SQLX_OFFLINE=false
export DAM_POSTGRES_URL=postgres://postgres:postgres@127.0.0.1:5432/postgres
export DAM_SQLITE_URL=sqlite:///tmp/dam.db
```

The two backends name different variables, so one shell can point both at their own database.

## Checks

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-features
cargo deny check
cargo xtask config-docs                       # then check nothing moved
cargo xtask db-prepare --check                # needs Docker and sqlx-cli
bash .github/scripts/readme-variables.sh      # the README payload, as CI will render it
bash .github/scripts/verify-architecture.sh   # the domain crate's dependency rule
bash .github/scripts/prose-check.sh README.md CONTRIBUTING.md SECURITY.md docs/COMMANDS.md docs/ROUTES.md
```

Clippy runs with `-D warnings`. The lint set is declared once in `[workspace.lints]` and inherited
by every member, so a crate cannot be held to a looser standard than its neighbours. Use
`#[expect(…, reason = "…")]` rather than a bare `#[allow(…)]`: an `allow` goes on silently
forever, an `expect` warns the moment its claim stops holding.

The version appears in two manifests, because the README payload action cannot read a version a
crate inherits from its workspace. `readme-variables.sh` fails when `[workspace.package]` in
`Cargo.toml` and `[package]` in `crates/discord-alertmanager/Cargo.toml` disagree. Neither is
edited by hand: the release pull request writes both, along with every member version in
`Cargo.lock`.

## Releases

release-please keeps a pull request open holding the next version and the changelog entry for
everything merged since the last tag. Merging it creates the tag, and
`.github/workflows/release-please.yaml` builds the image on both architectures and publishes it to
Docker Hub and to GitHub Container Registry, signs each index with cosign, and attaches the
configuration contract to the pushed digest as a signed OCI referrer.

`release-please-config.json` sets `release-type: simple` and does the version bumps through
`extra-files`, which is not the obvious choice for a Rust project. `rust` is unusable here: its
updater throws on a manifest with no `[package]` table, which is what a virtual workspace root is,
and it expands `workspace.members` literally, so the `crates/*` glob resolves to a directory that
does not exist and no member is bumped. The three `extra-files` entries do the same three
substitutions the `rust` strategy would have: `[workspace.package]`, the one member that states
its version literally, and every local package in `Cargo.lock`, selected by the absence of a
`source` field, which is what distinguishes a path dependency from a registry one.

## Prose

Everything written here — README, template, `docs/` page, doc comment, commit body, pull request
description — follows
[TimSchoenle/actions/docs](https://github.com/TimSchoenle/actions/tree/main/docs).
[readme/PROSE.md](https://github.com/TimSchoenle/actions/blob/main/docs/readme/PROSE.md) is the
one to read before writing a sentence, and review holds every change to it.
[readme/GUIDE.md](https://github.com/TimSchoenle/actions/blob/main/docs/readme/GUIDE.md) covers
the README's section order and what generates each part.

Doc comments follow
[doc-comments/RUST.md](https://github.com/TimSchoenle/actions/blob/main/docs/doc-comments/RUST.md).
The short version: a one-line summary in the third person that starts with its verb, a blank
line, and a body only where the signature cannot say it. Rationale a caller needs goes in `///`;
rationale whoever edits the line needs goes in `//`; the design record goes in `//!` at the crate
root.
