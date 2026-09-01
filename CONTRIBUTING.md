# Contributing

## Commit messages

[Conventional Commits](https://www.conventionalcommits.org). The type decides the changelog
section and the version bump: `feat` and a breaking change move the minor while the project is
pre-1.0, `fix` moves the patch, and `docs`, `chore`, `refactor` and `test` move nothing.

## Generated files

Four artefacts are committed and none of them is written by hand. Each says so in its own first
lines, and editing the output instead of its source is reverted by the next render.

| File | Written by | Source |
| --- | --- | --- |
| `README.md` | `.github/workflows/docs.yml` | `.github/templates/README.md.hbs` |
| `docs/config.md`, `docs/config.json`, `docs/config.contract.json`, `config.example.toml` | `cargo xtask config-docs` | the types in `crates/discord-alertmanager-config` |
| `CHANGELOG.md`, the version in both manifests and in `Cargo.lock` | `.github/workflows/release-please.yaml` | the conventional-commit subjects since the last tag |
| `crates/*/.sqlx/` | `cargo xtask db-prepare` | the queries and each backend's `migrations/` |

The README is rendered on every pull request and committed back to the branch, so a change to the
template arrives already rendered. A push to `main` whose `README.md` does not match its template
fails the `readme` job.

Adding a configuration key means adding it to the type, running `cargo xtask config-docs`, and
committing the four files that come out. Nothing else needs touching: the README's key count and
its table of required keys are lifted out of `docs/config.md` by
`.github/scripts/readme-variables.sh` when the README is rendered.

`cargo xtask db-prepare` needs a container runtime. It starts a throwaway PostgreSQL container and
a temporary SQLite file, applies each backend's migrations, and records the checked queries; the
committed caches are what let a plain `cargo build` need neither Docker nor a database.

## Checks

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets --all-features
cargo test --workspace --all-features
cargo deny check
cargo xtask config-docs                       # then check nothing moved
bash .github/scripts/readme-variables.sh      # the README payload, as CI will render it
bash .github/scripts/verify-architecture.sh   # the domain crate's dependency rule
```

Clippy runs with `-D warnings`. The lint set is declared once in `[workspace.lints]` and inherited
by every member, so a crate cannot be held to a looser standard than its neighbours. Use
`#[expect(…, reason = "…")]` rather than a bare `#[allow(…)]`: an `allow` goes on silently
forever, an `expect` warns the moment its claim stops holding.

The version appears in two manifests, because the README payload action cannot read a version a
crate inherits from its workspace. `readme-variables.sh` fails when `[workspace.package]` in
`Cargo.toml` and `[package]` in `crates/discord-alertmanager/Cargo.toml` disagree. Neither is
edited by hand: the release pull request writes both, along with the eleven member versions in
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
its version literally, and every local package in `Cargo.lock` — selected by the absence of a
`source` field, which is what distinguishes a path dependency from a registry one.

## Prose

Everything written here — README, template, `docs/` page, doc comment, commit body, pull request
description — follows
[TimSchoenle/actions/docs](https://github.com/TimSchoenle/actions/tree/main/docs).
[readme/PROSE.md](https://github.com/TimSchoenle/actions/blob/main/docs/readme/PROSE.md) is the
one to read before writing a sentence, and it is the review criteria rather than a suggestion.
[readme/GUIDE.md](https://github.com/TimSchoenle/actions/blob/main/docs/readme/GUIDE.md) covers
the README's section order and what generates each part.

Doc comments follow
[doc-comments/RUST.md](https://github.com/TimSchoenle/actions/blob/main/docs/doc-comments/RUST.md).
The short version: a verb-first one-line summary, a blank line, and a body only where the
signature cannot say it. Rationale a caller needs goes in `///`; rationale whoever edits the line
needs goes in `//`; the design record goes in `//!` at the crate root.
