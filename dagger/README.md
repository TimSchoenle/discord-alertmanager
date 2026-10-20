# Dagger pipeline

A Dagger module that builds this repository's container image, and can run its test suite, as
typed functions that behave the same on a laptop and on a GitHub runner.

**Under evaluation.** `.github/workflows/dagger-compare.yml` builds the image through this module
next to `build.yaml`'s `docker-build-test` job, which builds the same image from the `Dockerfile`,
so the two can be timed against each other. Whichever loses is removed. Until then, CI gates on
the `Dockerfile` build only.

## Layout

| Path | Purpose |
| --- | --- |
| `dagger.json` | Module manifest at the repository root: name, engine version, SDK, source directory. |
| `dagger/src/index.ts` | The `DiscordAlertmanager` object and its functions. |
| `dagger/src/config.ts` | Pinned versions, image layout, and pure parsing helpers. |
| `dagger/src/platform.ts` | Per-architecture build parameters (musl triple, UPX). |
| `.github/workflows/dagger-compare.yml` | The comparison job, including the cargo cache carried between runs. |
| `.github/scripts/dagger-cache.sh` | Enforces the cache archive size limit before a save. |

The module uses the TypeScript SDK. The Rust SDK (`dagger-sdk` on crates.io) is a client library
only: it can drive an engine from a Rust program, but it cannot define a module, so `dagger call`
cannot reach it. A Rust client would also add `reqwest` 0.11 and a second TLS stack to
`Cargo.lock`, which `deny.toml` exists to prevent.

## Functions

| Call | What it does | Today's equivalent |
| --- | --- | --- |
| `dagger call test` | `cargo test --workspace --all-features --locked` on glibc, with a Docker daemon sidecar for the PostgreSQL test | `Test` job |
| `dagger call build` | Static musl release binary, stripped, UPX-packed on amd64 | Dockerfile `builder` stage |
| `dagger call contract` | `contract.json` and `contract.labels` from the generator | Dockerfile `contract-builder` stage |
| `dagger call docker` | The `FROM scratch` runtime image | Dockerfile `runtime` stage |
| `dagger call smoke-test` | The binary starts, and the contract label points at the generated document | `Test Docker image runs` and the contract checks in `Docker Build and Test` |
| `dagger call publish --address=…` | Pushes one or more platforms as one index | `release-please.yaml` build and merge jobs |
| `dagger call cache-archive` | Packs the cargo caches into one `tar.zst` | `Swatinem/rust-cache` save |
| `dagger call cache-restore --archive=…` | Unpacks that archive into a fresh engine | `Swatinem/rust-cache` restore |

Every function builds from the repository root, filtered to `Cargo.toml`, `Cargo.lock`,
`rust-toolchain.toml`, `.cargo/`, `crates/` and `xtask/`. Nothing else can reach a container,
including a local `config.toml` holding a bot token, and an edit anywhere else does not invalidate
the build cache.

### Caching

Dagger caches every step by its inputs and skips a step whose inputs are unchanged. Re-running
`test` on an unchanged tree returns the previous output without running cargo.

Steps that do run reuse three kinds of cache volume:

- `CARGO_HOME`: registry index, crate sources and git checkouts, shared by every build. It is
  one volume rather than separate `registry` and `git` mounts because cargo's lock file for those
  directories lives in `CARGO_HOME` itself. Mounting only the subdirectories gives each container
  its own lock, and two concurrent builds then corrupt the shared checkout. That is the failure the
  Dockerfile's `contract-builder` stage avoids with separate cache ids.
- One target directory per toolchain, purpose (test, release, contract) and architecture, mounted
  at `/app/target`. Debug builds have no debug info (`CARGO_PROFILE_DEV_DEBUG=0`), which is most
  of a debug target directory's size; a failing test still reports its file and line.
- `/var/lib/docker` of the test sidecar, so the PostgreSQL image is pulled once.

Dependencies are compiled first, from a `cargo chef` recipe. The recipe depends only on the
manifests and the lockfile, so after a source-only change Dagger skips that step and only the
workspace crates compile.

### Deviations from the Dockerfile

- **Labels come from the generator at build time.** The Dockerfile pastes the generator's output
  into `LABEL` lines and CI checks that the paste is still accurate. The module applies the
  labels from the same generator run that produces `contract.json`, so the two cannot disagree.
- **`test` runs on `rust:<channel>-trixie`** (glibc), like the `Test` job on Ubuntu. The release
  binary and the contract generator are built on `rust:<channel>-alpine` (musl), so an image
  build pulls one toolchain image.
- **The toolchain comes from `rust-toolchain.toml`.** The builder image tag is read from the file,
  so a toolchain bump moves the pipeline without a second edit.
- **No Sentry debug-file upload.** The Dockerfile's opt-in upload is release-only; adding it here
  is a `Secret` argument on `build`.
- **The PostgreSQL test resolves the daemon's host** (`get_host()`) instead of assuming
  `127.0.0.1`. Locally and in the `Test` job the two are the same; with the daemon in a sidecar,
  only the former reaches the published port.

## Running it locally

Requirements: Docker (or another container runtime the Dagger CLI supports) and the Dagger CLI at
the version in `dagger.json`. Node is not needed; the engine runs the TypeScript itself.

1. Install the CLI at the pinned version:

   ```bash
   curl -fsSL https://dl.dagger.io/dagger/install.sh | DAGGER_VERSION=0.21.10 BIN_DIR="$HOME/.local/bin" sh
   dagger version
   ```

2. Generate the SDK bindings, for editor completion and type checking. The engine regenerates them
   on every load, so the generated `dagger/sdk/` is ignored by git:

   ```bash
   dagger develop
   ```

   The module already exists, so `dagger init` is not needed. A new module of the same shape is
   created with `dagger init --sdk=typescript --source=dagger --name=discord-alertmanager`; it
   writes the same `dagger.json` and a placeholder `dagger/src/index.ts`. If `dagger develop`
   adds fields to `dagger/package.json` or `dagger/tsconfig.json`, commit them.

3. List the functions and their arguments:

   ```bash
   dagger functions
   dagger call docker --help
   ```

4. Run the pipeline:

   ```bash
   dagger call test
   dagger call smoke-test
   dagger call docker export --path=./image.tar && docker load -i image.tar
   dagger call contract export --path=./contract-out
   ```

5. Push a throwaway image to check publishing without credentials:

   ```bash
   dagger call publish --address=ttl.sh/discord-alertmanager-$(uuidgen | tr A-Z a-z):1h
   ```

   With credentials, pass `--username=… --password=env:REGISTRY_TOKEN`. `--platforms` takes a
   comma-separated list; a platform other than the engine's own is built under emulation, which
   for a Rust release build is very slow. The release workflow builds each architecture on a
   native runner for that reason, and would keep doing so with Dagger.

Run the same command twice to see the cache: the second run of an unchanged tree finishes in
seconds. `dagger call -i …` opens a terminal in the failing container when a step fails.

## How CI persists the cache

Every run starts a fresh engine, and only cargo's caches cross from one run to the next:

1. `actions/cache` restores the newest archive for this architecture, lockfile and toolchain, or
   else the newest for the architecture.
2. `dagger call cache-restore` unpacks it into the new engine's cargo volumes.
3. `dagger call docker … export-image` builds the image and loads it into the runner's Docker
   daemon, where the existing checks (labels, contract, start-up, Trivy, size) read it.
4. When nothing for this lockfile and toolchain was restored, `dagger call cache-archive` packs
   the volumes again and the archive is saved under a new key, unless it exceeds
   `CACHE_ARCHIVE_LIMIT_BYTES`.

The archive holds `CARGO_HOME` without unpacked crate sources or git working copies (cargo
recreates both from the downloads it keeps) and the release and contract target directories. It
leaves out base images and Dagger's own step cache: images are pulled again in seconds, and the
whole engine state was 13 GiB. Dagger's step skipping therefore works within a run but not across
runs; cargo's reuse of compiled dependencies does.

Saving only when the lockfile or toolchain changed keeps it to one entry per dependency change,
architecture and scope. A pull request's entry is readable only by that pull request, so `main`
never restores what a branch built.

## Pinned versions

Renovate maintains every pin through the custom managers in `renovate.json`:

| Pin | Where | How Renovate finds it |
| --- | --- | --- |
| Dagger CLI and engine | `DAGGER_VERSION` in `dagger-compare.yml`, `engineVersion` in `dagger.json` | `renovate:` comment, and a manager for `dagger.json`; grouped into one pull request |
| Base images (`alpine`, `docker:…-dind`) | `dagger/src/config.ts` | Any `"name:tag@sha256:digest"` string in the module |
| `cargo-chef` release binary (per architecture, with SHA-256) | `dagger/src/config.ts` | `renovate:` comment; Renovate bumps the tag and recomputes both digests |
| Rust builder images | derived from `rust-toolchain.toml` | Renovate already updates the toolchain file |

A new image pin must be written `name:tag@sha256:digest`. Without the digest no manager matches
it, and it would never be updated.

## Before moving releases

- `release-please.yaml` builds the published image from the `Dockerfile`, so for now the image CI
  checks and the image that ships are built by two definitions. Both are checked against the same
  contract and run the same start-up test, but a change to one must be mirrored in the other
  until releases move.
- The Sentry debug-file upload exists only in the Dockerfile. It would be a `Secret` argument on
  `build`.
- Signing, the contract referrer and the multi-platform index merge stay in `release-please.yaml`
  either way. `publish` returns the digest they need.
- For a cache that does not ride through `actions/cache`, the alternatives are a self-hosted
  runner with a long-lived engine (no archive at all, and Dagger skips unchanged steps across
  runs too) or Dagger Cloud's managed cache.
