# Dagger pipeline (proof of concept)

A Dagger module that runs this repository's test suite and builds its container image as typed
functions, the same way on a laptop and on a GitHub runner. `.github/workflows/ci.yml` runs it
next to `build.yaml` on every pull request and push to `main`, so the two can be compared run for
run. It gates nothing and publishes nothing.

## Layout

| Path | Purpose |
| --- | --- |
| `dagger.json` | Module manifest at the repository root: name, engine version, SDK, source directory. |
| `dagger/src/index.ts` | The `DiscordAlertmanager` object and its functions. |
| `dagger/src/config.ts` | Pinned versions, image layout, and pure parsing helpers. |
| `dagger/src/platform.ts` | Per-architecture build parameters (musl triple, UPX). |
| `.github/workflows/ci.yml` | The CI job, including the engine-state cache. |
| `.github/scripts/dagger-engine.sh` | Starts, stops, archives and restores the engine; writes the timing report. |

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
- One target directory per toolchain, libc and architecture, mounted at `/app/target`.
- `/var/lib/docker` of the test sidecar, so the PostgreSQL image is pulled once.

Dependencies are compiled first, from a `cargo chef` recipe. The recipe depends only on the
manifests and the lockfile, so after a source-only change Dagger skips that step and only the
workspace crates compile.

### Deviations from the Dockerfile

- **Labels come from the generator at build time.** The Dockerfile pastes the generator's output
  into `LABEL` lines and CI checks that the paste is still accurate. The module applies the
  labels from the same generator run that produces `contract.json`, so the two cannot disagree.
- **Tests run on `rust:<channel>-trixie`** (glibc), like the `Test` job on Ubuntu. The release
  binary is built on `rust:<channel>-alpine` (musl), like the Dockerfile.
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

A hosted runner starts with an empty engine, so `ci.yml` runs the engine itself and moves its
state directory through `actions/cache`:

1. Restore the newest archive for this lockfile and toolchain, or else the newest at all.
2. Extract it with `sudo tar` into a host directory.
3. Start `registry.dagger.io/engine` with that directory bind-mounted at `/var/lib/dagger`, and
   point the CLI at it with `_EXPERIMENTAL_DAGGER_RUNNER_HOST`.
4. Run `test`, `docker` (exported as a tarball) and `smoke-test`.
5. Prune the engine cache to `DAGGER_GC_MAX_USED_SPACE`, stop the engine gracefully, and archive
   the directory with zstd.
6. Save the archive under a new key, unless it exceeds `DAGGER_ARCHIVE_LIMIT_BYTES`.

`sudo tar` instead of letting `actions/cache` archive the directory: the state is root-owned and
holds overlayfs whiteouts (device nodes) and `trusted.*` xattrs, none of which the runner user can
read. Archiving a running engine would capture its metadata databases mid-write, hence the stop.

Pushes to `main` and same-repository pull requests save. A pull request's entry is readable only
by that pull request, so `main` never restores state a branch wrote. The job summary lists each
phase's duration and the archive and state sizes.

## What to watch

- **The archive cost.** Download, extract, archive and upload are separate rows in the job summary.
  The two target directories dominate the state. If those four rows together approach what a cold
  `cargo build` costs, the cache is not paying for itself on hosted runners.
- **Cache eviction.** Every run that saves adds one archive to the repository's 10 GB quota, which
  `build.yaml`'s buildx cache shares. GitHub evicts least recently used entries first, so frequent
  pushes can push the buildx cache out. `DAGGER_ARCHIVE_LIMIT_BYTES` caps one entry; it does not
  cap how many there are. The repository's cache usage page shows the total.

## Before adopting

- Renovate does not see the pins in `dagger/src/config.ts`, `dagger.json` and `ci.yml`. A regex
  custom manager would cover them.
- Signing, the contract referrer and the multi-platform index merge stay in `release-please.yaml`.
  `publish` returns the digest they need.
- For a cache that does not ride through `actions/cache`, the alternatives are a self-hosted
  runner with a long-lived engine (no archive at all) or Dagger Cloud's managed cache.
