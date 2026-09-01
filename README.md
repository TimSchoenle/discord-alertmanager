<!--
Generated from .github/templates/README.md.hbs. Edit that file, not this one.

CI renders it on every pull request and commits the result back to the branch. A push to
`main` whose README.md does not match its template fails the `readme` job in
.github/workflows/docs.yml, which is a required check.

The payload is collected by TimSchoenle/actions/actions/common/readme-variables, which reads
crates/discord-alertmanager/Cargo.toml and walks docs/, merged over the output of one command:

    bash .github/scripts/readme-variables.sh

Every fact this page states about itself comes from that payload: the version, the MSRV, the
licence, the crate map, the configuration tables and the count of keys behind them. The release
pull request is therefore the commit that corrects them.

Nothing in this comment may contain a mustache that is not a real reference.
-->

# discord-alertmanager

A Discord operator surface for Prometheus Alertmanager.

[![Release](https://img.shields.io/github/v/release/TimSchoenle/discord-alertmanager?sort=semver)](https://github.com/TimSchoenle/discord-alertmanager/releases)
[![Licence](https://img.shields.io/github/license/TimSchoenle/discord-alertmanager)](LICENSE)
[![MSRV](https://img.shields.io/badge/MSRV-1.94-blue)](Cargo.toml)

## What this is

Alertmanager posts an alert to a webhook. This service turns it into a live status card in a
Discord channel and lets an operator acknowledge it, mute it, silence it or open its graph without
leaving the client. A silence goes to Alertmanager and stops every receiver, the pager included.
An ignore is bot-local and stops only Discord, which is what "stop pinging #ops at 3am but keep
paging" actually asks for.

It has not been released. Every part of it is written and covered by tests — the listener, the
Alertmanager client, the decision pipeline, both storage backends, the gateway and the commands —
and it has not yet run anywhere long enough for that to mean what a version number would.

## Quick start

Two things need supplying before it does anything: a bot token and somewhere to send its cards.
Everything else has a default.

```bash
git clone https://github.com/TimSchoenle/discord-alertmanager
cd discord-alertmanager

# Every key the service reads, rendered from the Rust types that load it.
cargo run -p discord-alertmanager-config --features config-schema \
    --example config-schema -- --format markdown
```

The same generator writes `docs/config.md`, `docs/config.json` and `config.example.toml`.
`cargo xtask config-docs` runs all three and is what CI compares against.

## Table of contents

- [Features](#features)
- [Installation](#installation)
- [Usage](#usage)
- [Configuration](#configuration)
- [Operations](#operations)
- [Compatibility](#compatibility)
- [Documentation](#documentation)
- [Contributing](#contributing)
- [Security](#security)
- [Licence](#licence)

## Features

- **Ignore and silence are different buttons.** A silence is written to Alertmanager, gossips
  across its cluster and survives this bot being down. An ignore is a row in this bot's database
  and suppresses nothing but Discord. Every command and label says which one it is, because an
  operator who confuses them either wakes someone up or fails to.
- The Alertmanager client takes a list of peers and tries them in order, so a high-availability
  set has no primary to fail over from. A request that fails retries on a bounded backoff and then
  gives up, which is what keeps a struggling Alertmanager from being hammered by its own client.
- A webhook can be lost to a restart, a partition, or a receiver that never sent `send_resolved`.
  A reconciler polls the alert set on a fixed cadence and compares it, so the push path is not the
  only way an alert state reaches the bot. The same poll feeds what `/readyz` reports.
- Every outbound Discord action is a row before it is a request. A dispatcher claims a batch under
  a lease, and the claim is the one query the two backends implement differently:
  `FOR UPDATE SKIP LOCKED` on PostgreSQL, `BEGIN IMMEDIATE` on SQLite.
- One `Store` trait, one conformance suite, and both backends run it. A behaviour that holds on
  SQLite and not on PostgreSQL is a failing test rather than a production surprise. Which backend
  a process opens is a configuration key, not a build flag, so one binary answers both.
- A route past its alert threshold stops posting one card per alert and rolls one card per window
  instead, saying on the card why it did. Discord's per-channel limits are strict enough that an
  unthrottled storm produces rate-limit responses rather than notifications, and a worse card in a
  readable channel beats a better one nobody receives.
- An alert that resolves and fires again minutes later reuses its card and counts the flap. One
  that comes back a week later gets a new card carrying a link to the old one, because reviving a
  card that scrolled away days ago tells nobody anything.
- A route can escalate. A card that stays firing and unacknowledged past its deadline mentions the
  people the route names, once. The failure this exists for is the quiet one: the message arrived,
  it scrolled past, and the channel is silent precisely because everybody assumes somebody else
  took it.
- A deadman watches the bot itself. When no webhook has arrived inside its window *and*
  Alertmanager cannot be reached, it says so in the administrative channel, as does a route that
  has stopped delivering because the bot lacks a permission in it.
- The configuration tables below are generated, not written. They come out of the Rust types that
  load the configuration, as do `config.example.toml`, the JSON Schema and the contract document
  the image carries, so renaming a key corrects all five in the commit that renames it.
- A key can arrive from a TOML file, a `DAM_`-prefixed variable, a file in a mounted secrets
  directory, or a `_FILE` variable naming a path. The last two exist because a Kubernetes `Secret`
  arrives as a directory of files, and a key supplied by two of them fails the load instead of
  picking one.

## Installation

Each release publishes one multi-platform image, `linux/amd64` and `linux/arm64`, to two
registries. Both receive the same manifests from the same build, so a digest read from one
resolves in the other.

```bash
docker pull timschoenle/discord-alertmanager:v0.3.0
docker pull ghcr.io/timschoenle/discord-alertmanager:v0.3.0
```

Pin the digest in production. Every published index is signed with cosign against a GitHub OIDC
identity, and the configuration contract is attached to that digest as an OCI referrer and signed
alongside it, so an image can be checked without trusting the tag it arrived under:

```bash
cosign verify ghcr.io/timschoenle/discord-alertmanager:v0.3.0 \
    --certificate-identity-regexp '^https://github.com/TimSchoenle/discord-alertmanager/\.github/workflows/' \
    --certificate-oidc-issuer https://token.actions.githubusercontent.com
```

There is no chart yet.

From source:

```bash
git clone https://github.com/TimSchoenle/discord-alertmanager
cd discord-alertmanager
git checkout v0.3.0
cargo build --release
```

Both backends are in every build, and `storage.backend` decides which one a process opens. The
pool is opened at boot rather than at the first query: a container that accepts webhooks and drops
them is worse than one that will not start.

## Usage

Configuration is read at startup from the layers described under
[Configuration](#configuration). With none of it supplied, the process still loads, installs
logging and metrics, and then stops on the first thing it cannot do.

```bash
DAM_LOG_LEVEL=info \
DAM_ALERTMANAGER__ENDPOINTS='["http://alertmanager:9093"]' \
    cargo run -p discord-alertmanager
```

Point Alertmanager at the listener with an ordinary `webhook_config` receiver:

```yaml
receivers:
  - name: discord
    webhook_configs:
      - url: http://discord-alertmanager:9099/webhook
        send_resolved: true
        http_config:
          authorization:
            type: Bearer
            credentials_file: /etc/alertmanager/discord-token
```

### The workspace

Ten crates, each written against a trait rather than against its neighbour. The import name
differs from the package name throughout, so both are listed.

| Crate | Import | Purpose |
| --- | --- | --- |
| [`discord-alertmanager-am`](crates/discord-alertmanager-am) | `dam_am` | Alertmanager API v2 client and the v4 webhook payload types. |
| [`discord-alertmanager-config`](crates/discord-alertmanager-config) | `dam_config` | The configuration surface: terrace-config layers, Describe derives, generated reference. |
| [`discord-alertmanager-core`](crates/discord-alertmanager-core) | `dam_core` | Domain types, matcher semantics and the notification state machine. No I/O. |
| [`discord-alertmanager-discord`](crates/discord-alertmanager-discord) | `dam_discord` | The serenity layer: command registry, component handlers, card renderer, DiscordSink. |
| [`discord-alertmanager-engine`](crates/discord-alertmanager-engine) | `dam_engine` | The decision pipeline: ingest, route, decide, outbox. Owns the two outbound ports. |
| [`discord-alertmanager-ingest`](crates/discord-alertmanager-ingest) | `dam_ingest` | The axum listener: the Alertmanager webhook, the health probes, and /metrics. |
| [`discord-alertmanager-store-postgres`](crates/discord-alertmanager-store-postgres) | `dam_store_postgres` | PostgreSQL backend: checked queries, migrations, and the FOR UPDATE SKIP LOCKED claim. |
| [`discord-alertmanager-store-sqlite`](crates/discord-alertmanager-store-sqlite) | `dam_store_sqlite` | SQLite backend: checked queries, migrations, and the BEGIN IMMEDIATE claim. |
| [`discord-alertmanager-store`](crates/discord-alertmanager-store) | `dam_store` | The Store trait, its row types, and the conformance suite both backends run. |
| [`discord-alertmanager`](crates/discord-alertmanager) | — | A Discord operator surface for Prometheus Alertmanager. |

`xtask` sits beside them and is build tooling: it regenerates the configuration reference and the
`sqlx` offline caches. `cargo xtask` is aliased in `.cargo/config.toml`.

## Configuration

Four variables decide where configuration comes from. They are read straight from the environment,
before there is a configuration to describe them, so no file can supply one.

| Variable | Role | Default | Purpose |
| --- | --- | --- | --- |
| `DAM_CONFIG` | config | `config.toml` | Names the TOML layer: a file, or a directory whose `*.toml` files are all merged in name order. |
| `DAM_SECRETS_DIR` | secrets dir | — | Names a directory of key-named files — a mounted Kubernetes `Secret` volume. Each file supplies the key its name spells. |
| `DAM_LOG_FORMAT` | reserved | — | Read directly from the environment before the layered config exists, so no file may supply it. |
| `DAM_LOG_LEVEL` | reserved | — | Read directly from the environment before the layered config exists, so no file may supply it. |

Behind them are 63 keys. Each is spelled the same way in every layer: `__`
separates nesting levels and case is folded, so `discord.token` is `DAM_DISCORD__TOKEN` as a
variable and `discord__token` as a file name in the secrets directory.

These are the ones with no default, which the process will not start without:

| TOML | Type | Environment | Default | Flags | Purpose |
| --- | --- | --- | --- | --- | --- |
| `discord.token` | `SecretString` | `DAM_DISCORD__TOKEN` | unset | secret | Bot token. Supply it through `DAM_DISCORD__TOKEN_FILE` or the secrets directory. |
| `alertmanager.endpoints` | `Vec<Url>` | `DAM_ALERTMANAGER__ENDPOINTS` | `[]` | — | Base URLs of the Alertmanager peers, tried in order. |
| `storage.backend` | `Backend`: `sqlite` \| `postgres` | `DAM_STORAGE__BACKEND` | `sqlite` | — | Which backend the bot connects to. |
| `ingest.bind` | `SocketAddr` | `DAM_INGEST__BIND` | `0.0.0.0:9099` | — | Address and port to listen on. |
| `ingest.webhook_token` | `SecretString` | `DAM_INGEST__WEBHOOK_TOKEN` | unset | secret | Bearer token every webhook request has to carry. |

[docs/config.md](docs/config.md) has all 63 of them with their defaults,
[docs/config.json](docs/config.json) is the JSON Schema, and
[config.example.toml](config.example.toml) is the same surface as a commented file. All three are
written by `cargo xtask config-docs`.

A key marked `secret` should not go in the TOML file, which is usually committed. Give it a
`_FILE` variable or a file in the secrets directory.

## Operations

The listener serves three paths besides the webhook. `/healthz` answers as soon as the process is
up. `/readyz` answers only when the bot can do its job, which includes how recently Alertmanager
was heard from, so a bot that has silently stopped receiving stops claiming to be ready. `/metrics`
serves the Prometheus registry and is switched off with a single key.

Logging is installed before the configuration is read, from `DAM_LOG_LEVEL` and `DAM_LOG_FORMAT`,
because a configuration error is the failure most worth seeing and it happens before there is a
configuration to describe how to report it. `DAM_LOG_FORMAT=json` switches the subscriber to JSON
for a log aggregator.

Five things run on their own clocks beside the listener: the reconciler, the silence sync, the
lease janitor, the escalation sweep and the retention pruner. Each has its own interval key, a
failed pass is logged rather than retried early, and the next tick is the retry.

`observability.admin_channel_id` is where the bot reports on itself. Leaving it unset is
supported and silences both notices, which is the right choice only where something else is
watching the process.

`SIGTERM` and `SIGINT` both cancel the shutdown token. In-flight requests are given a bounded
drain before the listener stops, and every dispatcher finishes the item it holds rather than
leaving a claimed row for its lease to release.

## Compatibility

| | Supported |
| --- | --- |
| Rust | 1.94 or newer, edition 2024 |
| Toolchain | 1.97.1, pinned in [rust-toolchain.toml](rust-toolchain.toml) |
| Alertmanager | API v2, which is the only one Alertmanager 0.27 and newer serves |
| Storage | PostgreSQL and SQLite, one conformance suite between them |

The pinned toolchain is about reproducibility and is not the floor. 1.94 is the
floor, and it comes from `terrace-config`, which declares the highest MSRV in the tree.

## Documentation

| Document | Purpose |
| --- | --- |
| [docs/config.contract.json](docs/config.contract.json) | — |
| [docs/config.json](docs/config.json) | — |
| [Configuration reference](docs/config.md) | Every key the service reads, and the variables that decide where those keys come from. |

## Contributing

[CONTRIBUTING.md](CONTRIBUTING.md) covers the commit convention, the four generated artefacts and
the checks to run before opening a pull request.

Several files here are generated, this one included. Each says so in its first lines, and editing
the output instead of its source is reverted by the next render.

## Security

Do not open a public issue for a vulnerability. [SECURITY.md](SECURITY.md) has the reporting
instructions and says which failures matter most: a credential reaching a log, and a user reaching
past the capability they hold.

## Licence

`MIT`. [LICENSE](LICENSE) has the terms.
