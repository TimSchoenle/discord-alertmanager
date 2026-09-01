# Security policy

## Reporting a vulnerability

Do not open a public issue. Use GitHub's private vulnerability reporting on this repository, under
the Security tab, which opens an advisory only the maintainers can read:

<https://github.com/TimSchoenle/discord-alertmanager/security/advisories/new>

Include what you did, what happened, and which features the binary was built with. `--features
sqlite` and `--features postgres` select different backends with different claim strategies, so a
report against one is not a report against the other.

## Supported versions

The project is pre-1.0 and there is no maintenance branch to backport to. A fix lands on `main`
and goes out in the next tag. Only the newest tag is supported.

## What a report is about

This service holds a Discord bot token, an Alertmanager credential, a database URL and a webhook
bearer token, and it acts on Alertmanager on behalf of Discord users. Two classes of failure
matter most.

**A credential reaching somewhere it was not meant to go.** Every secret in the configuration is a
`secrecy::SecretString`, no error in the tree prints one, and `terrace-config` redacts its own.
A log line, a `Debug` output, a panic message or a Discord message carrying one is a defect.

**A user doing something their capability does not permit.** Authorisation happens bot-side in
`CommandCtx` before any handler body runs; `default_member_permissions` only decides what Discord
draws. The four capabilities are ordered — `view`, `operate`, `silence`, `admin` — and `silence`
is the one that reaches past Discord, because an Alertmanager silence stops every receiver
including the pager. A path that mutates Alertmanager without a `silence` check is the highest
severity report this repository can receive.

Also in scope: a webhook accepted without its bearer token, a request that bypasses the body
limit, and any bot-local ignore that suppresses an alert without writing an audit row.

Panics on malformed Alertmanager input are ordinary issues rather than advisories. The listener
rejects a body it cannot parse and the process survives it, so a reproducing payload is more
useful attached to an issue than held in an embargo.
