# Slash commands

Every slash command the bot registers, and the capability each one needs.

## Authorisation

Each command checks the caller's roles against `discord.capabilities`, and every denial is written
to the audit log. Discord's own command permissions only decide what the client shows.

There are four capabilities, ordered from least to most: `view`, `operate`, `silence` and `admin`.
`view` defaults to `@everyone` and the other three to nobody, so a fresh deployment can read its
alerts and cannot silence a page.

## Commands

Where a row lists two capabilities, the subcommands before the slash need the first and those
after it need the second.

| Command | Subcommands | Capability |
| --- | --- | --- |
| `/alerts` | `list`, `show` / `ack`, `unack`, `assign` | view / operate |
| `/ignore` | `list` / `add`, `remove` | view / operate |
| `/silence` | `list` / `create`, `extend`, `expire` | view / silence |
| `/subscribe` | `add`, `list`, `remove` | view |
| `/status` | `alertmanager`, `bot` / `config` | view / admin |
| `/route` | `add`, `list`, `remove`, `test` | admin |
| `/cards` | `resync` | admin |
| `/debug` | `merge` | view |

`/ignore` and `/silence` differ in reach. An ignore is a row in this bot's database and stops only
Discord. A silence is written to Alertmanager and stops every receiver, the pager included, which
is why it has a capability of its own.
