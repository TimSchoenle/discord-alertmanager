# Forum routes

How a route posts each alert as its own post in a Discord forum channel, with a complete example.

## What a forum route needs

A forum route sets `target.kind = "forum"`, sets `target.id` to the forum channel, and adds
`target.policy` for the tag and lifecycle behaviour that only a forum has. The matcher, the
mentions and the escalation work as they do for a text channel.

## Example

```toml
[[routes]]
name = "prod-forum"
guild_id = 111111111111111111
matchers = "namespace=~prod-.*"
min_severity = "warning"
priority = 10

[routes.target]
kind = "forum"
id = 222222222222222222

[routes.target.policy]
manage_tags = true
label_tags = ["namespace"]
default_tag = "unclassified"

[routes.mentions]
roles = [333333333333333333]
min_severity = "critical"

[routes.escalation]
after_secs = 900
roles = [333333333333333333]
```

## What it does

Each firing alert becomes its own post, tagged with its state, its severity and the value of its
`namespace` label. The post is titled `[SEVERITY] alertname`, followed by the first non-empty
value among `render.key_labels`.

When the gateway connects, the bot reads each forum route's tags and, if it holds
`Manage Channels`, creates the state tags, the severity tags and `default_tag` (here
`unclassified`) that the forum lacks. That is why this route also needs `Manage Channels`, listed
under [Discord permissions](../README.md#discord-permissions). The bot never creates a label tag:
a `namespace` value is applied only when a tag named after it already exists, so create one for
each namespace you expect. The tag name is the value cut to 20 characters, with whitespace replaced
by hyphens.

A post whose tags all fail to resolve gets `default_tag`. A forum set to require a tag rejects a
post that carries none, so in such a forum make sure `default_tag` or the state tags exist before
the route posts. The bot creates no tag in a forum that already holds 18, two short of Discord's
20, so that a person can still add one. In a forum that full, `Manage Channels` changes nothing:
create the state tags and `default_tag` by hand before the gateway connects.

The policy keys left out keep their defaults. An unacknowledged post is pinned once its alert is
critical, it is archived as soon as the alert resolves, and it is never locked, so a flap can
reopen it without `Manage Threads`. Discord allows one pinned post per forum, so while one critical
post holds the pin, the next one stays unpinned and the bot asks again at its next state change.

## Forum policy keys

These keys sit under `[routes.target.policy]`. The generated [configuration reference](config.md)
lists `routes` as one value, so this table is where the forum keys are described.

| Key | Default | Effect |
| --- | --- | --- |
| `title_template` | `{{ labels.alertname }}` | Stored, and `/route test` reports an empty one, but not applied yet. Post titles come from the alert as described above. |
| `manage_tags` | `false` | Stored but not checked yet. The missing state, severity and default tags are created whenever the bot holds `Manage Channels`. |
| `state_tags.firing`, `.acked`, `.silenced`, `.resolved` | the key's own name | Names of the four state tags. Exactly one is applied at a time. |
| `severity_tags` | `true` | Whether each post gets a tag named after the alert's severity. |
| `label_tags` | `[]` | Labels whose values become tags, at most three. A value is applied only when a tag with that name exists, and the bot does not create one. Use low-cardinality labels: a channel holds 20 tags. |
| `default_tag` | empty | The tag applied when nothing else resolves, for a channel that requires one. |
| `auto_archive_minutes` | unset | Inactivity before a resolved post archives: 60, 1440, 4320 or 10080. Unset takes `render.thread_archive_after_minutes`. |
| `archive_on_resolve` | `true` | Whether a post is archived when its alert resolves. |
| `lock_on_resolve` | `false` | Whether a post is locked when its alert resolves. Reopening a locked post needs `Manage Threads`. |
| `pin_min_severity` | `critical` | The lowest severity that pins an unacknowledged post. The pin is dropped on acknowledgement or resolution. |
| `max_pinned` | `5` | Stored but not enforced. Discord allows one pinned post per forum, which is the effective limit. |
| `bump_on_state_change` | `true` | Whether each state change posts a one-line note, which moves the post up the forum's activity sort. |
