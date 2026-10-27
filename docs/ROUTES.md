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
title_template = "{{ labels.alertname }} — {{ labels.namespace }}"
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

Each firing alert becomes its own post, titled from `title_template` and tagged with its severity,
its state and the value of its `namespace` label.

`manage_tags = true` lets the bot create `unclassified`, a state tag, or a `namespace` value it has
not seen before, the first time a post needs one. That is why this route also needs
`Manage Channels`, listed under [Discord permissions](../README.md#discord-permissions). With
`manage_tags = false` the bot applies only tags that already exist.

The policy keys left out keep their defaults. An unacknowledged post is pinned once its alert is
critical, it is archived as soon as the alert resolves, and it is never locked, so a flap can
reopen it without `Manage Threads`.

## Forum policy keys

These keys sit under `[routes.target.policy]`. The generated [configuration reference](config.md)
lists `routes` as one value, so this table is where the forum keys are described.

| Key | Default | Effect |
| --- | --- | --- |
| `title_template` | `{{ labels.alertname }}` | The post title, rendered with `minijinja`. An empty result falls back to the alert name, then to the fingerprint. |
| `manage_tags` | `false` | Whether missing tags are created. Needs `Manage Channels`. |
| `state_tags.firing`, `.acked`, `.silenced`, `.resolved` | the key's own name | Names of the four state tags. Exactly one is applied at a time. |
| `severity_tags` | `true` | Whether each post gets a tag named after the alert's severity. |
| `label_tags` | `[]` | Labels whose values become tags, at most three. Use low-cardinality labels: a channel holds 20 tags. |
| `default_tag` | empty | The tag applied when nothing else resolves, for a channel that requires one. |
| `auto_archive_minutes` | unset | Inactivity before a resolved post archives: 60, 1440, 4320 or 10080. Unset takes `render.thread_archive_after_minutes`. |
| `archive_on_resolve` | `true` | Whether a post is archived when its alert resolves. |
| `lock_on_resolve` | `false` | Whether a post is locked when its alert resolves. Reopening a locked post needs `Manage Threads`. |
| `pin_min_severity` | `critical` | The lowest severity that pins an unacknowledged post. The pin is dropped on acknowledgement or resolution. |
| `max_pinned` | `5` | The most posts pinned at once. |
| `bump_on_state_change` | `true` | Whether each state change posts a one-line note, which moves the post up the forum's activity sort. |
