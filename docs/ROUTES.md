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
title_template = "{{ labels.alertname }} in {{ labels.namespace }}"
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

Each firing alert becomes its own post, titled from `title_template` and tagged with its state,
its severity and the value of its `namespace` label. A title template that cannot render, such as
one naming a label the alert lacks, falls back to `[SEVERITY] alertname` and the first non-empty
value among `render.key_labels`.

`manage_tags = true` lets the bot create the tags the forum lacks: the state tags, the severity
tags and `default_tag` (here `unclassified`) when the gateway connects, and a `namespace` value's
tag the first time a post carries it. A label tag is named after the value, cut to 20 characters,
with whitespace replaced by hyphens. That is why this route also needs `Manage Channels`, listed
under [Discord permissions](../README.md#discord-permissions). With `manage_tags = false`, or
without the permission, the bot applies only tags that already exist. A label tag it fails to
create is not asked for again for ten minutes, so a forum it cannot edit costs a few calls an
hour rather than one per post.

A post whose tags all fail to resolve gets `default_tag`, but only when the forum already has a
tag by that name; otherwise it carries none. A forum set to require a tag rejects a post that
carries none, so in such a forum make sure `default_tag` or the state tags exist before the route
posts. The bot creates no tag once a forum already holds 18 or more of Discord's 20. Below 18 it
adds every missing tag in one request, up to 20, so the forum can still end up full. Once the bot
stops creating tags, `Manage Channels` changes nothing: create the state tags and `default_tag` by
hand before the gateway connects.

The policy keys left out keep their defaults. An unacknowledged critical post is pinned, whether
it is created critical or becomes so later, it is archived as soon as the alert resolves, and it
is never locked, so a flap can reopen it without `Manage Threads`. Discord allows one pinned post
per forum, so while one critical post holds the pin, the next one stays unpinned and the bot asks
again at its next state change.

## Forum policy keys

These keys sit under `[routes.target.policy]`. The generated [configuration reference](config.md)
lists `routes` as one value, so this table is where the forum keys are described.

| Key | Default | Effect |
| --- | --- | --- |
| `title_template` | `{{ labels.alertname }}` | The post title, rendered with `minijinja` when the post is created. A template that fails or renders empty falls back to the thread name. |
| `manage_tags` | `false` | Whether missing tags are created: the state, severity and default tags when the gateway connects, and label tags as values appear. Needs `Manage Channels`. |
| `state_tags.firing`, `.acked`, `.silenced`, `.resolved` | the key's own name | Names of the four state tags. Exactly one is applied at a time. |
| `severity_tags` | `true` | Whether each post gets a tag named after the alert's severity. |
| `label_tags` | `[]` | Labels whose values become tags, at most three. Without `manage_tags`, a value is applied only when its tag exists. Use low-cardinality labels: a channel holds 20 tags. |
| `default_tag` | empty | The tag applied when nothing else resolves, for a channel that requires one. |
| `auto_archive_minutes` | unset | Inactivity before a resolved post archives: 60, 1440, 4320 or 10080. Unset takes `render.thread_archive_after_minutes`. A post whose alert is still open, including a new one, holds 10080, and drops to this window when the alert resolves, whether or not `archive_on_resolve` archives it. |
| `archive_on_resolve` | `true` | Whether a post is archived when its alert resolves. |
| `lock_on_resolve` | `false` | Whether a post is locked when its alert resolves. Applies only with `archive_on_resolve`. Reopening a locked post needs `Manage Threads`. |
| `pin_min_severity` | `critical` | The lowest severity that pins an unacknowledged post. The pin is dropped on acknowledgement or resolution. |
| `max_pinned` | `1` | The most of the bot's posts pinned at once in the channel. `0` turns pinning off. Discord allows one pinned post per forum, so higher values change nothing. |
| `bump_on_state_change` | `true` | Whether each state change posts a one-line note, which moves the post up the forum's activity sort. |
