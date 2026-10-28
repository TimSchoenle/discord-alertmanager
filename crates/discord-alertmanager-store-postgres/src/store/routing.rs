//! Routes, ignore rules, subscriptions, and the forum tags a route can apply.

use crate::PostgresStore;
use crate::convert::{
    ForumTagRow, IgnoreRow, RouteRow, SubscriptionRow, backend, decode_all, json,
};
use chrono::{DateTime, Utc};
use dam_store::{
    ChannelId, ForumTag, GuildId, IgnoreId, IgnoreRule, Route, RouteId, RouteSource, StoreError,
    Subscription, SubscriptionId, UserId,
};

pub(super) async fn upsert_ignore(
    store: &PostgresStore,
    rule: &IgnoreRule,
) -> Result<IgnoreId, StoreError> {
    let mut tx = store.write().await?;

    let id = if rule.id.get() == 0 {
        let id = sqlx::query_scalar!(
            "INSERT INTO ignore_rules (scope, guild_id, channel_id, matcher_source, reason, \
             created_by, created_at, expires_at, revoked_at) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9) RETURNING id",
            rule.scope.as_str(),
            rule.guild_id.to_db(),
            rule.channel_id.map(ChannelId::to_db),
            rule.matcher_source,
            rule.reason,
            rule.created_by.to_db(),
            rule.created_at,
            rule.expires_at,
            rule.revoked_at,
        )
        .fetch_one(&mut *tx)
        .await
        .map_err(backend)?;

        IgnoreId::new(id)
    } else {
        sqlx::query!(
            "UPDATE ignore_rules SET scope = $1, guild_id = $2, channel_id = $3, \
             matcher_source = $4, reason = $5, expires_at = $6, revoked_at = $7 WHERE id = $8",
            rule.scope.as_str(),
            rule.guild_id.to_db(),
            rule.channel_id.map(ChannelId::to_db),
            rule.matcher_source,
            rule.reason,
            rule.expires_at,
            rule.revoked_at,
            rule.id.get(),
        )
        .execute(&mut *tx)
        .await
        .map_err(backend)?;

        rule.id
    };

    tx.commit().await.map_err(backend)?;

    Ok(id)
}

pub(super) async fn revoke_ignore(
    store: &PostgresStore,
    id: IgnoreId,
    guild: GuildId,
    now: DateTime<Utc>,
) -> Result<(), StoreError> {
    let affected = sqlx::query!(
        "UPDATE ignore_rules SET revoked_at = $1 \
         WHERE id = $2 AND guild_id = $3 AND revoked_at IS NULL",
        now,
        id.get(),
        guild.to_db(),
    )
    .execute(&store.pool)
    .await
    .map_err(backend)?
    .rows_affected();

    if affected == 0 {
        return Err(StoreError::NotFound {
            kind: "ignore rule",
            key: id.to_string(),
        });
    }

    Ok(())
}

pub(super) async fn active_ignores(
    store: &PostgresStore,
    guild: GuildId,
    now: DateTime<Utc>,
) -> Result<Vec<IgnoreRule>, StoreError> {
    decode_all(
        sqlx::query_as!(
            IgnoreRow,
            "SELECT id, scope, guild_id, channel_id, matcher_source, reason, created_by, \
             created_at, expires_at, revoked_at FROM ignore_rules WHERE guild_id = $1 \
             AND revoked_at IS NULL AND (expires_at IS NULL OR expires_at > $2) ORDER BY id",
            guild.to_db(),
            now,
        )
        .fetch_all(&store.pool)
        .await
        .map_err(backend)?,
    )
}

pub(super) async fn upsert_route(
    store: &PostgresStore,
    route: &Route,
) -> Result<RouteId, StoreError> {
    let mut tx = store.write().await?;
    let min_severity = route.min_severity.map(dam_core::Severity::as_str);

    let id = if route.id.get() != 0 {
        sqlx::query!(
            "UPDATE routes SET guild_id = $1, name = $2, matcher_source = $3, min_severity = $4, \
             target = $5, group_strategy = $6, mentions = $7, escalation = $8, priority = $9, \
             continue_to_next = $10, source = $11, enabled = $12, created_by = $13 \
             WHERE id = $14",
            route.guild_id.to_db(),
            route.name,
            route.matcher_source,
            min_severity,
            json(&route.target),
            route.group_strategy.as_str(),
            json(&route.mentions),
            route.escalation.as_ref().map(json),
            route.priority,
            route.continue_to_next,
            route.source.as_str(),
            route.enabled,
            route.created_by.map(UserId::to_db),
            route.id.get(),
        )
        .execute(&mut *tx)
        .await
        .map_err(backend)?;

        route.id
    } else {
        // A route declared in the file is synchronised by name on every start, so its insert
        // has to be an update the second time. One created from Discord is not: a second
        // `/route add` under a name the guild already uses is a mistake, and the unique index
        // is what tells the operator so.
        let target = json(&route.target);
        let mentions = json(&route.mentions);
        let escalation = route.escalation.as_ref().map(json);
        let created_by = route.created_by.map(UserId::to_db);

        let id = if route.source == RouteSource::Config {
            sqlx::query_scalar!(
                "INSERT INTO routes (guild_id, name, matcher_source, min_severity, target, \
                 group_strategy, mentions, escalation, priority, continue_to_next, source, \
                 enabled, created_by, created_at) \
                 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14) \
                 ON CONFLICT (guild_id, name) DO UPDATE SET \
                 matcher_source = excluded.matcher_source, min_severity = excluded.min_severity, \
                 target = excluded.target, group_strategy = excluded.group_strategy, \
                 mentions = excluded.mentions, escalation = excluded.escalation, \
                 priority = excluded.priority, continue_to_next = excluded.continue_to_next, \
                 source = excluded.source, enabled = excluded.enabled \
                 RETURNING id",
                route.guild_id.to_db(),
                route.name,
                route.matcher_source,
                min_severity,
                target,
                route.group_strategy.as_str(),
                mentions,
                escalation,
                route.priority,
                route.continue_to_next,
                route.source.as_str(),
                route.enabled,
                created_by,
                route.created_at,
            )
            .fetch_one(&mut *tx)
            .await
        } else {
            sqlx::query_scalar!(
                "INSERT INTO routes (guild_id, name, matcher_source, min_severity, target, \
                 group_strategy, mentions, escalation, priority, continue_to_next, source, \
                 enabled, created_by, created_at) \
                 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14) \
                 RETURNING id",
                route.guild_id.to_db(),
                route.name,
                route.matcher_source,
                min_severity,
                target,
                route.group_strategy.as_str(),
                mentions,
                escalation,
                route.priority,
                route.continue_to_next,
                route.source.as_str(),
                route.enabled,
                created_by,
                route.created_at,
            )
            .fetch_one(&mut *tx)
            .await
        }
        .map_err(backend)?;

        RouteId::new(id)
    };

    tx.commit().await.map_err(backend)?;

    Ok(id)
}

pub(super) async fn routes(store: &PostgresStore) -> Result<Vec<Route>, StoreError> {
    decode_all(
        sqlx::query_as!(
            RouteRow,
            "SELECT id, guild_id, name, matcher_source, min_severity, target, group_strategy, \
             mentions, escalation, priority, continue_to_next, source, enabled, created_by, \
             created_at FROM routes ORDER BY priority, id"
        )
        .fetch_all(&store.pool)
        .await
        .map_err(backend)?,
    )
}

pub(super) async fn disable_missing_config_routes(
    store: &PostgresStore,
    keep: &[String],
) -> Result<u64, StoreError> {
    // An empty `keep` matches no name, so every file route is disabled, which is what a
    // configuration declaring none of them means.
    let result = sqlx::query!(
        "UPDATE routes SET enabled = FALSE WHERE source = 'config' AND enabled = TRUE \
         AND NOT (name = ANY($1))",
        keep,
    )
    .execute(&store.pool)
    .await
    .map_err(backend)?;

    Ok(result.rows_affected())
}

pub(super) async fn upsert_subscription(
    store: &PostgresStore,
    subscription: &Subscription,
) -> Result<SubscriptionId, StoreError> {
    let mut tx = store.write().await?;
    let min_severity = subscription.min_severity.map(dam_core::Severity::as_str);

    let id = if subscription.id.get() == 0 {
        let id = sqlx::query_scalar!(
            "INSERT INTO subscriptions (user_id, matcher_source, min_severity, created_at) \
             VALUES ($1, $2, $3, $4) RETURNING id",
            subscription.user_id.to_db(),
            subscription.matcher_source,
            min_severity,
            subscription.created_at,
        )
        .fetch_one(&mut *tx)
        .await
        .map_err(backend)?;

        SubscriptionId::new(id)
    } else {
        // The owner is in the predicate, not only in the key: an update that trusted the id
        // alone would let one person rewrite another's subscription by guessing a number.
        let changed = sqlx::query!(
            "UPDATE subscriptions SET matcher_source = $1, min_severity = $2 \
             WHERE id = $3 AND user_id = $4",
            subscription.matcher_source,
            min_severity,
            subscription.id.get(),
            subscription.user_id.to_db(),
        )
        .execute(&mut *tx)
        .await
        .map_err(backend)?
        .rows_affected();

        if changed == 0 {
            return Err(StoreError::NotFound {
                kind: "subscription",
                key: subscription.id.to_string(),
            });
        }

        subscription.id
    };

    tx.commit().await.map_err(backend)?;

    Ok(id)
}

pub(super) async fn subscriptions(store: &PostgresStore) -> Result<Vec<Subscription>, StoreError> {
    decode_all(
        sqlx::query_as!(
            SubscriptionRow,
            "SELECT id, user_id, matcher_source, min_severity, created_at FROM subscriptions \
             ORDER BY id"
        )
        .fetch_all(&store.pool)
        .await
        .map_err(backend)?,
    )
}

pub(super) async fn remove_subscription(
    store: &PostgresStore,
    id: SubscriptionId,
    user: UserId,
) -> Result<(), StoreError> {
    let mut tx = store.write().await?;

    let removed = sqlx::query!(
        "DELETE FROM subscriptions WHERE id = $1 AND user_id = $2",
        id.get(),
        user.to_db(),
    )
    .execute(&mut *tx)
    .await
    .map_err(backend)?
    .rows_affected();

    if removed == 0 {
        return Err(StoreError::NotFound {
            kind: "subscription",
            key: id.to_string(),
        });
    }

    tx.commit().await.map_err(backend)
}

pub(super) async fn sync_forum_tags(
    store: &PostgresStore,
    channel: ChannelId,
    tags: &[ForumTag],
) -> Result<(), StoreError> {
    let mut tx = store.write().await?;

    // Replaced rather than merged: Discord's list is the whole truth about a channel's tags,
    // and a tag a human deleted has to leave the cache or the next apply fails on an id that
    // no longer exists.
    sqlx::query!(
        "DELETE FROM forum_tags WHERE channel_id = $1",
        channel.to_db()
    )
    .execute(&mut *tx)
    .await
    .map_err(backend)?;

    for tag in tags {
        sqlx::query!(
            "INSERT INTO forum_tags (channel_id, tag_name, tag_id, moderated, synced_at) \
             VALUES ($1, $2, $3, $4, $5)",
            channel.to_db(),
            tag.name,
            tag.id.to_db(),
            tag.moderated,
            tag.synced_at,
        )
        .execute(&mut *tx)
        .await
        .map_err(backend)?;
    }

    tx.commit().await.map_err(backend)
}

pub(super) async fn forum_tags(
    store: &PostgresStore,
    channel: ChannelId,
) -> Result<Vec<ForumTag>, StoreError> {
    Ok(sqlx::query_as!(
        ForumTagRow,
        "SELECT channel_id, tag_name, tag_id, moderated, synced_at FROM forum_tags \
         WHERE channel_id = $1 ORDER BY tag_name",
        channel.to_db(),
    )
    .fetch_all(&store.pool)
    .await
    .map_err(backend)?
    .into_iter()
    .map(ForumTag::from)
    .collect())
}
