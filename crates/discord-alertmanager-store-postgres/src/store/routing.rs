//! Routes, ignore rules, subscriptions, and the forum tags a route can apply.

use chrono::{DateTime, Utc};
use dam_store::{
    ChannelId, ForumTag, GuildId, IgnoreId, IgnoreRule, Route, RouteId, RouteSource, StoreError,
    Subscription, SubscriptionId, UserId,
};
use sqlx::{Postgres, QueryBuilder, Row};

use super::const_format;
use crate::PostgresStore;
use crate::convert;
use crate::convert::{Timestamp, backend, forum_tag, ignore_rule, json, route};

pub(super) async fn upsert_ignore(
    store: &PostgresStore,
    rule: &IgnoreRule,
) -> Result<IgnoreId, StoreError> {
    let mut tx = store.write().await?;

    let id = if rule.id.get() == 0 {
        let row = sqlx::query(
            "INSERT INTO ignore_rules (scope, guild_id, channel_id, matcher_source, reason, \
             created_by, created_at, expires_at, revoked_at) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9) RETURNING id",
        )
        .bind(rule.scope.as_str())
        .bind(rule.guild_id.to_db())
        .bind(rule.channel_id.map(dam_store::ChannelId::to_db))
        .bind(rule.matcher_source.clone())
        .bind(rule.reason.clone())
        .bind(rule.created_by.to_db())
        .bind(rule.created_at)
        .bind(rule.expires_at)
        .bind(rule.revoked_at)
        .fetch_one(&mut *tx)
        .await
        .map_err(backend)?;

        IgnoreId::new(row.try_get::<i64, _>("id").map_err(backend)?)
    } else {
        sqlx::query(
            "UPDATE ignore_rules SET scope = $1, guild_id = $2, channel_id = $3, \
             matcher_source = $4, reason = $5, expires_at = $6, revoked_at = $7 WHERE id = $8",
        )
        .bind(rule.scope.as_str())
        .bind(rule.guild_id.to_db())
        .bind(rule.channel_id.map(dam_store::ChannelId::to_db))
        .bind(rule.matcher_source.clone())
        .bind(rule.reason.clone())
        .bind(rule.expires_at)
        .bind(rule.revoked_at)
        .bind(rule.id.get())
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
    let affected = sqlx::query(
        "UPDATE ignore_rules SET revoked_at = $1 \
         WHERE id = $2 AND guild_id = $3 AND revoked_at IS NULL",
    )
    .bind(now)
    .bind(id.get())
    .bind(guild.to_db())
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
    let sql = const_format(&[
        "SELECT ",
        IGNORE_COLUMNS,
        " FROM ignore_rules WHERE guild_id = ",
    ]);

    QueryBuilder::<Postgres>::new(sql)
        .push_bind(guild.to_db())
        .push(" AND revoked_at IS NULL AND (expires_at IS NULL OR expires_at > ")
        .push_bind(now)
        .push(") ORDER BY id")
        .build()
        .fetch_all(&store.pool)
        .await
        .map_err(backend)?
        .iter()
        .map(ignore_rule)
        .collect()
}

pub(super) async fn upsert_route(
    store: &PostgresStore,
    route: &Route,
) -> Result<RouteId, StoreError> {
    let mut tx = store.write().await?;
    let min_severity = route.min_severity.map(dam_core::Severity::as_str);

    let id = if route.id.get() != 0 {
        sqlx::query(
            "UPDATE routes SET guild_id = $1, name = $2, matcher_source = $3, min_severity = $4, \
             target = $5, group_strategy = $6, mentions = $7, escalation = $8, priority = $9, \
             continue_to_next = $10, source = $11, enabled = $12, created_by = $13 \
             WHERE id = $14",
        )
        .bind(route.guild_id.to_db())
        .bind(route.name.clone())
        .bind(route.matcher_source.clone())
        .bind(min_severity)
        .bind(json(&route.target))
        .bind(route.group_strategy.as_str())
        .bind(json(&route.mentions))
        .bind(route.escalation.as_ref().map(json))
        .bind(route.priority)
        .bind(route.continue_to_next)
        .bind(route.source.as_str())
        .bind(route.enabled)
        .bind(route.created_by.map(dam_store::UserId::to_db))
        .bind(route.id.get())
        .execute(&mut *tx)
        .await
        .map_err(backend)?;

        route.id
    } else {
        // A route declared in the file is synchronised by name on every start, so its insert
        // has to be an update the second time. One created from Discord is not: a second
        // `/route add` under a name the guild already uses is a mistake, and the unique index
        // is what tells the operator so.
        let conflict = if route.source == RouteSource::Config {
            "ON CONFLICT (guild_id, name) DO UPDATE SET matcher_source = excluded.matcher_source, \
             min_severity = excluded.min_severity, target = excluded.target, \
             group_strategy = excluded.group_strategy, mentions = excluded.mentions, \
             escalation = excluded.escalation, \
             priority = excluded.priority, continue_to_next = excluded.continue_to_next, \
             source = excluded.source, enabled = excluded.enabled "
        } else {
            ""
        };

        let mut builder = QueryBuilder::<Postgres>::new(
            "INSERT INTO routes (guild_id, name, matcher_source, min_severity, target, \
             group_strategy, mentions, escalation, priority, continue_to_next, source, \
             enabled, created_by, created_at) VALUES (",
        );
        let mut values = builder.separated(", ");
        values.push_bind(route.guild_id.to_db());
        values.push_bind(route.name.clone());
        values.push_bind(route.matcher_source.clone());
        values.push_bind(min_severity);
        values.push_bind(json(&route.target));
        values.push_bind(route.group_strategy.as_str());
        values.push_bind(json(&route.mentions));
        values.push_bind(route.escalation.as_ref().map(json));
        values.push_bind(route.priority);
        values.push_bind(route.continue_to_next);
        values.push_bind(route.source.as_str());
        values.push_bind(route.enabled);
        values.push_bind(route.created_by.map(dam_store::UserId::to_db));
        values.push_bind(route.created_at);
        builder.push(") ");
        builder.push(conflict);
        builder.push("RETURNING id");

        let row = builder.build().fetch_one(&mut *tx).await.map_err(backend)?;

        RouteId::new(row.try_get::<i64, _>("id").map_err(backend)?)
    };

    tx.commit().await.map_err(backend)?;

    Ok(id)
}

pub(super) async fn routes(store: &PostgresStore) -> Result<Vec<Route>, StoreError> {
    let sql = const_format(&[
        "SELECT ",
        ROUTE_COLUMNS,
        " FROM routes ORDER BY priority, id",
    ]);

    QueryBuilder::<Postgres>::new(sql)
        .build()
        .fetch_all(&store.pool)
        .await
        .map_err(backend)?
        .iter()
        .map(route)
        .collect()
}

pub(super) async fn disable_missing_config_routes(
    store: &PostgresStore,
    keep: &[String],
) -> Result<u64, StoreError> {
    let mut builder = QueryBuilder::<Postgres>::new(
        "UPDATE routes SET enabled = FALSE WHERE source = 'config' AND enabled = TRUE",
    );

    if !keep.is_empty() {
        builder.push(" AND name NOT IN (");
        let mut names = builder.separated(", ");
        for name in keep {
            names.push_bind(name.clone());
        }
        builder.push(")");
    }

    let result = builder
        .build()
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
        let row = sqlx::query(
            "INSERT INTO subscriptions (user_id, matcher_source, min_severity, created_at)                  VALUES ($1, $2, $3, $4) RETURNING id",
        )
        .bind(subscription.user_id.to_db())
        .bind(subscription.matcher_source.clone())
        .bind(min_severity)
        .bind(Timestamp::from(subscription.created_at))
        .fetch_one(&mut *tx)
        .await
        .map_err(backend)?;

        SubscriptionId::new(row.try_get::<i64, _>("id").map_err(backend)?)
    } else {
        // The owner is in the predicate, not only in the key: an update that trusted the id
        // alone would let one person rewrite another's subscription by guessing a number.
        let changed = sqlx::query(
            "UPDATE subscriptions SET matcher_source = $1, min_severity = $2                  WHERE id = $3 AND user_id = $4",
        )
        .bind(subscription.matcher_source.clone())
        .bind(min_severity)
        .bind(subscription.id.get())
        .bind(subscription.user_id.to_db())
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
    sqlx::query(
        "SELECT id, user_id, matcher_source, min_severity, created_at FROM subscriptions              ORDER BY id",
    )
    .fetch_all(&store.pool)
    .await
    .map_err(backend)?
    .iter()
    .map(convert::subscription)
    .collect()
}

pub(super) async fn remove_subscription(
    store: &PostgresStore,
    id: SubscriptionId,
    user: UserId,
) -> Result<(), StoreError> {
    let mut tx = store.write().await?;

    let removed = sqlx::query("DELETE FROM subscriptions WHERE id = $1 AND user_id = $2")
        .bind(id.get())
        .bind(user.to_db())
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
    sqlx::query("DELETE FROM forum_tags WHERE channel_id = $1")
        .bind(channel.to_db())
        .execute(&mut *tx)
        .await
        .map_err(backend)?;

    for tag in tags {
        sqlx::query(
            "INSERT INTO forum_tags (channel_id, tag_name, tag_id, moderated, synced_at) \
             VALUES ($1, $2, $3, $4, $5)",
        )
        .bind(channel.to_db())
        .bind(tag.name.clone())
        .bind(tag.id.to_db())
        .bind(tag.moderated)
        .bind(tag.synced_at)
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
    sqlx::query(
        "SELECT channel_id, tag_name, tag_id, moderated, synced_at FROM forum_tags \
         WHERE channel_id = $1 ORDER BY tag_name",
    )
    .bind(channel.to_db())
    .fetch_all(&store.pool)
    .await
    .map_err(backend)?
    .iter()
    .map(forum_tag)
    .collect()
}

/// Every column of `routes`.
const ROUTE_COLUMNS: &str = "id, guild_id, name, matcher_source, min_severity, target, \
     group_strategy, mentions, escalation, priority, continue_to_next, source, enabled, \
     created_by, created_at";

/// Every column of `ignore_rules`.
const IGNORE_COLUMNS: &str = "id, scope, guild_id, channel_id, matcher_source, reason, created_by, \
     created_at, expires_at, revoked_at";
