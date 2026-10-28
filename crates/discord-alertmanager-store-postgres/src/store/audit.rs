//! The audit log and the retention pass that prunes old rows.

use chrono::{DateTime, Utc};
use dam_store::{AuditEntry, PruneReport, RetentionPolicy, StoreError};
use sqlx::PgConnection;

use crate::PostgresStore;
use crate::convert::{Timestamp, backend, json};

pub(super) async fn append_audit(
    store: &PostgresStore,
    entry: &AuditEntry,
) -> Result<(), StoreError> {
    sqlx::query(
        "INSERT INTO audit_log (actor, guild_id, action, subject, detail, result, created_at) \
         VALUES ($1, $2, $3, $4, $5, $6, $7)",
    )
    .bind(entry.actor.map(dam_store::UserId::to_db))
    .bind(entry.guild_id.map(dam_store::GuildId::to_db))
    .bind(entry.action.clone())
    .bind(entry.subject.clone())
    .bind(json(&entry.detail))
    .bind(entry.result.as_str())
    .bind(entry.at)
    .execute(&store.pool)
    .await
    .map_err(backend)?;

    Ok(())
}

pub(super) async fn prune(
    store: &PostgresStore,
    policy: &RetentionPolicy,
    now: DateTime<Utc>,
) -> Result<PruneReport, StoreError> {
    let limit = i64::from(policy.batch_limit);
    let mut tx = store.write().await?;

    let events = delete_batch(
        &mut tx,
        "DELETE FROM alert_events WHERE id IN \
         (SELECT id FROM alert_events WHERE received_at < $1 ORDER BY id LIMIT $2)",
        now - policy.events,
        limit,
    )
    .await?;

    let notifications = delete_batch(
        &mut tx,
        "DELETE FROM notifications WHERE id IN \
         (SELECT id FROM notifications WHERE state = 'resolved' AND updated_at < $1 \
          ORDER BY id LIMIT $2)",
        now - policy.resolved,
        limit,
    )
    .await?;

    let alerts = delete_batch(
        &mut tx,
        "DELETE FROM alerts WHERE fingerprint IN \
         (SELECT fingerprint FROM alerts WHERE status = 'resolved' AND resolved_at < $1 \
          ORDER BY fingerprint LIMIT $2)",
        now - policy.resolved,
        limit,
    )
    .await?;

    let audit = delete_batch(
        &mut tx,
        "DELETE FROM audit_log WHERE id IN \
         (SELECT id FROM audit_log WHERE created_at < $1 ORDER BY id LIMIT $2)",
        now - policy.audit,
        limit,
    )
    .await?;

    tx.commit().await.map_err(backend)?;

    let batch = u64::try_from(limit).unwrap_or(u64::MAX);

    Ok(PruneReport {
        events,
        alerts,
        notifications,
        audit,
        // A pass that filled its batch has left rows behind, and the caller schedules another
        // rather than waiting a whole retention interval to delete the rest.
        more: [events, notifications, alerts, audit]
            .iter()
            .any(|count| *count >= batch),
    })
}

/// Runs one bounded delete and reports how many rows it took.
async fn delete_batch(
    conn: &mut PgConnection,
    sql: &'static str,
    cutoff: Timestamp,
    limit: i64,
) -> Result<u64, StoreError> {
    let result = sqlx::query(sql)
        .bind(cutoff)
        .bind(limit)
        .execute(conn)
        .await
        .map_err(backend)?;

    Ok(result.rows_affected())
}
