//! The audit log and the retention pass that prunes old rows.

use crate::PostgresStore;
use crate::convert::{backend, json};
use chrono::{DateTime, Utc};
use dam_store::{AuditEntry, PruneReport, RetentionPolicy, StoreError};

pub(super) async fn append_audit(
    store: &PostgresStore,
    entry: &AuditEntry,
) -> Result<(), StoreError> {
    sqlx::query!(
        "INSERT INTO audit_log (actor, guild_id, action, subject, detail, result, created_at) \
         VALUES ($1, $2, $3, $4, $5, $6, $7)",
        entry.actor.map(dam_store::UserId::to_db),
        entry.guild_id.map(dam_store::GuildId::to_db),
        entry.action,
        entry.subject.as_deref(),
        json(&entry.detail),
        entry.result.as_str(),
        entry.at,
    )
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

    // One statement per table, each bounded by the batch, so a backlog is worked off over
    // several passes rather than in one transaction that holds every lock it takes.
    let events = sqlx::query!(
        "DELETE FROM alert_events WHERE id IN \
         (SELECT id FROM alert_events WHERE received_at < $1 ORDER BY id LIMIT $2)",
        now - policy.events,
        limit,
    )
    .execute(&mut *tx)
    .await
    .map_err(backend)?
    .rows_affected();

    let notifications = sqlx::query!(
        "DELETE FROM notifications WHERE id IN \
         (SELECT id FROM notifications WHERE state = 'resolved' AND updated_at < $1 \
          ORDER BY id LIMIT $2)",
        now - policy.resolved,
        limit,
    )
    .execute(&mut *tx)
    .await
    .map_err(backend)?
    .rows_affected();

    let alerts = sqlx::query!(
        "DELETE FROM alerts WHERE fingerprint IN \
         (SELECT fingerprint FROM alerts WHERE status = 'resolved' AND resolved_at < $1 \
          ORDER BY fingerprint LIMIT $2)",
        now - policy.resolved,
        limit,
    )
    .execute(&mut *tx)
    .await
    .map_err(backend)?
    .rows_affected();

    let audit = sqlx::query!(
        "DELETE FROM audit_log WHERE id IN \
         (SELECT id FROM audit_log WHERE created_at < $1 ORDER BY id LIMIT $2)",
        now - policy.audit,
        limit,
    )
    .execute(&mut *tx)
    .await
    .map_err(backend)?
    .rows_affected();

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
