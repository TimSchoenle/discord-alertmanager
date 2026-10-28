//! The outbox: Discord effects queued, claimed by one worker each, and settled.

use chrono::{DateTime, Utc};
use dam_store::{
    AppliedEffect, ClaimRequest, Effect, NewOutboxItem, NotificationId, OUTBOX_LANES, OutboxId,
    OutboxItem, SilenceLifecycle, SilenceLink, StoreError, WorkerId,
};
use sqlx::SqliteConnection;

use super::alerts::write_silence;
use crate::SqliteStore;
use crate::convert::{backend, decode_all, encode_json, encode_time};

pub(super) async fn enqueue_effects(
    store: &SqliteStore,
    items: &[NewOutboxItem],
    at: DateTime<Utc>,
) -> Result<(), StoreError> {
    let mut tx = store.write().await?;

    for item in items {
        enqueue(&mut tx, item, at).await?;
    }

    tx.commit().await.map_err(backend)
}

pub(super) async fn claim_outbox(
    store: &SqliteStore,
    worker: &WorkerId,
    request: ClaimRequest,
    now: DateTime<Utc>,
) -> Result<Vec<OutboxItem>, StoreError> {
    let mut tx = store.write().await?;

    // The write transaction is `BEGIN IMMEDIATE`, so this read and the claim below cannot
    // interleave with another worker's. Two statements rather than one with an optional lane,
    // so neither carries a predicate that is true of every row.
    let limit = i64::from(request.limit);
    let ids: Vec<i64> = match request.lane {
        Some(lane) => {
            sqlx::query_scalar!(
                "SELECT id FROM outbox WHERE claimed_at IS NULL AND not_before <= ?1 \
                 AND lane % ?2 = ?3 ORDER BY not_before, id LIMIT ?4",
                encode_time(now),
                i64::from(lane.of),
                i64::from(lane.index),
                limit,
            )
            .fetch_all(&mut *tx)
            .await
        }
        None => {
            sqlx::query_scalar!(
                "SELECT id FROM outbox WHERE claimed_at IS NULL AND not_before <= ?1 \
                 ORDER BY not_before, id LIMIT ?2",
                encode_time(now),
                limit,
            )
            .fetch_all(&mut *tx)
            .await
        }
    }
    .map_err(backend)?;

    if ids.is_empty() {
        tx.commit().await.map_err(backend)?;
        return Ok(Vec::new());
    }

    // The attempt counter moves on the claim rather than on the failure, so an item that
    // kills its worker before it can report anything still runs out of attempts.
    sqlx::query!(
        "UPDATE outbox SET claimed_by = ?1, claimed_at = ?2, attempts = attempts + 1 \
         WHERE id IN (SELECT value FROM json_each(?3))",
        worker.as_str(),
        encode_time(now),
        encode_json(&ids),
    )
    .execute(&mut *tx)
    .await
    .map_err(backend)?;

    let items = decode_all(
        select_outbox!(
            "WHERE id IN (SELECT value FROM json_each(?1)) ORDER BY not_before, id",
            encode_json(&ids)
        )
        .fetch_all(&mut *tx)
        .await
        .map_err(backend)?,
    )?;

    tx.commit().await.map_err(backend)?;

    Ok(items)
}

pub(super) async fn complete_outbox(
    store: &SqliteStore,
    worker: &WorkerId,
    id: OutboxId,
    applied: &AppliedEffect,
) -> Result<(), StoreError> {
    let now = Utc::now();
    let mut tx = store.write().await?;
    let effect = take_claim(&mut tx, worker, id).await?;

    if let Some(notification) = effect.notification() {
        apply_to_card(&mut tx, notification, applied, now).await?;
    }

    match (&effect, applied.am_silence_id.as_deref()) {
        (Effect::CreateSilence { request }, Some(am_id)) => {
            write_silence(
                &mut tx,
                &SilenceLink {
                    am_id: am_id.to_owned(),
                    matchers: request.matchers.clone(),
                    starts_at: request.starts_at,
                    ends_at: request.ends_at,
                    created_by: request.created_by.clone(),
                    discord_user_id: request.discord_user_id,
                    origin_message: request.origin_message.clone(),
                    comment: request.comment.clone(),
                    state: SilenceLifecycle::Active,
                    synced_at: now,
                },
            )
            .await?;
        }
        (Effect::ExpireSilence { am_id }, _) => {
            sqlx::query!(
                "UPDATE silences SET state = 'expired', synced_at = ?1 WHERE am_id = ?2",
                encode_time(now),
                am_id,
            )
            .execute(&mut *tx)
            .await
            .map_err(backend)?;
        }
        _ => {}
    }

    sqlx::query!("DELETE FROM outbox WHERE id = ?1", id.get())
        .execute(&mut *tx)
        .await
        .map_err(backend)?;

    tx.commit().await.map_err(backend)
}

pub(super) async fn fail_outbox(
    store: &SqliteStore,
    worker: &WorkerId,
    id: OutboxId,
    error: &str,
    retry_at: Option<DateTime<Utc>>,
) -> Result<(), StoreError> {
    let mut tx = store.write().await?;
    take_claim(&mut tx, worker, id).await?;

    match retry_at {
        Some(retry_at) => {
            sqlx::query!(
                "UPDATE outbox SET claimed_by = NULL, claimed_at = NULL, not_before = ?1, \
                 last_error = ?2 WHERE id = ?3",
                encode_time(retry_at),
                error,
                id.get(),
            )
            .execute(&mut *tx)
            .await
            .map_err(backend)?;
        }
        // Abandoned rather than parked: a row nobody will ever claim again is queue depth
        // that never drains, and the dispatcher has already logged and audited the failure.
        None => {
            sqlx::query!("DELETE FROM outbox WHERE id = ?1", id.get())
                .execute(&mut *tx)
                .await
                .map_err(backend)?;
        }
    }

    tx.commit().await.map_err(backend)
}

pub(super) async fn reclaim_expired(
    store: &SqliteStore,
    older_than: DateTime<Utc>,
    now: DateTime<Utc>,
) -> Result<u64, StoreError> {
    let result = sqlx::query!(
        "UPDATE outbox SET claimed_by = NULL, claimed_at = NULL, not_before = ?1 \
         WHERE claimed_at IS NOT NULL AND claimed_at < ?2",
        encode_time(now),
        encode_time(older_than),
    )
    .execute(&store.pool)
    .await
    .map_err(backend)?;

    Ok(result.rows_affected())
}

pub(super) async fn outbox_depth(store: &SqliteStore) -> Result<Vec<(String, u64)>, StoreError> {
    let rows = sqlx::query!(
        r#"SELECT kind, count(*) AS "depth!" FROM outbox WHERE claimed_at IS NULL GROUP BY kind
         ORDER BY kind"#
    )
    .fetch_all(&store.pool)
    .await
    .map_err(backend)?;

    Ok(rows
        .into_iter()
        .map(|row| (row.kind, u64::try_from(row.depth).unwrap_or(0)))
        .collect())
}

/// Queues one effect, folding it into a pending one for the same card where that loses nothing.
///
/// Two queued edits of one card are one edit of its current state; two queued thread notes are two
/// different sentences. The fold keeps the earlier `not_before`, so a sustained stream of changes
/// still produces an edit one debounce after the first of them rather than never.
///
/// The card is the scope and the dedupe key is not. One alert fans out to every route that matches
/// it, and those cards share one per-alert key across channels of their own; folding on the key
/// alone let one card's edit overwrite another's, and left that card showing whatever it had last
/// rendered, which for a resolved alert is the word "firing".
pub(super) async fn enqueue(
    conn: &mut SqliteConnection,
    item: &NewOutboxItem,
    at: DateTime<Utc>,
) -> Result<(), StoreError> {
    let kind = item.effect.kind();
    let payload = encode_json(&item.effect);
    let notification = item.effect.notification();

    // Every coalescable effect names a card, and the binding is what makes that a fact the query
    // can hold rather than a claim in a comment: one that names none is queued, never folded.
    if item.effect.is_coalescable()
        && let Some(notification) = notification
    {
        let folded = sqlx::query!(
            "UPDATE outbox SET payload = ?1, not_before = min(not_before, ?2) \
             WHERE claimed_at IS NULL AND kind = ?3 AND notification_id = ?4",
            payload,
            encode_time(item.not_before),
            kind,
            notification.get(),
        )
        .execute(&mut *conn)
        .await
        .map_err(backend)?
        .rows_affected();

        if folded > 0 {
            return Ok(());
        }
    }

    sqlx::query!(
        "INSERT INTO outbox \
         (lane, kind, dedupe_key, notification_id, payload, not_before, created_at) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
        i64::from(item.dedupe_key.lane(OUTBOX_LANES)),
        kind,
        item.dedupe_key.as_str(),
        notification.map(NotificationId::get),
        payload,
        encode_time(item.not_before),
        encode_time(at),
    )
    .execute(conn)
    .await
    .map_err(backend)?;

    Ok(())
}

/// Takes the claim on an outbox row, or reports that it is no longer this worker's.
///
/// Reading the effect back out here rather than trusting what the dispatcher passed is what makes
/// the completion safe: a worker whose lease expired mid-flight cannot write the result of work
/// somebody else has since redone.
async fn take_claim(
    conn: &mut SqliteConnection,
    worker: &WorkerId,
    id: OutboxId,
) -> Result<Effect, StoreError> {
    let row = sqlx::query!(
        "SELECT payload, claimed_by FROM outbox WHERE id = ?1",
        id.get()
    )
    .fetch_optional(&mut *conn)
    .await
    .map_err(backend)?
    .ok_or(StoreError::LeaseLost { id })?;

    if row.claimed_by.as_deref() != Some(worker.as_str()) {
        return Err(StoreError::LeaseLost { id });
    }

    serde_json::from_str(&row.payload).map_err(|error| StoreError::Decode {
        kind: "outbox effect",
        detail: error.to_string(),
    })
}

/// Writes back what a completed effect changed about its card.
async fn apply_to_card(
    conn: &mut SqliteConnection,
    id: NotificationId,
    applied: &AppliedEffect,
    now: DateTime<Utc>,
) -> Result<(), StoreError> {
    // Every field the effect did not report is bound as null and kept by the `COALESCE`, so one
    // statement writes back whichever subset an effect produced.
    let affected = sqlx::query!(
        "UPDATE notifications SET updated_at = ?1, \
         message_id = COALESCE(?2, message_id), thread_id = COALESCE(?3, thread_id), \
         render_hash = COALESCE(?4, render_hash), applied_tags = COALESCE(?5, applied_tags), \
         tags_hash = COALESCE(?6, tags_hash), pinned = COALESCE(?7, pinned), \
         archived = COALESCE(?8, archived) WHERE id = ?9",
        encode_time(now),
        applied.message_id.map(dam_store::MessageId::to_db),
        applied.thread_id.map(dam_store::ChannelId::to_db),
        applied.render_hash.as_deref(),
        applied.applied_tags.as_ref().map(encode_json),
        applied.tags_hash.as_deref(),
        applied.pinned.map(i64::from),
        applied.archived.map(i64::from),
        id.get(),
    )
    .execute(conn)
    .await
    .map_err(backend)?
    .rows_affected();

    if affected == 0 {
        return Err(StoreError::no_such_notification(id));
    }

    Ok(())
}
