//! The outbox: Discord effects queued, claimed by one worker each, and settled.

use chrono::{DateTime, Utc};
use dam_store::{
    AppliedEffect, ClaimRequest, Effect, NewOutboxItem, NotificationId, OUTBOX_LANES, OutboxId,
    OutboxItem, SilenceLifecycle, SilenceLink, StoreError, WorkerId,
};
use sqlx::{PgConnection, Postgres, QueryBuilder, Row};

use super::alerts::write_silence;
use super::const_format;
use crate::PostgresStore;
use crate::convert::{backend, json, outbox_item};

pub(super) async fn enqueue_effects(
    store: &PostgresStore,
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
    store: &PostgresStore,
    worker: &WorkerId,
    request: ClaimRequest,
    now: DateTime<Utc>,
) -> Result<Vec<OutboxItem>, StoreError> {
    let mut tx = store.write().await?;

    let mut selector = QueryBuilder::<Postgres>::new(
        "SELECT id FROM outbox WHERE claimed_at IS NULL AND not_before <= ",
    );
    selector.push_bind(now);

    if let Some(lane) = request.lane {
        selector.push(" AND lane % ");
        selector.push_bind(i32::from(lane.of));
        selector.push(" = ");
        selector.push_bind(i32::from(lane.index));
    }

    selector.push(" ORDER BY not_before, id LIMIT ");
    selector.push_bind(i64::from(request.limit));
    // The rows this worker takes are locked and the rest are skipped rather than waited for,
    // so several dispatchers claim disjoint work at once instead of queueing behind each
    // other. It is the whole reason this backend is the supported path above one replica.
    selector.push(" FOR UPDATE SKIP LOCKED");

    let ids: Vec<i64> = selector
        .build_query_scalar()
        .fetch_all(&mut *tx)
        .await
        .map_err(backend)?;

    if ids.is_empty() {
        tx.commit().await.map_err(backend)?;
        return Ok(Vec::new());
    }

    // The attempt counter moves on the claim rather than on the failure, so an item that
    // kills its worker before it can report anything still runs out of attempts.
    let mut claimer = QueryBuilder::<Postgres>::new("UPDATE outbox SET claimed_by = ");
    claimer.push_bind(worker.as_str().to_owned());
    claimer.push(", claimed_at = ");
    claimer.push_bind(now);
    claimer.push(", attempts = attempts + 1 WHERE id IN ");
    push_id_list(&mut claimer, &ids);
    claimer.build().execute(&mut *tx).await.map_err(backend)?;

    let mut reader = QueryBuilder::<Postgres>::new(const_format(&[
        "SELECT ",
        OUTBOX_COLUMNS,
        " FROM outbox WHERE id IN ",
    ]));
    push_id_list(&mut reader, &ids);
    reader.push(" ORDER BY not_before, id");

    let rows = reader.build().fetch_all(&mut *tx).await.map_err(backend)?;
    let items = rows
        .iter()
        .map(outbox_item)
        .collect::<Result<Vec<_>, _>>()?;

    tx.commit().await.map_err(backend)?;

    Ok(items)
}

pub(super) async fn complete_outbox(
    store: &PostgresStore,
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
            sqlx::query("UPDATE silences SET state = 'expired', synced_at = $1 WHERE am_id = $2")
                .bind(now)
                .bind(am_id.clone())
                .execute(&mut *tx)
                .await
                .map_err(backend)?;
        }
        _ => {}
    }

    sqlx::query("DELETE FROM outbox WHERE id = $1")
        .bind(id.get())
        .execute(&mut *tx)
        .await
        .map_err(backend)?;

    tx.commit().await.map_err(backend)
}

pub(super) async fn fail_outbox(
    store: &PostgresStore,
    worker: &WorkerId,
    id: OutboxId,
    error: &str,
    retry_at: Option<DateTime<Utc>>,
) -> Result<(), StoreError> {
    let mut tx = store.write().await?;
    take_claim(&mut tx, worker, id).await?;

    match retry_at {
        Some(retry_at) => {
            sqlx::query(
                "UPDATE outbox SET claimed_by = NULL, claimed_at = NULL, not_before = $1, \
                 last_error = $2 WHERE id = $3",
            )
            .bind(retry_at)
            .bind(error.to_owned())
            .bind(id.get())
            .execute(&mut *tx)
            .await
            .map_err(backend)?;
        }
        // Abandoned rather than parked: a row nobody will ever claim again is queue depth
        // that never drains, and the dispatcher has already logged and audited the failure.
        None => {
            sqlx::query("DELETE FROM outbox WHERE id = $1")
                .bind(id.get())
                .execute(&mut *tx)
                .await
                .map_err(backend)?;
        }
    }

    tx.commit().await.map_err(backend)
}

pub(super) async fn reclaim_expired(
    store: &PostgresStore,
    older_than: DateTime<Utc>,
    now: DateTime<Utc>,
) -> Result<u64, StoreError> {
    let result = sqlx::query(
        "UPDATE outbox SET claimed_by = NULL, claimed_at = NULL, not_before = $1 \
         WHERE claimed_at IS NOT NULL AND claimed_at < $2",
    )
    .bind(now)
    .bind(older_than)
    .execute(&store.pool)
    .await
    .map_err(backend)?;

    Ok(result.rows_affected())
}

pub(super) async fn outbox_depth(store: &PostgresStore) -> Result<Vec<(String, u64)>, StoreError> {
    let rows = sqlx::query(
        "SELECT kind, count(*) AS depth FROM outbox WHERE claimed_at IS NULL GROUP BY kind \
         ORDER BY kind",
    )
    .fetch_all(&store.pool)
    .await
    .map_err(backend)?;

    rows.iter()
        .map(|row| {
            let kind: String = row.try_get("kind").map_err(backend)?;
            let depth: i64 = row.try_get("depth").map_err(backend)?;

            Ok((kind, u64::try_from(depth).unwrap_or(0)))
        })
        .collect()
}

/// Every column of `outbox`.
const OUTBOX_COLUMNS: &str = "id, lane, kind, dedupe_key, payload, not_before, attempts, claimed_by, claimed_at, \
     last_error, created_at";

/// Queues one effect, folding it into a pending one for the same card where that loses nothing.
///
/// Two queued edits of one card are one edit of its current state; two queued thread notes are two
/// different sentences. The fold keeps the earlier `not_before`, so a sustained stream of changes
/// still produces an edit one debounce after the first of them rather than never.
///
/// The card is the scope and the dedupe key is not. One alert fans out to every route that matches
/// it, and those cards share one per-alert key across channels of their own; folding on the key
/// alone let one card's edit overwrite another's, and left that card showing whatever it had last
/// rendered — which for a resolved alert is the word "firing".
pub(super) async fn enqueue(
    conn: &mut PgConnection,
    item: &NewOutboxItem,
    at: DateTime<Utc>,
) -> Result<(), StoreError> {
    let kind = item.effect.kind();
    let payload = json(&item.effect);
    let key = item.dedupe_key.as_str().to_owned();
    let notification = item.effect.notification();

    // Every coalescable effect names a card, and the binding is what makes that a fact the query
    // can hold rather than a claim in a comment: one that names none is queued, never folded.
    if item.effect.is_coalescable()
        && let Some(notification) = notification
    {
        let folded = sqlx::query(
            "UPDATE outbox SET payload = $1, not_before = LEAST(not_before, $2) \
             WHERE claimed_at IS NULL AND kind = $3 AND notification_id = $4",
        )
        .bind(payload.clone())
        .bind(item.not_before)
        .bind(kind)
        .bind(notification.get())
        .execute(&mut *conn)
        .await
        .map_err(backend)?
        .rows_affected();

        if folded > 0 {
            return Ok(());
        }
    }

    sqlx::query(
        "INSERT INTO outbox \
         (lane, kind, dedupe_key, notification_id, payload, not_before, created_at) \
         VALUES ($1, $2, $3, $4, $5, $6, $7)",
    )
    .bind(i32::from(item.dedupe_key.lane(OUTBOX_LANES)))
    .bind(kind)
    .bind(key.clone())
    .bind(notification.map(NotificationId::get))
    .bind(payload.clone())
    .bind(item.not_before)
    .bind(at)
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
    conn: &mut PgConnection,
    worker: &WorkerId,
    id: OutboxId,
) -> Result<Effect, StoreError> {
    let row = sqlx::query("SELECT payload, claimed_by FROM outbox WHERE id = $1")
        .bind(id.get())
        .fetch_optional(&mut *conn)
        .await
        .map_err(backend)?
        .ok_or(StoreError::LeaseLost { id })?;

    let claimed_by: Option<String> = row.try_get("claimed_by").map_err(backend)?;
    if claimed_by.as_deref() != Some(worker.as_str()) {
        return Err(StoreError::LeaseLost { id });
    }

    let payload: serde_json::Value = row.try_get("payload").map_err(backend)?;

    serde_json::from_value(payload).map_err(|error| StoreError::Decode {
        kind: "outbox effect",
        detail: error.to_string(),
    })
}

/// Writes back what a completed effect changed about its card.
async fn apply_to_card(
    conn: &mut PgConnection,
    id: NotificationId,
    applied: &AppliedEffect,
    now: DateTime<Utc>,
) -> Result<(), StoreError> {
    let mut builder = QueryBuilder::<Postgres>::new("UPDATE notifications SET updated_at = ");
    builder.push_bind(now);

    if let Some(message) = applied.message_id {
        builder.push(", message_id = ");
        builder.push_bind(message.to_db());
    }
    if let Some(thread) = applied.thread_id {
        builder.push(", thread_id = ");
        builder.push_bind(thread.to_db());
    }
    if let Some(hash) = applied.render_hash.as_ref() {
        builder.push(", render_hash = ");
        builder.push_bind(hash.clone());
    }
    if let Some(tags) = applied.applied_tags.as_ref() {
        builder.push(", applied_tags = ");
        builder.push_bind(json(tags));
    }
    if let Some(hash) = applied.tags_hash.as_ref() {
        builder.push(", tags_hash = ");
        builder.push_bind(hash.clone());
    }
    if let Some(pinned) = applied.pinned {
        builder.push(", pinned = ");
        builder.push_bind(pinned);
    }
    if let Some(archived) = applied.archived {
        builder.push(", archived = ");
        builder.push_bind(archived);
    }

    builder.push(" WHERE id = ");
    builder.push_bind(id.get());

    let affected = builder
        .build()
        .execute(conn)
        .await
        .map_err(backend)?
        .rows_affected();

    if affected == 0 {
        return Err(StoreError::no_such_notification(id));
    }

    Ok(())
}

/// Adds a parenthesised list of surrogate keys.
fn push_id_list(builder: &mut QueryBuilder<Postgres>, ids: &[i64]) {
    builder.push("(");
    let mut list = builder.separated(", ");
    for id in ids {
        list.push_bind(*id);
    }
    builder.push(")");
}
