//! Decisions applied to notification cards, plus acknowledgements, thread replies and escalations.

use chrono::{DateTime, Utc};
use dam_core::{DedupeKey, Fingerprint, NotificationState, Trigger, next_state};
use dam_store::{
    AckCommand, AckOutcome, Acknowledgement, ChannelId, Decision, Effect, NewNotification,
    NewOutboxItem, Notification, NotificationId, RouteId, StoreError, ThreadReply,
};
use sqlx::{QueryBuilder, Row, Sqlite, SqliteConnection};

use super::const_format;
use super::outbox::enqueue;
use crate::SqliteStore;
use crate::convert;
use crate::convert::{backend, encode_time};

pub(super) async fn apply_decision(
    store: &SqliteStore,
    decision: &Decision,
) -> Result<Vec<NotificationId>, StoreError> {
    let mut tx = store.write().await?;
    let mut created = Vec::with_capacity(decision.new_cards.len());

    for planned in &decision.new_cards {
        if let Some(replaced) = planned.card.supersedes {
            retire_key(&mut tx, replaced, &planned.card.dedupe_key).await?;
        }

        let id = insert_notification(&mut tx, &planned.card).await?;

        enqueue(
            &mut tx,
            &NewOutboxItem {
                effect: Effect::PostCard {
                    notification: id,
                    mention: planned.mention,
                },
                dedupe_key: planned.card.dedupe_key.clone(),
                not_before: planned.not_before,
            },
            decision.at,
        )
        .await?;

        created.push(id);
    }

    for update in &decision.updates {
        // The alert moves whether or not the state does: an annotation change re-renders a
        // card without transitioning it, and the card still has to show the alert that
        // changed rather than the one it was created for.
        set_card_alert(&mut tx, update.id, &update.fingerprint, decision.at).await?;

        if let Some(state) = update.state {
            move_state(&mut tx, update.id, state, decision.at).await?;
        }

        for item in &update.effects {
            enqueue(&mut tx, item, decision.at).await?;
        }
    }

    tx.commit().await.map_err(backend)?;

    Ok(created)
}

pub(super) async fn acknowledge(
    store: &SqliteStore,
    command: &AckCommand,
) -> Result<AckOutcome, StoreError> {
    let mut tx = store.write().await?;
    let fingerprint = command.fingerprint.as_str().to_owned();
    let at = encode_time(command.at);

    let changed = if command.revoke {
        sqlx::query(
            "UPDATE acknowledgements SET revoked_at = ? \
             WHERE fingerprint = ? AND revoked_at IS NULL",
        )
        .bind(at.clone())
        .bind(fingerprint.clone())
        .execute(&mut *tx)
        .await
        .map_err(backend)?
        .rows_affected()
            > 0
    } else {
        // The partial unique index is what makes a double-click one acknowledgement. The
        // loser is told it changed nothing and is handed the holder, so it can say who has it
        // rather than posting a second identical card.
        sqlx::query(
            "INSERT INTO acknowledgements (fingerprint, user_id, kind, note, created_at) \
             VALUES (?, ?, ?, ?, ?) \
             ON CONFLICT (fingerprint) WHERE revoked_at IS NULL DO NOTHING",
        )
        .bind(fingerprint.clone())
        .bind(command.user_id.to_db())
        .bind(command.kind.as_str())
        .bind(command.note.clone())
        .bind(at.clone())
        .execute(&mut *tx)
        .await
        .map_err(backend)?
        .rows_affected()
            > 0
    };

    let holder = sqlx::query(
        "SELECT user_id, created_at FROM acknowledgements \
         WHERE fingerprint = ? AND revoked_at IS NULL",
    )
    .bind(fingerprint.clone())
    .fetch_optional(&mut *tx)
    .await
    .map_err(backend)?;

    let (holder, acknowledged_at) = match holder {
        Some(row) => {
            let user: i64 = row.try_get("user_id").map_err(backend)?;
            let created: String = row.try_get("created_at").map_err(backend)?;
            let created = DateTime::parse_from_rfc3339(&created)
                .map(|value| value.with_timezone(&Utc))
                .map_err(|error| StoreError::Decode {
                    kind: "acknowledgement time",
                    detail: error.to_string(),
                })?;

            (Some(dam_store::UserId::from_db(user)), Some(created))
        }
        None => (None, None),
    };

    let acknowledged = holder.is_some();
    let trigger = if command.revoke {
        Trigger::AckRevoked
    } else {
        Trigger::Acknowledged
    };

    let mut cards = read_cards_for(&mut tx, &command.fingerprint).await?;

    if changed {
        for card in &mut cards {
            if let Some(state) = next_state(card.state, trigger, acknowledged) {
                move_state(&mut tx, card.id, state, command.at).await?;
                card.state = state;
                card.updated_at = command.at;
            }
        }
    }

    tx.commit().await.map_err(backend)?;

    Ok(AckOutcome {
        changed,
        holder,
        acknowledged_at,
        cards,
    })
}

pub(super) async fn acknowledgement(
    store: &SqliteStore,
    fingerprint: &Fingerprint,
) -> Result<Option<Acknowledgement>, StoreError> {
    let row = sqlx::query(
        "SELECT user_id, kind, note, created_at FROM acknowledgements              WHERE fingerprint = ? AND revoked_at IS NULL",
    )
    .bind(fingerprint.as_str().to_owned())
    .fetch_optional(&store.pool)
    .await
    .map_err(backend)?;

    row.as_ref().map(convert::acknowledgement).transpose()
}

pub(super) async fn record_reply(
    store: &SqliteStore,
    reply: &ThreadReply,
) -> Result<Option<Notification>, StoreError> {
    let mut tx = store.write().await?;

    let sql = const_format(&[
        "SELECT ",
        NOTIFICATION_COLUMNS,
        " FROM notifications WHERE thread_id = ",
    ]);
    let row = QueryBuilder::<Sqlite>::new(sql)
        .push_bind(reply.thread_id.to_db())
        .build()
        .fetch_optional(&mut *tx)
        .await
        .map_err(backend)?;

    let Some(row) = row else {
        tx.commit().await.map_err(backend)?;
        return Ok(None);
    };

    let mut card = convert::notification(&row)?;
    let first = card.responded_at.is_none();

    sqlx::query(
        "UPDATE notifications SET reply_count = reply_count + 1, \
         responded_at = COALESCE(responded_at, ?), updated_at = ? WHERE id = ?",
    )
    .bind(encode_time(reply.at))
    .bind(encode_time(reply.at))
    .bind(card.id.get())
    .execute(&mut *tx)
    .await
    .map_err(backend)?;

    tx.commit().await.map_err(backend)?;

    card.reply_count = card.reply_count.saturating_add(1);
    card.updated_at = reply.at;
    if first {
        card.responded_at = Some(reply.at);
    }

    // Only the first reply changes what the card says. Later ones move a counter the card
    // shows, and re-rendering for each of them is how a busy thread turns into an edit storm.
    Ok(first.then_some(card))
}

pub(super) async fn notification_for(
    store: &SqliteStore,
    key: &DedupeKey,
    channel: ChannelId,
) -> Result<Option<Notification>, StoreError> {
    let sql = const_format(&[
        "SELECT ",
        NOTIFICATION_COLUMNS,
        " FROM notifications WHERE dedupe_key = ",
    ]);

    let row = QueryBuilder::<Sqlite>::new(sql)
        .push_bind(key.as_str().to_owned())
        .push(" AND channel_id = ")
        .push_bind(channel.to_db())
        .build()
        .fetch_optional(&store.pool)
        .await
        .map_err(backend)?;

    row.as_ref().map(convert::notification).transpose()
}

pub(super) async fn notification_for_thread(
    store: &SqliteStore,
    thread: ChannelId,
) -> Result<Option<Notification>, StoreError> {
    let sql = const_format(&[
        "SELECT ",
        NOTIFICATION_COLUMNS,
        " FROM notifications WHERE thread_id = ",
    ]);

    let row = QueryBuilder::<Sqlite>::new(sql)
        .push_bind(thread.to_db())
        .build()
        .fetch_optional(&store.pool)
        .await
        .map_err(backend)?;

    row.as_ref().map(convert::notification).transpose()
}

pub(super) async fn notification(
    store: &SqliteStore,
    id: NotificationId,
) -> Result<Option<Notification>, StoreError> {
    let sql = const_format(&[
        "SELECT ",
        NOTIFICATION_COLUMNS,
        " FROM notifications WHERE id = ",
    ]);

    let row = QueryBuilder::<Sqlite>::new(sql)
        .push_bind(id.get())
        .build()
        .fetch_optional(&store.pool)
        .await
        .map_err(backend)?;

    row.as_ref().map(convert::notification).transpose()
}

pub(super) async fn set_notification_state(
    store: &SqliteStore,
    id: NotificationId,
    state: NotificationState,
    now: DateTime<Utc>,
) -> Result<(), StoreError> {
    let mut tx = store.write().await?;
    move_state(&mut tx, id, state, now).await?;

    tx.commit().await.map_err(backend)
}

pub(super) async fn orphan_notification(
    store: &SqliteStore,
    id: NotificationId,
    now: DateTime<Utc>,
) -> Result<(), StoreError> {
    // The key is rewritten rather than the row deleted: the unique index on
    // (channel_id, dedupe_key) is what a fresh card needs freed, and the row is what the
    // alert's history hangs off.
    let affected = sqlx::query(
        "UPDATE notifications SET state = 'orphaned', message_id = NULL, \
         dedupe_key = 'orphaned:' || id || ':' || dedupe_key, updated_at = ? WHERE id = ?",
    )
    .bind(encode_time(now))
    .bind(id.get())
    .execute(&store.pool)
    .await
    .map_err(backend)?
    .rows_affected();

    if affected == 0 {
        return Err(StoreError::no_such_notification(id));
    }

    Ok(())
}

pub(super) async fn live_cards(
    store: &SqliteStore,
    routes: &[RouteId],
    limit: u32,
) -> Result<Vec<Notification>, StoreError> {
    // An empty `IN ()` is a syntax error in both dialects, and the answer is knowable without
    // asking: no route owns no card.
    if routes.is_empty() {
        return Ok(Vec::new());
    }

    let sql = const_format(&[
        "SELECT ",
        NOTIFICATION_COLUMNS,
        " FROM notifications WHERE message_id IS NOT NULL AND state NOT IN (",
    ]);

    let mut builder = QueryBuilder::<Sqlite>::new(sql);
    builder.push_bind(NotificationState::Resolved.as_str());
    builder.push(", ");
    builder.push_bind(NotificationState::Orphaned.as_str());
    builder.push(") AND route_id IN (");

    for (index, route) in routes.iter().enumerate() {
        if index > 0 {
            builder.push(", ");
        }
        builder.push_bind(route.get());
    }

    // Oldest first, so a run that hits its ceiling repairs the cards that have been wrong
    // longest rather than an arbitrary slice of them.
    builder
        .push(") ORDER BY created_at, id LIMIT ")
        .push_bind(i64::from(limit))
        .build()
        .fetch_all(&store.pool)
        .await
        .map_err(backend)?
        .iter()
        .map(convert::notification)
        .collect()
}

pub(super) async fn pending_escalations(
    store: &SqliteStore,
    created_before: DateTime<Utc>,
    limit: u32,
) -> Result<Vec<Notification>, StoreError> {
    let sql = const_format(&[
        "SELECT ",
        NOTIFICATION_COLUMNS,
        " FROM notifications WHERE escalated_at IS NULL AND message_id IS NOT NULL \
         AND state = ",
    ]);

    // Oldest first, so a sweep that hits its limit during a storm escalates the cards that
    // have been waiting longest rather than an arbitrary slice of them.
    QueryBuilder::<Sqlite>::new(sql)
        .push_bind(NotificationState::Firing.as_str())
        .push(" AND created_at <= ")
        .push_bind(encode_time(created_before))
        .push(" ORDER BY created_at, id LIMIT ")
        .push_bind(i64::from(limit))
        .build()
        .fetch_all(&store.pool)
        .await
        .map_err(backend)?
        .iter()
        .map(convert::notification)
        .collect()
}

pub(super) async fn mark_escalated(
    store: &SqliteStore,
    id: NotificationId,
    at: DateTime<Utc>,
) -> Result<bool, StoreError> {
    let claimed = sqlx::query(
        "UPDATE notifications SET escalated_at = ?, updated_at = ? \
         WHERE id = ? AND escalated_at IS NULL",
    )
    .bind(encode_time(at))
    .bind(encode_time(at))
    .bind(id.get())
    .execute(&store.pool)
    .await
    .map_err(backend)?
    .rows_affected()
        > 0;

    if claimed {
        return Ok(true);
    }

    // Nothing was claimed for one of two reasons, and they are not the same answer: another
    // sweep got there first, or there is no such card at all.
    let exists = sqlx::query("SELECT 1 AS present FROM notifications WHERE id = ?")
        .bind(id.get())
        .fetch_optional(&store.pool)
        .await
        .map_err(backend)?
        .is_some();

    if exists {
        Ok(false)
    } else {
        Err(StoreError::no_such_notification(id))
    }
}

/// Every column of `notifications`.
const NOTIFICATION_COLUMNS: &str = "id, dedupe_key, fingerprint, route_id, guild_id, channel_id, message_id, \
     thread_id, state, render_hash, applied_tags, tags_hash, pinned, archived, responded_at, \
     escalated_at, supersedes, resolved_at, reply_count, created_at, updated_at";

/// Inserts a card row and hands back the key a button will carry.
async fn insert_notification(
    conn: &mut SqliteConnection,
    card: &NewNotification,
) -> Result<NotificationId, StoreError> {
    let row = sqlx::query(
        "INSERT INTO notifications \
         (dedupe_key, fingerprint, route_id, guild_id, channel_id, state, supersedes, \
          created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) RETURNING id",
    )
    .bind(card.dedupe_key.as_str().to_owned())
    .bind(card.fingerprint.as_str().to_owned())
    .bind(card.route_id.get())
    .bind(card.guild_id.to_db())
    .bind(card.channel_id.to_db())
    .bind(card.state.as_str())
    .bind(card.supersedes.map(NotificationId::get))
    .bind(encode_time(card.created_at))
    .bind(encode_time(card.created_at))
    .fetch_one(conn)
    .await
    .map_err(backend)?;

    Ok(NotificationId::new(
        row.try_get::<i64, _>("id").map_err(backend)?,
    ))
}

/// Records the alert a card is now showing.
async fn set_card_alert(
    conn: &mut SqliteConnection,
    id: NotificationId,
    fingerprint: &Fingerprint,
    now: DateTime<Utc>,
) -> Result<(), StoreError> {
    let affected =
        sqlx::query("UPDATE notifications SET fingerprint = ?, updated_at = ? WHERE id = ?")
            .bind(fingerprint.as_str().to_owned())
            .bind(encode_time(now))
            .bind(id.get())
            .execute(conn)
            .await
            .map_err(backend)?
            .rows_affected();

    if affected == 0 {
        return Err(StoreError::no_such_notification(id));
    }

    Ok(())
}

/// Moves a replaced card off the key its replacement is about to take.
///
/// Conditional on the card still holding that key. A card that has already given it up, to an
/// orphaning or to another replacement racing this one, is left alone; the insert that follows
/// then meets the unique index and reports the conflict the caller already handles by re-reading.
async fn retire_key(
    conn: &mut SqliteConnection,
    id: NotificationId,
    key: &DedupeKey,
) -> Result<(), StoreError> {
    sqlx::query(
        "UPDATE notifications SET dedupe_key = 'superseded:' || id || ':' || dedupe_key \
         WHERE id = ? AND dedupe_key = ?",
    )
    .bind(id.get())
    .bind(key.as_str().to_owned())
    .execute(conn)
    .await
    .map_err(backend)?;

    Ok(())
}

/// Moves a card to a new state.
async fn move_state(
    conn: &mut SqliteConnection,
    id: NotificationId,
    state: NotificationState,
    now: DateTime<Utc>,
) -> Result<(), StoreError> {
    // The resolution time is kept while the card stays resolved and forgotten the moment it
    // leaves, so it always answers "resolved since when" for the card as it stands.
    let affected = sqlx::query(
        "UPDATE notifications SET state = ?1, updated_at = ?2, resolved_at = CASE \
         WHEN ?1 = 'resolved' THEN COALESCE(resolved_at, ?2) ELSE NULL END WHERE id = ?3",
    )
    .bind(state.as_str())
    .bind(encode_time(now))
    .bind(id.get())
    .execute(conn)
    .await
    .map_err(backend)?
    .rows_affected();

    if affected == 0 {
        return Err(StoreError::no_such_notification(id));
    }

    Ok(())
}

/// Reads every card showing one alert, across every channel it was posted to.
async fn read_cards_for(
    conn: &mut SqliteConnection,
    fingerprint: &Fingerprint,
) -> Result<Vec<Notification>, StoreError> {
    // By the alert a card shows as well as by its key. A card whose identity merges several
    // fingerprints is keyed by none of them, and a card that was replaced keeps showing the alert
    // under a retired key while it is still on somebody's screen; acknowledging answers both. An
    // orphaned row names a message that is gone, so there is nothing of it to re-render.
    let sql = const_format(&[
        "SELECT ",
        NOTIFICATION_COLUMNS,
        " FROM notifications WHERE state <> 'orphaned' AND (fingerprint = ",
    ]);

    QueryBuilder::<Sqlite>::new(sql)
        .push_bind(fingerprint.as_str().to_owned())
        .push(" OR dedupe_key = ")
        .push_bind(DedupeKey::per_alert(fingerprint).as_str().to_owned())
        .push(") ORDER BY id")
        .build()
        .fetch_all(conn)
        .await
        .map_err(backend)?
        .iter()
        .map(convert::notification)
        .collect()
}
