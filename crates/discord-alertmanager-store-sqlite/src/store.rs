//! The trait implementation: one method per use case, one transaction per method.
//!
//! Every method that writes more than one row opens `BEGIN IMMEDIATE` and closes it before
//! returning, so no transaction ever crosses a trait boundary. `SQLite`'s default deferred
//! transaction takes its write lock at the first write rather than at `BEGIN`, which turns two
//! read-then-write transactions into a busy error instead of a queue — and the outbox claim is
//! exactly that shape.

use std::collections::HashSet;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use dam_core::{
    Alert, AlertDelta, AmState, DedupeKey, EventKind, EventSource, Fingerprint, NotificationState,
    Trigger, next_state,
};
use dam_store::{
    AckCommand, AckOutcome, Acknowledgement, AlertQuery, AlertRecord, AppliedEffect, AuditEntry,
    ChannelId, ClaimRequest, Decision, Effect, ForumTag, GuildId, IgnoreId, IgnoreRule,
    IngestBatch, IngestOutcome, NewNotification, NewOutboxItem, Notification, NotificationId,
    OUTBOX_LANES, OutboxId, OutboxItem, Page, PruneReport, RetentionPolicy, Route, RouteId,
    RouteSource, SilenceLifecycle, SilenceLink, SilenceState, Store, StoreError, Subscription,
    SubscriptionId, ThreadReply, Transition, UserId, WorkerId, classify, matches_regex_matchers,
    needs_in_memory_filter, severities_at_or_above, suppression_map,
};
use sqlx::{QueryBuilder, Row, Sqlite, SqliteConnection, Transaction};

use crate::SqliteStore;
use crate::convert;
use crate::convert::{
    acknowledgement, alert_record, backend, encode_json, encode_time, encode_time_opt, forum_tag,
    ignore_rule, notification, outbox_item, route, silence_link,
};

/// Every column of `alerts`, in the order the mapper expects to find them by name.
const ALERT_COLUMNS: &str = "fingerprint, labels_hash, group_key, labels, annotations, starts_at, \
     ends_at, generator_url, status, am_state, severity, silenced_by, inhibited_by, \
     first_seen_at, last_seen_at, resolved_at, flap_count, episode, updated_at";

/// Every column of `notifications`.
const NOTIFICATION_COLUMNS: &str = "id, dedupe_key, fingerprint, route_id, guild_id, channel_id, message_id, \
     thread_id, state, render_hash, applied_tags, tags_hash, pinned, archived, responded_at, \
     escalated_at, supersedes, reply_count, created_at, updated_at";

/// Every column of `outbox`.
const OUTBOX_COLUMNS: &str = "id, lane, kind, dedupe_key, payload, not_before, attempts, claimed_by, claimed_at, \
     last_error, created_at";

/// Every column of `routes`.
const ROUTE_COLUMNS: &str = "id, guild_id, name, matcher_source, min_severity, target, \
     group_strategy, mentions, escalation, priority, continue_to_next, source, enabled, \
     created_by, created_at";

/// Every column of `ignore_rules`.
const IGNORE_COLUMNS: &str = "id, scope, guild_id, channel_id, matcher_source, reason, created_by, \
     created_at, expires_at, revoked_at";

/// Every column of `silences`.
const SILENCE_COLUMNS: &str = "am_id, matchers, starts_at, ends_at, created_by, discord_user_id, \
     origin_message, comment, state, synced_at";

impl SqliteStore {
    /// Opens a write transaction.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::Backend`] when no connection can be acquired.
    async fn write(&self) -> Result<Transaction<'static, Sqlite>, StoreError> {
        self.pool
            .begin_with("BEGIN IMMEDIATE")
            .await
            .map_err(backend)
    }
}

#[async_trait]
impl Store for SqliteStore {
    async fn ingest_batch(&self, batch: &IngestBatch) -> Result<IngestOutcome, StoreError> {
        let mut tx = self.write().await?;
        let mut outcome = IngestOutcome::default();

        for incoming in &batch.alerts {
            // The group key is a property of the delivery, not of the alert, and only the webhook
            // knows it. Carrying it onto the alert here is what lets a per-group route find it
            // later without the pipeline having to remember which source produced the change.
            let mut alert = incoming.clone();
            if alert.group_key.is_none() {
                alert.group_key.clone_from(&batch.group_key);
            }

            let previous = read_alert(&mut tx, &alert.fingerprint).await?;
            let Some(transition) = classify(
                previous.as_ref(),
                &alert,
                batch.received_at,
                self.regroup_window,
            ) else {
                touch_alert(&mut tx, &alert.fingerprint, batch.received_at).await?;
                outcome.duplicates = outcome.duplicates.saturating_add(1);
                continue;
            };

            upsert_alert(&mut tx, &alert, &transition, batch.received_at).await?;

            if self.persist_events {
                append_event(
                    &mut tx,
                    &alert,
                    previous.as_ref(),
                    &transition,
                    batch.source,
                    batch.received_at,
                )
                .await?;
            }

            outcome.deltas.push(AlertDelta {
                kind: transition.kind,
                source: batch.source,
                alert,
                flap_count: transition.flap_count,
                episode: transition.episode,
                observed_at: batch.received_at,
            });
        }

        tx.commit().await.map_err(backend)?;

        Ok(outcome)
    }

    async fn alert(&self, fingerprint: &Fingerprint) -> Result<Option<AlertRecord>, StoreError> {
        let sql = const_format(&[
            "SELECT ",
            ALERT_COLUMNS,
            " FROM alerts WHERE fingerprint = ",
        ]);

        let row = QueryBuilder::<Sqlite>::new(sql)
            .push_bind(fingerprint.as_str().to_owned())
            .build()
            .fetch_optional(&self.pool)
            .await
            .map_err(backend)?;

        row.as_ref().map(alert_record).transpose()
    }

    async fn query_alerts(&self, query: &AlertQuery) -> Result<Page<AlertRecord>, StoreError> {
        // Compiling every matcher first is what turns a malformed one into a refusal rather than
        // into a filter that quietly matches more than it was asked to.
        for matcher in &query.matchers {
            matcher.compile().map_err(|error| StoreError::Decode {
                kind: "query matcher",
                detail: error.to_string(),
            })?;
        }

        if needs_in_memory_filter(query) {
            return self.scan_alerts(query).await;
        }

        let mut counter = QueryBuilder::<Sqlite>::new("SELECT count(*) FROM alerts WHERE 1 = 1");
        push_alert_filter(&mut counter, query);
        let total: i64 = counter
            .build_query_scalar()
            .fetch_one(&self.pool)
            .await
            .map_err(backend)?;

        let mut reader = QueryBuilder::<Sqlite>::new(const_format(&[
            "SELECT ",
            ALERT_COLUMNS,
            " FROM alerts WHERE 1 = 1",
        ]));
        push_alert_filter(&mut reader, query);
        reader.push(" ORDER BY last_seen_at DESC, fingerprint LIMIT ");
        reader.push_bind(i64::from(query.limit));
        reader.push(" OFFSET ");
        reader.push_bind(i64::from(query.offset));

        let rows = reader
            .build()
            .fetch_all(&self.pool)
            .await
            .map_err(backend)?;

        Ok(Page {
            items: rows
                .iter()
                .map(alert_record)
                .collect::<Result<Vec<_>, _>>()?,
            total: u64::try_from(total).unwrap_or(0),
            offset: query.offset,
            limit: query.limit,
        })
    }

    async fn firing_not_in(
        &self,
        present: &[Fingerprint],
        cutoff: DateTime<Utc>,
    ) -> Result<Vec<AlertRecord>, StoreError> {
        let sql = const_format(&[
            "SELECT ",
            ALERT_COLUMNS,
            " FROM alerts WHERE status = 'firing' AND last_seen_at < ",
        ]);

        let rows = QueryBuilder::<Sqlite>::new(sql)
            .push_bind(encode_time(cutoff))
            .build()
            .fetch_all(&self.pool)
            .await
            .map_err(backend)?;

        // The set Alertmanager reported is subtracted here rather than in a `NOT IN` list: a poll
        // of a busy Alertmanager carries thousands of fingerprints, and binding them all would
        // cost more than filtering a firing set that is already bounded by the cutoff.
        let present: HashSet<&str> = present.iter().map(Fingerprint::as_str).collect();
        let mut missing = Vec::new();

        for row in &rows {
            let record = alert_record(row)?;

            if !present.contains(record.fingerprint().as_str()) {
                missing.push(record);
            }
        }

        Ok(missing)
    }

    async fn apply_decision(&self, decision: &Decision) -> Result<Vec<NotificationId>, StoreError> {
        let mut tx = self.write().await?;
        let mut created = Vec::with_capacity(decision.new_cards.len());

        for planned in &decision.new_cards {
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

    async fn enqueue_effects(
        &self,
        items: &[NewOutboxItem],
        at: DateTime<Utc>,
    ) -> Result<(), StoreError> {
        let mut tx = self.write().await?;

        for item in items {
            enqueue(&mut tx, item, at).await?;
        }

        tx.commit().await.map_err(backend)
    }

    async fn claim_outbox(
        &self,
        worker: &WorkerId,
        request: ClaimRequest,
        now: DateTime<Utc>,
    ) -> Result<Vec<OutboxItem>, StoreError> {
        let mut tx = self.write().await?;

        let mut selector = QueryBuilder::<Sqlite>::new(
            "SELECT id FROM outbox WHERE claimed_at IS NULL AND not_before <= ",
        );
        selector.push_bind(encode_time(now));

        if let Some(lane) = request.lane {
            selector.push(" AND lane % ");
            selector.push_bind(i64::from(lane.of));
            selector.push(" = ");
            selector.push_bind(i64::from(lane.index));
        }

        selector.push(" ORDER BY not_before, id LIMIT ");
        selector.push_bind(i64::from(request.limit));

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
        let mut claimer = QueryBuilder::<Sqlite>::new("UPDATE outbox SET claimed_by = ");
        claimer.push_bind(worker.as_str().to_owned());
        claimer.push(", claimed_at = ");
        claimer.push_bind(encode_time(now));
        claimer.push(", attempts = attempts + 1 WHERE id IN ");
        push_id_list(&mut claimer, &ids);
        claimer.build().execute(&mut *tx).await.map_err(backend)?;

        let mut reader = QueryBuilder::<Sqlite>::new(const_format(&[
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

    async fn complete_outbox(
        &self,
        worker: &WorkerId,
        id: OutboxId,
        applied: &AppliedEffect,
    ) -> Result<(), StoreError> {
        let now = Utc::now();
        let mut tx = self.write().await?;
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
                sqlx::query("UPDATE silences SET state = 'expired', synced_at = ? WHERE am_id = ?")
                    .bind(encode_time(now))
                    .bind(am_id.clone())
                    .execute(&mut *tx)
                    .await
                    .map_err(backend)?;
            }
            _ => {}
        }

        sqlx::query("DELETE FROM outbox WHERE id = ?")
            .bind(id.get())
            .execute(&mut *tx)
            .await
            .map_err(backend)?;

        tx.commit().await.map_err(backend)
    }

    async fn fail_outbox(
        &self,
        worker: &WorkerId,
        id: OutboxId,
        error: &str,
        retry_at: Option<DateTime<Utc>>,
    ) -> Result<(), StoreError> {
        let mut tx = self.write().await?;
        take_claim(&mut tx, worker, id).await?;

        match retry_at {
            Some(retry_at) => {
                sqlx::query(
                    "UPDATE outbox SET claimed_by = NULL, claimed_at = NULL, not_before = ?, \
                     last_error = ? WHERE id = ?",
                )
                .bind(encode_time(retry_at))
                .bind(error.to_owned())
                .bind(id.get())
                .execute(&mut *tx)
                .await
                .map_err(backend)?;
            }
            // Abandoned rather than parked: a row nobody will ever claim again is queue depth
            // that never drains, and the dispatcher has already logged and audited the failure.
            None => {
                sqlx::query("DELETE FROM outbox WHERE id = ?")
                    .bind(id.get())
                    .execute(&mut *tx)
                    .await
                    .map_err(backend)?;
            }
        }

        tx.commit().await.map_err(backend)
    }

    async fn reclaim_expired(
        &self,
        older_than: DateTime<Utc>,
        now: DateTime<Utc>,
    ) -> Result<u64, StoreError> {
        let result = sqlx::query(
            "UPDATE outbox SET claimed_by = NULL, claimed_at = NULL, not_before = ? \
             WHERE claimed_at IS NOT NULL AND claimed_at < ?",
        )
        .bind(encode_time(now))
        .bind(encode_time(older_than))
        .execute(&self.pool)
        .await
        .map_err(backend)?;

        Ok(result.rows_affected())
    }

    async fn outbox_depth(&self) -> Result<Vec<(String, u64)>, StoreError> {
        let rows = sqlx::query(
            "SELECT kind, count(*) AS depth FROM outbox WHERE claimed_at IS NULL GROUP BY kind \
             ORDER BY kind",
        )
        .fetch_all(&self.pool)
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

    async fn acknowledge(&self, command: &AckCommand) -> Result<AckOutcome, StoreError> {
        let mut tx = self.write().await?;
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

    async fn acknowledgement(
        &self,
        fingerprint: &Fingerprint,
    ) -> Result<Option<Acknowledgement>, StoreError> {
        let row = sqlx::query(
            "SELECT user_id, kind, note, created_at FROM acknowledgements              WHERE fingerprint = ? AND revoked_at IS NULL",
        )
        .bind(fingerprint.as_str().to_owned())
        .fetch_optional(&self.pool)
        .await
        .map_err(backend)?;

        row.as_ref().map(acknowledgement).transpose()
    }

    async fn record_reply(&self, reply: &ThreadReply) -> Result<Option<Notification>, StoreError> {
        let mut tx = self.write().await?;

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

        let mut card = notification(&row)?;
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

    async fn notification_for(
        &self,
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
            .fetch_optional(&self.pool)
            .await
            .map_err(backend)?;

        row.as_ref().map(notification).transpose()
    }

    async fn notification(&self, id: NotificationId) -> Result<Option<Notification>, StoreError> {
        let sql = const_format(&[
            "SELECT ",
            NOTIFICATION_COLUMNS,
            " FROM notifications WHERE id = ",
        ]);

        let row = QueryBuilder::<Sqlite>::new(sql)
            .push_bind(id.get())
            .build()
            .fetch_optional(&self.pool)
            .await
            .map_err(backend)?;

        row.as_ref().map(notification).transpose()
    }

    async fn set_notification_state(
        &self,
        id: NotificationId,
        state: NotificationState,
        now: DateTime<Utc>,
    ) -> Result<(), StoreError> {
        let mut tx = self.write().await?;
        move_state(&mut tx, id, state, now).await?;

        tx.commit().await.map_err(backend)
    }

    async fn orphan_notification(
        &self,
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
        .execute(&self.pool)
        .await
        .map_err(backend)?
        .rows_affected();

        if affected == 0 {
            return Err(StoreError::no_such_notification(id));
        }

        Ok(())
    }

    async fn pending_escalations(
        &self,
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
            .fetch_all(&self.pool)
            .await
            .map_err(backend)?
            .iter()
            .map(notification)
            .collect()
    }

    async fn mark_escalated(
        &self,
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
        .execute(&self.pool)
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
            .fetch_optional(&self.pool)
            .await
            .map_err(backend)?
            .is_some();

        if exists {
            Ok(false)
        } else {
            Err(StoreError::no_such_notification(id))
        }
    }

    async fn record_silence(&self, link: &SilenceLink) -> Result<(), StoreError> {
        let mut tx = self.write().await?;
        write_silence(&mut tx, link).await?;

        tx.commit().await.map_err(backend)
    }

    async fn sync_silences(
        &self,
        snapshot: &[SilenceState],
        now: DateTime<Utc>,
    ) -> Result<Vec<AlertDelta>, StoreError> {
        let mut tx = self.write().await?;
        let stamp = encode_time(now);

        for silence in snapshot {
            sqlx::query(
                "UPDATE silences SET state = ?, ends_at = ?, synced_at = ? WHERE am_id = ?",
            )
            .bind(silence.state.as_str())
            .bind(encode_time(silence.ends_at))
            .bind(stamp.clone())
            .bind(silence.am_id.clone())
            .execute(&mut *tx)
            .await
            .map_err(backend)?;
        }

        let desired = suppression_map(snapshot);

        // Only two kinds of alert can have changed: one Alertmanager now suppresses, and one this
        // database still thinks is suppressed. Reading the whole table to find them would make the
        // syncer's cost a function of history rather than of what is silenced.
        let sql = const_format(&[
            "SELECT ",
            ALERT_COLUMNS,
            " FROM alerts WHERE silenced_by <> '[]'",
        ]);
        let mut candidates: Vec<AlertRecord> = QueryBuilder::<Sqlite>::new(sql)
            .build()
            .fetch_all(&mut *tx)
            .await
            .map_err(backend)?
            .iter()
            .map(alert_record)
            .collect::<Result<_, _>>()?;

        let known: HashSet<String> = candidates
            .iter()
            .map(|record| record.fingerprint().as_str().to_owned())
            .collect();

        for fingerprint in desired.keys() {
            if !known.contains(fingerprint.as_str())
                && let Some(record) = read_alert(&mut tx, fingerprint).await?
            {
                candidates.push(record);
            }
        }

        let mut deltas = Vec::new();

        for mut record in candidates {
            let ids = desired
                .get(record.fingerprint())
                .cloned()
                .unwrap_or_default();

            if ids == record.alert.silenced_by {
                continue;
            }

            let was_suppressed = !record.alert.silenced_by.is_empty();
            let is_suppressed = !ids.is_empty();

            record.alert.silenced_by = ids;
            record.alert.am_state = if is_suppressed || !record.alert.inhibited_by.is_empty() {
                AmState::Suppressed
            } else {
                AmState::Active
            };

            sqlx::query(
                "UPDATE alerts SET silenced_by = ?, am_state = ?, updated_at = ? \
                 WHERE fingerprint = ?",
            )
            .bind(encode_json(&record.alert.silenced_by))
            .bind(record.alert.am_state.as_str())
            .bind(stamp.clone())
            .bind(record.fingerprint().as_str().to_owned())
            .execute(&mut *tx)
            .await
            .map_err(backend)?;

            // A silence replaced by another silence leaves the alert suppressed throughout, and a
            // card that says "silenced" both before and after has nothing to show for the change.
            let kind = match (was_suppressed, is_suppressed) {
                (false, true) => EventKind::Silenced,
                (true, false) => EventKind::Unsilenced,
                _ => continue,
            };

            deltas.push(AlertDelta {
                kind,
                source: EventSource::Reconciler,
                flap_count: record.flap_count,
                episode: record.episode,
                alert: record.alert,
                observed_at: now,
            });
        }

        tx.commit().await.map_err(backend)?;

        Ok(deltas)
    }

    async fn silences(&self, active_only: bool) -> Result<Vec<SilenceLink>, StoreError> {
        let mut builder = QueryBuilder::<Sqlite>::new(const_format(&[
            "SELECT ",
            SILENCE_COLUMNS,
            " FROM silences",
        ]));

        if active_only {
            builder.push(" WHERE state = 'active'");
        }

        builder.push(" ORDER BY starts_at DESC");

        builder
            .build()
            .fetch_all(&self.pool)
            .await
            .map_err(backend)?
            .iter()
            .map(silence_link)
            .collect()
    }

    async fn upsert_ignore(&self, rule: &IgnoreRule) -> Result<IgnoreId, StoreError> {
        let mut tx = self.write().await?;

        let id = if rule.id.get() == 0 {
            let row = sqlx::query(
                "INSERT INTO ignore_rules (scope, guild_id, channel_id, matcher_source, reason, \
                 created_by, created_at, expires_at, revoked_at) \
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) RETURNING id",
            )
            .bind(rule.scope.as_str())
            .bind(rule.guild_id.to_db())
            .bind(rule.channel_id.map(dam_store::ChannelId::to_db))
            .bind(rule.matcher_source.clone())
            .bind(rule.reason.clone())
            .bind(rule.created_by.to_db())
            .bind(encode_time(rule.created_at))
            .bind(encode_time_opt(rule.expires_at))
            .bind(encode_time_opt(rule.revoked_at))
            .fetch_one(&mut *tx)
            .await
            .map_err(backend)?;

            IgnoreId::new(row.try_get::<i64, _>("id").map_err(backend)?)
        } else {
            sqlx::query(
                "UPDATE ignore_rules SET scope = ?, guild_id = ?, channel_id = ?, \
                 matcher_source = ?, reason = ?, expires_at = ?, revoked_at = ? WHERE id = ?",
            )
            .bind(rule.scope.as_str())
            .bind(rule.guild_id.to_db())
            .bind(rule.channel_id.map(dam_store::ChannelId::to_db))
            .bind(rule.matcher_source.clone())
            .bind(rule.reason.clone())
            .bind(encode_time_opt(rule.expires_at))
            .bind(encode_time_opt(rule.revoked_at))
            .bind(rule.id.get())
            .execute(&mut *tx)
            .await
            .map_err(backend)?;

            rule.id
        };

        tx.commit().await.map_err(backend)?;

        Ok(id)
    }

    async fn revoke_ignore(
        &self,
        id: IgnoreId,
        guild: GuildId,
        now: DateTime<Utc>,
    ) -> Result<(), StoreError> {
        let affected = sqlx::query(
            "UPDATE ignore_rules SET revoked_at = ? \
             WHERE id = ? AND guild_id = ? AND revoked_at IS NULL",
        )
        .bind(encode_time(now))
        .bind(id.get())
        .bind(guild.to_db())
        .execute(&self.pool)
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

    async fn active_ignores(
        &self,
        guild: GuildId,
        now: DateTime<Utc>,
    ) -> Result<Vec<IgnoreRule>, StoreError> {
        let sql = const_format(&[
            "SELECT ",
            IGNORE_COLUMNS,
            " FROM ignore_rules WHERE guild_id = ",
        ]);

        QueryBuilder::<Sqlite>::new(sql)
            .push_bind(guild.to_db())
            .push(" AND revoked_at IS NULL AND (expires_at IS NULL OR expires_at > ")
            .push_bind(encode_time(now))
            .push(") ORDER BY id")
            .build()
            .fetch_all(&self.pool)
            .await
            .map_err(backend)?
            .iter()
            .map(ignore_rule)
            .collect()
    }

    async fn upsert_route(&self, route: &Route) -> Result<RouteId, StoreError> {
        let mut tx = self.write().await?;
        let min_severity = route.min_severity.map(dam_core::Severity::as_str);

        let id = if route.id.get() != 0 {
            sqlx::query(
                "UPDATE routes SET guild_id = ?, name = ?, matcher_source = ?, min_severity = ?, \
                 target = ?, group_strategy = ?, mentions = ?, escalation = ?, priority = ?, \
                 continue_to_next = ?, source = ?, enabled = ?, created_by = ? WHERE id = ?",
            )
            .bind(route.guild_id.to_db())
            .bind(route.name.clone())
            .bind(route.matcher_source.clone())
            .bind(min_severity)
            .bind(encode_json(&route.target))
            .bind(route.group_strategy.as_str())
            .bind(encode_json(&route.mentions))
            .bind(route.escalation.as_ref().map(encode_json))
            .bind(i64::from(route.priority))
            .bind(i64::from(route.continue_to_next))
            .bind(route.source.as_str())
            .bind(i64::from(route.enabled))
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

            let mut builder = QueryBuilder::<Sqlite>::new(
                "INSERT INTO routes (guild_id, name, matcher_source, min_severity, target, \
                 group_strategy, mentions, escalation, priority, continue_to_next, source, \
                 enabled, created_by, created_at) VALUES (",
            );
            let mut values = builder.separated(", ");
            values.push_bind(route.guild_id.to_db());
            values.push_bind(route.name.clone());
            values.push_bind(route.matcher_source.clone());
            values.push_bind(min_severity);
            values.push_bind(encode_json(&route.target));
            values.push_bind(route.group_strategy.as_str());
            values.push_bind(encode_json(&route.mentions));
            values.push_bind(route.escalation.as_ref().map(encode_json));
            values.push_bind(i64::from(route.priority));
            values.push_bind(i64::from(route.continue_to_next));
            values.push_bind(route.source.as_str());
            values.push_bind(i64::from(route.enabled));
            values.push_bind(route.created_by.map(dam_store::UserId::to_db));
            values.push_bind(encode_time(route.created_at));
            builder.push(") ");
            builder.push(conflict);
            builder.push("RETURNING id");

            let row = builder.build().fetch_one(&mut *tx).await.map_err(backend)?;

            RouteId::new(row.try_get::<i64, _>("id").map_err(backend)?)
        };

        tx.commit().await.map_err(backend)?;

        Ok(id)
    }

    async fn routes(&self) -> Result<Vec<Route>, StoreError> {
        let sql = const_format(&[
            "SELECT ",
            ROUTE_COLUMNS,
            " FROM routes ORDER BY priority, id",
        ]);

        QueryBuilder::<Sqlite>::new(sql)
            .build()
            .fetch_all(&self.pool)
            .await
            .map_err(backend)?
            .iter()
            .map(route)
            .collect()
    }

    async fn disable_missing_config_routes(&self, keep: &[String]) -> Result<u64, StoreError> {
        let mut builder = QueryBuilder::<Sqlite>::new(
            "UPDATE routes SET enabled = 0 WHERE source = 'config' AND enabled = 1",
        );

        if !keep.is_empty() {
            builder.push(" AND name NOT IN (");
            let mut names = builder.separated(", ");
            for name in keep {
                names.push_bind(name.clone());
            }
            builder.push(")");
        }

        let result = builder.build().execute(&self.pool).await.map_err(backend)?;

        Ok(result.rows_affected())
    }

    async fn upsert_subscription(
        &self,
        subscription: &Subscription,
    ) -> Result<SubscriptionId, StoreError> {
        let mut tx = self.write().await?;
        let min_severity = subscription.min_severity.map(dam_core::Severity::as_str);

        let id = if subscription.id.get() == 0 {
            let row = sqlx::query(
                "INSERT INTO subscriptions (user_id, matcher_source, min_severity, created_at)                  VALUES (?, ?, ?, ?) RETURNING id",
            )
            .bind(subscription.user_id.to_db())
            .bind(subscription.matcher_source.clone())
            .bind(min_severity)
            .bind(encode_time(subscription.created_at))
            .fetch_one(&mut *tx)
            .await
            .map_err(backend)?;

            SubscriptionId::new(row.try_get::<i64, _>("id").map_err(backend)?)
        } else {
            // The owner is in the predicate, not only in the key: an update that trusted the id
            // alone would let one person rewrite another's subscription by guessing a number.
            let changed = sqlx::query(
                "UPDATE subscriptions SET matcher_source = ?, min_severity = ?                  WHERE id = ? AND user_id = ?",
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

    async fn subscriptions(&self) -> Result<Vec<Subscription>, StoreError> {
        sqlx::query(
            "SELECT id, user_id, matcher_source, min_severity, created_at FROM subscriptions              ORDER BY id",
        )
        .fetch_all(&self.pool)
        .await
        .map_err(backend)?
        .iter()
        .map(convert::subscription)
        .collect()
    }

    async fn remove_subscription(
        &self,
        id: SubscriptionId,
        user: UserId,
    ) -> Result<(), StoreError> {
        let mut tx = self.write().await?;

        let removed = sqlx::query("DELETE FROM subscriptions WHERE id = ? AND user_id = ?")
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

    async fn sync_forum_tags(
        &self,
        channel: ChannelId,
        tags: &[ForumTag],
    ) -> Result<(), StoreError> {
        let mut tx = self.write().await?;

        // Replaced rather than merged: Discord's list is the whole truth about a channel's tags,
        // and a tag a human deleted has to leave the cache or the next apply fails on an id that
        // no longer exists.
        sqlx::query("DELETE FROM forum_tags WHERE channel_id = ?")
            .bind(channel.to_db())
            .execute(&mut *tx)
            .await
            .map_err(backend)?;

        for tag in tags {
            sqlx::query(
                "INSERT INTO forum_tags (channel_id, tag_name, tag_id, moderated, synced_at) \
                 VALUES (?, ?, ?, ?, ?)",
            )
            .bind(channel.to_db())
            .bind(tag.name.clone())
            .bind(tag.id.to_db())
            .bind(i64::from(tag.moderated))
            .bind(encode_time(tag.synced_at))
            .execute(&mut *tx)
            .await
            .map_err(backend)?;
        }

        tx.commit().await.map_err(backend)
    }

    async fn forum_tags(&self, channel: ChannelId) -> Result<Vec<ForumTag>, StoreError> {
        sqlx::query(
            "SELECT channel_id, tag_name, tag_id, moderated, synced_at FROM forum_tags \
             WHERE channel_id = ? ORDER BY tag_name",
        )
        .bind(channel.to_db())
        .fetch_all(&self.pool)
        .await
        .map_err(backend)?
        .iter()
        .map(forum_tag)
        .collect()
    }

    async fn append_audit(&self, entry: &AuditEntry) -> Result<(), StoreError> {
        sqlx::query(
            "INSERT INTO audit_log (actor, guild_id, action, subject, detail, result, created_at) \
             VALUES (?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(entry.actor.map(dam_store::UserId::to_db))
        .bind(entry.guild_id.map(dam_store::GuildId::to_db))
        .bind(entry.action.clone())
        .bind(entry.subject.clone())
        .bind(encode_json(&entry.detail))
        .bind(entry.result.as_str())
        .bind(encode_time(entry.at))
        .execute(&self.pool)
        .await
        .map_err(backend)?;

        Ok(())
    }

    async fn prune(
        &self,
        policy: &RetentionPolicy,
        now: DateTime<Utc>,
    ) -> Result<PruneReport, StoreError> {
        let limit = i64::from(policy.batch_limit);
        let mut tx = self.write().await?;

        let events = delete_batch(
            &mut tx,
            "DELETE FROM alert_events WHERE id IN \
             (SELECT id FROM alert_events WHERE received_at < ? ORDER BY id LIMIT ?)",
            encode_time(now - policy.events),
            limit,
        )
        .await?;

        let notifications = delete_batch(
            &mut tx,
            "DELETE FROM notifications WHERE id IN \
             (SELECT id FROM notifications WHERE state = 'resolved' AND updated_at < ? \
              ORDER BY id LIMIT ?)",
            encode_time(now - policy.resolved),
            limit,
        )
        .await?;

        let alerts = delete_batch(
            &mut tx,
            "DELETE FROM alerts WHERE fingerprint IN \
             (SELECT fingerprint FROM alerts WHERE status = 'resolved' AND resolved_at < ? \
              ORDER BY fingerprint LIMIT ?)",
            encode_time(now - policy.resolved),
            limit,
        )
        .await?;

        let audit = delete_batch(
            &mut tx,
            "DELETE FROM audit_log WHERE id IN \
             (SELECT id FROM audit_log WHERE created_at < ? ORDER BY id LIMIT ?)",
            encode_time(now - policy.audit),
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

    async fn health(&self) -> Result<(), StoreError> {
        sqlx::query("SELECT 1")
            .fetch_one(&self.pool)
            .await
            .map_err(backend)?;

        Ok(())
    }
}

/// Reads one alert row inside a transaction.
async fn read_alert(
    conn: &mut SqliteConnection,
    fingerprint: &Fingerprint,
) -> Result<Option<AlertRecord>, StoreError> {
    let sql = const_format(&[
        "SELECT ",
        ALERT_COLUMNS,
        " FROM alerts WHERE fingerprint = ",
    ]);

    let row = QueryBuilder::<Sqlite>::new(sql)
        .push_bind(fingerprint.as_str().to_owned())
        .build()
        .fetch_optional(conn)
        .await
        .map_err(backend)?;

    row.as_ref().map(alert_record).transpose()
}

/// Moves `last_seen_at` for a delivery that changed nothing else.
///
/// A redelivery still says the alert is there, and the reconciler's "firing here, gone from
/// Alertmanager" test reads exactly this column. Skipping the write would make a steady stream of
/// duplicates look like an alert nobody has seen.
async fn touch_alert(
    conn: &mut SqliteConnection,
    fingerprint: &Fingerprint,
    at: DateTime<Utc>,
) -> Result<(), StoreError> {
    sqlx::query("UPDATE alerts SET last_seen_at = ? WHERE fingerprint = ?")
        .bind(encode_time(at))
        .bind(fingerprint.as_str().to_owned())
        .execute(conn)
        .await
        .map_err(backend)?;

    Ok(())
}

/// Writes the current state of one alert.
async fn upsert_alert(
    conn: &mut SqliteConnection,
    alert: &Alert,
    transition: &Transition,
    at: DateTime<Utc>,
) -> Result<(), StoreError> {
    sqlx::query(
        "INSERT INTO alerts (fingerprint, labels_hash, group_key, labels, annotations, starts_at, \
         ends_at, generator_url, status, am_state, severity, silenced_by, inhibited_by, \
         first_seen_at, last_seen_at, resolved_at, flap_count, episode, updated_at) \
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) \
         ON CONFLICT (fingerprint) DO UPDATE SET \
         labels_hash = excluded.labels_hash, group_key = excluded.group_key, \
         labels = excluded.labels, annotations = excluded.annotations, \
         starts_at = excluded.starts_at, ends_at = excluded.ends_at, \
         generator_url = excluded.generator_url, status = excluded.status, \
         am_state = excluded.am_state, severity = excluded.severity, \
         silenced_by = excluded.silenced_by, inhibited_by = excluded.inhibited_by, \
         last_seen_at = excluded.last_seen_at, resolved_at = excluded.resolved_at, \
         flap_count = excluded.flap_count, episode = excluded.episode, \
         updated_at = excluded.updated_at",
    )
    .bind(alert.fingerprint.as_str().to_owned())
    .bind(alert.labels_hash().as_str().to_owned())
    .bind(alert.group_key.as_ref().map(|key| key.as_str().to_owned()))
    .bind(encode_json(&alert.labels))
    .bind(encode_json(&alert.annotations))
    .bind(encode_time(alert.starts_at))
    .bind(encode_time_opt(alert.ends_at))
    .bind(alert.generator_url.clone())
    .bind(alert.status.as_str())
    .bind(alert.am_state.as_str())
    .bind(alert.severity().as_str())
    .bind(encode_json(&alert.silenced_by))
    .bind(encode_json(&alert.inhibited_by))
    .bind(encode_time(transition.first_seen_at))
    .bind(encode_time(at))
    .bind(encode_time_opt(transition.resolved_at))
    .bind(i64::from(transition.flap_count))
    .bind(i64::from(transition.episode))
    .bind(encode_time(at))
    .execute(conn)
    .await
    .map_err(backend)?;

    Ok(())
}

/// Appends one history row, discarding an exact repeat.
///
/// The payload is trimmed: a transition keeps the fields that moved, and an update keeps only the
/// annotations whose values changed. The label set is already on the `alerts` row, and copying it
/// per event is what makes this table dominate storage without buying anything.
async fn append_event(
    conn: &mut SqliteConnection,
    alert: &Alert,
    previous: Option<&AlertRecord>,
    transition: &Transition,
    source: EventSource,
    at: DateTime<Utc>,
) -> Result<(), StoreError> {
    let payload = if transition.kind == EventKind::Updated {
        serde_json::json!({
            "annotations": previous.map_or_else(
                || alert.annotations.clone(),
                |previous| alert.annotations.changed_from(&previous.alert.annotations),
            ),
        })
    } else {
        serde_json::json!({
            "status": alert.status.as_str(),
            "am_state": alert.am_state.as_str(),
            "severity": alert.severity().as_str(),
            "silenced_by": alert.silenced_by,
        })
    };

    sqlx::query(
        "INSERT INTO alert_events \
         (fingerprint, kind, source, starts_at, ends_at, payload, received_at) \
         VALUES (?, ?, ?, ?, ?, ?, ?) \
         ON CONFLICT (fingerprint, kind, starts_at, \n         COALESCE(ends_at, '1970-01-01T00:00:00.000000Z')) DO NOTHING",
    )
    .bind(alert.fingerprint.as_str().to_owned())
    .bind(transition.kind.as_str())
    .bind(source.as_str())
    .bind(encode_time(alert.starts_at))
    .bind(encode_time_opt(alert.ends_at))
    .bind(encode_json(&payload))
    .bind(encode_time(at))
    .execute(conn)
    .await
    .map_err(backend)?;

    Ok(())
}

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

/// Queues one effect, folding it into a pending one where that loses nothing.
///
/// Two queued edits of one card are one edit of its current state; two queued thread notes are two
/// different sentences. The fold keeps the earlier `not_before`, so a sustained stream of changes
/// still produces an edit one debounce after the first of them rather than never.
async fn enqueue(
    conn: &mut SqliteConnection,
    item: &NewOutboxItem,
    at: DateTime<Utc>,
) -> Result<(), StoreError> {
    let kind = item.effect.kind();
    let payload = encode_json(&item.effect);
    let key = item.dedupe_key.as_str().to_owned();

    if item.effect.is_coalescable() {
        let folded = sqlx::query(
            "UPDATE outbox SET payload = ?, not_before = min(not_before, ?) \
             WHERE claimed_at IS NULL AND kind = ? AND dedupe_key = ?",
        )
        .bind(payload.clone())
        .bind(encode_time(item.not_before))
        .bind(kind)
        .bind(key.clone())
        .execute(&mut *conn)
        .await
        .map_err(backend)?
        .rows_affected();

        if folded > 0 {
            return Ok(());
        }
    }

    sqlx::query(
        "INSERT INTO outbox (lane, kind, dedupe_key, payload, not_before, created_at) \
         VALUES (?, ?, ?, ?, ?, ?)",
    )
    .bind(i64::from(item.dedupe_key.lane(OUTBOX_LANES)))
    .bind(kind)
    .bind(key.clone())
    .bind(payload.clone())
    .bind(encode_time(item.not_before))
    .bind(encode_time(at))
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
    let row = sqlx::query("SELECT payload, claimed_by FROM outbox WHERE id = ?")
        .bind(id.get())
        .fetch_optional(&mut *conn)
        .await
        .map_err(backend)?
        .ok_or(StoreError::LeaseLost { id })?;

    let claimed_by: Option<String> = row.try_get("claimed_by").map_err(backend)?;
    if claimed_by.as_deref() != Some(worker.as_str()) {
        return Err(StoreError::LeaseLost { id });
    }

    let payload: String = row.try_get("payload").map_err(backend)?;

    serde_json::from_str(&payload).map_err(|error| StoreError::Decode {
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
    let mut builder = QueryBuilder::<Sqlite>::new("UPDATE notifications SET updated_at = ");
    builder.push_bind(encode_time(now));

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
        builder.push_bind(encode_json(tags));
    }
    if let Some(hash) = applied.tags_hash.as_ref() {
        builder.push(", tags_hash = ");
        builder.push_bind(hash.clone());
    }
    if let Some(pinned) = applied.pinned {
        builder.push(", pinned = ");
        builder.push_bind(i64::from(pinned));
    }
    if let Some(archived) = applied.archived {
        builder.push(", archived = ");
        builder.push_bind(i64::from(archived));
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

/// Moves a card to a new state.
async fn move_state(
    conn: &mut SqliteConnection,
    id: NotificationId,
    state: NotificationState,
    now: DateTime<Utc>,
) -> Result<(), StoreError> {
    let affected = sqlx::query("UPDATE notifications SET state = ?, updated_at = ? WHERE id = ?")
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

/// Reads every card carrying one dedupe key, across every channel it was posted to.
async fn read_cards_for(
    conn: &mut SqliteConnection,
    fingerprint: &Fingerprint,
) -> Result<Vec<Notification>, StoreError> {
    // Every episode, not just the current one. Acknowledging answers the alert wherever it is
    // shown, and after a re-fire that is spread across two keys: the card for the episode that
    // just started, and the card for the one before it that is still on somebody's screen.
    let (current, prefix) = DedupeKey::per_alert_episodes(fingerprint);

    let sql = const_format(&[
        "SELECT ",
        NOTIFICATION_COLUMNS,
        " FROM notifications WHERE dedupe_key = ",
    ]);

    QueryBuilder::<Sqlite>::new(sql)
        .push_bind(current.as_str().to_owned())
        .push(" OR dedupe_key LIKE ")
        .push_bind(format!("{prefix}%"))
        .push(" ORDER BY id")
        .build()
        .fetch_all(conn)
        .await
        .map_err(backend)?
        .iter()
        .map(notification)
        .collect()
}

/// Writes the bot's record of one Alertmanager silence.
async fn write_silence(conn: &mut SqliteConnection, link: &SilenceLink) -> Result<(), StoreError> {
    sqlx::query(
        "INSERT INTO silences (am_id, matchers, starts_at, ends_at, created_by, discord_user_id, \
         origin_message, comment, state, synced_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?) \
         ON CONFLICT (am_id) DO UPDATE SET matchers = excluded.matchers, \
         starts_at = excluded.starts_at, ends_at = excluded.ends_at, \
         created_by = excluded.created_by, discord_user_id = excluded.discord_user_id, \
         origin_message = excluded.origin_message, comment = excluded.comment, \
         state = excluded.state, synced_at = excluded.synced_at",
    )
    .bind(link.am_id.clone())
    .bind(link.matchers.clone())
    .bind(encode_time(link.starts_at))
    .bind(encode_time(link.ends_at))
    .bind(link.created_by.clone())
    .bind(link.discord_user_id.map(dam_store::UserId::to_db))
    .bind(link.origin_message.clone())
    .bind(link.comment.clone())
    .bind(link.state.as_str())
    .bind(encode_time(link.synced_at))
    .execute(conn)
    .await
    .map_err(backend)?;

    Ok(())
}

/// Runs one bounded delete and reports how many rows it took.
async fn delete_batch(
    conn: &mut SqliteConnection,
    sql: &'static str,
    cutoff: String,
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

impl SqliteStore {
    /// Answers a query whose regex matchers SQL cannot express.
    ///
    /// The other predicates still run in the database; what comes back is filtered, counted and
    /// paginated here. The read is capped, because a regex that matches everything would
    /// otherwise pull the whole table into the process to serve one page of it.
    async fn scan_alerts(&self, query: &AlertQuery) -> Result<Page<AlertRecord>, StoreError> {
        let mut builder = QueryBuilder::<Sqlite>::new(const_format(&[
            "SELECT ",
            ALERT_COLUMNS,
            " FROM alerts WHERE 1 = 1",
        ]));
        push_alert_filter(&mut builder, query);
        builder.push(" ORDER BY last_seen_at DESC, fingerprint LIMIT ");
        builder.push_bind(i64::from(dam_store::IN_MEMORY_SCAN_LIMIT));

        let matched: Vec<AlertRecord> = builder
            .build()
            .fetch_all(&self.pool)
            .await
            .map_err(backend)?
            .iter()
            .map(alert_record)
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .filter(|record| matches_regex_matchers(record, query))
            .collect();

        let total = u64::try_from(matched.len()).unwrap_or(u64::MAX);
        let items = matched
            .into_iter()
            .skip(usize::try_from(query.offset).unwrap_or(usize::MAX))
            .take(usize::try_from(query.limit).unwrap_or(usize::MAX))
            .collect();

        Ok(Page {
            items,
            total,
            offset: query.offset,
            limit: query.limit,
        })
    }
}

/// Adds every part of a query that SQL can express to a builder already holding a `WHERE`.
///
/// Label names reach the SQL text rather than a bind parameter, because a JSON path is not a
/// parameter position in either dialect. They are safe there and only there: every name has been
/// through [`dam_core::LabelName`], whose grammar is `[a-zA-Z_][a-zA-Z0-9_]*`, so nothing that
/// could close the quoted path can survive to this point.
fn push_alert_filter(builder: &mut QueryBuilder<Sqlite>, query: &AlertQuery) {
    if !query.statuses.is_empty() {
        builder.push(" AND status IN (");
        let mut list = builder.separated(", ");
        for status in &query.statuses {
            list.push_bind(status.as_str());
        }
        builder.push(")");
    }

    if let Some(floor) = query.min_severity {
        builder.push(" AND severity IN (");
        let mut list = builder.separated(", ");
        for severity in severities_at_or_above(floor) {
            list.push_bind(severity);
        }
        builder.push(")");
    }

    for matcher in &query.matchers {
        let Ok(compiled) = matcher.compile() else {
            continue;
        };
        if !matcher.is_sql_expressible() {
            continue;
        }

        // Alertmanager matches an absent label against the empty string, so the extraction has to
        // produce one rather than a null that no comparison is true of.
        builder.push(" AND COALESCE(json_extract(labels, '$.\"");
        builder.push(compiled.name().as_str());
        builder.push("\"'), '') ");
        builder.push(if compiled.op().is_equal() {
            "= "
        } else {
            "<> "
        });
        builder.push_bind(matcher.value.clone());
    }

    if let Some(state) = query.notification_state {
        builder.push(
            " AND EXISTS (SELECT 1 FROM notifications WHERE \
             notifications.dedupe_key = 'a:' || alerts.fingerprint AND notifications.state = ",
        );
        builder.push_bind(state.as_str());
        builder.push(")");
    }
}

/// Adds a parenthesised list of surrogate keys.
fn push_id_list(builder: &mut QueryBuilder<Sqlite>, ids: &[i64]) {
    builder.push("(");
    let mut list = builder.separated(", ");
    for id in ids {
        list.push_bind(*id);
    }
    builder.push(")");
}

/// Joins fragments into one query string.
///
/// The column lists are constants and the fragments around them are literals, so the result never
/// contains anything a caller supplied. It exists because `concat!` cannot take a `const` and the
/// alternative is writing every column list out once per query that reads the table.
fn const_format(parts: &[&str]) -> String {
    parts.concat()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_column_list_is_joined_without_separators() {
        assert_eq!(
            const_format(&["SELECT ", "a, b", " FROM t"]),
            "SELECT a, b FROM t"
        );
    }
}
