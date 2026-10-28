//! Alert rows, the events behind them, and the Alertmanager silences linked to them.

use std::collections::HashSet;

use chrono::{DateTime, Utc};
use dam_core::{Alert, AlertDelta, AmState, EventKind, EventSource, Fingerprint};
use dam_store::{
    AlertQuery, AlertRecord, IngestBatch, IngestOutcome, Page, SilenceLink, SilenceState,
    SqlAlertFilter, StoreError, Transition, carry_over, classify, matches_regex_matchers,
    needs_in_memory_filter, suppression_map,
};
use sqlx::SqliteConnection;

use crate::SqliteStore;
use crate::convert::{AlertRow, backend, decode_all, encode_json, encode_time, encode_time_opt};

/// Selects the `alerts` rows an [`SqlAlertFilter`] admits.
///
/// `$head` is everything before the `WHERE` and `$tail` everything after the filter; the filter
/// binds `?1` to `?6`, so a caller's own parameters start at `?7`. Each list is bound as a JSON
/// array, and an empty one constrains nothing, which is what lets one checked statement serve
/// every filter a command can build. The label matchers are three parallel arrays joined back
/// into rows by position, and an alert passes when none of them disagrees with it; Alertmanager
/// matches an absent label against the empty string, so the extraction produces one rather than
/// a null that no comparison is true of. A label name reaches the JSON path only through the
/// bound array, and every name has been through [`dam_core::LabelName`], whose grammar admits
/// nothing that could close the quoted path.
macro_rules! filtered_alerts {
    ($row:path, $head:literal, $tail:literal, $filter:expr $(, $arg:expr)* $(,)?) => {{
        let filter: &SqlAlertFilter = $filter;
        sqlx::query_as!(
            $row,
            $head + " WHERE (json_array_length(?1) = 0 \
                 OR status IN (SELECT value FROM json_each(?1))) \
             AND (json_array_length(?2) = 0 \
                 OR severity IN (SELECT value FROM json_each(?2))) \
             AND NOT EXISTS (SELECT 1 FROM json_each(?3) AS name \
                 JOIN json_each(?4) AS expected ON expected.key = name.key \
                 JOIN json_each(?5) AS equal ON equal.key = name.key \
                 WHERE (COALESCE(json_extract(alerts.labels, '$.\"' || name.value || '\"'), '') \
                     = expected.value) <> equal.value) \
             AND (?6 IS NULL OR EXISTS (SELECT 1 FROM notifications \
                 WHERE notifications.dedupe_key = 'a:' || alerts.fingerprint \
                 AND notifications.state = ?6)) " + $tail,
            encode_json(&filter.statuses),
            encode_json(&filter.severities),
            encode_json(&filter.label_names),
            encode_json(&filter.label_values),
            encode_json(&filter.label_equal),
            filter.notification_state.as_deref()
            $(, $arg)*
        )
    }};
}

/// The count a filtered read reports alongside its page.
struct AlertCount {
    total: i64,
}

pub(super) async fn ingest_batch(
    store: &SqliteStore,
    batch: &IngestBatch,
) -> Result<IngestOutcome, StoreError> {
    let mut tx = store.write().await?;
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
        carry_over(previous.as_ref(), &mut alert);

        let Some(transition) = classify(
            previous.as_ref(),
            &alert,
            batch.received_at,
            store.regroup_window,
        ) else {
            touch_alert(&mut tx, &alert.fingerprint, batch.received_at).await?;
            outcome.duplicates = outcome.duplicates.saturating_add(1);
            continue;
        };

        upsert_alert(&mut tx, &alert, &transition, batch.received_at).await?;

        if store.persist_events {
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
            observed_at: batch.received_at,
        });
    }

    tx.commit().await.map_err(backend)?;

    Ok(outcome)
}

pub(super) async fn alert(
    store: &SqliteStore,
    fingerprint: &Fingerprint,
) -> Result<Option<AlertRecord>, StoreError> {
    select_alerts!("WHERE fingerprint = ?1", fingerprint.as_str())
        .fetch_optional(&store.pool)
        .await
        .map_err(backend)?
        .map(AlertRecord::try_from)
        .transpose()
}

pub(super) async fn query_alerts(
    store: &SqliteStore,
    query: &AlertQuery,
) -> Result<Page<AlertRecord>, StoreError> {
    // Compiling every matcher first is what turns a malformed one into a refusal rather than
    // into a filter that quietly matches more than it was asked to.
    for matcher in &query.matchers {
        matcher.compile().map_err(|error| StoreError::Decode {
            kind: "query matcher",
            detail: error.to_string(),
        })?;
    }

    if needs_in_memory_filter(query) {
        return store.scan_alerts(query).await;
    }

    let filter = SqlAlertFilter::from_query(query);

    let total = filtered_alerts!(
        AlertCount,
        "SELECT count(*) AS \"total!: i64\" FROM alerts",
        "",
        &filter
    )
    .fetch_one(&store.pool)
    .await
    .map_err(backend)?
    .total;

    let rows = filtered_alerts!(
        AlertRow,
        "SELECT fingerprint AS \"fingerprint!\", labels_hash, group_key, labels, annotations, starts_at, ends_at, \
         generator_url, status, am_state, silenced_by, inhibited_by, first_seen_at, \
         last_seen_at, resolved_at, flap_count, updated_at FROM alerts",
        "ORDER BY last_seen_at DESC, fingerprint LIMIT ?7 OFFSET ?8",
        &filter,
        i64::from(query.limit),
        i64::from(query.offset),
    )
    .fetch_all(&store.pool)
    .await
    .map_err(backend)?;

    Ok(Page {
        items: decode_all(rows)?,
        total: u64::try_from(total).unwrap_or(0),
        offset: query.offset,
        limit: query.limit,
    })
}

pub(super) async fn firing_alerts_named(
    store: &SqliteStore,
    alertname: Option<&str>,
) -> Result<Vec<AlertRecord>, StoreError> {
    // Spelled exactly as the partial index `alerts_firing_by_name` spells it, which is what
    // lets the planner answer this from the index rather than from the firing set. Two
    // statements rather than one with an `IS`, which no index can serve.
    let rows = match alertname {
        Some(name) => {
            select_alerts!(
                "WHERE status = 'firing' AND json_extract(labels, '$.alertname') = ?1 \
             ORDER BY fingerprint",
                name
            )
            .fetch_all(&store.pool)
            .await
        }
        None => {
            select_alerts!(
                "WHERE status = 'firing' AND json_extract(labels, '$.alertname') IS NULL \
             ORDER BY fingerprint"
            )
            .fetch_all(&store.pool)
            .await
        }
    }
    .map_err(backend)?;

    decode_all(rows)
}

pub(super) async fn firing_not_in(
    store: &SqliteStore,
    present: &[Fingerprint],
    cutoff: DateTime<Utc>,
) -> Result<Vec<AlertRecord>, StoreError> {
    let rows = select_alerts!(
        "WHERE status = 'firing' AND last_seen_at < ?1",
        encode_time(cutoff)
    )
    .fetch_all(&store.pool)
    .await
    .map_err(backend)?;

    // The set Alertmanager reported is subtracted here rather than in a `NOT IN` list: a poll
    // of a busy Alertmanager carries thousands of fingerprints, and binding them all would
    // cost more than filtering a firing set that is already bounded by the cutoff.
    let present: HashSet<&str> = present.iter().map(Fingerprint::as_str).collect();
    let mut missing = Vec::new();

    for row in rows {
        let record = AlertRecord::try_from(row)?;

        if !present.contains(record.fingerprint().as_str()) {
            missing.push(record);
        }
    }

    Ok(missing)
}

pub(super) async fn record_silence(
    store: &SqliteStore,
    link: &SilenceLink,
) -> Result<(), StoreError> {
    let mut tx = store.write().await?;
    write_silence(&mut tx, link).await?;

    tx.commit().await.map_err(backend)
}

pub(super) async fn sync_silences(
    store: &SqliteStore,
    snapshot: &[SilenceState],
    now: DateTime<Utc>,
) -> Result<Vec<AlertDelta>, StoreError> {
    let mut tx = store.write().await?;
    let stamp = encode_time(now);

    for silence in snapshot {
        sqlx::query!(
            "UPDATE silences SET state = ?, ends_at = ?, synced_at = ? WHERE am_id = ?",
            silence.state.as_str(),
            encode_time(silence.ends_at),
            stamp,
            silence.am_id,
        )
        .execute(&mut *tx)
        .await
        .map_err(backend)?;
    }

    let desired = suppression_map(snapshot);

    // Only two kinds of alert can have changed: one Alertmanager now suppresses, and one this
    // database still thinks is suppressed. Reading the whole table to find them would make the
    // syncer's cost a function of history rather than of what is silenced.
    let mut candidates: Vec<AlertRecord> = decode_all(
        select_alerts!("WHERE silenced_by <> '[]'")
            .fetch_all(&mut *tx)
            .await
            .map_err(backend)?,
    )?;

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

        sqlx::query!(
            "UPDATE alerts SET silenced_by = ?, am_state = ?, updated_at = ? \
             WHERE fingerprint = ?",
            encode_json(&record.alert.silenced_by),
            record.alert.am_state.as_str(),
            stamp,
            record.fingerprint().as_str(),
        )
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
            alert: record.alert,
            observed_at: now,
        });
    }

    tx.commit().await.map_err(backend)?;

    Ok(deltas)
}

pub(super) async fn silences(
    store: &SqliteStore,
    active_only: bool,
) -> Result<Vec<SilenceLink>, StoreError> {
    let rows = if active_only {
        select_silences!("WHERE state = 'active' ORDER BY starts_at DESC")
            .fetch_all(&store.pool)
            .await
    } else {
        select_silences!("ORDER BY starts_at DESC")
            .fetch_all(&store.pool)
            .await
    }
    .map_err(backend)?;

    decode_all(rows)
}

/// Reads one alert row inside a transaction.
async fn read_alert(
    conn: &mut SqliteConnection,
    fingerprint: &Fingerprint,
) -> Result<Option<AlertRecord>, StoreError> {
    select_alerts!("WHERE fingerprint = ?1", fingerprint.as_str())
        .fetch_optional(conn)
        .await
        .map_err(backend)?
        .map(AlertRecord::try_from)
        .transpose()
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
    sqlx::query!(
        "UPDATE alerts SET last_seen_at = ? WHERE fingerprint = ?",
        encode_time(at),
        fingerprint.as_str(),
    )
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
    let labels_hash = alert.labels_hash();

    sqlx::query!(
        "INSERT INTO alerts (fingerprint, labels_hash, group_key, labels, annotations, starts_at, \
         ends_at, generator_url, status, am_state, severity, silenced_by, inhibited_by, \
         first_seen_at, last_seen_at, resolved_at, flap_count, updated_at) \
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) \
         ON CONFLICT (fingerprint) DO UPDATE SET \
         labels_hash = excluded.labels_hash, group_key = excluded.group_key, \
         labels = excluded.labels, annotations = excluded.annotations, \
         starts_at = excluded.starts_at, ends_at = excluded.ends_at, \
         generator_url = excluded.generator_url, status = excluded.status, \
         am_state = excluded.am_state, severity = excluded.severity, \
         silenced_by = excluded.silenced_by, inhibited_by = excluded.inhibited_by, \
         last_seen_at = excluded.last_seen_at, resolved_at = excluded.resolved_at, \
         flap_count = excluded.flap_count, \
         updated_at = excluded.updated_at",
        alert.fingerprint.as_str(),
        labels_hash.as_str(),
        alert.group_key.as_ref().map(dam_core::GroupKey::as_str),
        encode_json(&alert.labels),
        encode_json(&alert.annotations),
        encode_time(alert.starts_at),
        encode_time_opt(alert.ends_at),
        alert.generator_url,
        alert.status.as_str(),
        alert.am_state.as_str(),
        alert.severity().as_str(),
        encode_json(&alert.silenced_by),
        encode_json(&alert.inhibited_by),
        encode_time(transition.first_seen_at),
        encode_time(at),
        encode_time_opt(transition.resolved_at),
        i64::from(transition.flap_count),
        encode_time(at),
    )
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

    sqlx::query!(
        "INSERT INTO alert_events \
         (fingerprint, kind, source, starts_at, ends_at, payload, received_at) \
         VALUES (?, ?, ?, ?, ?, ?, ?) \
         ON CONFLICT (fingerprint, kind, starts_at, \n         COALESCE(ends_at, '1970-01-01T00:00:00.000000Z')) DO NOTHING",
        alert.fingerprint.as_str(),
        transition.kind.as_str(),
        source.as_str(),
        encode_time(alert.starts_at),
        encode_time_opt(alert.ends_at),
        encode_json(&payload),
        encode_time(at),
    )
    .execute(conn)
    .await
    .map_err(backend)?;

    Ok(())
}

/// Writes the bot's record of one Alertmanager silence.
pub(super) async fn write_silence(
    conn: &mut SqliteConnection,
    link: &SilenceLink,
) -> Result<(), StoreError> {
    sqlx::query!(
        "INSERT INTO silences (am_id, matchers, starts_at, ends_at, created_by, discord_user_id, \
         origin_message, comment, state, synced_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?) \
         ON CONFLICT (am_id) DO UPDATE SET matchers = excluded.matchers, \
         starts_at = excluded.starts_at, ends_at = excluded.ends_at, \
         created_by = excluded.created_by, discord_user_id = excluded.discord_user_id, \
         origin_message = excluded.origin_message, comment = excluded.comment, \
         state = excluded.state, synced_at = excluded.synced_at",
        link.am_id,
        link.matchers,
        encode_time(link.starts_at),
        encode_time(link.ends_at),
        link.created_by,
        link.discord_user_id.map(dam_store::UserId::to_db),
        link.origin_message,
        link.comment,
        link.state.as_str(),
        encode_time(link.synced_at),
    )
    .execute(conn)
    .await
    .map_err(backend)?;

    Ok(())
}

impl SqliteStore {
    /// Answers a query whose regex matchers SQL cannot express.
    ///
    /// The other predicates still run in the database; what comes back is filtered, counted and
    /// paginated here. The read is capped, because a regex that matches everything would
    /// otherwise pull the whole table into the process to serve one page of it.
    async fn scan_alerts(&self, query: &AlertQuery) -> Result<Page<AlertRecord>, StoreError> {
        let rows = filtered_alerts!(
            AlertRow,
            "SELECT fingerprint AS \"fingerprint!\", labels_hash, group_key, labels, annotations, starts_at, ends_at, \
             generator_url, status, am_state, silenced_by, inhibited_by, first_seen_at, \
             last_seen_at, resolved_at, flap_count, updated_at FROM alerts",
            "ORDER BY last_seen_at DESC, fingerprint LIMIT ?7",
            &SqlAlertFilter::from_query(query),
            i64::from(dam_store::IN_MEMORY_SCAN_LIMIT),
        )
        .fetch_all(&self.pool)
        .await
        .map_err(backend)?;

        let matched: Vec<AlertRecord> = decode_all::<_, AlertRecord>(rows)?
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
