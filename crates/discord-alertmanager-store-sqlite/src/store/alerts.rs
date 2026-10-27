//! Alert rows, the events behind them, and the Alertmanager silences linked to them.

use std::collections::HashSet;

use chrono::{DateTime, Utc};
use dam_core::{Alert, AlertDelta, AmState, EventKind, EventSource, Fingerprint};
use dam_store::{
    AlertQuery, AlertRecord, IngestBatch, IngestOutcome, Page, SilenceLink, SilenceState,
    StoreError, Transition, carry_over, classify, matches_regex_matchers, needs_in_memory_filter,
    severities_at_or_above, suppression_map,
};
use sqlx::{QueryBuilder, Sqlite, SqliteConnection};

use super::const_format;
use crate::SqliteStore;
use crate::convert::{
    alert_record, backend, encode_json, encode_time, encode_time_opt, silence_link,
};

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
    let sql = const_format(&[
        "SELECT ",
        ALERT_COLUMNS,
        " FROM alerts WHERE fingerprint = ",
    ]);

    let row = QueryBuilder::<Sqlite>::new(sql)
        .push_bind(fingerprint.as_str().to_owned())
        .build()
        .fetch_optional(&store.pool)
        .await
        .map_err(backend)?;

    row.as_ref().map(alert_record).transpose()
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

    let mut counter = QueryBuilder::<Sqlite>::new("SELECT count(*) FROM alerts WHERE 1 = 1");
    push_alert_filter(&mut counter, query);
    let total: i64 = counter
        .build_query_scalar()
        .fetch_one(&store.pool)
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
        .fetch_all(&store.pool)
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

pub(super) async fn firing_alerts_named(
    store: &SqliteStore,
    alertname: Option<&str>,
) -> Result<Vec<AlertRecord>, StoreError> {
    // Spelled exactly as the partial index `alerts_firing_by_name` spells it, which is what
    // lets the planner answer this from the index rather than from the firing set.
    let sql = const_format(&[
        "SELECT ",
        ALERT_COLUMNS,
        " FROM alerts WHERE status = 'firing' AND json_extract(labels, '$.alertname')",
    ]);

    let mut builder = QueryBuilder::<Sqlite>::new(sql);
    match alertname {
        Some(name) => {
            builder.push(" = ");
            builder.push_bind(name.to_owned());
        }
        None => {
            builder.push(" IS NULL");
        }
    }

    builder
        .push(" ORDER BY fingerprint")
        .build()
        .fetch_all(&store.pool)
        .await
        .map_err(backend)?
        .iter()
        .map(alert_record)
        .collect()
}

pub(super) async fn firing_not_in(
    store: &SqliteStore,
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
        .fetch_all(&store.pool)
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
        sqlx::query("UPDATE silences SET state = ?, ends_at = ?, synced_at = ? WHERE am_id = ?")
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
        .fetch_all(&store.pool)
        .await
        .map_err(backend)?
        .iter()
        .map(silence_link)
        .collect()
}

/// Every column of `alerts`, in the order the mapper expects to find them by name.
const ALERT_COLUMNS: &str = "fingerprint, labels_hash, group_key, labels, annotations, starts_at, \
     ends_at, generator_url, status, am_state, severity, silenced_by, inhibited_by, \
     first_seen_at, last_seen_at, resolved_at, flap_count, updated_at";

/// Every column of `silences`.
const SILENCE_COLUMNS: &str = "am_id, matchers, starts_at, ends_at, created_by, discord_user_id, \
     origin_message, comment, state, synced_at";

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

/// Writes the bot's record of one Alertmanager silence.
pub(super) async fn write_silence(
    conn: &mut SqliteConnection,
    link: &SilenceLink,
) -> Result<(), StoreError> {
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
