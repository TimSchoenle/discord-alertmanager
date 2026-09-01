//! The embeds a command or a button answers with, and the small parsers they share.
//!
//! Everything here is read-only and pure over what it is handed. The reason it is one module
//! rather than a helper inside each command is that a button and a slash command are two entry
//! points to the same view: `Details` on a card and `/alerts show` have to draw the same thing, or
//! an operator who used one and then the other has to work out which is lying.
//!
//! # References an operator can actually type
//!
//! Discord has no way to hand a command a fingerprint the user did not read off a card first, so
//! `<ref>` accepts what people have in front of them: a full fingerprint, the short one a card's
//! footer shows, a notification id, or an alert name. An ambiguous name is answered with the
//! candidates rather than with a guess, because acknowledging the wrong alert during an incident
//! is worse than being asked again.

use chrono::{DateTime, Duration, Utc};
use dam_core::MatcherSet;
use dam_core::{Alert, Fingerprint, NotificationState, Severity};
use dam_store::{
    Acknowledgement, AlertQuery, AlertRecord, NotificationId, QueryMatcher, Store, StoreError,
};
use serenity::all::{CreateActionRow, CreateButton, CreateEmbed, CreateEmbedFooter};

use crate::custom_id::{Action, CustomId};
use crate::links::RenderedLink;
use crate::render::Renderer;

/// Rows scanned when resolving a reference that is not an exact fingerprint.
///
/// A bound rather than a full scan: the reference is something a person just read off a card, so
/// the alert is recent, and a command that reads the whole table during a storm is a command that
/// times out during exactly the incident it was called for.
const RESOLVE_SCAN: u32 = 250;

/// Alerts one page of `/alerts list` shows.
pub(crate) const PAGE_SIZE: u32 = 10;

/// The filter a list view was built with, and what a page button has to carry.
///
/// Held as the operator's own words rather than as a compiled query, because it has to survive a
/// round trip through a hundred-byte custom id and come back meaning the same thing.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct ListFilter {
    /// Keep only alerts whose card is in this state.
    pub(crate) state: Option<NotificationState>,

    /// Keep only alerts at or above this severity.
    pub(crate) severity: Option<Severity>,

    /// The matcher expression, as written.
    pub(crate) matchers: String,

    /// Rows per page.
    pub(crate) limit: u32,
}

impl ListFilter {
    /// The store query this filter and an offset produce.
    ///
    /// # Errors
    ///
    /// Returns the matcher expression's parse error, so a typo is reported to whoever typed it
    /// rather than silently widening the filter to everything.
    pub(crate) fn query(&self, offset: u32) -> Result<AlertQuery, dam_core::CoreError> {
        let matchers = MatcherSet::parse(&self.matchers)?
            .as_slice()
            .iter()
            .map(|matcher| QueryMatcher {
                name: matcher.name().as_str().to_owned(),
                op: matcher.op(),
                value: matcher.value().to_owned(),
            })
            .collect();

        Ok(AlertQuery {
            statuses: Vec::new(),
            min_severity: self.severity,
            matchers,
            notification_state: self.state,
            offset,
            limit: self.limit.clamp(1, 25),
        })
    }

    /// Packs the filter and an offset into a page button's argument.
    ///
    /// Positional and dot-separated, with the matcher expression last so its own dots survive.
    /// The codec refuses a colon, which is the custom id's separator, so an expression carrying
    /// one produces no page buttons rather than a control that decodes into a different filter.
    pub(crate) fn pack(&self, offset: u32) -> String {
        format!(
            "{offset}.{}.{}.{}.{}",
            self.limit,
            self.state.map_or("-", NotificationState::as_str),
            self.severity.map_or("-", Severity::as_str),
            self.matchers
        )
    }

    /// Reads back what [`ListFilter::pack`] wrote.
    pub(crate) fn unpack(raw: &str) -> Option<(Self, u32)> {
        let mut parts = raw.splitn(5, '.');
        let offset: u32 = parts.next()?.parse().ok()?;
        let limit: u32 = parts.next()?.parse().ok()?;
        let state = parts.next()?;
        let severity = parts.next()?;
        let matchers = parts.next().unwrap_or_default().to_owned();

        Some((
            Self {
                state: state_from(state),
                severity: severity_from(severity),
                matchers,
                limit,
            },
            offset,
        ))
    }
}

/// The notification state one word names.
pub(crate) fn state_from(word: &str) -> Option<NotificationState> {
    Some(match word {
        "firing" => NotificationState::Firing,
        "acked" => NotificationState::Acked,
        "silenced" => NotificationState::Silenced,
        "ignored" => NotificationState::Ignored,
        "resolved" => NotificationState::Resolved,
        "orphaned" => NotificationState::Orphaned,
        _ => return None,
    })
}

/// The severity one word names.
pub(crate) fn severity_from(word: &str) -> Option<Severity> {
    Some(match word {
        "critical" => Severity::Critical,
        "warning" => Severity::Warning,
        "info" => Severity::Info,
        _ => return None,
    })
}

/// Why a reference did not name exactly one alert.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ResolveError {
    /// Nothing matched.
    Unknown,

    /// Several alerts matched, and these are their short fingerprints.
    Ambiguous(Vec<String>),

    /// The database could not be read.
    Backend(String),
}

impl std::fmt::Display for ResolveError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Unknown => f.write_str(
                "no alert matches that reference. Use the short fingerprint from a card's footer, \
                 or the alert name.",
            ),
            Self::Ambiguous(candidates) => write!(
                f,
                "that reference matches {} alerts: {}. Use one of those fingerprints.",
                candidates.len(),
                candidates.join(", ")
            ),
            Self::Backend(detail) => write!(f, "cannot read the alert: {detail}"),
        }
    }
}

impl From<StoreError> for ResolveError {
    fn from(error: StoreError) -> Self {
        Self::Backend(error.to_string())
    }
}

/// Finds the one alert a reference names.
///
/// Tried in order of how specific the reference is: a full fingerprint, then a notification id,
/// then a short fingerprint or an alert name over a bounded recent scan.
///
/// # Errors
///
/// Returns [`ResolveError::Unknown`] when nothing matches, [`ResolveError::Ambiguous`] when
/// several do, and [`ResolveError::Backend`] when the database refused the read.
pub(crate) async fn resolve(
    store: &dyn Store,
    reference: &str,
) -> Result<AlertRecord, ResolveError> {
    let reference = reference.trim();

    if let Ok(fingerprint) = Fingerprint::new(reference.to_owned())
        && let Some(record) = store.alert(&fingerprint).await?
    {
        return Ok(record);
    }

    if let Ok(id) = reference.parse::<i64>()
        && let Some(card) = store.notification(NotificationId::new(id)).await?
        && let Some(record) = store.alert(&card.fingerprint).await?
    {
        return Ok(record);
    }

    let page = store
        .query_alerts(&AlertQuery {
            limit: RESOLVE_SCAN,
            ..AlertQuery::default()
        })
        .await?;

    let wanted = reference.to_ascii_lowercase();
    let matched: Vec<AlertRecord> = page
        .items
        .into_iter()
        .filter(|record| {
            record.fingerprint().as_str().starts_with(reference)
                || record.alert.name().to_ascii_lowercase() == wanted
        })
        .collect();

    match matched.len() {
        0 => Err(ResolveError::Unknown),
        1 => Ok(matched.into_iter().next().unwrap_or_else(|| unreachable!())),
        _ => Err(ResolveError::Ambiguous(
            matched
                .iter()
                .take(5)
                .map(|record| record.fingerprint().short().to_owned())
                .collect(),
        )),
    }
}

/// The matcher expression that names exactly one alert.
///
/// Every label, quoted and escaped, so it round-trips through the parser and means the same thing
/// to Alertmanager. Narrower than an operator would usually write by hand, and deliberately so: a
/// silence created from a card silences the alert on the card, and nothing the operator did not
/// look at.
pub(crate) fn matchers_of(alert: &Alert) -> String {
    let mut parts = Vec::with_capacity(alert.labels.len());

    for (name, value) in alert.labels.iter() {
        parts.push(format!("{name}=\"{}\"", escape(value)));
    }

    parts.join(", ")
}

/// Escapes a label value for a quoted matcher: the backslash first, or it would double the ones
/// the quote escape adds.
fn escape(value: &str) -> String {
    value.replace('\\', "\\\\").replace('"', "\\\"")
}

/// Parses `30m`, `2h`, `1d` or a bare number of minutes.
///
/// # Errors
///
/// Returns the offending text when it is not a duration, or when it is zero or negative — a
/// silence that expires the moment it is created is a request nobody meant to make.
pub(crate) fn parse_duration(raw: &str) -> Result<Duration, String> {
    let raw = raw.trim();
    let (value, unit) = raw.split_at(
        raw.find(|ch: char| !ch.is_ascii_digit())
            .unwrap_or(raw.len()),
    );

    let value: i64 = value
        .parse()
        .map_err(|_| format!("`{raw}` is not a duration; try `30m`, `2h` or `1d`"))?;

    let duration = match unit.trim() {
        "" | "m" | "min" | "mins" => Duration::minutes(value),
        "s" | "sec" | "secs" => Duration::seconds(value),
        "h" | "hr" | "hrs" => Duration::hours(value),
        "d" | "day" | "days" => Duration::days(value),
        "w" | "week" | "weeks" => Duration::weeks(value),
        other => return Err(format!("`{other}` is not a unit; try `m`, `h`, `d` or `w`")),
    };

    if duration <= Duration::zero() {
        return Err("a duration has to be positive".to_owned());
    }

    Ok(duration)
}

/// A Discord relative timestamp, which every client renders in the reader's own locale.
pub(crate) fn relative(at: DateTime<Utc>) -> String {
    format!("<t:{}:R>", at.timestamp())
}

/// The full detail view of one alert.
///
/// Every label and every annotation, which is what separates this from the card: the card is
/// budgeted for a channel an operator scans, and this is the ephemeral answer to "show me all of
/// it".
pub(crate) fn detail_embed(
    record: &AlertRecord,
    held: Option<&Acknowledgement>,
    state: Option<NotificationState>,
) -> CreateEmbed {
    let alert = &record.alert;
    let mut embed = CreateEmbed::new()
        .title(truncated(
            &format!(
                "[{}] {}",
                alert.severity().as_str().to_uppercase(),
                alert.name()
            ),
            256,
        ))
        .timestamp(Utc::now());

    if let Some(summary) = alert.annotations.summary() {
        embed = embed.description(truncated(summary, 4096));
    }

    embed = embed
        .field("Status", status_line(alert, state, held), true)
        .field("Since", relative(alert.starts_at), true)
        .field("First seen", relative(record.first_seen_at), true);

    if record.flap_count > 0 {
        embed = embed.field("Flapped", format!("×{}", record.flap_count), true);
    }

    if let Some(ends_at) = alert.ends_at {
        embed = embed.field("Ended", relative(ends_at), true);
    }

    if !alert.silenced_by.is_empty() {
        embed = embed.field("Silenced by", code_list(&alert.silenced_by), false);
    }

    if !alert.inhibited_by.is_empty() {
        embed = embed.field("Inhibited by", code_list(&alert.inhibited_by), false);
    }

    embed = embed.field("Labels", truncated(&label_block(alert), 1024), false);

    let annotations = annotation_block(alert);
    if !annotations.is_empty() {
        embed = embed.field("Annotations", truncated(&annotations, 1024), false);
    }

    embed.footer(CreateEmbedFooter::new(format!(
        "{} · {}",
        alert.fingerprint.short(),
        alert.am_state.as_str()
    )))
}

/// The one-line status a detail view opens with.
fn status_line(
    alert: &Alert,
    state: Option<NotificationState>,
    held: Option<&Acknowledgement>,
) -> String {
    if let Some(held) = held {
        return format!("Acknowledged by <@{}> {}", held.user_id, relative(held.at));
    }

    match state {
        Some(state) => state.as_str().to_owned(),
        None => alert.status.as_str().to_owned(),
    }
}

/// The label set as a code block, which is what makes a long value readable and unclickable.
fn label_block(alert: &Alert) -> String {
    let mut block = String::from("```\n");

    for (name, value) in alert.labels.iter() {
        block.push_str(name.as_str());
        block.push('=');
        block.push_str(value);
        block.push('\n');
    }

    block.push_str("```");
    block
}

/// The annotations as a code block.
fn annotation_block(alert: &Alert) -> String {
    if alert.annotations.is_empty() {
        return String::new();
    }

    let mut block = String::from("```\n");

    for (name, value) in alert.annotations.iter() {
        block.push_str(name);
        block.push_str(": ");
        block.push_str(value);
        block.push('\n');
    }

    block.push_str("```");
    block
}

/// A comma-separated list of identifiers, each in inline code.
fn code_list(values: &[String]) -> String {
    values
        .iter()
        .map(|value| format!("`{value}`"))
        .collect::<Vec<_>>()
        .join(", ")
}

/// The link buttons for a detail view.
///
/// Link-style buttons cost no interaction handling, so they are free to put on an ephemeral
/// answer that nothing will ever be dispatched from.
pub(crate) fn link_row(renderer: &Renderer, alert: &Alert) -> Option<CreateActionRow> {
    let links: Vec<RenderedLink> = renderer.links(alert, Utc::now());

    if links.is_empty() {
        return None;
    }

    Some(CreateActionRow::Buttons(
        links
            .iter()
            .take(5)
            .map(|link| CreateButton::new_link(&link.url).label(truncated(&link.label, 80)))
            .collect(),
    ))
}

/// The previous and next controls under a page of a list.
///
/// Omitted rather than disabled when the filter cannot be packed into a hundred bytes: a control
/// that decodes into a different filter than the one on screen is worse than no control.
pub(crate) fn page_row(filter: &ListFilter, offset: u32, total: u64) -> Option<CreateActionRow> {
    let mut buttons = Vec::new();

    if offset > 0 {
        let back = offset.saturating_sub(filter.limit);
        buttons.push(page_button(filter, back, "Previous")?);
    }

    if u64::from(offset + filter.limit) < total {
        buttons.push(page_button(filter, offset + filter.limit, "Next")?);
    }

    if buttons.is_empty() {
        None
    } else {
        Some(CreateActionRow::Buttons(buttons))
    }
}

/// One page control, or `None` when its identifier would not fit.
fn page_button(filter: &ListFilter, offset: u32, label: &str) -> Option<CreateButton> {
    let id = CustomId::with_argument(Action::Page, NotificationId::new(0), filter.pack(offset))
        .encode()
        .ok()?;

    Some(
        CreateButton::new(id)
            .label(label)
            .style(serenity::all::ButtonStyle::Secondary),
    )
}

/// One line of a list page.
pub(crate) fn list_line(record: &AlertRecord) -> String {
    format!(
        "`{}` **{}** — {} · {}",
        record.fingerprint().short(),
        record.alert.name(),
        record.severity().as_str(),
        relative(record.alert.starts_at)
    )
}

/// Truncates on a character boundary, marking that it did.
pub(crate) fn truncated(value: &str, limit: usize) -> String {
    if value.len() <= limit {
        return value.to_owned();
    }

    let mut end = limit.saturating_sub('…'.len_utf8());
    while end > 0 && !value.is_char_boundary(end) {
        end -= 1;
    }

    format!("{}…", &value[..end])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_filter_survives_a_round_trip_through_a_button() {
        let filter = ListFilter {
            state: Some(NotificationState::Acked),
            severity: Some(Severity::Warning),
            matchers: "namespace=~prod-.*".to_owned(),
            limit: PAGE_SIZE,
        };

        let (back, offset) = ListFilter::unpack(&filter.pack(20)).expect("the packing round-trips");

        assert_eq!(offset, 20);
        assert_eq!(back, filter);
    }

    #[test]
    fn an_empty_filter_round_trips_too() {
        let filter = ListFilter {
            limit: PAGE_SIZE,
            ..ListFilter::default()
        };

        let (back, offset) = ListFilter::unpack(&filter.pack(0)).expect("the packing round-trips");

        assert_eq!(offset, 0);
        assert_eq!(back, filter);
    }

    #[test]
    fn a_matcher_expression_with_a_colon_produces_no_page_controls() {
        // The custom id codec refuses its own separator, and a control that silently dropped the
        // matcher would page through a different result set than the one on screen.
        let filter = ListFilter {
            matchers: "instance=host:9090".to_owned(),
            limit: PAGE_SIZE,
            ..ListFilter::default()
        };

        assert!(page_row(&filter, 0, 100).is_none());
    }

    #[test]
    fn a_label_value_cannot_break_out_of_its_matcher() {
        let mut alert = alert();
        alert
            .labels
            .insert(
                dam_core::LabelName::new("job").expect("a valid label name"),
                "a\"b\\c".to_owned(),
            )
            .expect("a valid label value");

        let expression = matchers_of(&alert);
        let parsed = MatcherSet::parse(&expression).expect("the expression parses");

        assert!(parsed.matches(&alert.labels));
    }

    #[test]
    fn durations_are_read_the_way_they_are_written() {
        assert_eq!(parse_duration("30m"), Ok(Duration::minutes(30)));
        assert_eq!(parse_duration("2h"), Ok(Duration::hours(2)));
        assert_eq!(parse_duration("1d"), Ok(Duration::days(1)));
        assert_eq!(parse_duration("45"), Ok(Duration::minutes(45)));
        assert!(parse_duration("0h").is_err());
        assert!(parse_duration("soon").is_err());
    }

    fn alert() -> Alert {
        let mut labels = dam_core::Labels::new();
        labels
            .insert(
                dam_core::LabelName::new("alertname").expect("a valid label name"),
                "Test".to_owned(),
            )
            .expect("a valid label value");

        Alert {
            fingerprint: Fingerprint::new("abcdef0123456789".to_owned())
                .expect("a valid fingerprint"),
            labels,
            annotations: dam_core::Annotations::new(),
            starts_at: Utc::now(),
            ends_at: None,
            generator_url: None,
            status: dam_core::AlertStatus::Firing,
            am_state: dam_core::AmState::Active,
            silenced_by: Vec::new(),
            inhibited_by: Vec::new(),
            group_key: None,
        }
    }
}
