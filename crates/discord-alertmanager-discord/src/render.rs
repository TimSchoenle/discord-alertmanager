//! Turning one alert into one card, inside Discord's limits.
//!
//! The limits are not advisory. An embed over 6000 characters, a field value over 1024, a
//! twenty-sixth field or a hundred-and-first byte of `custom_id` is a rejected request, and the
//! alert that produces one is the alert with sixty labels and a five-thousand-character
//! annotation — which is to say, the interesting one. Everything here is budgeted rather than
//! hoped about, and the tests assert the budgets rather than the prose.
//!
//! # The render hash is what keeps a storm inside the rate limits
//!
//! Every card carries a hash of what was last posted. An edit whose freshly computed hash matches
//! is skipped without a request. Which means the hash has to cover exactly what a viewer can see
//! and nothing else: fold in the render time and every card re-renders every debounce interval;
//! leave out a field and a change to it never reaches the channel.

use chrono::{DateTime, Utc};
use dam_config::Render;
use dam_core::{Alert, NotificationState, Severity};
use dam_engine::{CardData, Mention, PreviousCard};
use serenity::all::{
    ButtonStyle, Colour, CreateActionRow, CreateButton, CreateEmbed, CreateEmbedFooter,
};

use crate::custom_id::{Action, CustomId};
use crate::links::{LinkRenderer, RenderedLink};

/// Discord's cap on the whole embed.
const EMBED_BUDGET: usize = 6000;

/// Discord's cap on an embed title.
const TITLE_LIMIT: usize = 256;

/// Discord's cap on an embed description.
const DESCRIPTION_LIMIT: usize = 4096;

/// Discord's cap on one field's value.
const FIELD_LIMIT: usize = 1024;

/// Discord's cap on the number of fields.
const FIELD_COUNT_LIMIT: usize = 25;

/// Discord's cap on buttons in one action row.
const ROW_LIMIT: usize = 5;

/// Discord's cap on a forum post or thread name.
const NAME_LIMIT: usize = 100;

/// What the renderer produced, ready to be posted or to be compared against what was.
pub struct RenderedCard {
    /// The embed itself.
    pub embed: CreateEmbed,

    /// The controls under it.
    pub components: Vec<CreateActionRow>,

    /// The message body, which carries the mentions and is otherwise empty.
    ///
    /// Mentions live here rather than in the embed because Discord does not notify anyone for a
    /// mention inside an embed. An edit never carries one: re-mentioning on every update is the
    /// fastest way to have the bot muted by the people it exists to reach.
    pub content: String,

    /// The name a forum post or thread takes.
    pub name: String,

    /// Hash of everything a viewer can see.
    pub hash: String,
}

/// Renders cards.
pub struct Renderer {
    config: Render,
    links: LinkRenderer,
}

impl Renderer {
    /// Builds a renderer around the layout configuration and the compiled link templates.
    #[must_use]
    pub fn new(config: Render, links: LinkRenderer) -> Self {
        Self { config, links }
    }

    /// Renders one card.
    #[must_use]
    pub fn render(&self, card: &CardData) -> RenderedCard {
        let name = self.name(card);
        let links = self.links.render(&card.alert, card.rendered_at);
        let mut fields = self.fields(card);
        let description = self.description(card);
        let title = truncate(&name, TITLE_LIMIT);
        let footer = self.footer(card);

        budget(&title, &description, &footer, &mut fields);

        let mut embed = CreateEmbed::new()
            .title(&title)
            .colour(colour(card))
            .footer(CreateEmbedFooter::new(&footer))
            .timestamp(card.rendered_at);

        if !description.is_empty() {
            embed = embed.description(&description);
        }

        for (label, value, inline) in &fields {
            embed = embed.field(label, value, *inline);
        }

        let components = Self::components(card, &links);

        RenderedCard {
            hash: hash(card, &title, &description, &footer, &fields, &links),
            embed,
            components,
            content: mention_text(&card.mentions),
            name,
        }
    }

    /// The name a forum post or a thread takes for this alert.
    ///
    /// Discord requires it, caps it at a hundred characters and rejects an empty one, so the
    /// fallback chain ends somewhere that always produces text: the alert name, then a key label,
    /// then the short fingerprint.
    #[must_use]
    pub fn name(&self, card: &CardData) -> String {
        let severity = card.severity();
        let key = self.config.key_labels.iter().find_map(|label| {
            card.alert
                .labels
                .get(label)
                .filter(|value| !value.is_empty())
        });

        let name = match key {
            Some(key) => format!(
                "[{}] {} — {key}",
                severity.as_str().to_uppercase(),
                card.alert.name()
            ),
            None => format!(
                "[{}] {}",
                severity.as_str().to_uppercase(),
                card.alert.name()
            ),
        };

        truncate(&name, NAME_LIMIT)
    }

    /// The link buttons an alert produces, for the detail views that draw them outside a card.
    ///
    /// Exposed rather than duplicated: `/alerts show` and the `Details` button both want the same
    /// buttons a card carries, and computing them a second way is how the two drift apart.
    #[must_use]
    pub fn links(&self, alert: &Alert, now: DateTime<Utc>) -> Vec<RenderedLink> {
        self.links.render(alert, now)
    }

    /// The prose of the card: the summary, then the description, then the truncation marker.
    fn description(&self, card: &CardData) -> String {
        let mut text = String::new();

        if let Some(summary) = card.alert.annotations.summary() {
            text.push_str(summary);
        }

        if let Some(description) = card.alert.annotations.description() {
            if !text.is_empty() {
                text.push_str("\n\n");
            }
            text.push_str(description);
        }

        let budget = self.config.description_budget.min(DESCRIPTION_LIMIT);

        if text.len() > budget {
            // The marker names the command that has the rest, because a truncated description
            // without one reads as a bug in the bot rather than as a budget.
            text = truncate(&text, budget.saturating_sub(24));
            text.push_str("\n… `/alerts show` for all");
        }

        text
    }

    /// The fields, in the order an operator reads them.
    fn fields(&self, card: &CardData) -> Vec<(String, String, bool)> {
        let mut fields = Vec::new();

        fields.push(("Status".to_owned(), status_line(card), false));
        fields.push((
            "Since".to_owned(),
            format!("<t:{}:R>", card.alert.starts_at.timestamp()),
            true,
        ));

        if card.flap_count > 0 {
            fields.push((
                "Flapped".to_owned(),
                format!(
                    "×{} — first seen <t:{}:R>",
                    card.flap_count,
                    card.first_seen_at.timestamp()
                ),
                true,
            ));
        }

        if let Some(previous) = &card.previous {
            // An alert that comes back after a week gets a card of its own rather than reviving
            // one nobody is still reading. This is what stops that from throwing away the
            // history: one link, one step back along the chain.
            fields.push(("Previous card".to_owned(), message_url(previous), false));
        }

        if let Some(digest) = &card.digest {
            // A digest is a worse card than the one it replaced, and an operator who is not told
            // why reads it as the bot having quietly started summarising.
            fields.push((
                "Digest".to_owned(),
                truncate(
                    &format!(
                        "This route posted {} cards in {}s, past its threshold of {}. One \
                         rolling card per window until the rate drops.",
                        digest.cards, digest.window_secs, digest.threshold
                    ),
                    FIELD_LIMIT,
                ),
                false,
            ));
        }

        for label in &self.config.key_labels {
            if let Some(value) = card
                .alert
                .labels
                .get(label)
                .filter(|value| !value.is_empty())
            {
                fields.push((title_case(label), truncate(value, FIELD_LIMIT), true));
            }
        }

        if let Some(value) = card.alert.annotations.value() {
            fields.push(("Value".to_owned(), truncate(value, FIELD_LIMIT), true));
        }

        if let Some(silence) = &card.silence {
            fields.push((
                "Silenced by".to_owned(),
                format!(
                    "`{}` by {} until <t:{}:R>",
                    silence.am_id,
                    silence.created_by,
                    silence.ends_at.timestamp()
                ),
                false,
            ));
        }

        if !card.alert.inhibited_by.is_empty() {
            fields.push((
                "Inhibited by".to_owned(),
                truncate(&card.alert.inhibited_by.join(", "), FIELD_LIMIT),
                false,
            ));
        }

        if let Some(reason) = &card.ignore_reason {
            // Said in full on the card, because "why is this channel quiet" is a question an
            // operator should never have to run a command to answer.
            fields.push((
                "Ignored".to_owned(),
                truncate(
                    &format!("{reason} — Alertmanager is still notifying every other receiver"),
                    FIELD_LIMIT,
                ),
                false,
            ));
        }

        fields
    }

    /// The footer: which rule sent this, which alert it is, and when it last changed.
    fn footer(&self, card: &CardData) -> String {
        use std::fmt::Write as _;

        let mut footer = format!("route {}", card.route_name);

        if self.config.show_fingerprint {
            let _ = write!(footer, " · {}", card.alert.fingerprint.short());
        }

        if card.reply_count > 0 {
            let _ = write!(footer, " · {} replies", card.reply_count);
        }

        footer
    }

    /// The controls, which depend only on the state the card is in.
    fn components(card: &CardData, links: &[RenderedLink]) -> Vec<CreateActionRow> {
        let mut rows = Vec::new();
        let mut actions = Vec::new();

        match card.state {
            NotificationState::Firing => {
                actions.push(button(
                    Action::Ack,
                    card,
                    "Acknowledge",
                    ButtonStyle::Primary,
                ));
                actions.push(button(
                    Action::SilenceMenu,
                    card,
                    "Silence…",
                    ButtonStyle::Secondary,
                ));
                actions.push(button(
                    Action::IgnoreMenu,
                    card,
                    "Ignore…",
                    ButtonStyle::Secondary,
                ));
                actions.push(button(
                    Action::Details,
                    card,
                    "Details",
                    ButtonStyle::Secondary,
                ));
            }
            NotificationState::Acked => {
                actions.push(button(
                    Action::Unack,
                    card,
                    "Unacknowledge",
                    ButtonStyle::Secondary,
                ));
                actions.push(button(
                    Action::SilenceMenu,
                    card,
                    "Silence…",
                    ButtonStyle::Secondary,
                ));
                actions.push(button(
                    Action::Details,
                    card,
                    "Details",
                    ButtonStyle::Secondary,
                ));
            }
            NotificationState::Silenced | NotificationState::Ignored => {
                actions.push(button(
                    Action::Details,
                    card,
                    "Details",
                    ButtonStyle::Secondary,
                ));
            }
            NotificationState::Resolved => {
                // A single disabled control rather than none: a card with nothing under it invites
                // the question of whether the bot is still working.
                actions.push(
                    CreateButton::new("resolved")
                        .label("Resolved")
                        .style(ButtonStyle::Success)
                        .disabled(true),
                );
            }
            NotificationState::Orphaned => {}
        }

        let actions: Vec<CreateButton> = actions.into_iter().take(ROW_LIMIT).collect();
        if !actions.is_empty() {
            rows.push(CreateActionRow::Buttons(actions));
        }

        let links: Vec<CreateButton> = links
            .iter()
            .take(ROW_LIMIT)
            .map(|link| CreateButton::new_link(&link.url).label(truncate(&link.label, 80)))
            .collect();

        if !links.is_empty() {
            rows.push(CreateActionRow::Buttons(links));
        }

        rows
    }
}

/// One control on a card.
///
/// An identifier that does not fit is dropped rather than posted: Discord rejects the whole
/// message for one oversized `custom_id`, so a button nobody can press is better than a card
/// nobody gets.
fn button(action: Action, card: &CardData, label: &str, style: ButtonStyle) -> CreateButton {
    let id = CustomId::new(action, card.notification)
        .encode()
        .unwrap_or_else(|_| format!("{}:overflow", action.as_str()));

    CreateButton::new(id).label(label).style(style)
}

/// The line the `Status` field carries.
fn status_line(card: &CardData) -> String {
    match card.state {
        NotificationState::Firing => "Firing — nobody has taken this yet".to_owned(),
        NotificationState::Acked => match (card.acknowledged_by, card.acknowledged_at) {
            (Some(user), Some(at)) => {
                format!("Acknowledged by <@{user}> <t:{}:R>", at.timestamp())
            }
            (Some(user), None) => format!("Acknowledged by <@{user}>"),
            _ => "Acknowledged".to_owned(),
        },
        NotificationState::Silenced => {
            "Silenced in Alertmanager — every receiver is quiet".to_owned()
        }
        NotificationState::Ignored => {
            "Ignored here — Alertmanager is still notifying everyone else".to_owned()
        }
        NotificationState::Resolved => match card.alert.ends_at {
            Some(ends) => format!("Resolved <t:{}:R>", ends.timestamp()),
            None => "Resolved".to_owned(),
        },
        NotificationState::Orphaned => "This card no longer matches anything in Discord".to_owned(),
    }
}

/// The colour that carries the card's state at a glance.
///
/// State first, severity second: once somebody has taken an alert or silenced it, what a person
/// scanning a channel needs to see is that it has been dealt with.
fn colour(card: &CardData) -> Colour {
    match card.state {
        NotificationState::Acked => Colour::new(0x33_99_FF),
        NotificationState::Silenced => Colour::new(0x8A_63_D2),
        NotificationState::Ignored => Colour::new(0x77_7A_7E),
        NotificationState::Resolved => Colour::new(0x2E_A0_43),
        NotificationState::Orphaned => Colour::new(0x3A_3D_41),
        NotificationState::Firing => match card.severity() {
            Severity::Critical => Colour::new(0xD9_2D_20),
            Severity::Warning => Colour::new(0xE0_9B_00),
            Severity::Info => Colour::new(0x64_74_8B),
        },
    }
}

/// The message body, which is the mentions and nothing else.
pub(crate) fn mention_text(mentions: &[Mention]) -> String {
    mentions
        .iter()
        .map(|mention| match mention {
            Mention::Role(role) => format!("<@&{role}>"),
            Mention::User(user) => format!("<@{user}>"),
        })
        .collect::<Vec<_>>()
        .join(" ")
}

/// Drops fields from the end until the embed fits.
///
/// From the end, because [`Renderer::fields`] emits them in the order an operator reads them: the
/// status and the age survive, a twenty-first label does not. The marker says what happened, so a
/// card that lost fields does not look like a card that never had them.
fn budget(title: &str, description: &str, footer: &str, fields: &mut Vec<(String, String, bool)>) {
    let fixed = title.len() + description.len() + footer.len();
    let mut total = fixed
        + fields
            .iter()
            .map(|(k, v, _)| k.len() + v.len())
            .sum::<usize>();
    let mut dropped = false;

    while fields.len() > FIELD_COUNT_LIMIT || (total > EMBED_BUDGET && !fields.is_empty()) {
        if let Some((label, value, _)) = fields.pop() {
            total -= label.len() + value.len();
            dropped = true;
        }
    }

    if dropped && fields.len() < FIELD_COUNT_LIMIT {
        fields.push((
            "Truncated".to_owned(),
            "Some fields did not fit — `/alerts show` has all of them".to_owned(),
            false,
        ));
    }
}

/// Hashes everything a viewer can see, and nothing else.
///
/// The render time is deliberately absent. Folding it in would make every card differ from itself
/// one debounce interval later, which turns the skip-if-unchanged rule into an edit per interval
/// per open alert — the exact load it exists to prevent.
fn hash(
    card: &CardData,
    title: &str,
    description: &str,
    footer: &str,
    fields: &[(String, String, bool)],
    links: &[RenderedLink],
) -> String {
    let mut hasher = Fnv::new();

    hasher.write(card.state.as_str().as_bytes());
    hasher.write(title.as_bytes());
    hasher.write(description.as_bytes());
    hasher.write(footer.as_bytes());
    hasher.write(&colour(card).0.to_be_bytes());

    for (label, value, _) in fields {
        hasher.write(label.as_bytes());
        hasher.write(value.as_bytes());
    }

    for link in links {
        hasher.write(link.label.as_bytes());
        hasher.write(link.url.as_bytes());
    }

    format!("{:016x}", hasher.finish())
}

/// A 64-bit FNV-1a, which is enough for "did this change" and costs nothing.
struct Fnv(u64);

impl Fnv {
    /// The offset basis.
    fn new() -> Self {
        Self(0xcbf2_9ce4_8422_2325)
    }

    /// Folds in one field, with a separator so two adjacent values cannot be confused for one.
    fn write(&mut self, bytes: &[u8]) {
        for byte in bytes.iter().chain(std::iter::once(&0)) {
            self.0 ^= u64::from(*byte);
            self.0 = self.0.wrapping_mul(0x0000_0100_0000_01b3);
        }
    }

    /// The digest.
    fn finish(&self) -> u64 {
        self.0
    }
}

/// Cuts a string to a byte budget on a character boundary, marker included.
///
/// The marker counts against the budget rather than being added to it. Discord measures the limit
/// on what it receives, so a truncation that lands exactly on the cap and then appends three bytes
/// produces the rejected request it was meant to prevent.
fn truncate(value: &str, limit: usize) -> String {
    const MARKER: char = '…';

    if value.len() <= limit {
        return value.to_owned();
    }

    let mut end = limit.saturating_sub(MARKER.len_utf8()).min(value.len());
    while end > 0 && !value.is_char_boundary(end) {
        end -= 1;
    }

    let mut cut = value[..end].to_owned();
    cut.push(MARKER);
    cut
}

/// A permalink to a card, as Discord spells one.
fn message_url(card: &PreviousCard) -> String {
    // `@me` is what Discord's own links use for a direct message, whose card carries no guild.
    let guild = if card.guild.get() == 0 {
        "@me".to_owned()
    } else {
        card.guild.to_string()
    };

    format!(
        "https://discord.com/channels/{guild}/{}/{}",
        card.channel, card.message
    )
}

/// Capitalises a label name for use as a field heading.
fn title_case(label: &str) -> String {
    let mut characters = label.chars();

    match characters.next() {
        Some(first) => first.to_uppercase().collect::<String>() + characters.as_str(),
        None => String::new(),
    }
}

#[cfg(test)]
mod tests {
    use chrono::{TimeZone, Utc};
    use dam_core::{Alert, AlertStatus, AmState, Annotations, Fingerprint, LabelName, Labels};
    use dam_store::NotificationId;

    use super::*;

    fn renderer() -> Renderer {
        Renderer::new(
            Render::default(),
            LinkRenderer::new(&dam_config::Links::default()).expect("no buttons compile"),
        )
    }

    fn card(state: NotificationState, labels: &[(&str, &str)]) -> CardData {
        let at = Utc.timestamp_opt(1_700_000_000, 0).single().expect("valid");

        CardData {
            notification: NotificationId::new(42),
            digest: None,
            previous: None,
            alert: Alert {
                fingerprint: Fingerprint::new("deadbeefcafe").expect("hexadecimal"),
                labels: labels
                    .iter()
                    .map(|(name, value)| {
                        (
                            LabelName::new(*name).expect("the label name is valid"),
                            (*value).to_owned(),
                        )
                    })
                    .collect::<Labels>(),
                annotations: Annotations::new(),
                starts_at: at,
                ends_at: None,
                generator_url: None,
                status: AlertStatus::Firing,
                am_state: AmState::Active,
                silenced_by: Vec::new(),
                inhibited_by: Vec::new(),
                group_key: None,
            },
            state,
            route_name: "critical".to_owned(),
            acknowledged_by: None,
            acknowledged_at: None,
            reply_count: 0,
            flap_count: 0,
            first_seen_at: at,
            silence: None,
            ignore_reason: None,
            mentions: Vec::new(),
            rendered_at: at,
        }
    }

    #[test]
    fn an_unchanged_card_hashes_the_same_at_a_later_time() {
        let renderer = renderer();
        let first = renderer.render(&card(NotificationState::Firing, &[("alertname", "Down")]));

        let mut later = card(NotificationState::Firing, &[("alertname", "Down")]);
        later.rendered_at += chrono::Duration::hours(3);
        let second = renderer.render(&later);

        assert_eq!(
            first.hash, second.hash,
            "the render time is not part of the hash, or every open card would be edited once \
             per debounce interval forever"
        );
    }

    #[test]
    fn a_state_change_changes_the_hash() {
        let renderer = renderer();
        let firing = renderer.render(&card(NotificationState::Firing, &[("alertname", "Down")]));
        let acked = renderer.render(&card(NotificationState::Acked, &[("alertname", "Down")]));

        assert_ne!(firing.hash, acked.hash);
    }

    #[test]
    fn a_pathological_alert_still_fits_every_limit() {
        let renderer = renderer();
        let labels: Vec<(String, String)> = (0..60)
            .map(|index| (format!("label_{index}"), "x".repeat(200)))
            .collect();
        let borrowed: Vec<(&str, &str)> = labels
            .iter()
            .map(|(name, value)| (name.as_str(), value.as_str()))
            .collect();

        let mut data = card(NotificationState::Firing, &borrowed);
        data.alert
            .annotations
            .insert("description", "y".repeat(5000));

        let produced = renderer.render(&data);
        let json = serde_json::to_value(&produced.embed).expect("the embed serialises");

        let title = json["title"].as_str().unwrap_or_default();
        let description = json["description"].as_str().unwrap_or_default();
        let fields = json["fields"].as_array().map(Vec::len).unwrap_or_default();

        assert!(title.len() <= TITLE_LIMIT, "title is {} bytes", title.len());
        assert!(description.len() <= DESCRIPTION_LIMIT);
        assert!(fields <= FIELD_COUNT_LIMIT, "{fields} fields");

        let total: usize = title.len()
            + description.len()
            + json["fields"]
                .as_array()
                .map(|fields| {
                    fields
                        .iter()
                        .map(|field| {
                            field["name"].as_str().unwrap_or_default().len()
                                + field["value"].as_str().unwrap_or_default().len()
                        })
                        .sum::<usize>()
                })
                .unwrap_or_default();

        assert!(total <= EMBED_BUDGET, "the embed is {total} characters");
    }

    #[test]
    fn a_name_is_never_empty_and_never_too_long() {
        let renderer = renderer();
        let nameless = renderer.name(&card(NotificationState::Firing, &[]));

        assert!(
            !nameless.is_empty(),
            "post creation fails on an empty title"
        );
        assert!(
            nameless.contains("deadbeef"),
            "the fallback is the short fingerprint"
        );

        let long = renderer.name(&card(
            NotificationState::Firing,
            &[("alertname", &"A".repeat(300))],
        ));

        assert!(
            long.len() <= NAME_LIMIT,
            "the marker counts against the budget, not on top of it: {} bytes",
            long.len()
        );
    }

    #[test]
    fn a_resolved_card_keeps_one_disabled_control() {
        let renderer = renderer();
        let produced = renderer.render(&card(NotificationState::Resolved, &[]));

        assert_eq!(produced.components.len(), 1);
    }

    #[test]
    fn an_orphaned_card_carries_none() {
        let renderer = renderer();
        let produced = renderer.render(&card(NotificationState::Orphaned, &[]));

        assert!(produced.components.is_empty());
    }
}
