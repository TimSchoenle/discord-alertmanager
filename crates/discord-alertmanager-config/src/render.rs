//! How an alert card is laid out, and how often it may be edited.

use serde::Deserialize;

/// Card layout and edit budget.
#[derive(Debug, Clone, Deserialize)]
#[cfg_attr(
    feature = "config-schema",
    derive(serde::Serialize, terrace_config::schema::Describe)
)]
#[serde(default, deny_unknown_fields)]
pub struct Render {
    /// Seconds to coalesce edits to one card before sending them.
    ///
    /// Every edit is also skipped when the newly computed render hash matches the stored one, so
    /// a burst of updates that change nothing visible costs no API calls at all.
    pub debounce_secs: u64,

    /// Characters of annotation text a card may carry before it is truncated.
    ///
    /// Discord caps an embed at 6000 characters across every field, and a single alert can carry
    /// a five-thousand-character annotation. The truncation marker links to `/alerts show`, which
    /// has the full text.
    pub description_budget: usize,

    /// Labels promoted to their own inline field on the card, in order.
    ///
    /// Everything else stays in `/alerts show`. Three or four is the useful number; a card that
    /// lists sixty labels is a card nobody reads during an incident.
    pub key_labels: Vec<String>,

    /// Minutes of inactivity after which an alert thread archives.
    ///
    /// Discord accepts only 60, 1440, 4320 and 10080. A firing alert holds 10080 regardless, so
    /// that a week-long incident never archives underneath the people working it.
    pub thread_archive_after_minutes: u32,

    /// Show a short fingerprint in the card footer.
    pub show_fingerprint: bool,
}

impl Default for Render {
    fn default() -> Self {
        Self {
            debounce_secs: 3,
            description_budget: 1500,
            key_labels: vec![
                "namespace".to_owned(),
                "instance".to_owned(),
                "job".to_owned(),
            ],
            thread_archive_after_minutes: 1440,
            show_fingerprint: true,
        }
    }
}
