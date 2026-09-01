//! The sandbox every configured template renders in, and the context it renders against.
//!
//! Label values come from metric targets. In any environment that scrapes workloads somebody else
//! wrote, that makes them attacker-influenced, and they end up in two places where that matters: a
//! URL a person is invited to click, and Discord markdown a person reads. `minijinja`'s
//! autoescaping is HTML-oriented and does neither, so the two filters below are registered and the
//! URL templates are refused at boot unless they use one.
//!
//! The environment has no filesystem loader, no `include`, and strict undefined behaviour: a
//! template naming a label that is not there fails the render, which drops one button, rather than
//! quietly producing a URL with a hole in it.

use chrono::{DateTime, Utc};
use dam_core::Alert;
use minijinja::{Environment, UndefinedBehavior, Value};
use serde_json::json;

/// Builds the sandbox.
///
/// # Panics
///
/// Never. The two filters are registered by name and neither registration can fail.
#[must_use]
pub fn environment() -> Environment<'static> {
    let mut env = Environment::new();

    // Strict, so a missing label is a render error the caller turns into a dropped button rather
    // than an empty substitution nobody notices until the link goes nowhere.
    env.set_undefined_behavior(UndefinedBehavior::Strict);
    env.set_keep_trailing_newline(false);
    env.add_filter("urlencode", urlencode);
    env.add_filter("mdescape", mdescape);

    env
}

/// Percent-encodes a value for a URL.
///
/// Encodes everything outside the unreserved set rather than trying to preserve structure: the
/// filter is only ever applied to a value being substituted into a URL somebody else wrote, and a
/// value that is allowed to carry `?`, `#` or `/` can rewrite that URL.
///
/// Takes a whole value rather than a string, because a template that interpolates a timestamp or
/// a count would otherwise fail to render and silently lose its button — and the rule that every
/// substitution passes through this filter has to hold for those too.
fn urlencode(value: &Value) -> String {
    use std::fmt::Write as _;

    let value = value.to_string();
    let mut encoded = String::with_capacity(value.len());

    for byte in value.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b'~') {
            encoded.push(char::from(byte));
        } else {
            encoded.push('%');
            let _ = write!(encoded, "{byte:02X}");
        }
    }

    encoded
}

/// Escapes Discord's markdown so a label value cannot format the message it appears in.
///
/// The backslash is escaped first, or escaping the rest would produce escapes of the escapes.
fn mdescape(value: &Value) -> String {
    let value = value.to_string();
    let mut escaped = String::with_capacity(value.len());

    for character in value.chars() {
        if matches!(
            character,
            '\\' | '*' | '_' | '~' | '`' | '|' | '>' | '#' | '-' | '[' | ']' | '(' | ')' | ':'
        ) {
            escaped.push('\\');
        }
        escaped.push(character);
    }

    escaped
}

/// The values a configured template may read.
///
/// Flat and explicit: a template can reach the alert, its labels, its annotations, the configured
/// base URLs and the graph window, and nothing else. Handing it the whole configuration would put
/// the bot token one dotted path away from a rendered button.
#[must_use]
pub fn context(
    alert: &Alert,
    bases: &[(&str, String)],
    window: Option<(i64, i64)>,
    now: DateTime<Utc>,
) -> Value {
    let labels: serde_json::Map<String, serde_json::Value> = alert
        .labels
        .iter()
        .map(|(name, value)| (name.to_string(), json!(value)))
        .collect();

    let annotations: serde_json::Map<String, serde_json::Value> = alert
        .annotations
        .iter()
        .map(|(name, value)| ((*name).to_owned(), json!(value)))
        .collect();

    let links: serde_json::Map<String, serde_json::Value> = bases
        .iter()
        .map(|(name, value)| ((*name).to_owned(), json!(value)))
        .collect();

    let mut root = json!({
        "alert": {
            "fingerprint": alert.fingerprint.as_str(),
            "name": alert.name(),
            "severity": alert.severity().as_str(),
            "status": alert.status.as_str(),
            "generator_url": alert.generator_url,
            "starts_at": alert.starts_at.to_rfc3339(),
            "ends_at": alert.ends_at.map(|ends| ends.to_rfc3339()),
        },
        "labels": labels,
        "annotations": annotations,
        "links": links,
        "now_ms": now.timestamp_millis(),
    });

    if let Some((from_ms, to_ms)) = window {
        root["window"] = json!({ "from_ms": from_ms, "to_ms": to_ms });
    }

    Value::from_serialize(&root)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_value_that_could_rewrite_a_url_is_encoded_whole() {
        assert_eq!(
            urlencode(&Value::from("../admin?x=1#f")),
            "..%2Fadmin%3Fx%3D1%23f",
            "a label value is a value, never a piece of the URL's structure"
        );
    }

    #[test]
    fn markdown_is_escaped_backslash_first() {
        assert_eq!(mdescape(&Value::from(r"a\*b")), r"a\\\*b");
    }

    #[test]
    fn a_missing_label_fails_the_render_rather_than_leaving_a_hole() {
        let env = environment();

        let rendered = env.render_str(
            "{{ labels.absent }}",
            Value::from_serialize(json!({
                "labels": { "present": "1" },
            })),
        );

        assert!(
            rendered.is_err(),
            "strict undefined behaviour is what turns a typo into a dropped button rather than a \
             link that goes nowhere"
        );
    }
}
