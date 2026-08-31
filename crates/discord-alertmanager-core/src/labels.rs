//! Label sets, alert identity, and the two hashes that identify an alert.

use std::borrow::Borrow;
use std::collections::BTreeMap;
use std::fmt;

use serde::{Deserialize, Serialize};

use crate::CoreError;

/// Longest label name or value this crate will accept.
///
/// Prometheus imposes no limit of its own, and a scrape target is free to emit a megabyte of
/// label value. Everything downstream — an embed field, a forum tag, a URL parameter — is
/// bounded, so the bound belongs here where it is applied once.
pub const MAX_LABEL_LEN: usize = 4096;

/// A validated Prometheus label name.
///
/// The grammar is Prometheus's own, `[a-zA-Z_][a-zA-Z0-9_]*`. Validating on construction is what
/// lets the matcher, the renderer and the URL templates treat a name as safe without each
/// re-checking it.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct LabelName(String);

impl LabelName {
    /// Validates and wraps a label name.
    ///
    /// # Errors
    ///
    /// Returns [`CoreError::InvalidLabelName`] when the name is empty, over [`MAX_LABEL_LEN`], or
    /// contains a character outside `[a-zA-Z0-9_]`, or starts with a digit.
    pub fn new(name: impl Into<String>) -> Result<Self, CoreError> {
        let name = name.into();
        let valid = !name.is_empty()
            && name.len() <= MAX_LABEL_LEN
            && !name.starts_with(|c: char| c.is_ascii_digit())
            && name.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_');

        if valid {
            Ok(Self(name))
        } else {
            Err(CoreError::InvalidLabelName { name })
        }
    }

    /// The name as a string slice.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for LabelName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

// What lets a `BTreeMap<LabelName, _>` be probed with a plain `&str`. Without it every lookup
// against a literal would have to build and validate a `LabelName` first, which is a validation
// the caller already knows it does not need.
impl Borrow<str> for LabelName {
    fn borrow(&self) -> &str {
        &self.0
    }
}

impl TryFrom<String> for LabelName {
    type Error = CoreError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl From<LabelName> for String {
    fn from(value: LabelName) -> Self {
        value.0
    }
}

/// The label set of one alert.
///
/// Ordered rather than hashed, because the order is load-bearing three times over: the local
/// hash below is defined over sorted pairs, the rendered card lists labels in a stable order so
/// two consecutive renders of an unchanged alert compare equal, and a diff between two label sets
/// is a merge of two sorted sequences rather than a pair of lookups.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Labels(BTreeMap<LabelName, String>);

impl Labels {
    /// An empty set.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Looks a label up by name.
    ///
    /// Takes `&str` rather than `&LabelName` so a caller with a literal does not have to
    /// construct and unwrap a validated name to answer a question about one.
    #[must_use]
    pub fn get(&self, name: &str) -> Option<&str> {
        self.0.get(name).map(String::as_str)
    }

    /// The value of a label, or the empty string when it is absent.
    ///
    /// Alertmanager matches an absent label against the empty string, so a matcher asking
    /// `severity!=critical` matches an alert carrying no `severity` at all. Every matcher
    /// comparison goes through here rather than through [`Labels::get`], so that rule is applied
    /// in one place instead of at each of the four operators.
    #[must_use]
    pub fn get_or_empty(&self, name: &str) -> &str {
        self.get(name).unwrap_or("")
    }

    /// Inserts a label, replacing any previous value.
    ///
    /// # Errors
    ///
    /// Returns [`CoreError::LabelValueTooLong`] when the value exceeds [`MAX_LABEL_LEN`].
    pub fn insert(&mut self, name: LabelName, value: impl Into<String>) -> Result<(), CoreError> {
        let value = value.into();
        if value.len() > MAX_LABEL_LEN {
            return Err(CoreError::LabelValueTooLong {
                name: name.to_string(),
                len: value.len(),
            });
        }
        self.0.insert(name, value);
        Ok(())
    }

    /// Number of labels in the set.
    #[must_use]
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Whether the set carries no labels.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Iterates the set in name order.
    pub fn iter(&self) -> impl Iterator<Item = (&LabelName, &str)> {
        self.0.iter().map(|(name, value)| (name, value.as_str()))
    }

    /// The `alertname` label, which every Prometheus alert carries.
    #[must_use]
    pub fn alertname(&self) -> Option<&str> {
        self.get("alertname")
    }

    /// A hash of the whole set, computed locally.
    ///
    /// Alertmanager's `fingerprint` is the identity this bot stores against, and it is computed
    /// by Alertmanager. This hash is computed here over the same information, so a change in
    /// Alertmanager's hashing across an upgrade shows up as two identities disagreeing about one
    /// alert rather than as silently duplicated cards.
    #[must_use]
    pub fn labels_hash(&self) -> LabelsHash {
        // `name=value` pairs in name order, joined by NUL. NUL cannot appear in a label name and
        // is vanishingly unlikely in a value, so `ab=c` and `a=bc` differ by construction rather
        // than by luck.
        let mut hash = FNV_OFFSET;
        for (name, value) in self.iter() {
            hash = fnv1a(hash, name.as_str().as_bytes());
            hash = fnv1a(hash, b"=");
            hash = fnv1a(hash, value.as_bytes());
            hash = fnv1a(hash, b"\0");
        }

        LabelsHash(format!("{hash:016x}"))
    }
}

impl FromIterator<(LabelName, String)> for Labels {
    fn from_iter<T: IntoIterator<Item = (LabelName, String)>>(iter: T) -> Self {
        Self(iter.into_iter().collect())
    }
}

/// Alertmanager's per-alert fingerprint, and this bot's primary key for an alert.
///
/// Alertmanager derives it from the label set, so the same alert firing twice carries the same
/// fingerprint and a redelivered webhook cannot create a second row.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Fingerprint(String);

impl Fingerprint {
    /// Wraps a fingerprint received from Alertmanager.
    ///
    /// # Errors
    ///
    /// Returns [`CoreError::InvalidFingerprint`] unless the value is 1 to 64 lowercase hex
    /// digits. Alertmanager emits sixteen; the range is wider so a future hash width is a
    /// configuration problem rather than a deserialisation failure.
    pub fn new(value: impl Into<String>) -> Result<Self, CoreError> {
        let value = value.into();
        let valid = (1..=64).contains(&value.len())
            && value
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b));

        if valid {
            Ok(Self(value))
        } else {
            Err(CoreError::InvalidFingerprint { value })
        }
    }

    /// The fingerprint as a string slice.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// The leading eight characters, for a card footer.
    #[must_use]
    pub fn short(&self) -> &str {
        let end = self
            .0
            .char_indices()
            .nth(8)
            .map_or(self.0.len(), |(i, _)| i);
        &self.0[..end]
    }
}

impl fmt::Display for Fingerprint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl TryFrom<String> for Fingerprint {
    type Error = CoreError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

/// The locally computed hash of a label set, stored beside the fingerprint.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct LabelsHash(String);

impl LabelsHash {
    /// The hash as a string slice.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for LabelsHash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// Alertmanager's group key, identifying one group of alerts under one route.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct GroupKey(String);

impl GroupKey {
    /// Wraps a group key received from Alertmanager.
    #[must_use]
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    /// The key as a string slice.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for GroupKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// Starting value of the 64-bit FNV-1a hash.
pub(crate) const FNV_OFFSET: u64 = 0xcbf2_9ce4_8422_2325;

/// Multiplier of the 64-bit FNV-1a hash.
const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;

/// Folds `bytes` into a running 64-bit FNV-1a hash.
///
/// Not a cryptographic hash and not asked to be one. It identifies a label set and it picks a
/// worker lane; both want a short, stable, dependency-free digest, and neither is defending
/// against somebody constructing a collision.
pub(crate) fn fnv1a(mut hash: u64, bytes: &[u8]) -> u64 {
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    hash
}

#[cfg(test)]
mod tests {
    use super::*;

    fn labels(pairs: &[(&str, &str)]) -> Labels {
        pairs
            .iter()
            .map(|(name, value)| {
                (
                    LabelName::new(*name).expect("test label name is valid"),
                    (*value).to_owned(),
                )
            })
            .collect()
    }

    #[test]
    fn label_name_rejects_the_prometheus_grammar_violations() {
        assert!(LabelName::new("alertname").is_ok());
        assert!(LabelName::new("_private").is_ok());
        assert!(LabelName::new("k8s_pod_0").is_ok());
        assert!(LabelName::new("").is_err());
        assert!(LabelName::new("0leading").is_err());
        assert!(LabelName::new("has-dash").is_err());
        assert!(LabelName::new("has space").is_err());
    }

    #[test]
    fn absent_label_reads_as_the_empty_string() {
        let labels = labels(&[("alertname", "Down")]);

        assert_eq!(labels.get("severity"), None);
        assert_eq!(labels.get_or_empty("severity"), "");
    }

    #[test]
    fn labels_hash_is_order_independent_and_value_sensitive() {
        let one = labels(&[("a", "1"), ("b", "2")]);
        let other = labels(&[("b", "2"), ("a", "1")]);
        let different = labels(&[("a", "1"), ("b", "3")]);

        assert_eq!(one.labels_hash(), other.labels_hash());
        assert_ne!(one.labels_hash(), different.labels_hash());
    }

    #[test]
    fn labels_hash_separates_pairs_that_would_otherwise_concatenate() {
        // `ab=c` and `a=bc` are the same byte sequence without the separators.
        let one = labels(&[("ab", "c")]);
        let other = labels(&[("a", "bc")]);

        assert_ne!(one.labels_hash(), other.labels_hash());
    }

    #[test]
    fn fingerprint_accepts_alertmanagers_shape_only() {
        assert!(Fingerprint::new("0123456789abcdef").is_ok());
        assert!(Fingerprint::new("").is_err());
        assert!(Fingerprint::new("0123456789ABCDEF").is_err());
        assert!(Fingerprint::new("not-hex").is_err());
    }

    #[test]
    fn short_fingerprint_never_panics_on_a_short_value() {
        let fingerprint = Fingerprint::new("abc").expect("hex is a valid fingerprint");

        assert_eq!(fingerprint.short(), "abc");
    }
}
