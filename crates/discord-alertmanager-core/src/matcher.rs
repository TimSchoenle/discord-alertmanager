//! Alertmanager's matcher semantics, including the two edges that are easy to get wrong.

use std::fmt;
use std::sync::Arc;

use regex::{Regex, RegexBuilder};
use serde::{Deserialize, Serialize};

use crate::CoreError;
use crate::labels::{LabelName, Labels};

/// Longest regex source a matcher may carry.
///
/// Patterns arrive from `/route add` and `/ignore add`, so from anyone holding the capability,
/// not only from whoever writes the configuration file.
pub const MAX_REGEX_LEN: usize = 512;

/// Compiled-program size ceiling for one matcher regex.
const REGEX_SIZE_LIMIT: usize = 64 * 1024;

/// Lazy-DFA cache ceiling for one matcher regex.
const REGEX_DFA_SIZE_LIMIT: usize = 256 * 1024;

/// The four comparisons Alertmanager supports.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MatchOp {
    /// `=`, equality.
    Equal,

    /// `!=`, inequality.
    NotEqual,

    /// `=~`, a fully anchored regex match.
    RegexMatch,

    /// `!~`, a fully anchored regex non-match.
    RegexNotMatch,
}

impl MatchOp {
    /// The operator as it is written in a matcher expression.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Equal => "=",
            Self::NotEqual => "!=",
            Self::RegexMatch => "=~",
            Self::RegexNotMatch => "!~",
        }
    }

    /// Whether the right-hand side is a regex.
    #[must_use]
    pub fn is_regex(self) -> bool {
        matches!(self, Self::RegexMatch | Self::RegexNotMatch)
    }

    /// Whether a match of the right-hand side means the matcher is satisfied.
    ///
    /// This is Alertmanager's `isEqual` field, which pairs with `isRegex` to encode all four
    /// operators in two booleans. Both are sent on the wire when a silence is created.
    #[must_use]
    pub fn is_equal(self) -> bool {
        matches!(self, Self::Equal | Self::RegexMatch)
    }
}

impl fmt::Display for MatchOp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// One label comparison.
///
/// The compiled regex is built once, at construction, and shared by every evaluation. Compiling
/// on the hot path would put a regex compile inside the loop that runs for every alert against
/// every route.
#[derive(Debug, Clone)]
pub struct Matcher {
    name: LabelName,
    op: MatchOp,
    value: String,
    regex: Option<Arc<Regex>>,
}

impl Matcher {
    /// Builds and, for a regex operator, compiles a matcher.
    ///
    /// The pattern is anchored with `^(?:…)$` exactly as Alertmanager anchors it, so
    /// `severity=~crit` does not match `critical` here either. The non-capturing group matters:
    /// `^crit|warn$` without it would parse as `(^crit)|(warn$)` and quietly match far more than
    /// the operator who wrote it intended.
    ///
    /// # Errors
    ///
    /// Returns [`CoreError::RegexTooLong`] above [`MAX_REGEX_LEN`], or [`CoreError::BadRegex`]
    /// when the pattern does not compile inside the size limits.
    pub fn new(name: LabelName, op: MatchOp, value: impl Into<String>) -> Result<Self, CoreError> {
        let value = value.into();

        let regex = if op.is_regex() {
            if value.len() > MAX_REGEX_LEN {
                return Err(CoreError::RegexTooLong {
                    len: value.len(),
                    max: MAX_REGEX_LEN,
                });
            }

            let compiled = RegexBuilder::new(&format!("^(?:{value})$"))
                .size_limit(REGEX_SIZE_LIMIT)
                .dfa_size_limit(REGEX_DFA_SIZE_LIMIT)
                .build()
                .map_err(|source| CoreError::BadRegex {
                    pattern: value.clone(),
                    detail: source.to_string(),
                })?;

            Some(Arc::new(compiled))
        } else {
            None
        };

        Ok(Self {
            name,
            op,
            value,
            regex,
        })
    }

    /// The label this matcher reads.
    #[must_use]
    pub fn name(&self) -> &LabelName {
        &self.name
    }

    /// The comparison.
    #[must_use]
    pub fn op(&self) -> MatchOp {
        self.op
    }

    /// The right-hand side, as written.
    #[must_use]
    pub fn value(&self) -> &str {
        &self.value
    }

    /// Whether a label set satisfies this matcher.
    #[must_use]
    pub fn matches(&self, labels: &Labels) -> bool {
        // An absent label is the empty string, not a non-match. `instance!=db-1` therefore holds
        // for an alert carrying no `instance`, which is what Alertmanager does and what an
        // operator who has written a silence expects.
        let actual = labels.get_or_empty(self.name.as_str());

        match self.op {
            MatchOp::Equal => actual == self.value,
            MatchOp::NotEqual => actual != self.value,
            MatchOp::RegexMatch => self.regex.as_ref().is_some_and(|re| re.is_match(actual)),
            MatchOp::RegexNotMatch => self.regex.as_ref().is_some_and(|re| !re.is_match(actual)),
        }
    }
}

impl fmt::Display for Matcher {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}{}{}", self.name, self.op, self.value)
    }
}

impl PartialEq for Matcher {
    fn eq(&self, other: &Self) -> bool {
        // The compiled regex is a function of the other three fields, so comparing it would only
        // compare two pointers that happen to differ.
        self.name == other.name && self.op == other.op && self.value == other.value
    }
}

impl Eq for Matcher {}

/// A conjunction of matchers. All of them must hold.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MatcherSet(Vec<Matcher>);

impl MatcherSet {
    /// Wraps a list of matchers.
    #[must_use]
    pub fn new(matchers: Vec<Matcher>) -> Self {
        Self(matchers)
    }

    /// Parses an Alertmanager matcher expression such as `severity=critical, namespace=~prod-.*`.
    ///
    /// Commas separate matchers, and a value may be quoted to carry one. Surrounding braces are
    /// accepted and ignored, so an expression copied out of `amtool` or a Prometheus rule works
    /// unchanged.
    ///
    /// # Errors
    ///
    /// Returns [`CoreError::BadMatcherExpression`] for a matcher with no operator or an empty
    /// name, and propagates the label-name and regex errors of [`Matcher::new`].
    pub fn parse(expression: &str) -> Result<Self, CoreError> {
        let trimmed = expression.trim();
        let trimmed = trimmed
            .strip_prefix('{')
            .and_then(|rest| rest.strip_suffix('}'))
            .unwrap_or(trimmed);

        let mut matchers = Vec::new();
        for part in split_top_level(trimmed) {
            let part = part.trim();
            if part.is_empty() {
                continue;
            }
            matchers.push(parse_matcher(part)?);
        }

        Ok(Self(matchers))
    }

    /// Whether every matcher in the set holds.
    ///
    /// An empty set matches everything, which is what makes a route with no matchers a catch-all
    /// and is Alertmanager's behaviour for a route with no `match` block.
    #[must_use]
    pub fn matches(&self, labels: &Labels) -> bool {
        self.0.iter().all(|matcher| matcher.matches(labels))
    }

    /// The matchers, in the order they were written.
    #[must_use]
    pub fn as_slice(&self) -> &[Matcher] {
        &self.0
    }

    /// Number of matchers in the set.
    #[must_use]
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Whether the set is a catch-all.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl fmt::Display for MatcherSet {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut first = true;
        for matcher in &self.0 {
            if !first {
                f.write_str(", ")?;
            }
            write!(f, "{matcher}")?;
            first = false;
        }
        Ok(())
    }
}

/// Splits on commas that are not inside a quoted value.
///
/// `namespace=~"prod,staging"` is one matcher, and splitting it into two would produce a pair of
/// unparseable halves rather than an error anyone could act on.
fn split_top_level(expression: &str) -> Vec<&str> {
    let mut parts = Vec::new();
    let mut start = 0;
    let mut quoted = false;
    let mut escaped = false;

    for (index, ch) in expression.char_indices() {
        if escaped {
            escaped = false;
            continue;
        }
        match ch {
            '\\' if quoted => escaped = true,
            '"' => quoted = !quoted,
            ',' if !quoted => {
                parts.push(&expression[start..index]);
                start = index + 1;
            }
            _ => {}
        }
    }

    parts.push(&expression[start..]);
    parts
}

/// Parses one `name<op>value` matcher.
fn parse_matcher(part: &str) -> Result<Matcher, CoreError> {
    // Longest operator first: `!=` and `=~` both contain a character that would otherwise be read
    // as the whole of `=`, splitting the matcher one byte too early.
    const OPERATORS: [(&str, MatchOp); 4] = [
        ("=~", MatchOp::RegexMatch),
        ("!~", MatchOp::RegexNotMatch),
        ("!=", MatchOp::NotEqual),
        ("=", MatchOp::Equal),
    ];

    let (index, op) = OPERATORS
        .iter()
        .filter_map(|(token, op)| part.find(token).map(|index| (index, *op, token.len())))
        .min_by_key(|(index, _, len)| (*index, usize::MAX - *len))
        .map(|(index, op, _)| (index, op))
        .ok_or_else(|| CoreError::BadMatcherExpression {
            expression: part.to_owned(),
            detail: "no =, !=, =~ or !~ operator".to_owned(),
        })?;

    let name = part[..index].trim();
    let value = part[index + op.as_str().len()..].trim();
    let value = unquote(value);

    Matcher::new(LabelName::new(name)?, op, value)
}

/// Strips surrounding double quotes and unescapes `\"` and `\\` inside them.
fn unquote(value: &str) -> String {
    let Some(inner) = value.strip_prefix('"').and_then(|v| v.strip_suffix('"')) else {
        return value.to_owned();
    };

    let mut out = String::with_capacity(inner.len());
    let mut escaped = false;
    for ch in inner.chars() {
        match (escaped, ch) {
            (false, '\\') => escaped = true,
            (_, ch) => {
                out.push(ch);
                escaped = false;
            }
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

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

    #[rstest]
    #[case("severity=critical", true)]
    #[case("severity=warning", false)]
    #[case("severity!=warning", true)]
    #[case("severity=~crit.*", true)]
    #[case("severity=~crit", false)]
    #[case("severity!~warn.*", true)]
    #[case("namespace=prod, severity=critical", true)]
    #[case("namespace=prod, severity=warning", false)]
    #[case("{namespace=prod}", true)]
    #[case("", true)]
    fn expressions_evaluate_the_way_alertmanager_evaluates_them(
        #[case] expression: &str,
        #[case] expected: bool,
    ) {
        let set = MatcherSet::parse(expression).expect("expression parses");
        let labels = labels(&[
            ("alertname", "PodDown"),
            ("namespace", "prod"),
            ("severity", "critical"),
        ]);

        assert_eq!(set.matches(&labels), expected, "{expression}");
    }

    #[test]
    fn regexes_are_fully_anchored() {
        let set = MatcherSet::parse("alertname=~Pod").expect("expression parses");

        assert!(!set.matches(&labels(&[("alertname", "PodDown")])));
    }

    #[test]
    fn an_alternation_is_anchored_as_a_whole() {
        // `^crit|warn$` without the non-capturing group matches anything containing `warn`.
        let set = MatcherSet::parse("severity=~crit|warn").expect("expression parses");

        assert!(set.matches(&labels(&[("severity", "warn")])));
        assert!(!set.matches(&labels(&[("severity", "prewarn")])));
    }

    #[test]
    fn an_absent_label_is_the_empty_string() {
        let empty = labels(&[("alertname", "PodDown")]);

        assert!(
            MatcherSet::parse("severity!=critical")
                .expect("expression parses")
                .matches(&empty)
        );
        assert!(
            MatcherSet::parse("severity=~.*")
                .expect("expression parses")
                .matches(&empty)
        );
        assert!(
            !MatcherSet::parse("severity=~.+")
                .expect("expression parses")
                .matches(&empty)
        );
    }

    #[test]
    fn a_quoted_value_may_carry_a_comma() {
        let set = MatcherSet::parse(r#"summary="one, two", severity=critical"#)
            .expect("expression parses");

        assert_eq!(set.len(), 2);
        assert_eq!(set.as_slice()[0].value(), "one, two");
    }

    #[test]
    fn the_longest_operator_at_the_earliest_position_wins() {
        let set = MatcherSet::parse("severity=~a=b").expect("expression parses");

        assert_eq!(set.as_slice()[0].op(), MatchOp::RegexMatch);
        assert_eq!(set.as_slice()[0].value(), "a=b");
    }

    #[test]
    fn a_matcher_without_an_operator_is_an_error() {
        assert!(MatcherSet::parse("severity").is_err());
    }

    #[test]
    fn an_oversized_pattern_is_refused_before_it_is_compiled() {
        let pattern = "a".repeat(MAX_REGEX_LEN + 1);

        assert!(MatcherSet::parse(&format!("severity=~{pattern}")).is_err());
    }

    #[test]
    fn is_regex_and_is_equal_encode_all_four_operators() {
        let cases = [
            (MatchOp::Equal, false, true),
            (MatchOp::NotEqual, false, false),
            (MatchOp::RegexMatch, true, true),
            (MatchOp::RegexNotMatch, true, false),
        ];

        for (op, is_regex, is_equal) in cases {
            assert_eq!(op.is_regex(), is_regex, "{op}");
            assert_eq!(op.is_equal(), is_equal, "{op}");
        }
    }
}
