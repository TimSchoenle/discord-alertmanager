//! The routing snapshot: what the hot path reads, and how the configuration file becomes rows.

use std::collections::HashMap;
use std::sync::Arc;

use arc_swap::ArcSwap;
use chrono::{DateTime, Utc};
use dam_config::{RouteConfig, Severity as ConfigSeverity, TargetKind, TargetPolicy};
use dam_core::{CoreError, Labels, MatcherSet, Severity};
use dam_store::{
    ChannelId, ForumPolicy, ForumTag, GroupStrategy, GuildId, IgnoreRule, Mentions, Route, RouteId,
    RouteSource, RouteTarget, StateTags, TagId, ThreadKind, ThreadPolicy, ThreadTrigger, UserId,
};

/// Everything route resolution needs, built once and read many times.
///
/// Rebuilt whole on any change to a route, an ignore rule or a forum's tag list, and published
/// through an [`ArcSwap`]. The hot path then never takes a lock and never compiles a regex, which
/// matters because it runs once per alert per route and a storm makes that number large.
#[derive(Debug, Default)]
pub struct RoutingSnapshot {
    routes: Vec<Route>,
    ignores: Vec<IgnoreRule>,
    tags: HashMap<ChannelId, HashMap<String, TagId>>,
}

impl RoutingSnapshot {
    /// Builds a snapshot, ordering the routes the way they will be evaluated.
    ///
    /// Sorting once here is what lets resolution be a single pass: ascending priority, then by id
    /// so that two routes sharing a priority still resolve in a stable, explainable order rather
    /// than in whatever order the database returned them.
    #[must_use]
    pub fn new(mut routes: Vec<Route>, ignores: Vec<IgnoreRule>, tags: Vec<ForumTag>) -> Self {
        routes.sort_by(|left, right| {
            left.priority
                .cmp(&right.priority)
                .then_with(|| left.id.get().cmp(&right.id.get()))
        });

        let mut by_channel: HashMap<ChannelId, HashMap<String, TagId>> = HashMap::new();
        for tag in tags {
            by_channel
                .entry(tag.channel_id)
                .or_default()
                .insert(tag.name, tag.id);
        }

        Self {
            routes,
            ignores,
            tags: by_channel,
        }
    }

    /// The routes an alert is delivered to.
    ///
    /// First match wins unless the route says otherwise, which is Alertmanager's own `continue`
    /// semantics and the one operators already hold a model of. Resolution is per guild: an alert
    /// is not a guild's property, so one guild's catch-all must not stop another guild's route
    /// from ever seeing it.
    #[must_use]
    pub fn resolve(&self, labels: &Labels, severity: Severity) -> Vec<&Route> {
        let mut matched = Vec::new();
        let mut settled: Vec<GuildId> = Vec::new();

        for route in &self.routes {
            if settled.contains(&route.guild_id) || !route.accepts(labels, severity) {
                continue;
            }

            matched.push(route);
            if !route.continue_to_next {
                settled.push(route.guild_id);
            }
        }
        matched
    }

    /// The ignore rule muting an alert in a channel, if one is.
    ///
    /// Guild-scoped rules are considered before channel-scoped ones, so the broader mute is the
    /// one reported when both apply and the answer to "why is this quiet" is the one somebody
    /// meant.
    #[must_use]
    pub fn ignore_for(
        &self,
        guild: GuildId,
        channel: ChannelId,
        labels: &Labels,
        now: DateTime<Utc>,
    ) -> Option<&IgnoreRule> {
        self.ignores
            .iter()
            .filter(|rule| rule.guild_id == guild && rule.mutes(labels, channel, now))
            .min_by_key(|rule| match rule.scope {
                dam_store::IgnoreScope::Guild => 0,
                dam_store::IgnoreScope::Channel => 1,
            })
    }

    /// One route by id.
    #[must_use]
    pub fn route(&self, id: RouteId) -> Option<&Route> {
        self.routes.iter().find(|route| route.id == id)
    }

    /// Every route in the snapshot, in evaluation order.
    #[must_use]
    pub fn routes(&self) -> &[Route] {
        &self.routes
    }

    /// The id of a tag by name on a forum channel.
    #[must_use]
    pub fn tag_id(&self, channel: ChannelId, name: &str) -> Option<TagId> {
        self.tags
            .get(&channel)
            .and_then(|tags| tags.get(name))
            .copied()
    }

    /// How many tags a forum channel already has, for the budget check.
    #[must_use]
    pub fn tag_count(&self, channel: ChannelId) -> usize {
        self.tags.get(&channel).map_or(0, HashMap::len)
    }
}

/// The published snapshot, swapped whole on every change.
///
/// A new snapshot replaces the old one in one atomic store. A reader that started before the swap
/// finishes against the old one, which is correct: it is a consistent view, just a slightly stale
/// one, and no alert is evaluated against half of a route table.
#[derive(Debug, Default)]
pub struct SharedRouting(ArcSwap<RoutingSnapshot>);

impl SharedRouting {
    /// Publishes an initial snapshot.
    #[must_use]
    pub fn new(snapshot: RoutingSnapshot) -> Self {
        Self(ArcSwap::from_pointee(snapshot))
    }

    /// The current snapshot.
    #[must_use]
    pub fn load(&self) -> Arc<RoutingSnapshot> {
        self.0.load_full()
    }

    /// Replaces the snapshot.
    pub fn store(&self, snapshot: RoutingSnapshot) {
        self.0.store(Arc::new(snapshot));
    }
}

/// Turns a route declared in the configuration file into a row.
///
/// The file's shape and the row's shape differ on purpose: a TOML table can only express a flat
/// set of keys with a discriminant, while the four delivery targets have different mechanics and
/// are better off as an enum. This is the one place that translation happens.
///
/// # Errors
///
/// Returns [`CoreError`] when the route's matcher expression does not parse or its regexes do not
/// compile. A route that cannot be evaluated is a startup failure, not a route that silently
/// matches nothing.
pub fn route_from_config(
    config: &RouteConfig,
    id: RouteId,
    now: DateTime<Utc>,
) -> Result<Route, CoreError> {
    let matchers = MatcherSet::parse(&config.matchers)?;

    Ok(Route {
        id,
        guild_id: GuildId::new(config.guild_id),
        name: config.name.clone(),
        matcher_source: config.matchers.clone(),
        matchers,
        min_severity: config.min_severity.map(severity_from_config),
        target: target_from_config(config),
        group_strategy: group_strategy_from_config(config.group_strategy),
        mentions: Mentions {
            roles: config
                .mentions
                .roles
                .iter()
                .copied()
                .map(Into::into)
                .collect(),
            users: config
                .mentions
                .users
                .iter()
                .copied()
                .map(Into::into)
                .collect(),
            min_severity: config.mentions.min_severity.map(severity_from_config),
        },
        priority: config.priority,
        continue_to_next: config.continue_to_next,
        source: RouteSource::Config,
        enabled: config.enabled,
        created_by: None,
        created_at: now,
    })
}

/// Reads the target out of a configured route.
fn target_from_config(config: &RouteConfig) -> RouteTarget {
    let channel = ChannelId::new(config.target.id);
    let policy = &config.target.policy;

    match config.target.kind {
        TargetKind::Text => RouteTarget::Text {
            channel,
            thread: thread_policy_from_config(policy),
        },
        TargetKind::Forum => RouteTarget::Forum {
            channel,
            policy: forum_policy_from_config(policy),
        },
        TargetKind::Thread => RouteTarget::Thread { thread: channel },
        TargetKind::Dm => RouteTarget::Dm {
            user: UserId::new(config.target.id),
        },
    }
}

/// Reads the thread policy out of the flat configuration table.
fn thread_policy_from_config(policy: &TargetPolicy) -> ThreadPolicy {
    ThreadPolicy {
        when: match policy.thread_when {
            dam_config::ThreadTrigger::Never => ThreadTrigger::Never,
            dam_config::ThreadTrigger::OnCreate => ThreadTrigger::OnCreate,
            dam_config::ThreadTrigger::OnAck => ThreadTrigger::OnAck,
            dam_config::ThreadTrigger::OnFirstReply => ThreadTrigger::OnFirstReply,
        },
        kind: match policy.thread_kind {
            dam_config::ThreadKind::Public => ThreadKind::Public,
            dam_config::ThreadKind::Private => ThreadKind::Private,
        },
        archive_after_minutes: policy.auto_archive_minutes,
    }
}

/// Reads the forum policy out of the flat configuration table.
fn forum_policy_from_config(policy: &TargetPolicy) -> ForumPolicy {
    ForumPolicy {
        title_template: policy.title_template.clone(),
        manage_tags: policy.manage_tags,
        state_tags: StateTags {
            firing: policy.state_tags.firing.clone(),
            acked: policy.state_tags.acked.clone(),
            silenced: policy.state_tags.silenced.clone(),
            resolved: policy.state_tags.resolved.clone(),
        },
        severity_tags: policy.severity_tags,
        // Three at most, because a forum channel holds twenty tags and four are already spent on
        // states and three on severities. A fourth label would leave no room for a new value.
        label_tags: policy.label_tags.iter().take(3).cloned().collect(),
        default_tag: Some(policy.default_tag.clone()).filter(|tag| !tag.is_empty()),
        auto_archive_minutes: policy.auto_archive_minutes,
        archive_on_resolve: policy.archive_on_resolve,
        lock_on_resolve: policy.lock_on_resolve,
        pin_min_severity: policy.pin_min_severity.map(severity_from_config),
        max_pinned: policy.max_pinned,
        bump_on_state_change: policy.bump_on_state_change,
    }
}

/// Converts the configuration file's grouping strategy into the stored one.
fn group_strategy_from_config(strategy: dam_config::GroupStrategy) -> GroupStrategy {
    match strategy {
        dam_config::GroupStrategy::PerAlert => GroupStrategy::PerAlert,
        dam_config::GroupStrategy::PerGroup => GroupStrategy::PerGroup,
        dam_config::GroupStrategy::Digest => GroupStrategy::Digest,
    }
}

/// Converts the configuration file's severity into the domain's.
///
/// The two enums stay separate deliberately. One is a documented configuration value with an
/// entry in the generated reference; the other is parsed out of a label that may say `crit` or
/// `page`. This is the single point where they meet.
#[must_use]
pub fn severity_from_config(severity: ConfigSeverity) -> Severity {
    match severity {
        ConfigSeverity::Info => Severity::Info,
        ConfigSeverity::Warning => Severity::Warning,
        ConfigSeverity::Critical => Severity::Critical,
    }
}

#[cfg(test)]
mod tests {
    use dam_core::LabelName;
    use dam_store::{IgnoreId, IgnoreScope};

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

    fn route(id: i64, expression: &str, priority: i32, continue_to_next: bool) -> Route {
        Route {
            id: RouteId::new(id),
            guild_id: GuildId::new(1),
            name: format!("route-{id}"),
            matcher_source: expression.to_owned(),
            matchers: MatcherSet::parse(expression).expect("expression parses"),
            min_severity: None,
            target: RouteTarget::Text {
                channel: ChannelId::new(100 + id.cast_unsigned()),
                thread: ThreadPolicy::default(),
            },
            group_strategy: GroupStrategy::PerAlert,
            mentions: Mentions::default(),
            priority,
            continue_to_next,
            source: RouteSource::Config,
            enabled: true,
            created_by: None,
            created_at: Utc::now(),
        }
    }

    #[test]
    fn the_first_matching_route_wins() {
        let snapshot = RoutingSnapshot::new(
            vec![
                route(2, "severity=critical", 50, false),
                route(1, "", 10, false),
            ],
            Vec::new(),
            Vec::new(),
        );

        let matched = snapshot.resolve(&labels(&[("severity", "critical")]), Severity::Critical);

        assert_eq!(matched.len(), 1);
        assert_eq!(matched[0].id, RouteId::new(1));
    }

    #[test]
    fn a_continuing_route_fans_out() {
        let snapshot = RoutingSnapshot::new(
            vec![
                route(1, "", 10, true),
                route(2, "severity=critical", 50, false),
                route(3, "", 90, false),
            ],
            Vec::new(),
            Vec::new(),
        );

        let matched = snapshot.resolve(&labels(&[("severity", "critical")]), Severity::Critical);

        assert_eq!(matched.len(), 2);
        assert_eq!(matched[1].id, RouteId::new(2));
    }

    #[test]
    fn each_guild_resolves_independently() {
        let mut other = route(2, "", 10, false);
        other.guild_id = GuildId::new(2);
        let snapshot =
            RoutingSnapshot::new(vec![route(1, "", 10, false), other], Vec::new(), Vec::new());

        let matched = snapshot.resolve(&Labels::new(), Severity::Critical);

        // One catch-all in each guild, and the first does not stop the second: an alert belongs
        // to no guild, so "first match wins" can only mean first within a guild.
        assert_eq!(matched.len(), 2);
        assert_eq!(matched[0].guild_id, GuildId::new(1));
        assert_eq!(matched[1].guild_id, GuildId::new(2));
    }

    #[test]
    fn a_guild_ignore_is_reported_before_a_channel_one() {
        let now = Utc::now();
        let ignore = |id: i64, scope: IgnoreScope| IgnoreRule {
            id: IgnoreId::new(id),
            scope,
            guild_id: GuildId::new(1),
            channel_id: Some(ChannelId::new(101)),
            matcher_source: String::new(),
            matchers: MatcherSet::default(),
            reason: format!("reason-{id}"),
            created_by: UserId::new(9),
            created_at: now,
            expires_at: None,
            revoked_at: None,
        };
        let snapshot = RoutingSnapshot::new(
            Vec::new(),
            vec![
                ignore(1, IgnoreScope::Channel),
                ignore(2, IgnoreScope::Guild),
            ],
            Vec::new(),
        );

        let found = snapshot
            .ignore_for(GuildId::new(1), ChannelId::new(101), &Labels::new(), now)
            .expect("a rule matches");

        assert_eq!(found.id, IgnoreId::new(2));
    }

    #[test]
    fn a_configured_route_becomes_a_row() {
        let mut config = RouteConfig {
            name: "critical".to_owned(),
            guild_id: 42,
            matchers: "severity=critical".to_owned(),
            ..RouteConfig::default()
        };
        config.target.kind = TargetKind::Forum;
        config.target.id = 777;
        config.target.policy.label_tags = vec![
            "a".to_owned(),
            "b".to_owned(),
            "c".to_owned(),
            "d".to_owned(),
        ];

        let route = route_from_config(&config, RouteId::new(1), Utc::now()).expect("route parses");

        assert_eq!(route.guild_id, GuildId::new(42));
        assert!(route.target.is_forum());
        assert_eq!(route.source, RouteSource::Config);
        match &route.target {
            RouteTarget::Forum { channel, policy } => {
                assert_eq!(*channel, ChannelId::new(777));
                // The fourth label tag is dropped rather than allowed to exhaust the channel's
                // budget of twenty.
                assert_eq!(policy.label_tags.len(), 3);
            }
            other => panic!("expected a forum target, got {other:?}"),
        }
    }

    #[test]
    fn a_route_whose_matchers_do_not_parse_is_refused() {
        let config = RouteConfig {
            matchers: "severity".to_owned(),
            ..RouteConfig::default()
        };

        assert!(route_from_config(&config, RouteId::new(1), Utc::now()).is_err());
    }
}
