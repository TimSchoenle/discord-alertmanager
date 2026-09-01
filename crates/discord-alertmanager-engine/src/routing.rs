//! The routing snapshot: what the hot path reads, and how the configuration file becomes rows.

use std::collections::HashMap;
use std::sync::Arc;

use arc_swap::ArcSwap;
use chrono::{DateTime, Utc};
use dam_config::{RouteConfig, Severity as ConfigSeverity, TargetKind, TargetPolicy};
use dam_core::{CoreError, Labels, MatcherSet, Severity};
use dam_store::{
    ChannelId, Escalation, ForumPolicy, ForumTag, GroupStrategy, GuildId, IgnoreRule, Mentions,
    RoleId, Route, RouteId, RouteSource, RouteTarget, StateTags, Subscription, TagId, ThreadKind,
    ThreadPolicy, ThreadTrigger, UserId,
};

/// The values a configured route falls back to when it names none of its own.
///
/// Passed in rather than read here, because they come from sections of the configuration this
/// module has no other business with, and a route built from Discord has to resolve them the same
/// way a route built from the file does.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RouteDefaults {
    /// Minutes of inactivity after which a thread that is no longer firing archives.
    pub archive_after_minutes: u32,
}

impl Default for RouteDefaults {
    fn default() -> Self {
        // The configuration's own default for `render.thread_archive_after_minutes`, so a caller
        // that builds a route without one lands where the file would have put it.
        Self {
            archive_after_minutes: 1440,
        }
    }
}

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
    defaults: RouteDefaults,
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
        target: target_from_config(config, defaults),
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
        escalation: escalation_from_config(config),
        priority: config.priority,
        continue_to_next: config.continue_to_next,
        source: RouteSource::Config,
        enabled: config.enabled,
        created_by: None,
        created_at: now,
    })
}

/// Reads the target out of a configured route.
fn target_from_config(config: &RouteConfig, defaults: RouteDefaults) -> RouteTarget {
    let channel = ChannelId::new(config.target.id);
    let policy = &config.target.policy;

    match config.target.kind {
        TargetKind::Text => RouteTarget::Text {
            channel,
            thread: thread_policy_from_config(policy, defaults),
        },
        TargetKind::Forum => RouteTarget::Forum {
            channel,
            policy: forum_policy_from_config(policy, defaults),
        },
        TargetKind::Thread => RouteTarget::Thread { thread: channel },
        TargetKind::Dm => RouteTarget::Dm {
            user: UserId::new(config.target.id),
        },
    }
}

/// Reads the escalation policy out of the file, or decides the route has none.
///
/// A route that sets no deadline does not escalate, and one that sets a deadline without naming
/// anybody escalates to whoever its ordinary mentions reach. That fallback is what makes the
/// common case one key: an operator who wants the on-call role chased again writes the deadline
/// and nothing else.
fn escalation_from_config(config: &RouteConfig) -> Option<Escalation> {
    let after_secs = config.escalation.after_secs?;

    let (roles, users) = if config.escalation.roles.is_empty() && config.escalation.users.is_empty()
    {
        (&config.mentions.roles, &config.mentions.users)
    } else {
        (&config.escalation.roles, &config.escalation.users)
    };

    let escalation = Escalation {
        after_secs,
        roles: roles.iter().copied().map(RoleId::new).collect(),
        users: users.iter().copied().map(UserId::new).collect(),
    };

    // A deadline with nobody behind it would post an unaddressed line into a thread the people
    // who need it are not reading, on a cadence, forever.
    (!escalation.is_empty()).then_some(escalation)
}

/// Reads the thread policy out of the flat configuration table.
fn thread_policy_from_config(policy: &TargetPolicy, defaults: RouteDefaults) -> ThreadPolicy {
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
        archive_after_minutes: policy
            .auto_archive_minutes
            .unwrap_or(defaults.archive_after_minutes),
    }
}

/// Reads the forum policy out of the flat configuration table.
fn forum_policy_from_config(policy: &TargetPolicy, defaults: RouteDefaults) -> ForumPolicy {
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
        auto_archive_minutes: policy
            .auto_archive_minutes
            .unwrap_or(defaults.archive_after_minutes),
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

/// Rebuilds the routing snapshot from the database.
///
/// Called at startup and after any change to a route or an ignore rule. The tag cache is folded in
/// here rather than fetched on use, so applying a tag to a forum post never costs a round trip to
/// read the channel's tag list.
///
/// # Errors
///
/// Returns the store's error. A route whose matchers no longer compile is skipped with a warning
/// rather than failing the whole rebuild: one bad rule should not take the routing table with it.
pub async fn load_snapshot(
    store: &dyn dam_store::Store,
    now: chrono::DateTime<chrono::Utc>,
) -> Result<RoutingSnapshot, dam_store::StoreError> {
    let mut routes = store.routes().await?;

    let mut guilds: Vec<dam_store::GuildId> = routes.iter().map(|route| route.guild_id).collect();
    guilds.sort_unstable();
    guilds.dedup();

    let mut ignores = Vec::new();
    for guild in guilds {
        ignores.extend(store.active_ignores(guild, now).await?);
    }

    let mut forums: Vec<ChannelId> = routes
        .iter()
        .filter_map(|route| match &route.target {
            RouteTarget::Forum { channel, .. } => Some(*channel),
            _ => None,
        })
        .collect();
    forums.sort_unstable();
    forums.dedup();

    let mut tags = Vec::new();
    for forum in forums {
        tags.extend(store.forum_tags(forum).await?);
    }

    // Folded in last, so a personal subscription is evaluated by the same pass a route is and the
    // decision needs no second concept. The alternative — a parallel resolution path for direct
    // messages — would be a second place for dedupe keys, ignore scoping and card reuse to be got
    // subtly differently.
    routes.extend(
        store
            .subscriptions()
            .await?
            .into_iter()
            .map(subscription_route),
    );

    Ok(RoutingSnapshot::new(routes, ignores, tags))
}

/// The pseudo-guild every subscription is resolved under.
///
/// Zero is not a snowflake Discord issues, so it collides with no real guild. That matters twice:
/// resolution settles per guild, so one person's subscription cannot stop another's, and ignore
/// rules are guild-scoped, so a server-wide mute cannot silently reach into somebody's direct
/// messages.
const SUBSCRIPTION_GUILD: u64 = 0;

/// A subscription as the route it behaves like.
///
/// The id is negative, which no database sequence issues, so a synthetic route and a real one can
/// share the snapshot and a card can still name the thing that produced it. `continue_to_next` is
/// always true: a subscription is an addition to whatever the channel routes decided, never a
/// replacement for it.
fn subscription_route(subscription: Subscription) -> Route {
    Route {
        id: RouteId::new(-subscription.id.get()),
        guild_id: GuildId::new(SUBSCRIPTION_GUILD),
        name: format!("subscription {}", subscription.id),
        matcher_source: subscription.matcher_source,
        matchers: subscription.matchers,
        min_severity: subscription.min_severity,
        target: RouteTarget::Dm {
            user: subscription.user_id,
        },
        group_strategy: GroupStrategy::PerAlert,
        // Never. A direct message is already addressed to exactly one person, and a mention on top
        // of it is a second notification for the same thing.
        mentions: Mentions::default(),
        // Nor does it escalate. Escalating to the one person a direct message already reached is
        // the same notification sent twice.
        escalation: None,
        priority: i32::MAX,
        continue_to_next: true,
        source: RouteSource::Discord,
        enabled: true,
        created_by: Some(subscription.user_id),
        created_at: subscription.created_at,
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
            escalation: None,
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

        let route = route_from_config(
            &config,
            RouteId::new(1),
            RouteDefaults::default(),
            Utc::now(),
        )
        .expect("route parses");

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

        assert!(
            route_from_config(
                &config,
                RouteId::new(1),
                RouteDefaults::default(),
                Utc::now()
            )
            .is_err()
        );
    }
}
