//! Cases for routes, ignore rules, subscriptions and forum tags.

use dam_core::MatcherSet;

use crate::ids::{ChannelId, GuildId, IgnoreId, RoleId, RouteId, SubscriptionId, UserId};
use crate::routing::{
    Escalation, ForumTag, GroupStrategy, IgnoreRule, IgnoreScope, Mentions, Route, RouteSource,
    RouteTarget, Subscription, ThreadPolicy,
};
use crate::{Store, StoreError};

use super::{at, labels};

/// A subscription belongs to the person who made it, in the predicate rather than in a check.
pub(super) async fn a_subscription_belongs_to_its_owner(store: &dyn Store) {
    let owner = UserId::new(4_001);
    let stranger = UserId::new(4_002);

    let id = store
        .upsert_subscription(&Subscription {
            id: SubscriptionId::new(0),
            user_id: owner,
            matcher_source: "alertname=Subscribed".to_owned(),
            matchers: MatcherSet::parse("alertname=Subscribed").expect("the expression parses"),
            min_severity: Some(dam_core::Severity::Warning),
            created_at: at(0),
        })
        .await
        .expect("the subscription is written");

    let listed = store.subscriptions().await.expect("the read succeeds");
    let mine = listed
        .iter()
        .find(|subscription| subscription.id == id)
        .expect("the subscription is listed");

    assert_eq!(mine.user_id, owner);
    assert_eq!(mine.min_severity, Some(dam_core::Severity::Warning));
    assert!(
        mine.matchers
            .matches(&labels(&[("alertname", "Subscribed")])),
        "the matchers are compiled on the way back out, not left as text"
    );

    // A stranger holding the id can neither rewrite it nor delete it. Both are one statement, so
    // there is no window between checking the owner and acting on the row.
    let hijack = store
        .upsert_subscription(&Subscription {
            id,
            user_id: stranger,
            matcher_source: "alertname=Anything".to_owned(),
            matchers: MatcherSet::parse("alertname=Anything").expect("the expression parses"),
            min_severity: None,
            created_at: at(0),
        })
        .await;

    assert!(
        matches!(hijack, Err(StoreError::NotFound { .. })),
        "somebody else's id is not found, rather than found and rewritten"
    );

    assert!(
        matches!(
            store.remove_subscription(id, stranger).await,
            Err(StoreError::NotFound { .. })
        ),
        "a stranger cannot unsubscribe the owner"
    );

    store
        .upsert_subscription(&Subscription {
            id,
            user_id: owner,
            matcher_source: "alertname=Subscribed, severity=critical".to_owned(),
            matchers: MatcherSet::parse("alertname=Subscribed, severity=critical")
                .expect("the expression parses"),
            min_severity: None,
            created_at: at(0),
        })
        .await
        .expect("the owner may rewrite their own");

    store
        .remove_subscription(id, owner)
        .await
        .expect("the owner may remove their own");

    assert!(
        store
            .subscriptions()
            .await
            .expect("the read succeeds")
            .iter()
            .all(|subscription| subscription.id != id),
        "a removed subscription is gone"
    );
}

/// A route's escalation policy survives the round trip, including having none.
pub(super) async fn a_route_keeps_its_escalation_policy(store: &dyn Store) {
    let mut route = Route {
        id: RouteId::new(0),
        guild_id: GuildId::new(3),
        name: "escalating".to_owned(),
        matcher_source: "severity=critical".to_owned(),
        matchers: MatcherSet::parse("severity=critical").expect("the expression parses"),
        min_severity: None,
        target: RouteTarget::Text {
            channel: ChannelId::new(122),
            thread: ThreadPolicy::default(),
        },
        group_strategy: GroupStrategy::PerAlert,
        mentions: Mentions::default(),
        escalation: Some(Escalation {
            after_secs: 900,
            roles: vec![RoleId::new(77)],
            users: vec![UserId::new(88)],
        }),
        priority: 100,
        continue_to_next: false,
        source: RouteSource::Discord,
        enabled: true,
        created_by: None,
        created_at: at(0),
    };

    route.id = store
        .upsert_route(&route)
        .await
        .expect("the route is written");

    let stored = read_route(store, route.id).await;

    assert_eq!(
        stored.escalation, route.escalation,
        "a policy that did not survive storage is a route that silently stops escalating"
    );

    route.escalation = None;
    store
        .upsert_route(&route)
        .await
        .expect("the route is written");

    assert!(
        read_route(store, route.id).await.escalation.is_none(),
        "removing the policy has to clear the column, not leave the old one behind"
    );
}

/// Reads one route back out of the table.
async fn read_route(store: &dyn Store, id: RouteId) -> Route {
    store
        .routes()
        .await
        .expect("the read succeeds")
        .into_iter()
        .find(|route| route.id == id)
        .expect("the route is there")
}

pub(super) async fn ignore_rules_expire_and_revoke(store: &dyn Store) {
    let guild = GuildId::new(1);
    let rule = IgnoreRule {
        id: IgnoreId::new(0),
        scope: IgnoreScope::Guild,
        guild_id: guild,
        channel_id: None,
        matcher_source: "alertname=Noisy".to_owned(),
        matchers: MatcherSet::parse("alertname=Noisy").expect("the expression parses"),
        reason: "known flapper".to_owned(),
        created_by: UserId::new(3),
        created_at: at(0),
        expires_at: Some(at(600)),
        revoked_at: None,
    };

    let id = store
        .upsert_ignore(&rule)
        .await
        .expect("the rule is written");

    let active = store
        .active_ignores(guild, at(10))
        .await
        .expect("the read succeeds");

    let found = active
        .iter()
        .find(|found| found.id == id)
        .expect("the rule is in force");

    assert_eq!(found.reason, "known flapper");
    assert_eq!(
        found.matchers.as_slice().len(),
        1,
        "the expression is compiled on read, so the hot path never compiles a regex"
    );

    let lapsed = store
        .active_ignores(guild, at(700))
        .await
        .expect("the read succeeds");

    assert!(
        !lapsed.iter().any(|found| found.id == id),
        "an expired rule stops muting without anything having to revoke it"
    );

    store
        .revoke_ignore(id, guild, at(20))
        .await
        .expect("the rule is revoked");

    let missing = store.revoke_ignore(id, guild, at(30)).await;

    assert!(
        matches!(missing, Err(StoreError::NotFound { .. })),
        "revoking twice is a missing row, not a silent success: {missing:?}"
    );
}

pub(super) async fn a_config_route_syncs_and_a_discord_route_collides(store: &dyn Store) {
    let declared = Route {
        id: RouteId::new(0),
        guild_id: GuildId::new(2),
        name: "critical".to_owned(),
        matcher_source: "severity=critical".to_owned(),
        matchers: MatcherSet::parse("severity=critical").expect("the expression parses"),
        min_severity: Some(dam_core::Severity::Warning),
        target: RouteTarget::Text {
            channel: ChannelId::new(200),
            thread: ThreadPolicy::default(),
        },
        group_strategy: GroupStrategy::PerAlert,
        mentions: Mentions::default(),
        escalation: None,
        priority: 10,
        continue_to_next: true,
        source: RouteSource::Config,
        enabled: true,
        created_by: None,
        created_at: at(0),
    };

    let first = store
        .upsert_route(&declared)
        .await
        .expect("the route is written");
    let second = store
        .upsert_route(&declared)
        .await
        .expect("a second start writes the same route again");

    assert_eq!(
        first, second,
        "a route declared in the file is synchronised by name, so restarting does not duplicate it"
    );

    let stored = store
        .routes()
        .await
        .expect("the read succeeds")
        .into_iter()
        .find(|route| route.id == first)
        .expect("the route is stored");

    assert_eq!(stored.min_severity, Some(dam_core::Severity::Warning));
    assert!(stored.continue_to_next);
    assert_eq!(stored.source, RouteSource::Config);
    assert!(
        stored
            .matchers
            .matches(&labels(&[("severity", "critical")])),
        "the matchers survive the round trip through the expression column"
    );

    let from_discord = Route {
        source: RouteSource::Discord,
        created_by: Some(UserId::new(9)),
        ..declared.clone()
    };
    let collision = store.upsert_route(&from_discord).await;

    assert!(
        matches!(collision, Err(StoreError::Conflict { .. })),
        "`/route add` under a name the guild already uses is a mistake the operator has to see: \
         {collision:?}"
    );

    let disabled = store
        .disable_missing_config_routes(&["something-else".to_owned()])
        .await
        .expect("the sweep succeeds");

    assert!(disabled >= 1, "a route that left the file is disabled");

    let after = store
        .routes()
        .await
        .expect("the read succeeds")
        .into_iter()
        .find(|route| route.id == first)
        .expect("the route is still there");

    assert!(
        !after.enabled,
        "disabled rather than deleted, so the notifications it created keep their history"
    );
}

pub(super) async fn forum_tags_are_replaced_wholesale(store: &dyn Store) {
    let channel = ChannelId::new(300);
    let tag = |name: &str, id: u64| ForumTag {
        channel_id: channel,
        name: name.to_owned(),
        id: crate::TagId::new(id),
        moderated: false,
        synced_at: at(0),
    };

    store
        .sync_forum_tags(channel, &[tag("firing", 1), tag("acked", 2)])
        .await
        .expect("the cache is written");

    store
        .sync_forum_tags(channel, &[tag("firing", 1)])
        .await
        .expect("the cache is rewritten");

    let cached = store.forum_tags(channel).await.expect("the read succeeds");

    assert_eq!(
        cached.len(),
        1,
        "a tag a human deleted has to leave the cache, or the next apply fails on an id that is \
         no longer there"
    );
    assert_eq!(cached[0].name, "firing");
}
