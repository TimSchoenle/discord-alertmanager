//! Which routes are posting fast enough that one card per alert has stopped being useful.
//!
//! Discord's per-channel limits are strict, and a route that trips them produces rate-limit
//! responses rather than notifications. Past a threshold a route therefore stops posting one card
//! per alert and posts one rolling card per window instead, with the alerts folded into it. That
//! is a worse card and a far better channel, and it is reversible the moment the rate drops.
//!
//! # Why this counts alerts and not the cards they produced
//!
//! Counting cards would count digest mode's own output: a route that switched to one rolling card
//! per window immediately looks quiet, drops back to a card per alert, and floods the channel
//! again a window later. The alerts a route accepted are the load it is under, and that number is
//! unaffected by what the bot decided to do about it.
//!
//! # Why the count is held here rather than in the database
//!
//! It is a sliding window over the last minute of arrivals, read on every alert and written on
//! every alert. A query per batch would be a query per batch during exactly the storm the counter
//! exists to detect. The cost is that a restart forgets, and one window later has relearned; and
//! that two replicas each count what they saw, so a `PostgreSQL` deployment running more than one
//! trips later than a single one would.

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;

use arc_swap::ArcSwap;
use chrono::{DateTime, Duration, Utc};
use dam_store::{Route, RouteId, RouteTarget};

/// The most arrivals one route keeps timestamps for.
///
/// A multiple of the threshold rather than the threshold itself, so the count is still honest
/// about how far past it a route has gone, and bounded, so a route firing tens of thousands of
/// alerts a minute costs a bounded amount of memory to describe as "storming".
const RETAINED_PER_ROUTE: usize = 4;

/// A sliding window over what each route has accepted.
///
/// Written on every accepted alert and read whenever the published state is rebuilt, which is
/// once per batch rather than once per alert.
#[derive(Debug)]
pub struct StormCounter {
    seen: HashMap<RouteId, VecDeque<DateTime<Utc>>>,
    threshold: u32,
    forum_threshold: u32,
    window: Duration,
}

impl StormCounter {
    /// Builds a counter over the configured thresholds and window.
    #[must_use]
    pub fn new(threshold: u32, forum_threshold: u32, window: Duration) -> Self {
        Self {
            seen: HashMap::new(),
            threshold,
            forum_threshold,
            window,
        }
    }

    /// Records that a route accepted one alert.
    pub fn observe(&mut self, route: RouteId, at: DateTime<Utc>) {
        let cap = usize::try_from(self.threshold.max(self.forum_threshold))
            .unwrap_or(usize::MAX)
            .saturating_mul(RETAINED_PER_ROUTE)
            .max(1);

        let arrivals = self.seen.entry(route).or_default();
        arrivals.push_back(at);

        while arrivals.len() > cap {
            arrivals.pop_front();
        }
    }

    /// Drops what has fallen out of the window and returns the state readers see.
    pub fn snapshot(&mut self, now: DateTime<Utc>) -> StormState {
        let cutoff = now - self.window;

        self.seen.retain(|_, arrivals| {
            while arrivals.front().is_some_and(|at| *at < cutoff) {
                arrivals.pop_front();
            }

            // A route nobody has heard from in a window is forgotten rather than kept as a zero,
            // so a deployment that has cycled through thousands of routes does not carry all of
            // them forever.
            !arrivals.is_empty()
        });

        let counts = self
            .seen
            .iter()
            .map(|(route, arrivals)| (*route, arrivals.len() as u64))
            .collect();

        StormState::new(counts, self.threshold, self.forum_threshold, self.window)
    }
}

/// How many alerts each route accepted inside one window, and the thresholds that judge them.
///
/// Built once per batch of changes rather than per alert: every delta in one batch has to reach
/// the same verdict, or half a webhook's alerts land on individual cards and the other half on a
/// digest.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StormState {
    counts: HashMap<RouteId, u64>,
    threshold: u32,
    forum_threshold: u32,
    window: Duration,
}

impl StormState {
    /// Builds a state from a set of counts.
    #[must_use]
    pub fn new(
        counts: HashMap<RouteId, u64>,
        threshold: u32,
        forum_threshold: u32,
        window: Duration,
    ) -> Self {
        Self {
            counts,
            threshold,
            forum_threshold,
            window,
        }
    }

    /// A state in which nothing is storming, for a process that has not counted yet.
    ///
    /// The safe direction to be wrong in: a route that should be digesting and is not posts too
    /// many cards for one window, while a route digesting when it should not have collapses a
    /// window of alerts into one message nobody asked for.
    #[must_use]
    pub fn empty(threshold: u32, forum_threshold: u32, window: Duration) -> Self {
        Self::new(HashMap::new(), threshold, forum_threshold, window)
    }

    /// Whether this route is over its threshold and should be posting a digest.
    #[must_use]
    pub fn is_storming(&self, route: &Route) -> bool {
        self.count(route.id) >= u64::from(self.threshold_for(route))
    }

    /// How many alerts this route accepted inside the window.
    #[must_use]
    pub fn count(&self, route: RouteId) -> u64 {
        self.counts.get(&route).copied().unwrap_or(0)
    }

    /// The threshold this route is judged against.
    ///
    /// A forum route is judged more strictly. Creating a post is heavier than sending a message
    /// and is rate-limited per channel, so a forum reaches trouble sooner than a text channel
    /// carrying the same alerts.
    #[must_use]
    pub fn threshold_for(&self, route: &Route) -> u32 {
        if matches!(route.target, RouteTarget::Forum { .. }) {
            self.forum_threshold
        } else {
            self.threshold
        }
    }

    /// The window the counts were taken over.
    #[must_use]
    pub fn window(&self) -> Duration {
        self.window
    }
}

/// A storm state readers can take a consistent view of while it is being replaced.
///
/// The same shape as [`crate::SharedRouting`], and for the same reason: the decision path and the
/// render path both read it, on every alert, and neither may block the task that recounts.
#[derive(Debug)]
pub struct SharedStorm(ArcSwap<StormState>);

impl SharedStorm {
    /// Publishes an initial state.
    #[must_use]
    pub fn new(state: StormState) -> Self {
        Self(ArcSwap::from_pointee(state))
    }

    /// The current state.
    #[must_use]
    pub fn load(&self) -> Arc<StormState> {
        self.0.load_full()
    }

    /// Replaces the state.
    pub fn store(&self, state: StormState) {
        self.0.store(Arc::new(state));
    }
}

#[cfg(test)]
mod tests {
    use chrono::Utc;
    use dam_core::MatcherSet;
    use dam_store::{
        ChannelId, ForumPolicy, GroupStrategy, GuildId, Mentions, RouteSource, StateTags,
        ThreadPolicy,
    };

    use super::*;

    fn route(id: i64, target: RouteTarget) -> Route {
        Route {
            id: RouteId::new(id),
            guild_id: GuildId::new(1),
            name: format!("route-{id}"),
            matcher_source: String::new(),
            matchers: MatcherSet::parse("").expect("an empty matcher set parses"),
            min_severity: None,
            target,
            group_strategy: GroupStrategy::PerAlert,
            mentions: Mentions::default(),
            escalation: None,
            priority: 100,
            continue_to_next: false,
            source: RouteSource::Config,
            enabled: true,
            created_by: None,
            created_at: Utc::now(),
        }
    }

    fn text() -> RouteTarget {
        RouteTarget::Text {
            channel: ChannelId::new(10),
            thread: ThreadPolicy::default(),
        }
    }

    fn forum() -> RouteTarget {
        RouteTarget::Forum {
            channel: ChannelId::new(11),
            policy: ForumPolicy {
                title_template: String::new(),
                manage_tags: false,
                state_tags: StateTags {
                    firing: "firing".to_owned(),
                    acked: "acked".to_owned(),
                    silenced: "silenced".to_owned(),
                    resolved: "resolved".to_owned(),
                },
                severity_tags: false,
                label_tags: Vec::new(),
                default_tag: None,
                auto_archive_minutes: 1440,
                archive_on_resolve: false,
                lock_on_resolve: false,
                pin_min_severity: None,
                max_pinned: 0,
                bump_on_state_change: false,
            },
        }
    }

    fn state(counts: &[(i64, u64)]) -> StormState {
        StormState::new(
            counts
                .iter()
                .map(|(id, count)| (RouteId::new(*id), *count))
                .collect(),
            50,
            20,
            Duration::seconds(60),
        )
    }

    #[test]
    fn a_route_under_its_threshold_is_not_storming() {
        assert!(!state(&[(1, 49)]).is_storming(&route(1, text())));
    }

    #[test]
    fn a_route_at_its_threshold_is_storming() {
        assert!(state(&[(1, 50)]).is_storming(&route(1, text())));
    }

    #[test]
    fn a_forum_route_is_judged_more_strictly() {
        let counts = state(&[(1, 20), (2, 20)]);

        assert!(counts.is_storming(&route(1, forum())));
        assert!(!counts.is_storming(&route(2, text())));
    }

    #[test]
    fn a_route_nobody_counted_is_not_storming() {
        assert!(!state(&[]).is_storming(&route(9, text())));
    }

    #[test]
    fn an_empty_state_storms_for_nobody() {
        let empty = StormState::empty(1, 1, Duration::seconds(60));

        assert!(!empty.is_storming(&route(1, text())));
    }

    #[test]
    fn the_counter_trips_once_the_window_holds_enough() {
        let mut counter = StormCounter::new(3, 3, Duration::seconds(60));
        let start = Utc::now();

        for step in 0..3 {
            counter.observe(RouteId::new(1), start + Duration::seconds(step));
        }

        assert!(
            counter
                .snapshot(start + Duration::seconds(3))
                .is_storming(&route(1, text()))
        );
    }

    #[test]
    fn arrivals_older_than_the_window_stop_counting() {
        let mut counter = StormCounter::new(3, 3, Duration::seconds(60));
        let start = Utc::now();

        for step in 0..3 {
            counter.observe(RouteId::new(1), start + Duration::seconds(step));
        }

        let later = counter.snapshot(start + Duration::seconds(120));

        assert_eq!(later.count(RouteId::new(1)), 0);
        assert!(!later.is_storming(&route(1, text())));
    }

    #[test]
    fn one_route_storming_leaves_the_others_alone() {
        let mut counter = StormCounter::new(2, 2, Duration::seconds(60));
        let start = Utc::now();

        counter.observe(RouteId::new(1), start);
        counter.observe(RouteId::new(1), start);
        counter.observe(RouteId::new(2), start);

        let state = counter.snapshot(start);

        assert!(state.is_storming(&route(1, text())));
        assert!(!state.is_storming(&route(2, text())));
    }

    #[test]
    fn a_route_far_past_its_threshold_costs_bounded_memory() {
        let mut counter = StormCounter::new(2, 2, Duration::seconds(60));
        let start = Utc::now();

        for _ in 0..1_000 {
            counter.observe(RouteId::new(1), start);
        }

        assert_eq!(
            counter.snapshot(start).count(RouteId::new(1)),
            2 * RETAINED_PER_ROUTE as u64
        );
    }
}
