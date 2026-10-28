//! A worker driven end to end: a real store, a recorded Discord, and the queue in between.
//!
//! Each case writes a card the way the pipeline does, lets one dispatcher drain the queue, and
//! asserts on two things only: what Discord was asked to do, and what the store recorded. The
//! helpers in the parent module are tested on their own; this is where they are tested together.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use chrono::Utc;
use dam_core::{
    Alert, AlertStatus, AmState, Annotations, DedupeKey, EventSource, Fingerprint, LabelName,
    Labels, MatcherSet, NotificationState, Severity,
};
use dam_discord::{LinkRenderer, Renderer};
use dam_engine::{
    AlertFilter, AlertmanagerApi, AmError, AmStatus, CardData, CardTarget, DiscordSink, Mention,
    MessageRef, Note, PostFlags, PostedMessage, Receiver, SharedRouting, SharedStorm,
    SilenceRecord, SilenceRequest, SinkError, StormState, TagSpec, load_snapshot,
};
use dam_store::{
    ChannelId, ClaimRequest, Decision, Effect, ForumPolicy, ForumTag, GroupStrategy, GuildId,
    IngestBatch, Mentions, MessageId, NewNotification, NewOutboxItem, NotificationId, PlannedCard,
    Route, RouteId, RouteSource, RouteTarget, StateTags, Store, TagId, WorkerId,
};
use dam_store_sqlite::{Settings, SqliteStore};

use super::super::Dispatcher;
use crate::admin::AdminChannel;
use crate::cards::Cards;

/// The forum every case posts into.
const FORUM_ID: u64 = 500;

/// [`FORUM_ID`] as a channel.
fn forum() -> ChannelId {
    ChannelId::new(FORUM_ID)
}

/// The tags the forum starts with: the four states and the three severities.
const STARTING_TAGS: [&str; 7] = [
    "firing", "acked", "silenced", "resolved", "critical", "warning", "info",
];

/// Discord as one forum channel, recording every call made to it.
///
/// Holds the forum's tag list, so that creating a tag is visible to the next read of it, and a
/// switch for each refusal the dispatcher has a path for.
struct Discord {
    calls: std::sync::Mutex<Vec<String>>,
    tags: std::sync::Mutex<Vec<ForumTag>>,
    /// Whether the bot holds `MANAGE_CHANNELS`, without which a tag request returns the tags
    /// already there.
    creates_tags: bool,
    /// What a tag request fails with outright, when it fails.
    tag_failure: Option<SinkError>,
    /// Whether the next post or tag write names a tag Discord no longer has.
    stale_tag: std::sync::Mutex<bool>,
    /// What a pin request fails with, when it fails.
    pin_refusal: Option<SinkError>,
    next_id: std::sync::Mutex<u64>,
}

impl Discord {
    fn new() -> Self {
        Self {
            calls: std::sync::Mutex::default(),
            tags: std::sync::Mutex::new(
                STARTING_TAGS
                    .iter()
                    .zip(1_u64..)
                    .map(|(name, id)| tag(name, id))
                    .collect(),
            ),
            creates_tags: true,
            tag_failure: None,
            stale_tag: std::sync::Mutex::new(false),
            pin_refusal: None,
            next_id: std::sync::Mutex::new(1_000),
        }
    }

    /// What it was asked to do, in order.
    fn calls(&self) -> Vec<String> {
        self.calls.lock().expect("no test poisons the lock").clone()
    }

    /// How many calls start with `prefix`.
    fn count(&self, prefix: &str) -> usize {
        self.calls()
            .iter()
            .filter(|call| call.starts_with(prefix))
            .count()
    }

    fn record(&self, call: String) {
        self.calls
            .lock()
            .expect("no test poisons the lock")
            .push(call);
    }

    fn fresh_id(&self) -> u64 {
        let mut next = self.next_id.lock().expect("no test poisons the lock");
        *next += 1;
        *next
    }

    /// The id the forum gives a tag name, if it has one.
    fn tag_id(&self, name: &str) -> Option<TagId> {
        self.tags
            .lock()
            .expect("no test poisons the lock")
            .iter()
            .find(|tag| tag.name == name)
            .map(|tag| tag.id)
    }

    /// Fails once with `UnknownTag` when the switch is set, as Discord does after a tag is
    /// deleted behind the bot's cache.
    fn refuse_stale_tag(&self) -> Result<(), SinkError> {
        let mut stale = self.stale_tag.lock().expect("no test poisons the lock");
        if *stale {
            *stale = false;
            return Err(SinkError::UnknownTag);
        }

        Ok(())
    }
}

/// Renders a tag list as the sorted ids, so a recorded call reads the same whatever the order.
fn ids(tags: &[TagId]) -> String {
    let mut ids: Vec<u64> = tags.iter().map(|tag| tag.get()).collect();
    ids.sort_unstable();
    format!("{ids:?}")
}

fn tag(name: &str, id: u64) -> ForumTag {
    ForumTag {
        channel_id: forum(),
        name: name.to_owned(),
        id: TagId::new(id),
        moderated: false,
        synced_at: Utc::now(),
    }
}

#[async_trait]
impl DiscordSink for Discord {
    async fn post_card(
        &self,
        _target: &CardTarget,
        _card: &CardData,
    ) -> Result<PostedMessage, SinkError> {
        unimplemented!("every route here is a forum")
    }

    async fn create_forum_post(
        &self,
        target: &CardTarget,
        _card: &CardData,
    ) -> Result<PostedMessage, SinkError> {
        self.record(format!("post {}", ids(&target.tags)));
        self.refuse_stale_tag()?;

        let id = self.fresh_id();

        Ok(PostedMessage {
            message: MessageId::new(id),
            thread: Some(ChannelId::new(id)),
        })
    }

    async fn edit_card(&self, _message: &MessageRef, _card: &CardData) -> Result<(), SinkError> {
        self.record("edit".to_owned());
        Ok(())
    }

    async fn open_thread(
        &self,
        _message: &MessageRef,
        _name: &str,
    ) -> Result<ChannelId, SinkError> {
        unimplemented!("a forum post is its own thread")
    }

    async fn post_thread_note(&self, _thread: ChannelId, _note: &Note) -> Result<(), SinkError> {
        unimplemented!("no case here posts a note")
    }

    async fn post_escalation(
        &self,
        _channel: ChannelId,
        _mentions: &[Mention],
        _text: &str,
    ) -> Result<(), SinkError> {
        unimplemented!("no case here escalates")
    }

    async fn disable_components(&self, _message: &MessageRef) -> Result<(), SinkError> {
        unimplemented!("no case here resolves")
    }

    async fn set_post_tags(&self, _thread: ChannelId, tags: &[TagId]) -> Result<(), SinkError> {
        self.record(format!("tags {}", ids(tags)));
        self.refuse_stale_tag()
    }

    async fn set_post_flags(&self, _thread: ChannelId, flags: PostFlags) -> Result<(), SinkError> {
        self.record(format!(
            "flags archived={} minutes={}",
            flags.archived, flags.auto_archive_minutes
        ));
        Ok(())
    }

    async fn set_post_pinned(&self, _thread: ChannelId, pinned: bool) -> Result<(), SinkError> {
        self.record(format!("pin {pinned}"));
        self.pin_refusal.clone().map_or(Ok(()), Err)
    }

    async fn forum_tags(&self, _forum: ChannelId) -> Result<Vec<ForumTag>, SinkError> {
        self.record("read tags".to_owned());
        Ok(self.tags.lock().expect("no test poisons the lock").clone())
    }

    async fn ensure_forum_tags(
        &self,
        _forum: ChannelId,
        want: &[TagSpec],
    ) -> Result<Vec<ForumTag>, SinkError> {
        let mut names: Vec<&str> = want.iter().map(|spec| spec.name.as_str()).collect();
        names.sort_unstable();
        self.record(format!("create tags {names:?}"));

        if let Some(error) = self.tag_failure.clone() {
            return Err(error);
        }

        if self.creates_tags {
            let created: Vec<ForumTag> = want
                .iter()
                .map(|spec| tag(&spec.name, self.fresh_id()))
                .collect();
            self.tags
                .lock()
                .expect("no test poisons the lock")
                .extend(created);
        }

        Ok(self.tags.lock().expect("no test poisons the lock").clone())
    }
}

/// An Alertmanager no case reaches.
struct NoAlertmanager;

#[async_trait]
impl AlertmanagerApi for NoAlertmanager {
    async fn list_alerts(&self, _filter: &AlertFilter) -> Result<Vec<Alert>, AmError> {
        unimplemented!("the dispatcher never lists alerts")
    }

    async fn list_silences(&self, _matchers: &[String]) -> Result<Vec<SilenceRecord>, AmError> {
        unimplemented!("the dispatcher never lists silences")
    }

    async fn upsert_silence(&self, _silence: &SilenceRequest) -> Result<String, AmError> {
        unimplemented!("no case here silences")
    }

    async fn expire_silence(&self, _id: &str) -> Result<(), AmError> {
        unimplemented!("no case here silences")
    }

    async fn status(&self) -> Result<AmStatus, AmError> {
        unimplemented!("the dispatcher never reads the status")
    }

    async fn receivers(&self) -> Result<Vec<Receiver>, AmError> {
        unimplemented!("the dispatcher never reads the receivers")
    }
}

/// The policy most cases start from: tags managed, label tags off, a pin for critical posts.
fn policy() -> ForumPolicy {
    ForumPolicy {
        title_template: "{{ labels.alertname }}".to_owned(),
        manage_tags: true,
        state_tags: StateTags {
            firing: "firing".to_owned(),
            acked: "acked".to_owned(),
            silenced: "silenced".to_owned(),
            resolved: "resolved".to_owned(),
        },
        severity_tags: true,
        label_tags: Vec::new(),
        default_tag: None,
        auto_archive_minutes: 1_440,
        archive_on_resolve: true,
        lock_on_resolve: false,
        pin_min_severity: Some(Severity::Critical),
        max_pinned: 1,
        bump_on_state_change: false,
    }
}

/// One store, one forum route, one recorded Discord and one worker over them.
struct World {
    store: Arc<dyn Store>,
    discord: Arc<Discord>,
    route: RouteId,
    dispatcher: Dispatcher,
}

impl World {
    async fn new(policy: ForumPolicy, discord: Discord) -> Self {
        let store: Arc<dyn Store> = Arc::new(
            SqliteStore::connect(&Settings {
                path: ":memory:".into(),
                // One connection, because every connection to `:memory:` is a database of its own.
                max_connections: 1,
                acquire_timeout: Duration::from_secs(5),
                migrate_on_start: true,
                persist_events: false,
                regroup_window: Duration::from_mins(30),
            })
            .await
            .expect("an in-memory database opens and migrates"),
        );

        let route = store
            .upsert_route(&Route {
                id: RouteId::new(0),
                guild_id: GuildId::new(1),
                name: "forum".to_owned(),
                matcher_source: "namespace=payments".to_owned(),
                matchers: MatcherSet::parse("namespace=payments").expect("the matcher parses"),
                min_severity: None,
                target: RouteTarget::Forum {
                    channel: forum(),
                    policy,
                },
                group_strategy: GroupStrategy::PerAlert,
                mentions: Mentions::default(),
                escalation: None,
                priority: 100,
                continue_to_next: false,
                source: RouteSource::Discord,
                enabled: true,
                created_by: None,
                created_at: Utc::now(),
            })
            .await
            .expect("the route is written");

        let starting = discord
            .tags
            .lock()
            .expect("no test poisons the lock")
            .clone();
        store
            .sync_forum_tags(forum(), &starting)
            .await
            .expect("the forum's tags are cached");

        let routing = Arc::new(SharedRouting::new(
            load_snapshot(store.as_ref(), Utc::now())
                .await
                .expect("the snapshot loads"),
        ));
        let renderer = Arc::new(Renderer::new(
            dam_config::Render::default(),
            LinkRenderer::new(&dam_config::Links::default()).expect("the default links compile"),
        ));
        let storms = Arc::new(SharedStorm::new(StormState::new(
            HashMap::new(),
            u32::MAX,
            u32::MAX,
            chrono::Duration::minutes(5),
        )));
        let discord = Arc::new(discord);

        let dispatcher = Dispatcher::new(
            Arc::clone(&store),
            Arc::clone(&discord) as Arc<dyn DiscordSink>,
            Arc::new(NoAlertmanager),
            Arc::new(Cards::new(
                Arc::clone(&store),
                Arc::clone(&routing),
                storms,
                Arc::clone(&renderer),
            )),
            renderer,
            routing,
            Arc::new(AdminChannel::new(None, Arc::clone(&store))),
            WorkerId::new("test-worker"),
            None,
            Duration::from_secs(60),
            10,
        );

        Self {
            store,
            discord,
            route,
            dispatcher,
        }
    }

    /// Writes a firing card for an alert the way the pipeline does, then posts it.
    async fn fire(&self, fingerprint: &str, severity: &str) -> NotificationId {
        let fingerprint = Fingerprint::new(fingerprint).expect("the fingerprint is hexadecimal");
        let now = Utc::now();

        self.store
            .ingest_batch(&IngestBatch::new(
                EventSource::Webhook,
                vec![alert(&fingerprint, severity)],
                now,
            ))
            .await
            .expect("the alert is ingested");

        let created = self
            .store
            .apply_decision(&Decision {
                new_cards: vec![PlannedCard {
                    card: NewNotification {
                        dedupe_key: DedupeKey::per_alert(&fingerprint),
                        fingerprint,
                        route_id: self.route,
                        guild_id: GuildId::new(1),
                        channel_id: forum(),
                        state: NotificationState::Firing,
                        supersedes: None,
                        created_at: now,
                    },
                    mention: false,
                    not_before: now,
                }],
                updates: Vec::new(),
                at: now,
            })
            .await
            .expect("the card is written");

        self.drain().await;

        *created.first().expect("one card was written")
    }

    /// Queues one effect against a card and runs it.
    async fn run(&self, card: NotificationId, effect: Effect) {
        let row = self.notification(card).await;

        self.store
            .enqueue_effects(
                &[NewOutboxItem {
                    effect,
                    dedupe_key: row.dedupe_key,
                    not_before: Utc::now(),
                }],
                Utc::now(),
            )
            .await
            .expect("the effect is queued");

        self.drain().await;
    }

    /// Runs everything due until the queue holds nothing a worker could claim.
    async fn drain(&self) {
        loop {
            let items = self
                .store
                .claim_outbox(
                    &WorkerId::new("test-worker"),
                    ClaimRequest {
                        lane: None,
                        lease_secs: 60,
                        limit: 10,
                    },
                    Utc::now() + chrono::Duration::seconds(1),
                )
                .await
                .expect("the claim succeeds");

            if items.is_empty() {
                return;
            }

            for item in &items {
                self.dispatcher.handle(item).await;
            }
        }
    }

    async fn notification(&self, card: NotificationId) -> dam_store::Notification {
        self.store
            .notification(card)
            .await
            .expect("the card reads")
            .expect("the card exists")
    }

    /// How many items are still queued, due or not.
    async fn queued(&self) -> u64 {
        self.store
            .outbox_depth()
            .await
            .expect("the depth reads")
            .iter()
            .map(|(_, depth)| depth)
            .sum()
    }
}

fn alert(fingerprint: &Fingerprint, severity: &str) -> Alert {
    Alert {
        fingerprint: fingerprint.clone(),
        labels: [
            ("alertname", "DiskFull"),
            ("severity", severity),
            ("namespace", "payments"),
        ]
        .iter()
        .map(|(name, value)| {
            (
                LabelName::new(*name).expect("the label name is valid"),
                (*value).to_owned(),
            )
        })
        .collect::<Labels>(),
        annotations: Annotations::new(),
        starts_at: Utc::now(),
        ends_at: None,
        generator_url: None,
        status: AlertStatus::Firing,
        am_state: AmState::Active,
        silenced_by: Vec::new(),
        inhibited_by: Vec::new(),
        group_key: None,
    }
}

#[tokio::test]
async fn a_critical_post_is_pinned_as_it_is_created() {
    let world = World::new(policy(), Discord::new()).await;

    let card = world.fire("aa01", "critical").await;

    assert_eq!(world.discord.count("pin true"), 1);
    assert!(world.notification(card).await.pinned);
}

#[tokio::test]
async fn a_post_below_the_pin_floor_is_not_pinned() {
    let world = World::new(policy(), Discord::new()).await;

    let card = world.fire("aa02", "warning").await;

    assert_eq!(world.discord.count("pin"), 0);
    assert!(!world.notification(card).await.pinned);
}

#[tokio::test]
async fn a_refused_pin_still_delivers_the_post_once() {
    let discord = Discord {
        pin_refusal: Some(SinkError::PinLimitReached),
        ..Discord::new()
    };
    let world = World::new(policy(), discord).await;

    let card = world.fire("aa03", "critical").await;
    let row = world.notification(card).await;

    assert!(row.message_id.is_some(), "the post is recorded");
    assert!(!row.pinned, "the card keeps the pin it actually has");
    assert_eq!(
        world.discord.count("post"),
        1,
        "a refused pin is no reason to post again"
    );
    assert_eq!(world.queued().await, 0, "nothing is left to retry");
}

#[tokio::test]
async fn the_route_limit_leaves_the_next_critical_post_unpinned() {
    let world = World::new(policy(), Discord::new()).await;

    let first = world.fire("aa04", "critical").await;
    let second = world.fire("aa05", "critical").await;

    assert_eq!(
        world.discord.count("pin true"),
        1,
        "the second post never asks"
    );
    assert!(world.notification(first).await.pinned);
    assert!(!world.notification(second).await.pinned);
}

#[tokio::test]
async fn an_unpin_frees_the_slot_for_the_next_post() {
    let world = World::new(policy(), Discord::new()).await;

    let first = world.fire("aa06", "critical").await;
    world
        .run(
            first,
            Effect::SetPinned {
                notification: first,
                pinned: false,
            },
        )
        .await;
    let second = world.fire("aa07", "critical").await;

    assert!(!world.notification(first).await.pinned);
    assert!(world.notification(second).await.pinned);
}

#[tokio::test]
async fn a_label_tag_the_forum_lacks_is_created_and_carried_by_the_post() {
    let world = World::new(
        ForumPolicy {
            label_tags: vec!["namespace".to_owned()],
            ..policy()
        },
        Discord::new(),
    )
    .await;

    let card = world.fire("aa08", "warning").await;

    let created = world
        .discord
        .tag_id("payments")
        .expect("the tag was created");
    assert_eq!(world.discord.count("create tags [\"payments\"]"), 1);
    assert!(
        world
            .notification(card)
            .await
            .applied_tags
            .contains(&created),
        "the post goes out with the tag it just created"
    );
    assert!(
        world
            .store
            .forum_tags(forum())
            .await
            .expect("the tags read")
            .iter()
            .any(|tag| tag.name == "payments"),
        "the new tag is cached for the next card"
    );
}

#[tokio::test]
async fn no_label_tag_is_created_unless_the_route_manages_tags() {
    let world = World::new(
        ForumPolicy {
            manage_tags: false,
            label_tags: vec!["namespace".to_owned()],
            ..policy()
        },
        Discord::new(),
    )
    .await;

    world.fire("aa09", "warning").await;

    assert_eq!(world.discord.count("create tags"), 0);
    assert_eq!(world.discord.count("post"), 1);
}

#[tokio::test]
async fn a_refused_label_tag_is_not_asked_for_again_within_the_window() {
    let discord = Discord {
        creates_tags: false,
        ..Discord::new()
    };
    let world = World::new(
        ForumPolicy {
            label_tags: vec!["namespace".to_owned()],
            ..policy()
        },
        discord,
    )
    .await;

    world.fire("aa0a", "warning").await;
    world.fire("aa0b", "warning").await;

    assert_eq!(world.discord.count("create tags"), 1);
    assert_eq!(
        world.discord.count("post"),
        2,
        "both posts go out without the tag"
    );
}

#[tokio::test]
async fn a_post_naming_a_deleted_tag_reloads_the_forum_and_goes_out() {
    let discord = Discord::new();
    *discord.stale_tag.lock().expect("no test poisons the lock") = true;
    let world = World::new(policy(), discord).await;

    let card = world.fire("aa0c", "warning").await;

    let calls = world.discord.calls();
    let posts: Vec<&String> = calls
        .iter()
        .filter(|call| call.starts_with("post"))
        .collect();
    assert_eq!(
        posts.len(),
        2,
        "one refused post and one that went out: {calls:?}"
    );
    assert!(calls.contains(&"read tags".to_owned()));
    assert!(world.notification(card).await.message_id.is_some());
}

#[tokio::test]
async fn a_retag_that_would_change_nothing_makes_no_call() {
    let world = World::new(policy(), Discord::new()).await;
    let card = world.fire("aa0d", "warning").await;

    world
        .run(
            card,
            Effect::SetTags {
                notification: card,
                tags: Vec::new(),
            },
        )
        .await;

    assert_eq!(world.discord.count("tags"), 0);
}

#[tokio::test]
async fn a_retag_writes_the_set_the_card_wants_now() {
    let world = World::new(policy(), Discord::new()).await;
    let card = world.fire("aa0e", "warning").await;
    let before = world.notification(card).await.tags_hash;

    world
        .store
        .set_notification_state(card, NotificationState::Acked, Utc::now())
        .await
        .expect("the state is written");
    world
        .run(
            card,
            Effect::SetTags {
                notification: card,
                // Deliberately stale: the worker resolves the set itself.
                tags: Vec::new(),
            },
        )
        .await;

    let acked = world
        .discord
        .tag_id("acked")
        .expect("the forum has the tag");
    let row = world.notification(card).await;
    assert_eq!(world.discord.count("tags"), 1);
    assert!(row.applied_tags.contains(&acked));
    assert_ne!(row.tags_hash, before);
}

/// The seal a resolve queued, as it would be retried after the alert fired again.
fn resolve_seal(card: NotificationId, planned_for: Option<NotificationState>) -> Effect {
    Effect::SetFlags {
        notification: card,
        archived: true,
        locked: false,
        auto_archive_minutes: 60,
        planned_for,
    }
}

#[tokio::test]
async fn a_seal_planned_for_resolution_is_dropped_once_the_alert_fires_again() {
    let world = World::new(policy(), Discord::new()).await;
    let card = world.fire("aa12", "warning").await;

    world
        .run(card, resolve_seal(card, Some(NotificationState::Resolved)))
        .await;

    assert_eq!(
        world.discord.count("flags"),
        0,
        "the post is firing; sealing it is stale"
    );
    assert!(!world.notification(card).await.archived);
    assert_eq!(
        world.queued().await,
        0,
        "a stale item is finished, not retried"
    );
}

#[tokio::test]
async fn a_seal_still_matching_the_card_is_applied() {
    let world = World::new(policy(), Discord::new()).await;
    let card = world.fire("aa13", "warning").await;
    world
        .store
        .set_notification_state(card, NotificationState::Resolved, Utc::now())
        .await
        .expect("the state is written");

    world
        .run(card, resolve_seal(card, Some(NotificationState::Resolved)))
        .await;

    assert_eq!(
        world.discord.calls().last().map(String::as_str),
        Some("flags archived=true minutes=60")
    );
    assert!(world.notification(card).await.archived);
}

#[tokio::test]
async fn flags_queued_before_the_planned_state_existed_apply_as_planned() {
    let world = World::new(policy(), Discord::new()).await;
    let card = world.fire("aa14", "warning").await;

    world.run(card, resolve_seal(card, None)).await;

    assert_eq!(world.discord.count("flags"), 1);
}

#[tokio::test]
async fn a_retag_naming_a_deleted_tag_reloads_the_forum_and_writes_again() {
    let world = World::new(policy(), Discord::new()).await;
    let card = world.fire("aa0f", "warning").await;

    world
        .store
        .set_notification_state(card, NotificationState::Acked, Utc::now())
        .await
        .expect("the state is written");
    *world
        .discord
        .stale_tag
        .lock()
        .expect("no test poisons the lock") = true;
    world
        .run(
            card,
            Effect::SetTags {
                notification: card,
                tags: Vec::new(),
            },
        )
        .await;

    assert_eq!(
        world.discord.count("tags"),
        2,
        "{:?}",
        world.discord.calls()
    );
    assert_eq!(world.discord.count("read tags"), 1);
    assert_eq!(world.queued().await, 0);
}

#[tokio::test]
async fn a_failed_tag_request_still_posts_and_is_held_back_like_a_refusal() {
    let discord = Discord {
        tag_failure: Some(SinkError::MissingPermissions {
            detail: "MANAGE_CHANNELS".to_owned(),
        }),
        ..Discord::new()
    };
    let world = World::new(
        ForumPolicy {
            label_tags: vec!["namespace".to_owned()],
            ..policy()
        },
        discord,
    )
    .await;

    let first = world.fire("aa10", "warning").await;
    world.fire("aa11", "warning").await;

    assert!(world.notification(first).await.message_id.is_some());
    assert_eq!(world.discord.count("create tags"), 1);
    assert_eq!(world.discord.count("post"), 2);
}
