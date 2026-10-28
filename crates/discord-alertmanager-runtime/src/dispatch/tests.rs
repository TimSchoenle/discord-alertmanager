use std::sync::Mutex;

use async_trait::async_trait;
use dam_engine::{CardData, CardTarget, PostedMessage, TagSpec};
use dam_store::{ChannelId, ForumTag};

use super::*;

/// A sink that records what it was asked to do and refuses notes until the thread is reopened.
///
/// Written by hand rather than mocked: the whole point of the test is the order of three
/// calls, which a recorded list states more plainly than an expectation does.
#[derive(Default)]
struct FakeSink {
    calls: Mutex<Vec<String>>,
    archived: Mutex<bool>,
    reopen: Option<SinkError>,
}

impl FakeSink {
    /// A sink whose thread is archived, and which lets it be reopened.
    fn archived() -> Self {
        Self {
            archived: Mutex::new(true),
            ..Self::default()
        }
    }

    /// A sink whose thread is archived and refuses to reopen, as a locked one does.
    fn sealed(error: SinkError) -> Self {
        Self {
            archived: Mutex::new(true),
            reopen: Some(error),
            ..Self::default()
        }
    }

    /// What it was asked to do, in order.
    fn calls(&self) -> Vec<String> {
        self.calls
            .lock()
            .expect("the test holds no poisoned lock")
            .clone()
    }

    /// Records one call.
    fn record(&self, call: &str) {
        self.calls
            .lock()
            .expect("the test holds no poisoned lock")
            .push(call.to_owned());
    }
}

#[async_trait]
impl DiscordSink for FakeSink {
    async fn post_thread_note(&self, _thread: ChannelId, _note: &Note) -> Result<(), SinkError> {
        self.record("note");

        if *self
            .archived
            .lock()
            .expect("the test holds no poisoned lock")
        {
            return Err(SinkError::ThreadArchived);
        }

        Ok(())
    }

    async fn set_post_flags(&self, _thread: ChannelId, flags: PostFlags) -> Result<(), SinkError> {
        self.record("flags");

        if let Some(error) = self.reopen.clone() {
            return Err(error);
        }

        *self
            .archived
            .lock()
            .expect("the test holds no poisoned lock") = flags.archived;

        Ok(())
    }

    async fn post_card(
        &self,
        _target: &CardTarget,
        _card: &CardData,
    ) -> Result<PostedMessage, SinkError> {
        unimplemented!("the reopen tests post no cards")
    }

    async fn create_forum_post(
        &self,
        _target: &CardTarget,
        _card: &CardData,
    ) -> Result<PostedMessage, SinkError> {
        unimplemented!("the reopen tests post no cards")
    }

    async fn edit_card(&self, _message: &MessageRef, _card: &CardData) -> Result<(), SinkError> {
        unimplemented!("the reopen tests edit no cards")
    }

    async fn open_thread(
        &self,
        _message: &MessageRef,
        _name: &str,
    ) -> Result<dam_store::ChannelId, SinkError> {
        unimplemented!("the reopen tests open no threads")
    }

    async fn post_escalation(
        &self,
        _channel: ChannelId,
        _mentions: &[Mention],
        _text: &str,
    ) -> Result<(), SinkError> {
        unimplemented!("the reopen tests escalate nothing")
    }

    async fn disable_components(&self, _message: &MessageRef) -> Result<(), SinkError> {
        unimplemented!("the reopen tests disable nothing")
    }

    async fn set_post_tags(
        &self,
        _thread: ChannelId,
        _tags: &[dam_store::TagId],
    ) -> Result<(), SinkError> {
        unimplemented!("the reopen tests set no tags")
    }

    async fn set_post_pinned(&self, _thread: ChannelId, _pinned: bool) -> Result<(), SinkError> {
        unimplemented!("the reopen tests pin nothing")
    }

    async fn forum_tags(&self, _forum: ChannelId) -> Result<Vec<ForumTag>, SinkError> {
        unimplemented!("the reopen tests read no tags")
    }

    async fn ensure_forum_tags(
        &self,
        _forum: ChannelId,
        _want: &[TagSpec],
    ) -> Result<Vec<ForumTag>, SinkError> {
        unimplemented!("the reopen tests create no tags")
    }
}

/// The note the reopen tests post.
fn note() -> Note {
    Note {
        text: "still firing".to_owned(),
    }
}

#[tokio::test]
async fn an_archived_thread_is_reopened_and_the_call_made_again() {
    let sink = FakeSink::archived();
    let thread = ChannelId::new(1);

    let note = note();

    let outcome = reopening(&sink, thread, || sink.post_thread_note(thread, &note)).await;

    assert_eq!(outcome, Ok(((), true)));
    assert_eq!(sink.calls(), vec!["note", "flags", "note"]);
}

#[tokio::test]
async fn an_open_thread_is_left_alone() {
    let sink = FakeSink::default();
    let thread = ChannelId::new(1);

    let note = note();

    let outcome = reopening(&sink, thread, || sink.post_thread_note(thread, &note)).await;

    assert_eq!(outcome, Ok(((), false)));
    assert_eq!(sink.calls(), vec!["note"]);
}

#[tokio::test]
async fn a_thread_that_will_not_reopen_reports_why_instead_of_trying_again() {
    let sink = FakeSink::sealed(SinkError::ThreadLocked);
    let thread = ChannelId::new(1);

    let note = note();

    let outcome = reopening(&sink, thread, || sink.post_thread_note(thread, &note)).await;

    assert_eq!(outcome, Err(SinkError::ThreadLocked));
    assert_eq!(sink.calls(), vec!["note", "flags"]);
}

#[test]
fn an_unchanged_tag_set_hashes_the_same() {
    let tags = vec![dam_store::TagId::new(1), dam_store::TagId::new(2)];

    assert_eq!(tags_hash(&tags), tags_hash(&tags.clone()));
    assert_ne!(tags_hash(&tags), tags_hash(&[dam_store::TagId::new(1)]));
}

/// A forum policy that names two labels as tags.
fn labelled_policy() -> ForumPolicy {
    ForumPolicy {
        title_template: "{{ labels.alertname }}".to_owned(),
        manage_tags: true,
        state_tags: dam_store::StateTags {
            firing: "firing".to_owned(),
            acked: "acked".to_owned(),
            silenced: "silenced".to_owned(),
            resolved: "resolved".to_owned(),
        },
        severity_tags: true,
        label_tags: vec!["namespace".to_owned(), "team".to_owned()],
        default_tag: None,
        auto_archive_minutes: 10_080,
        archive_on_resolve: true,
        lock_on_resolve: false,
        pin_min_severity: None,
        max_pinned: 1,
        bump_on_state_change: true,
    }
}

/// Builds a label set.
fn labels(pairs: &[(&str, &str)]) -> Labels {
    pairs
        .iter()
        .map(|(name, value)| {
            (
                dam_core::LabelName::new(*name).expect("the test label name is valid"),
                (*value).to_owned(),
            )
        })
        .collect()
}

#[test]
fn only_label_tags_the_channel_lacks_are_created_and_each_once() {
    let forum = ChannelId::new(200);
    let snapshot = RoutingSnapshot::new(
        Vec::new(),
        Vec::new(),
        vec![ForumTag {
            channel_id: forum,
            name: "payments".to_owned(),
            id: dam_store::TagId::new(1),
            moderated: false,
            synced_at: Utc::now(),
        }],
    );
    let mut policy = labelled_policy();
    policy.label_tags.push("owner".to_owned());

    let missing = missing_label_tags(
        &snapshot,
        forum,
        &policy,
        &labels(&[("namespace", "payments"), ("team", "sre"), ("owner", "sre")]),
    );

    assert_eq!(
        missing,
        vec![TagSpec {
            name: "sre".to_owned(),
            moderated: false,
        }],
        "an existing tag is not created again, and two labels sharing a value create one tag"
    );
}

#[test]
fn a_tag_in_another_channel_does_not_count() {
    let snapshot = RoutingSnapshot::new(
        Vec::new(),
        Vec::new(),
        vec![ForumTag {
            channel_id: ChannelId::new(300),
            name: "payments".to_owned(),
            id: dam_store::TagId::new(1),
            moderated: false,
            synced_at: Utc::now(),
        }],
    );

    let missing = missing_label_tags(
        &snapshot,
        ChannelId::new(200),
        &labelled_policy(),
        &labels(&[("namespace", "payments")]),
    );

    assert_eq!(missing.len(), 1);
}

#[test]
fn only_a_firing_post_at_or_above_the_floor_is_born_pinned() {
    let mut policy = labelled_policy();
    policy.pin_min_severity = Some(Severity::Critical);
    let forum = RouteTarget::Forum {
        channel: ChannelId::new(200),
        policy: policy.clone(),
    };

    assert!(born_pinned(
        &forum,
        NotificationState::Firing,
        Severity::Critical
    ));
    assert!(
        !born_pinned(&forum, NotificationState::Firing, Severity::Warning),
        "below the floor"
    );
    assert!(
        !born_pinned(&forum, NotificationState::Silenced, Severity::Critical),
        "somebody already decided what to do about it"
    );

    policy.pin_min_severity = None;
    let unpinned = RouteTarget::Forum {
        channel: ChannelId::new(200),
        policy,
    };
    assert!(
        !born_pinned(&unpinned, NotificationState::Firing, Severity::Critical),
        "the route turned pinning off"
    );
}

#[test]
fn pins_stop_at_the_route_limit_and_zero_turns_them_off() {
    assert!(has_pin_room(0, 1));
    assert!(!has_pin_room(1, 1));
    assert!(has_pin_room(4, 5));
    assert!(!has_pin_room(0, 0));
}

#[test]
fn a_refused_tag_is_not_asked_for_again_until_the_window_passes() {
    let backoff = TagBackoff::default();
    let channel = ChannelId::new(200);
    let spec = |name: &str| TagSpec {
        name: name.to_owned(),
        moderated: false,
    };
    let start = Instant::now();

    backoff.record(channel, &[spec("payments")], start);

    let mut wanted = vec![spec("payments"), spec("billing")];
    backoff.retain_due(channel, &mut wanted, start + Duration::from_secs(1));
    assert_eq!(
        wanted,
        vec![spec("billing")],
        "only the refused tag is held back"
    );

    let mut elsewhere = vec![spec("payments")];
    backoff.retain_due(ChannelId::new(300), &mut elsewhere, start);
    assert_eq!(
        elsewhere.len(),
        1,
        "a refusal in one forum says nothing about another"
    );

    let mut later = vec![spec("payments")];
    backoff.retain_due(channel, &mut later, start + TAG_RETRY_AFTER);
    assert_eq!(
        later.len(),
        1,
        "the tag is asked for again once the window has passed"
    );
}

#[test]
fn the_backoff_doubles_and_then_stops() {
    assert_eq!(backoff_secs(0), 1);
    assert_eq!(backoff_secs(4), 16);
    assert_eq!(backoff_secs(20), MAX_BACKOFF_SECS);
}

mod worker;
