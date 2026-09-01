//! The administrative channel: the one place the bot reports on itself.
//!
//! Two things are said here and nothing else. The deadman, when no webhook has arrived inside its
//! window and Alertmanager cannot be reached either — which is the only way a bot that has gone
//! silent can say so, since by then nothing is arriving to say it with. And route health, when an
//! effect fails for a reason no retry can change: a channel the bot cannot post in is a route
//! that is quietly delivering nothing, and it looks exactly like a quiet week.
//!
//! # Why a notice is queued rather than sent
//!
//! Every other outbound call in this process goes through the outbox, and these have the same
//! reasons to: the gateway may be down at the moment the deadman trips, and a notice that is lost
//! because Discord was unreachable is a notice about the bot being unable to reach Discord.
//!
//! # Why the same notice is not said twice
//!
//! A permission that is missing is missing for every card on the route, and a deadman that has
//! tripped stays tripped until something changes. Both would otherwise produce one message per
//! failure, which is a channel nobody can read at the moment they most need to.

use std::collections::HashSet;
use std::sync::{Arc, Mutex};

use chrono::Utc;
use dam_core::DedupeKey;
use dam_store::{ChannelId, Effect, NewOutboxItem, Store};
use tracing::warn;

/// The lane administrative notices are serialised on.
///
/// One lane for all of them, so two notices never race into the channel out of order, and one
/// that has nothing to do with any alert never lands in a lane a card's edits are queued on.
const ADMIN_LANE: &str = "admin";

/// Where the bot reports on itself, and what it has already said.
pub(crate) struct AdminChannel {
    channel: Option<ChannelId>,
    store: Arc<dyn Store>,
    said: Mutex<HashSet<String>>,
}

impl AdminChannel {
    /// Builds the reporter around the configured channel, if there is one.
    ///
    /// A deployment that configures none is not an error and not a warning on every notice: the
    /// channel is optional, and a bot in a server whose operators watch it another way has
    /// nowhere sensible to put these.
    pub(crate) fn new(channel: Option<u64>, store: Arc<dyn Store>) -> Self {
        Self {
            channel: channel.map(ChannelId::new),
            store,
            said: Mutex::new(HashSet::new()),
        }
    }

    /// Queues a notice.
    pub(crate) async fn say(&self, text: String) {
        let Some(channel) = self.channel else {
            return;
        };

        let item = NewOutboxItem::now(
            Effect::AdminNotice { channel, text },
            DedupeKey::from_stored(ADMIN_LANE),
            Utc::now(),
        );

        if let Err(error) = self.store.enqueue_effects(&[item], Utc::now()).await {
            // Logged rather than propagated: every caller is already reporting a failure, and a
            // failure to report a failure is not a reason to abandon what it was reporting on.
            warn!(%error, "cannot queue an administrative notice");
        }
    }

    /// Queues a notice unless this process has already sent one under `key`.
    ///
    /// The key is the condition, not the occurrence: one route that cannot be posted to produces
    /// one notice however many cards it swallows.
    pub(crate) async fn say_once(&self, key: String, text: String) {
        if self.channel.is_none() {
            return;
        }

        {
            let mut said = match self.said.lock() {
                Ok(said) => said,
                // A poisoned lock means a previous holder panicked while holding it. The set is
                // an optimisation and its contents are not load-bearing, so recovering is right.
                Err(poisoned) => poisoned.into_inner(),
            };

            if !said.insert(key) {
                return;
            }
        }

        self.say(text).await;
    }

    /// Forgets that `key` was said, so the next occurrence is reported again.
    ///
    /// What makes the deadman able to fire twice: it clears its own key on recovery, and the
    /// alternative is a bot that reports going silent once per process lifetime.
    pub(crate) fn forget(&self, key: &str) {
        let mut said = match self.said.lock() {
            Ok(said) => said,
            Err(poisoned) => poisoned.into_inner(),
        };

        said.remove(key);
    }
}
