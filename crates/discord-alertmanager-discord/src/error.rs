//! Turning `serenity`'s failures into the four or five things the dispatcher does about them.
//!
//! This is the only module in the workspace that knows a Discord error code. The engine reacts to
//! `UnknownMessage` by recreating a card and to `MissingPermissions` by marking a route unhealthy;
//! it does that without ever learning that those are 10008 and 50013, which is what keeps a
//! Discord constant out of a crate that does not depend on Discord.

use dam_engine::SinkError;
use serenity::Error as SerenityError;
use serenity::http::{HttpError, StatusCode};

/// The message was deleted.
const UNKNOWN_MESSAGE: isize = 10_008;

/// The channel is gone, which for a card is the same problem as a deleted message.
const UNKNOWN_CHANNEL: isize = 10_003;

/// The bot lacks a permission this call needs.
const MISSING_PERMISSIONS: isize = 50_013;

/// The bot cannot see the channel at all.
const MISSING_ACCESS: isize = 50_001;

/// The action does not apply to this kind of channel.
const WRONG_CHANNEL_TYPE: isize = 50_024;

/// The thread will not accept writes until it is reopened.
const THREAD_LOCKED: isize = 50_083;

/// The request body was rejected, which for this bot is almost always a tag that no longer exists.
const INVALID_FORM_BODY: isize = 50_035;

/// The forum channel is already holding every pinned post Discord lets it hold.
const MAX_PINNED_THREADS: isize = 30_047;

/// How long to wait when Discord rate-limits a request without saying for how long.
///
/// `serenity` waits out the limits it is told about, so reaching this means the response carried
/// no usable hint. A second is short enough not to stall a queue and long enough not to spend the
/// next attempt on the same refusal.
const DEFAULT_RETRY_MS: u64 = 1_000;

/// Maps a `serenity` failure onto the vocabulary the dispatcher reacts to.
///
/// `expected` names the channel kind the call assumed, so a route pointed at the wrong sort of
/// channel says which sort it wanted.
pub(crate) fn sink_error(error: &SerenityError, expected: &'static str) -> SinkError {
    let SerenityError::Http(HttpError::UnsuccessfulRequest(response)) = error else {
        return SinkError::Transient {
            detail: error.to_string(),
        };
    };

    if response.status_code == StatusCode::TOO_MANY_REQUESTS {
        return SinkError::RateLimited {
            retry_after_ms: DEFAULT_RETRY_MS,
        };
    }

    let message = response.error.message.as_str();

    match response.error.code {
        UNKNOWN_MESSAGE | UNKNOWN_CHANNEL => SinkError::UnknownMessage,
        MISSING_PERMISSIONS | MISSING_ACCESS => SinkError::MissingPermissions {
            detail: message.to_owned(),
        },
        WRONG_CHANNEL_TYPE => SinkError::WrongChannelType { expected },
        // Discord reports both states under one code, and the two need different handling: an
        // archived thread is reopened as an ordinary step, a locked one needs a permission the bot
        // may not hold and ends with the card orphaned.
        THREAD_LOCKED if message.to_ascii_lowercase().contains("archiv") => {
            SinkError::ThreadArchived
        }
        THREAD_LOCKED => SinkError::ThreadLocked,
        MAX_PINNED_THREADS => SinkError::PinLimitReached,
        INVALID_FORM_BODY if message.contains("applied_tags") => SinkError::UnknownTag,
        INVALID_FORM_BODY if message.contains("available_tags") => SinkError::TagBudgetExceeded,
        _ => SinkError::Transient {
            detail: format!("{} ({})", message, response.error.code),
        },
    }
}
