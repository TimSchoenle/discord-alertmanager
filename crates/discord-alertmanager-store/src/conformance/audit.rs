//! Cases for the audit log and retention pruning.

use crate::Store;
use crate::audit::{AuditEntry, RetentionPolicy};
use crate::ids::{GuildId, UserId};

use super::at;

pub(super) async fn pruning_is_bounded_and_says_so(store: &dyn Store) {
    for index in 0..3 {
        store
            .append_audit(&AuditEntry::ok(
                "prune.fixture",
                Some(UserId::new(1)),
                at(-86_400 * 400 + index),
            ))
            .await
            .expect("the entry is written");
    }

    let report = store
        .prune(
            &RetentionPolicy {
                batch_limit: 2,
                ..RetentionPolicy::default()
            },
            at(0),
        )
        .await
        .expect("the sweep succeeds");

    assert_eq!(report.audit, 2, "the batch limit bounds one pass");
    assert!(
        report.more,
        "a pass that filled its batch has left rows behind, and the caller schedules another"
    );

    let rest = store
        .prune(
            &RetentionPolicy {
                batch_limit: 100,
                ..RetentionPolicy::default()
            },
            at(0),
        )
        .await
        .expect("the sweep succeeds");

    assert_eq!(rest.audit, 1);
    assert!(!rest.more);
}

pub(super) async fn the_audit_log_accepts_every_result(store: &dyn Store) {
    let denied = AuditEntry::denied("silence.create", Some(UserId::new(4)), at(0));

    store
        .append_audit(&denied)
        .await
        .expect("a denial is recorded");

    let mut detailed = AuditEntry::ok("silence.create", Some(UserId::new(4)), at(1));
    detailed.guild_id = Some(GuildId::new(1));
    detailed.subject = Some("aaaa0001".to_owned());
    detailed.detail = serde_json::json!({ "am_id": "silence-1", "duration": "4h" });

    store
        .append_audit(&detailed)
        .await
        .expect("the resulting silence id is recorded with it");
}
