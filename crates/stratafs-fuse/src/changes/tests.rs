use std::time::{Duration, UNIX_EPOCH};

use stratafs_fs_engine::Timestamp;
use stratafs_fs_engine::attributes::{Hints, Ownership, Restrictions};

use super::*;

fn settable() -> SettableAttributes {
    SettableAttributes {
        mode: Mode::OWNER_READ,
        ownership: Ownership {
            user_id: 1,
            group_id: 2,
        },
        restrictions: Restrictions::empty(),
        hints: Hints::empty(),
        size: 10,
        last_accessed_at: Timestamp(3),
        content_changed_at: Timestamp(4),
        created_at: Timestamp(5),
    }
}

#[test]
fn a_change_sets_only_what_was_requested() {
    let mut changed = settable();
    Requested {
        size: Some(0),
        modified: Some(TimeOrNow::SpecificTime(
            UNIX_EPOCH + Duration::from_nanos(7),
        )),
        ..Requested::default()
    }
    .change()(&mut changed);
    assert_eq!(
        changed,
        SettableAttributes {
            size: 0,
            content_changed_at: Timestamp(7),
            ..settable()
        }
    );
}

#[test]
fn now_is_read_once_for_every_run_of_a_change() {
    let change = Requested {
        accessed: Some(TimeOrNow::Now),
        ..Requested::default()
    }
    .change();
    let (mut first, mut second) = (settable(), settable());
    change(&mut first);
    change(&mut second);
    assert_eq!(first.last_accessed_at, second.last_accessed_at);
    assert!(first.last_accessed_at > Timestamp(3));
}

#[test]
fn a_made_item_keeps_the_permission_bits_of_its_mode_and_the_callers_ownership() {
    let mut changed = settable();
    made(libc::S_IFREG | 0o2755, 1000, 100)(&mut changed);
    assert_eq!(changed.mode.bits(), 0o2755);
    assert_eq!(
        changed.ownership,
        Ownership {
            user_id: 1000,
            group_id: 100
        }
    );
    assert_eq!(changed.size, 10);
}
