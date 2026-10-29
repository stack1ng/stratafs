use stratafs_fs_engine::attributes::{
    ChangeVersion, Hints, Mode, Ownership, Restrictions, SettableAttributes,
};
use stratafs_fs_engine::{MountId, Timestamp};

use super::*;

fn file() -> Attributes {
    Attributes {
        settable: SettableAttributes {
            mode: Mode::OWNER_READ,
            ownership: Ownership {
                user_id: 501,
                group_id: 20,
            },
            restrictions: Restrictions::empty(),
            hints: Hints::empty(),
            size: 0,
            last_accessed_at: Timestamp(0),
            content_changed_at: Timestamp(0),
            created_at: Timestamp(0),
        },
        kind: FileType::File,
        hard_link_count: 1,
        attribute_changed_at: Timestamp(0),
        parent: Inode::ROOT,
        change_version: ChangeVersion {
            mount: MountId(1),
            number: 0,
        },
    }
}

#[test]
fn a_state_a_reply_hands_fskit_no_item_for_goes_once_the_reply_is_done() {
    let items = Items::default();
    items.reply([(Inode(5), file())], |_| {});
    assert!(items.state(Inode(5)).is_none());
}

#[test]
fn a_state_a_reply_hands_fskit_the_item_for_stays() {
    let items = Items::default();
    items.reply([(Inode(5), file())], |locked| {
        locked.item(Inode(5));
    });
    assert!(items.state(Inode(5)).is_some());
}
