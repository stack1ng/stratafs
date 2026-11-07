use std::cell::Cell;

use stratafs_fs_engine::attributes::{
    ChangeVersion, FileType, Hints, Mode, Ownership, Restrictions, SettableAttributes,
};
use stratafs_fs_engine::{MountId, ResultHandling, Timestamp};

use super::*;

fn file(number: u64, hard_link_count: u32) -> Attributes {
    Attributes {
        settable: SettableAttributes {
            mode: Mode::OWNER_READ,
            ownership: Ownership {
                user_id: 1000,
                group_id: 100,
            },
            restrictions: Restrictions::empty(),
            hints: Hints::empty(),
            size: number,
            last_accessed_at: Timestamp(0),
            content_changed_at: Timestamp(0),
            created_at: Timestamp(0),
        },
        kind: FileType::File,
        hard_link_count,
        attribute_changed_at: Timestamp(0),
        parent: Inode::ROOT,
        change_version: ChangeVersion {
            mount: MountId(1),
            number,
        },
    }
}

// handed hands the kernel count lookups of inode, as read, under a name in the root.
fn handed(items: &Items, inode: Inode, read: Attributes, count: u64) {
    let name = Bytes::from_static(b"name");
    items.reply(
        [(inode, read), (Inode::ROOT, file(0, 1))],
        &|| ResultHandling::Cache,
        |locked| {
            for _ in 0..count {
                locked.hand(Inode::ROOT, &name, inode);
            }
        },
    );
}

#[test]
fn the_kernel_holds_the_root_from_the_mount_on() {
    let items = Items::new();
    assert!(items.state(Inode::ROOT).is_some());
}

#[test]
fn a_state_a_reply_hands_no_lookup_of_goes_once_the_reply_is_done() {
    let items = Items::new();
    items.reply([(Inode(5), file(0, 1))], &|| ResultHandling::Cache, |_| {});
    assert!(items.state(Inode(5)).is_none());
}

#[test]
fn a_result_handled_with_retry_replies_nothing_and_keeps_nothing() {
    let items = Items::new();
    let replied = items.reply(
        [(Inode(5), file(0, 1))],
        &|| ResultHandling::Retry,
        |locked| {
            locked.hand(Inode::ROOT, &Bytes::from_static(b"name"), Inode(5));
        },
    );
    assert!(replied.is_none());
    assert!(items.state(Inode(5)).is_none());
}

#[test]
fn a_reply_hands_out_the_newest_attributes_read_whatever_order_they_arrive_in() {
    let items = Items::new();
    handed(&items, Inode(5), file(2, 1), 1);
    let replied = items.reply(
        [(Inode(5), file(1, 1))],
        &|| ResultHandling::Cache,
        |locked| locked.attr(Inode(5)).size,
    );
    assert_eq!(replied, Some(2));
}

#[test]
fn the_engine_forgets_an_inode_once_the_kernel_forgets_every_lookup() {
    let items = Items::new();
    handed(&items, Inode(5), file(0, 1), 2);
    let forgotten = Cell::new(0);
    assert_eq!(
        items.forget(Inode(5), 1, || forgotten.set(forgotten.get() + 1)),
        None
    );
    assert_eq!(forgotten.get(), 0);
    assert_eq!(
        items.forget(Inode(5), 1, || forgotten.set(forgotten.get() + 1)),
        Some(false)
    );
    assert_eq!(forgotten.get(), 1);
    assert!(items.state(Inode(5)).is_none());
}

#[test]
fn forgetting_an_inode_with_no_name_says_so() {
    let items = Items::new();
    handed(&items, Inode(5), file(0, 0), 1);
    assert_eq!(items.forget(Inode(5), 1, || {}), Some(true));
}

#[test]
fn forgetting_an_inode_the_kernel_was_never_handed_does_nothing() {
    let items = Items::new();
    assert_eq!(
        items.forget(Inode(5), 1, || panic!("nothing to forget")),
        None
    );
}

#[test]
fn release_forgets_only_an_inode_the_kernel_holds_no_lookup_of() {
    let items = Items::new();
    handed(&items, Inode(5), file(0, 1), 1);
    items.release(Inode(5), || panic!("the kernel holds a lookup"));
    let released = Cell::new(false);
    items.release(Inode(6), || released.set(true));
    assert!(released.get());
    assert!(items.state(Inode(6)).is_none());
}

#[test]
fn invalidating_a_directory_drops_the_names_handed_in_it_until_each_is_dropped() {
    let items = Items::new();
    let (kept, dropped) = (Bytes::from_static(b"kept"), Bytes::from_static(b"dropped"));
    items.reply(
        [(Inode::ROOT, file(0, 1))],
        &|| ResultHandling::Cache,
        |locked| {
            locked.name(Inode::ROOT, &kept, Inode(5));
            locked.name(Inode::ROOT, &dropped, Inode(6));
        },
    );
    items.reply(
        [(Inode::ROOT, file(0, 1))],
        &|| ResultHandling::Cache,
        |locked| {
            locked.unname(Inode::ROOT, &dropped);
        },
    );
    let handed = items.to_invalidate(Inode::ROOT).unwrap();
    let [(name, reply)] = handed.as_slice() else {
        panic!("one name is handed: {handed:?}");
    };
    assert_eq!(name, &kept);
    assert_eq!(items.to_invalidate(Inode::ROOT), Some(handed.clone()));
    items.dropped_name(Inode::ROOT, &kept, *reply);
    assert_eq!(items.to_invalidate(Inode::ROOT), Some(Vec::new()));
}

#[test]
fn a_name_handed_again_while_it_is_dropped_stays_to_drop() {
    let items = Items::new();
    let name = Bytes::from_static(b"name");
    let hand = |inode| {
        items.reply(
            [(Inode::ROOT, file(0, 1))],
            &|| ResultHandling::Cache,
            |locked| {
                locked.name(Inode::ROOT, &name, inode);
            },
        );
    };
    hand(Inode(5));
    let handed = items.to_invalidate(Inode::ROOT).unwrap();
    hand(Inode(6));
    for (name, reply) in handed {
        items.dropped_name(Inode::ROOT, &name, reply);
    }
    assert_eq!(items.named(Inode::ROOT, &name), Some(Inode(6)));
    assert_eq!(items.to_invalidate(Inode::ROOT).unwrap().len(), 1);
}

#[test]
fn invalidating_an_item_drops_only_the_names_in_it() {
    let items = Items::new();
    handed(&items, Inode(5), file(0, 1), 2);
    assert_eq!(items.to_invalidate(Inode(5)), Some(Vec::new()));
    assert_eq!(items.named(Inode::ROOT, b"name"), Some(Inode(5)));
}

#[test]
fn the_kernel_was_handed_no_name_it_was_not_handed_or_dropped() {
    let items = Items::new();
    handed(&items, Inode(5), file(0, 1), 1);
    assert_eq!(items.named(Inode::ROOT, b"other"), None);
    items.reply(
        [(Inode::ROOT, file(0, 1))],
        &|| ResultHandling::Cache,
        |locked| {
            locked.unname(Inode::ROOT, b"name");
        },
    );
    assert_eq!(items.named(Inode::ROOT, b"name"), None);
}

#[test]
fn the_unlinked_inodes_are_those_the_kernel_holds_with_no_name() {
    let items = Items::new();
    handed(&items, Inode(5), file(0, 0), 1);
    handed(&items, Inode(6), file(0, 1), 1);
    assert_eq!(items.unlinked(), [Inode(5)]);
}

#[test]
fn the_kernel_holds_nothing_to_invalidate_of_an_inode_it_does_not_hold() {
    assert_eq!(Items::new().to_invalidate(Inode(5)), None);
}

#[test]
fn an_uncached_reply_hands_a_lookup_and_a_name() {
    let items = Items::new();
    let name = Bytes::from_static(b"name");
    let replied = items.reply(
        [(Inode(5), file(0, 1)), (Inode::ROOT, file(0, 1))],
        &|| ResultHandling::NoCache,
        |locked| {
            locked.hand(Inode::ROOT, &name, Inode(5));
            locked.cached
        },
    );
    assert_eq!(replied, Some(false));
    assert_eq!(items.named(Inode::ROOT, &name), Some(Inode(5)));
    assert_eq!(items.forget(Inode(5), 1, || {}), Some(false));
}

// asked says whether the kernel asked for inode's attributes, as a reply about inode and the root finds.
fn asked(items: &Items, inode: Inode) -> bool {
    let replied = items.reply(
        [(inode, file(0, 1)), (Inode::ROOT, file(0, 1))],
        &|| ResultHandling::Cache,
        |locked| locked.asked(inode),
    );
    replied.expect("a cached reply replies")
}

#[test]
fn the_kernel_asked_for_the_roots_attributes_from_the_mount_on() {
    assert!(asked(&Items::new(), Inode::ROOT));
}

#[test]
fn the_kernel_asked_for_an_items_attributes_from_a_getattr_until_it_forgets_a_lookup_of_it() {
    let items = Items::new();
    handed(&items, Inode(5), file(0, 1), 2);
    assert!(!asked(&items, Inode(5)));
    items.reply(
        [(Inode(5), file(0, 1))],
        &|| ResultHandling::Cache,
        |locked| {
            locked.ask(Inode(5));
        },
    );
    assert!(asked(&items, Inode(5)));
    assert_eq!(items.forget(Inode(5), 1, || {}), None);
    assert!(!asked(&items, Inode(5)));
}

// looked_up says whether a lookup of inode under name in the root, found at change number found, hands it over.
fn looked_up(
    items: &Items,
    name: &'static [u8],
    inode: Inode,
    found: u64,
    handling: ResultHandling,
) -> bool {
    let name = Bytes::from_static(name);
    let replied = items.reply(
        [(inode, file(found, 1)), (Inode::ROOT, file(0, 1))],
        &|| handling,
        |locked| locked.look_up(Inode::ROOT, &name, inode, found),
    );
    replied.expect("a reply handling for replies")
}

#[test]
fn a_lookup_hands_nothing_for_an_item_found_newer_than_an_uncached_lookup_found_it_since() {
    let items = Items::new();
    assert!(looked_up(
        &items,
        b"old",
        Inode(5),
        1,
        ResultHandling::NoCache
    ));
    assert!(looked_up(
        &items,
        b"old",
        Inode(5),
        1,
        ResultHandling::Cache
    ));
    assert!(!looked_up(
        &items,
        b"new",
        Inode(5),
        2,
        ResultHandling::Cache
    ));
    assert_eq!(items.named(Inode::ROOT, b"new"), None);
    assert!(looked_up(
        &items,
        b"new",
        Inode(5),
        2,
        ResultHandling::Cache
    ));
}

#[test]
fn a_lookup_hands_an_item_found_newer_than_a_cached_lookup_found_it() {
    let items = Items::new();
    assert!(looked_up(
        &items,
        b"old",
        Inode(5),
        1,
        ResultHandling::Cache
    ));
    assert!(looked_up(
        &items,
        b"new",
        Inode(5),
        2,
        ResultHandling::Cache
    ));
}
