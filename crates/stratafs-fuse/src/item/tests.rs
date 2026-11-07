use stratafs_fs_engine::MountId;
use stratafs_fs_engine::attributes::{
    ChangeVersion, Hints, Mode, Ownership, Restrictions, SettableAttributes,
};

use super::*;

fn file(size: u64) -> Attributes {
    Attributes {
        settable: SettableAttributes {
            mode: Mode::OWNER_READ | Mode::OWNER_WRITE | Mode::SET_USER_ID,
            ownership: Ownership {
                user_id: 1000,
                group_id: 100,
            },
            restrictions: Restrictions::empty(),
            hints: Hints::empty(),
            size,
            last_accessed_at: Timestamp(1),
            content_changed_at: Timestamp(-1_500_000_000),
            created_at: Timestamp(0),
        },
        kind: FileType::File,
        hard_link_count: 2,
        attribute_changed_at: Timestamp(1_700_000_000_123_456_789),
        parent: Inode::ROOT,
        change_version: ChangeVersion {
            mount: MountId(1),
            number: 0,
        },
    }
}

#[test]
fn the_root_is_node_1() {
    assert_eq!(node_id(Inode::ROOT), INodeNo::ROOT);
    assert_eq!(inode(INodeNo::ROOT), Inode::ROOT);
}

#[test]
fn every_inode_the_engine_makes_has_a_node_that_names_it() {
    for number in [1, 2, u64::MAX / 2, u64::MAX - 2] {
        assert_eq!(inode(node_id(Inode(number))), Inode(number));
    }
}

#[test]
fn node_0_names_no_inode_the_engine_makes() {
    assert_eq!(inode(INodeNo(0)), Inode(u64::MAX));
}

#[test]
fn times_keep_their_nanoseconds_either_side_of_the_epoch() {
    for nanoseconds in [0, 1, -1, 1_700_000_000_123_456_789, -1_500_000_000] {
        assert_eq!(
            timestamp(system_time(Timestamp(nanoseconds))),
            Timestamp(nanoseconds)
        );
    }
}

#[test]
fn attributes_reach_the_kernel_with_their_node_and_permission_bits() {
    let attr = file_attr(Inode(41), &file(1));
    assert_eq!(attr.ino, INodeNo(42));
    assert_eq!(attr.perm, 0o4600);
    assert_eq!(attr.kind, fuser::FileType::RegularFile);
    assert_eq!((attr.uid, attr.gid, attr.nlink), (1000, 100, 2));
    assert_eq!(timestamp(attr.ctime), Timestamp(1_700_000_000_123_456_789));
}

#[test]
fn blocks_round_the_size_up_to_whole_blocks_in_512_byte_units() {
    assert_eq!(file_attr(Inode(1), &file(0)).blocks, 0);
    assert_eq!(file_attr(Inode(1), &file(1)).blocks, 8);
    assert_eq!(file_attr(Inode(1), &file(4097)).blocks, 16);
}
