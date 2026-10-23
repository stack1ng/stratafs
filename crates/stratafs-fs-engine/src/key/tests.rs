use std::borrow::Cow;
use std::ops::RangeBounds;

use super::*;

fn encoded(inode: u64, kind: KeyKind<'_>) -> Bytes {
    Bytes::from(FsKey {
        inode: Inode(inode),
        kind,
    })
}

#[test]
fn attributes_keys_are_the_inode_in_big_endian_then_the_attributes_tag() {
    assert_eq!(
        encoded(0x0102, KeyKind::Attributes)[..],
        [0, 0, 0, 0, 0, 0, 0x01, 0x02, 2]
    );
}

#[test]
fn extended_attribute_keys_end_with_the_name_and_a_terminator() {
    assert_eq!(
        encoded(0x0102, KeyKind::ExtendedAttribute(Cow::Borrowed(b"user.a")))[..],
        [
            0, 0, 0, 0, 0, 0, 0x01, 0x02, 3, b'u', b's', b'e', b'r', b'.', b'a', 0
        ]
    );
}

#[test]
fn zero_and_one_bytes_in_names_are_escaped() {
    assert_eq!(
        encoded(1, KeyKind::ExtendedAttribute(Cow::Borrowed(&[0, 1, 2])))[..],
        [0, 0, 0, 0, 0, 0, 0, 1, 3, 1, 0, 1, 1, 2, 0]
    );
}

#[test]
fn keys_group_by_inode_with_attributes_first_then_names_in_byte_order() {
    let ordered = [
        encoded(255, KeyKind::ExtendedAttribute(Cow::Borrowed(b"z"))),
        encoded(256, KeyKind::Attributes),
        encoded(256, KeyKind::ExtendedAttribute(Cow::Borrowed(&[0]))),
        encoded(256, KeyKind::ExtendedAttribute(Cow::Borrowed(&[1]))),
        encoded(256, KeyKind::ExtendedAttribute(Cow::Borrowed(b"user.a"))),
        encoded(256, KeyKind::ExtendedAttribute(Cow::Borrowed(b"user.ab"))),
        encoded(256, KeyKind::ExtendedAttribute(Cow::Borrowed(b"user.b"))),
        encoded(257, KeyKind::Attributes),
    ];
    let mut sorted = ordered.to_vec();
    sorted.reverse();
    sorted.sort();
    assert_eq!(sorted, ordered);
}

#[test]
fn the_extended_attribute_range_holds_exactly_the_inodes_extended_attribute_keys() {
    let range = extended_attributes_range(Inode(256));
    for (key, inside) in [
        (
            encoded(
                255,
                KeyKind::ExtendedAttribute(Cow::Borrowed(&[0xff, 0xff])),
            ),
            false,
        ),
        (encoded(256, KeyKind::Attributes), false),
        (
            encoded(256, KeyKind::ExtendedAttribute(Cow::Borrowed(&[0]))),
            true,
        ),
        (
            encoded(256, KeyKind::ExtendedAttribute(Cow::Borrowed(&[1]))),
            true,
        ),
        (
            encoded(256, KeyKind::ExtendedAttribute(Cow::Borrowed(b"user.a"))),
            true,
        ),
        (
            encoded(256, KeyKind::ExtendedAttribute(Cow::Borrowed(&[0xff; 255]))),
            true,
        ),
        (encoded(257, KeyKind::Attributes), false),
    ] {
        assert_eq!(range.contains(&key), inside, "{key:?}");
    }
}

#[test]
fn extended_attribute_names_decode_from_their_keys() {
    for name in [&b"user.a"[..], &[0, 1, 2], &[0xff; 255]] {
        let key = Bytes::from(FsKey::new_extended_attribute(Inode(256), name));
        assert_eq!(extended_attribute_name(&key).unwrap(), name);
    }
}

#[test]
fn decoding_a_name_from_an_attributes_key_is_an_unexpected_key() {
    let key = Bytes::from(FsKey::new_attributes(Inode(256)));
    assert!(matches!(
        extended_attribute_name(&key),
        Err(CorruptValue::UnexpectedKey)
    ));
}

#[test]
fn every_kind_of_key_sorts_together_under_its_inode_in_declaration_order() {
    let ordered = [
        Bytes::from(FsKey::new_symbolic_link_target(Inode(255))),
        Bytes::from(FsKey::new_attributes(Inode(256))),
        Bytes::from(FsKey::new_extended_attribute(Inode(256), b"user.a")),
        Bytes::from(FsKey::new_directory_entry(Inode(256), 3)),
        Bytes::from(FsKey::new_directory_entry(Inode(256), 0x0100)),
        Bytes::from(FsKey::new_directory_entry(Inode(256), u64::MAX >> 1)),
        Bytes::from(FsKey::new_hard_link(Inode(256), Inode(0), 0)),
        Bytes::from(FsKey::new_hard_link(Inode(256), Inode(7), 3)),
        Bytes::from(FsKey::new_symbolic_link_target(Inode(256))),
        Bytes::from(FsKey::new_extent(Inode(256), 1)),
        Bytes::from(FsKey::new_extent(Inode(256), 0x0100)),
        Bytes::from(FsKey::new_extent(Inode(256), u64::MAX)),
        Bytes::from(FsKey::new_attributes(Inode(257))),
    ];
    let mut sorted = ordered.to_vec();
    sorted.reverse();
    sorted.sort();
    assert_eq!(sorted, ordered);
}

#[test]
fn directory_entry_ranges_hold_exactly_the_requested_positions() {
    let entry = |dir, position| Bytes::from(FsKey::new_directory_entry(Inode(dir), position));
    let after = directory_entries_after(Inode(256), 5);
    for (key, inside) in [
        (
            Bytes::from(FsKey::new_extended_attribute(Inode(256), &[0xff; 255])),
            false,
        ),
        (entry(256, 5), false),
        (entry(256, 6), true),
        (entry(256, u64::MAX >> 1), true),
        (
            Bytes::from(FsKey::new_hard_link(Inode(256), Inode(0), 0)),
            false,
        ),
        (entry(257, 6), false),
    ] {
        assert_eq!(after.contains(&key), inside, "{key:?}");
    }
    let between = directory_entries_between(Inode(256), 5, 7);
    for (position, inside) in [(4, false), (5, true), (7, true), (8, false)] {
        assert_eq!(
            between.contains(&entry(256, position)),
            inside,
            "{position}"
        );
    }
}

#[test]
fn the_hard_link_range_holds_exactly_the_inodes_hard_links() {
    let range = hard_links_of(Inode(256));
    for (key, inside) in [
        (
            Bytes::from(FsKey::new_directory_entry(Inode(256), u64::MAX)),
            false,
        ),
        (
            Bytes::from(FsKey::new_hard_link(Inode(256), Inode(0), 0)),
            true,
        ),
        (
            Bytes::from(FsKey::new_hard_link(Inode(256), Inode(u64::MAX), u64::MAX)),
            true,
        ),
        (
            Bytes::from(FsKey::new_symbolic_link_target(Inode(256))),
            false,
        ),
        (
            Bytes::from(FsKey::new_hard_link(Inode(257), Inode(0), 0)),
            false,
        ),
    ] {
        assert_eq!(range.contains(&key), inside, "{key:?}");
    }
}

#[test]
fn every_key_of_an_inode_holds_all_its_kinds_and_no_neighbours() {
    let range = every_key_of(Inode(256));
    for (key, inside) in [
        (
            Bytes::from(FsKey::new_symbolic_link_target(Inode(255))),
            false,
        ),
        (Bytes::from(FsKey::new_attributes(Inode(256))), true),
        (
            Bytes::from(FsKey::new_extended_attribute(Inode(256), &[0xff; 255])),
            true,
        ),
        (
            Bytes::from(FsKey::new_directory_entry(Inode(256), u64::MAX)),
            true,
        ),
        (
            Bytes::from(FsKey::new_hard_link(Inode(256), Inode(u64::MAX), u64::MAX)),
            true,
        ),
        (
            Bytes::from(FsKey::new_symbolic_link_target(Inode(256))),
            true,
        ),
        (Bytes::from(FsKey::new_attributes(Inode(257))), false),
    ] {
        assert_eq!(range.contains(&key), inside, "{key:?}");
    }
}

#[test]
fn directory_entry_positions_and_hard_link_directories_decode_from_their_keys() {
    let entry = Bytes::from(FsKey::new_directory_entry(Inode(256), 0x0102));
    assert_eq!(directory_entry_position(&entry).unwrap(), 0x0102);
    let hard_link = Bytes::from(FsKey::new_hard_link(Inode(256), Inode(9), 3));
    assert_eq!(hard_link_directory(&hard_link).unwrap(), Inode(9));
    assert!(matches!(
        directory_entry_position(&hard_link),
        Err(CorruptValue::UnexpectedKey)
    ));
}
