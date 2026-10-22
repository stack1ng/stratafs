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
