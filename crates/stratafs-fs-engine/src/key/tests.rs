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
        encoded(0x0102, KeyKind::ExtendedAttribute(b"user.a"))[..],
        [
            0, 0, 0, 0, 0, 0, 0x01, 0x02, 3, b'u', b's', b'e', b'r', b'.', b'a', 0
        ]
    );
}

#[test]
fn zero_and_one_bytes_in_names_are_escaped() {
    assert_eq!(
        encoded(1, KeyKind::ExtendedAttribute(&[0, 1, 2]))[..],
        [0, 0, 0, 0, 0, 0, 0, 1, 3, 1, 0, 1, 1, 2, 0]
    );
}

#[test]
fn keys_group_by_inode_with_attributes_first_then_names_in_byte_order() {
    let ordered = [
        encoded(255, KeyKind::ExtendedAttribute(b"z")),
        encoded(256, KeyKind::Attributes),
        encoded(256, KeyKind::ExtendedAttribute(&[0])),
        encoded(256, KeyKind::ExtendedAttribute(&[1])),
        encoded(256, KeyKind::ExtendedAttribute(b"user.a")),
        encoded(256, KeyKind::ExtendedAttribute(b"user.ab")),
        encoded(256, KeyKind::ExtendedAttribute(b"user.b")),
        encoded(257, KeyKind::Attributes),
    ];
    let mut sorted = ordered.to_vec();
    sorted.reverse();
    sorted.sort();
    assert_eq!(sorted, ordered);
}
