use bytes::BytesMut;
use zerocopy::little_endian::U128;

use super::*;

fn stored_node(kind: NodeKind, epoch: u64, entries: &[u8]) -> Bytes {
    [
        NodeHeader {
            kind,
            epoch: U64::new(epoch),
        }
        .as_bytes(),
        entries,
    ]
    .concat()
    .into()
}

fn internal_entry(child: u128, key: &[u8]) -> Vec<u8> {
    [NodeId(U128::new(child)).as_bytes(), key].concat()
}

#[test]
fn an_empty_leaf_reads_back() {
    let entries = SlottedList::<BytesMut, LeafEntry, u16>::empty().into_inner();
    let (header, node) =
        Node::<Bytes, u16>::read(stored_node(NodeKind::Leaf, 3, &entries)).unwrap();
    assert_eq!((header.kind, header.epoch.get()), (NodeKind::Leaf, 3));
    assert!(matches!(node, Node::Leaf(list) if list.len() == 0));
}

#[test]
fn internal_entries_read_back_with_their_children() {
    let mut list = SlottedList::<BytesMut, InternalEntry, u16>::empty();
    for (index, entry) in [internal_entry(1, b""), internal_entry(2, b"m")]
        .iter()
        .enumerate()
    {
        list.insert(index, InternalEntry::ref_from_bytes(entry).unwrap());
    }
    let (header, node) =
        Node::<Bytes, u16>::read(stored_node(NodeKind::Internal, 0, &list.into_inner())).unwrap();
    assert_eq!(header.kind, NodeKind::Internal);
    let Node::Internal(list) = node else {
        panic!("expected an internal node");
    };
    let second = list.get(1).unwrap().unwrap();
    assert_eq!(
        (second.child, &second.key),
        (NodeId(U128::new(2)), &b"m"[..])
    );
}

#[test]
fn leaf_entries_read_back_with_their_keys_and_values() {
    let mut list = SlottedList::<BytesMut, LeafEntry, u16>::empty();
    list.insert(0, &LeafEntry::new(b"key", b"value"));
    let (_, node) =
        Node::<Bytes, u16>::read(stored_node(NodeKind::Leaf, 0, &list.into_inner())).unwrap();
    let Node::Leaf(list) = node else {
        panic!("expected a leaf");
    };
    let entry = list.get(0).unwrap().unwrap();
    assert_eq!(entry.key_and_value().unwrap(), (&b"key"[..], &b"value"[..]));
}

#[test]
fn a_new_leaf_entry_holds_its_key_and_value() {
    for (key, value) in [
        (&b"key"[..], &b"value"[..]),
        (b"", b""),
        (b"", b"value"),
        (b"key", b""),
    ] {
        assert_eq!(
            LeafEntry::new(key, value).key_and_value().unwrap(),
            (key, value)
        );
    }
}

#[test]
#[should_panic(expected = "a leaf entry's key is at most 65535 bytes")]
fn a_leaf_entry_key_cannot_outgrow_its_length() {
    LeafEntry::new(&vec![0; usize::from(u16::MAX) + 1], b"");
}

#[test]
fn a_key_longer_than_its_entry_is_reported() {
    let entry = [U16::new(4).as_bytes(), b"abc"].concat();
    assert!(matches!(
        LeafEntry::ref_from_bytes(&entry).unwrap().key_and_value(),
        Err(CorruptValue::KeyLongerThanEntry)
    ));
}

#[test]
fn an_unknown_kind_is_reported() {
    let entries = SlottedList::<BytesMut, LeafEntry, u16>::empty().into_inner();
    let mut stored = stored_node(NodeKind::Leaf, 0, &entries).to_vec();
    stored[0] = 2;
    assert!(matches!(
        Node::<Bytes, u16>::read(stored.into()),
        Err(CorruptValue::UnknownNodeKind)
    ));
}

#[test]
fn a_node_too_short_for_its_header_is_reported() {
    assert!(matches!(
        Node::<Bytes, u16>::read(Bytes::from_static(&[0, 1, 2])),
        Err(CorruptValue::WrongLength)
    ));
}

#[test]
fn corrupt_entries_are_reported() {
    assert!(matches!(
        Node::<Bytes, u16>::read(stored_node(NodeKind::Leaf, 0, &[5, 0])),
        Err(CorruptValue::WrongLength)
    ));
}
