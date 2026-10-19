use bytes::BytesMut;
use zerocopy::little_endian::{U16, U64, U128};

use super::*;

fn stored_node(kind: NodeKind, epoch: u64, entries: &[u8]) -> Bytes {
    [
        NodeHeader {
            kind,
            epoch: VolumeEpoch(U64::new(epoch)),
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
    let entries = SlottedList::<BytesMut, LeafEntry<u16>, u16>::empty().into_inner();
    let (epoch, node) = Node::<Bytes, u16>::read(stored_node(NodeKind::Leaf, 3, &entries)).unwrap();
    assert_eq!(epoch, VolumeEpoch(U64::new(3)));
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
    let (_, node) =
        Node::<Bytes, u16>::read(stored_node(NodeKind::Internal, 0, &list.into_inner())).unwrap();
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
    let mut list = SlottedList::<BytesMut, LeafEntry<u16>, u16>::empty();
    list.insert(0, &LeafEntry::<u16>::new(b"key", b"value").unwrap());
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
            LeafEntry::<u16>::new(key, value)
                .unwrap()
                .key_and_value()
                .unwrap(),
            (key, value)
        );
    }
}

#[test]
fn a_leaf_entry_key_must_fit_the_addressable_range() {
    assert!(LeafEntry::<u8>::new(&[0; 255], b"").is_some());
    assert!(LeafEntry::<u8>::new(&[0; 256], b"").is_none());
    assert!(LeafEntry::<u16>::new(&vec![0; usize::from(u16::MAX) + 1], b"").is_none());
}

#[test]
fn a_key_longer_than_its_entry_is_reported() {
    let entry = [U16::new(4).as_bytes(), b"abc"].concat();
    assert!(matches!(
        LeafEntry::<u16>::ref_from_bytes(&entry)
            .unwrap()
            .key_and_value(),
        Err(CorruptValue::KeyLongerThanEntry)
    ));
}

#[test]
fn a_written_node_reads_back() {
    let mut list = SlottedList::<BytesMut, LeafEntry<u16>, u16>::empty();
    list.insert(0, &LeafEntry::<u16>::new(b"key", b"value").unwrap());
    let stored = Node::<Bytes, u16>::write(
        NodeHeader {
            kind: NodeKind::Leaf,
            epoch: VolumeEpoch(U64::new(5)),
        },
        list,
    );
    let (epoch, node) = Node::<Bytes, u16>::read(stored).unwrap();
    assert_eq!(epoch, VolumeEpoch(U64::new(5)));
    let Node::Leaf(list) = node else {
        panic!("expected a leaf");
    };
    assert_eq!(
        list.get(0).unwrap().unwrap().key_and_value().unwrap(),
        (&b"key"[..], &b"value"[..])
    );
}

#[test]
fn an_unknown_kind_is_reported() {
    let entries = SlottedList::<BytesMut, LeafEntry<u16>, u16>::empty().into_inner();
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
