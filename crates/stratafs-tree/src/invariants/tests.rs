use bytes::BytesMut;
use zerocopy::little_endian::{U64, U128};
use zerocopy::{FromBytes, IntoBytes};

use super::*;
use crate::header::VolumeEpoch;
use crate::node::ObservedNode;

fn leaf_list(keys: &[&[u8]]) -> SlottedList<BytesMut, LeafEntry<u16>, u16> {
    let mut list = SlottedList::<BytesMut, LeafEntry<u16>, u16>::empty();
    for (index, key) in keys.iter().enumerate() {
        list.insert(index, &LeafEntry::<u16>::new(key, b"value").unwrap());
    }
    list
}

fn separator_list(keys: &[&[u8]]) -> SlottedList<BytesMut, InternalEntry, u16> {
    let mut list = SlottedList::<BytesMut, InternalEntry, u16>::empty();
    for (index, key) in keys.iter().enumerate() {
        let entry = [NodeId(U128::new(index as u128)).as_bytes(), key].concat();
        list.insert(index, InternalEntry::ref_from_bytes(&entry).unwrap());
    }
    list
}

fn observed(id: u128) -> ObservedNode {
    ObservedNode {
        id: NodeId(U128::new(id)),
        epoch: VolumeEpoch(U64::new(0)),
    }
}

fn untyped() -> SlottedList<BytesMut, [u8], u16> {
    SlottedList::<BytesMut, [u8], u16>::empty()
}

#[test]
fn leaf_keys_must_strictly_ascend() {
    assert!(leaf_keys_ascend(&leaf_list(&[])));
    assert!(leaf_keys_ascend(&leaf_list(&[b"a", b"b", b"c"])));
    assert!(!leaf_keys_ascend(&leaf_list(&[b"a", b"c", b"b"])));
    assert!(!leaf_keys_ascend(&leaf_list(&[b"a", b"a"])));
}

#[test]
fn separator_keys_must_strictly_ascend() {
    assert!(separator_keys_ascend(&separator_list(&[b"", b"m", b"t"])));
    assert!(!separator_keys_ascend(&separator_list(&[b"", b"t", b"m"])));
    assert!(!separator_keys_ascend(&separator_list(&[b"", b"m", b"m"])));
}

#[test]
fn a_plan_must_observe_each_node_once() {
    let update = |id| {
        Some(LayerMutationPlanAction::Update {
            node: observed(id),
            at: 0,
            entries: untyped(),
        })
    };
    let delete = |id| {
        Some(LayerMutationPlanAction::Delete {
            node: observed(id),
            at: 0,
        })
    };
    let create = || {
        Some(LayerMutationPlanAction::Create {
            separator: Box::from(&b"m"[..]),
            entries: untyped(),
        })
    };
    assert!(plan_has_at_most_one_action_per_node::<u16>(&[
        update(1),
        None
    ]));
    assert!(plan_has_at_most_one_action_per_node::<u16>(&[
        update(1),
        create()
    ]));
    assert!(plan_has_at_most_one_action_per_node::<u16>(&[
        update(1),
        delete(2)
    ]));
    assert!(!plan_has_at_most_one_action_per_node::<u16>(&[
        update(1),
        delete(1)
    ]));
    assert!(!plan_has_at_most_one_action_per_node::<u16>(&[
        update(1),
        update(1)
    ]));
}
