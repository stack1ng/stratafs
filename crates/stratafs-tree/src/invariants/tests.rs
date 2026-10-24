use bytes::BytesMut;
use smallvec::smallvec;
use vec1::smallvec_v1::SmallVec1;
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
    let replace = |id| {
        Some(LayerMutationPlanAction::Replace {
            node: observed(id),
            at: 0,
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
    assert!(plan_has_at_most_one_action_per_node::<u16>(&[
        update(1),
        replace(2)
    ]));
    assert!(!plan_has_at_most_one_action_per_node::<u16>(&[
        update(1),
        replace(1)
    ]));
}

fn step(separators: &[&[u8]], position: usize) -> DescentStep<u16, InternalEntry, usize> {
    DescentStep {
        node: observed(100),
        entries: SlottedList::new(separator_list(separators).into_inner().freeze()).unwrap(),
        position,
    }
}

fn node_at(position: usize) -> LayerNode<u16> {
    LayerNode {
        node: observed(position as u128),
        steps: smallvec![step(&[b"", b"m", b"t"], position)],
    }
}

fn layer(
    kind: NodeKind,
    nodes: Vec<LayerNode<u16>>,
    entries: SlottedList<BytesMut, [u8], u16>,
) -> Layer<u16> {
    Layer {
        kind,
        spans: smallvec![LayerSpan {
            nodes: SmallVec1::try_from_vec(nodes).unwrap(),
            entries,
        }],
    }
}

fn leaf_layer(nodes: Vec<LayerNode<u16>>, keys: &[&[u8]]) -> Layer<u16> {
    layer(NodeKind::Leaf, nodes, leaf_list(keys).into_untyped())
}

#[test]
fn a_layer_of_ordered_nodes_on_their_own_paths_is_well_formed() {
    assert!(layer_is_well_formed(&leaf_layer(
        vec![node_at(0), node_at(2)],
        &[b"a", b"u"]
    )));
    assert!(layer_is_well_formed(&leaf_layer(
        vec![node_at(1)],
        &[b"m", b"n"]
    )));
    assert!(layer_is_well_formed(&layer(
        NodeKind::Internal,
        vec![node_at(1)],
        separator_list(&[b"m", b"n"]).into_untyped()
    )));
}

#[test]
fn layer_nodes_must_strictly_ascend() {
    assert!(!layer_is_well_formed(&leaf_layer(
        vec![node_at(2), node_at(0)],
        &[b"a", b"u"]
    )));
    assert!(!layer_is_well_formed(&leaf_layer(
        vec![node_at(1), node_at(1)],
        &[b"m"]
    )));
}

#[test]
fn a_layer_node_must_be_the_child_its_steps_lead_to() {
    let mut node = node_at(1);
    node.node = observed(7);
    assert!(!layer_is_well_formed(&leaf_layer(vec![node], &[b"m"])));
}

#[test]
fn layer_entries_must_ascend_within_the_spans_bounds() {
    assert!(!layer_is_well_formed(&leaf_layer(
        vec![node_at(1)],
        &[b"n", b"m"]
    )));
    assert!(!layer_is_well_formed(&leaf_layer(
        vec![node_at(1)],
        &[b"a"]
    )));
    assert!(!layer_is_well_formed(&leaf_layer(
        vec![node_at(1)],
        &[b"t"]
    )));
    assert!(!layer_is_well_formed(&layer(
        NodeKind::Internal,
        vec![node_at(1)],
        separator_list(&[b"m", b"u"]).into_untyped()
    )));
    assert!(!layer_is_well_formed(&layer(
        NodeKind::Internal,
        vec![node_at(1)],
        separator_list(&[b"", b"n"]).into_untyped()
    )));
}
