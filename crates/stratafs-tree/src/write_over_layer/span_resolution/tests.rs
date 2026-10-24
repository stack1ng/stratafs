use zerocopy::little_endian::{U64, U128};

use super::*;
use crate::header::{NodeId, VolumeEpoch};
use crate::node::{LeafEntry, ObservedNode};
use crate::write_over_layer::LayerNode;

fn node(id: u128) -> LayerNode<u16> {
    LayerNode {
        node: ObservedNode {
            id: NodeId(U128::new(id)),
            epoch: VolumeEpoch(U64::new(0)),
        },
        steps: SmallVec::new(),
    }
}

fn leaves(keys: &[&[u8]]) -> SlottedList<BytesMut, [u8], u32> {
    let mut list = SlottedList::<BytesMut, LeafEntry<u16>, u32>::empty();
    for (index, key) in keys.iter().enumerate() {
        list.insert_entry(index, key, &b"value"[..]);
    }
    list.into_untyped()
}

fn pulled(
    ids: &[u128],
    keys: &[&[u8]],
    left_pulls: &[usize],
    right_pulls: &[usize],
) -> PulledSpan<u16> {
    PulledSpan {
        span: LayerSpan {
            nodes: ids.iter().map(|&id| node(id)).collect(),
            entries: leaves(keys),
        },
        left_pull_entry_counts: left_pulls.iter().copied().collect(),
        right_pull_entry_counts: right_pulls.iter().copied().collect(),
    }
}

fn ids(span: &LayerSpan<u16>) -> Vec<u128> {
    span.nodes.iter().map(|node| node.node.id.0.get()).collect()
}

fn keys(span: &LayerSpan<u16>) -> Vec<Vec<u8>> {
    let entries = span.entries.clone().into_typed::<LeafEntry<u16>>();
    (0..entries.len())
        .map(|index| {
            entries
                .get(index)
                .unwrap()
                .unwrap()
                .key_and_value()
                .unwrap()
                .0
                .to_vec()
        })
        .collect()
}

#[test]
fn spans_that_both_pulled_the_node_between_them_keep_it_once() {
    let joined = join_overlapping(
        pulled(&[1, 2, 3], &[b"a", b"b", b"g"], &[], &[1]),
        pulled(&[3, 4, 5], &[b"g", b"x", b"y"], &[1], &[]),
    );
    assert_eq!(ids(&joined.span), [1, 2, 3, 4, 5]);
    assert_eq!(keys(&joined.span), [b"a", b"b", b"g", b"x", b"y"]);
}

#[test]
fn a_node_pulled_from_the_next_span_keeps_the_next_spans_entries() {
    let joined = join_overlapping(
        pulled(&[1, 2, 3], &[b"a", b"b", b"stale"], &[], &[1]),
        pulled(&[3, 4], &[b"e", b"f"], &[], &[]),
    );
    assert_eq!(ids(&joined.span), [1, 2, 3, 4]);
    assert_eq!(keys(&joined.span), [b"a", b"b", b"e", b"f"]);
}

#[test]
fn a_node_pulled_from_the_previous_span_keeps_the_previous_spans_entries() {
    let joined = join_overlapping(
        pulled(&[1, 2], &[b"a", b"b"], &[], &[]),
        pulled(&[2, 3, 4], &[b"stale", b"x", b"y"], &[1], &[]),
    );
    assert_eq!(ids(&joined.span), [1, 2, 3, 4]);
    assert_eq!(keys(&joined.span), [b"a", b"b", b"x", b"y"]);
}

#[test]
fn joining_keeps_the_outer_pulls_for_the_next_join() {
    let joined = join_overlapping(
        pulled(&[0, 1, 2, 3], &[b"0", b"a", b"b", b"g"], &[1], &[1]),
        pulled(&[3, 4, 5, 6], &[b"g", b"x", b"y", b"z"], &[1], &[1]),
    );
    assert_eq!(ids(&joined.span), [0, 1, 2, 3, 4, 5, 6]);
    assert_eq!(
        keys(&joined.span),
        [b"0", b"a", b"b", b"g", b"x", b"y", b"z"]
    );
    assert_eq!(&joined.left_pull_entry_counts[..], [1]);
    assert_eq!(&joined.right_pull_entry_counts[..], [1]);
}
