use std::cmp::Ordering;

use crate::descent::DescentStep;
use crate::node::{InternalEntry, LeafEntry, NodeKind};
use crate::slotted::{AddressableRange, SlottedList};
use crate::write_over_layer::{Layer, LayerNode, LayerSpan};

pub(crate) fn leaf_keys_ascend<B: AsRef<[u8]>, K: AddressableRange, R: AddressableRange>(
    entries: &SlottedList<B, LeafEntry<K>, R>,
) -> bool {
    let mut previous: Option<&[u8]> = None;
    for index in 0..entries.len() {
        let Some(Ok(entry)) = entries.get(index) else {
            continue;
        };
        let Ok((key, _)) = entry.key_and_value() else {
            continue;
        };
        if previous.is_some_and(|previous| previous >= key) {
            return false;
        }
        previous = Some(key);
    }
    true
}

pub(crate) fn separator_keys_ascend<B: AsRef<[u8]>, R: AddressableRange>(
    entries: &SlottedList<B, InternalEntry, R>,
) -> bool {
    let mut previous: Option<&[u8]> = None;
    for index in 0..entries.len() {
        let Some(Ok(entry)) = entries.get(index) else {
            continue;
        };
        if previous.is_some_and(|previous| previous >= &entry.key) {
            return false;
        }
        previous = Some(&entry.key);
    }
    true
}

pub(crate) fn layer_is_well_formed<R: AddressableRange>(layer: &Layer<R>) -> bool {
    spans_are_ordered(&layer.spans)
        && layer
            .spans
            .iter()
            .all(|span| span_entries_ascend_within_bounds(layer.kind, span))
}

pub(crate) fn spans_are_ordered<R: AddressableRange>(spans: &[LayerSpan<R>]) -> bool {
    nodes_are_ordered(spans.iter().flat_map(|span| span.nodes.iter()))
}

pub(crate) fn span_is_well_formed<R: AddressableRange>(
    kind: NodeKind,
    span: &LayerSpan<R>,
) -> bool {
    nodes_are_ordered(span.nodes.iter()) && span_entries_ascend_within_bounds(kind, span)
}

fn nodes_are_ordered<'a, R: AddressableRange + 'a>(
    mut nodes: impl Iterator<Item = &'a LayerNode<R>>,
) -> bool {
    let Some(first) = nodes.next() else {
        return true;
    };
    if !steps_lead_to(first) {
        return false;
    }
    let mut previous = first;
    for node in nodes {
        if node.steps.len() != first.steps.len()
            || !steps_lead_to(node)
            || !precedes(previous, node)
        {
            return false;
        }
        previous = node;
    }
    true
}

fn steps_lead_to<R: AddressableRange>(node: &LayerNode<R>) -> bool {
    node.steps.last().is_none_or(|step| {
        matches!(step.entries.get(step.position), Some(Ok(entry)) if entry.child == node.node.id)
    })
}

fn precedes<R>(earlier: &LayerNode<R>, later: &LayerNode<R>) -> bool {
    for (earlier_step, later_step) in earlier.steps.iter().zip(&later.steps) {
        if earlier_step.node.id != later_step.node.id {
            return false;
        }
        match earlier_step.position.cmp(&later_step.position) {
            Ordering::Less => return true,
            Ordering::Greater => return false,
            Ordering::Equal => {}
        }
    }
    false
}

fn span_entries_ascend_within_bounds<R: AddressableRange>(
    kind: NodeKind,
    span: &LayerSpan<R>,
) -> bool {
    let bytes = &span.entries.get_ref()[..];
    match kind {
        NodeKind::Leaf => {
            let Ok(entries) = SlottedList::<&[u8], LeafEntry<R>, u32>::new(bytes) else {
                return false;
            };
            let key = |index: usize| {
                entries
                    .get(index)
                    .and_then(Result::ok)
                    .and_then(|entry| entry.key_and_value().ok())
                    .map(|(key, _)| key)
            };
            leaf_keys_ascend(&entries)
                && keys_within_bounds(span, key(0), entries.len().checked_sub(1).and_then(key))
        }
        NodeKind::Internal => {
            let Ok(entries) = SlottedList::<&[u8], InternalEntry, u32>::new(bytes) else {
                return false;
            };
            let key = |index: usize| {
                entries
                    .get(index)
                    .and_then(Result::ok)
                    .map(|entry| &entry.key)
            };
            separator_keys_ascend(&entries)
                && keys_within_bounds(span, key(0), entries.len().checked_sub(1).and_then(key))
        }
    }
}

fn keys_within_bounds<R: AddressableRange>(
    span: &LayerSpan<R>,
    first: Option<&[u8]>,
    last: Option<&[u8]>,
) -> bool {
    let lower = span
        .nodes
        .first()
        .and_then(|node| node.steps.last())
        .and_then(|step| separator_at(step, step.position));
    let upper = span
        .nodes
        .last()
        .and_then(|node| node.steps.last())
        .and_then(|step| separator_at(step, step.position + 1));
    first.zip(lower).is_none_or(|(first, lower)| first >= lower)
        && last.zip(upper).is_none_or(|(last, upper)| last < upper)
}

fn separator_at<R: AddressableRange>(
    step: &DescentStep<R, InternalEntry, usize>,
    index: usize,
) -> Option<&[u8]> {
    step.entries.get(index)?.ok().map(|entry| &entry.key)
}

#[cfg(test)]
mod tests;
