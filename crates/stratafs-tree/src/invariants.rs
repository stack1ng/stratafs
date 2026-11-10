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

// node_ref_counts_match_parents walks every volume's tree, and every node a pending job names,
// since a node's pointers count until the garbage collector deletes it. It checks that each walked node's
// stored count, where a missing count is 1, minus its pending jobs equals its parent pointers plus the
// headers whose root it is, that every stored node is walked, and that no count is stored for a node it
// did not walk.
#[cfg(test)]
pub(crate) async fn node_ref_counts_match_parents<T, R>(transaction: &T) -> bool
where
    T: stratafs_store::kv::ReadKeyValueTransaction,
    R: AddressableRange,
{
    use std::collections::{HashMap, HashSet};

    use bytes::Bytes;
    use zerocopy::FromBytes;
    use zerocopy::little_endian::{U64, U128};

    use crate::header::{JobId, NodeId, TreeContentId, TreeVolumeId, VolumeHeader};
    use crate::key::{Referenced, TreeKey};
    use crate::node::Node;

    let headers = transaction
        .get_range(
            Bytes::from(TreeKey::VolumeHeader(TreeVolumeId(0)))
                ..=Bytes::from(TreeKey::VolumeHeader(TreeVolumeId(u128::MAX))),
            usize::MAX,
        )
        .await
        .expect("the store reads the volume headers");
    let stored_jobs = transaction
        .get_range(
            Bytes::from(TreeKey::Dereference(
                TreeVolumeId(0),
                Referenced::Node(NodeId(U128::new(0))),
                JobId(0),
            ))
                ..=Bytes::from(TreeKey::Dereference(
                    TreeVolumeId(u128::MAX),
                    Referenced::Content(TreeContentId(u128::MAX)),
                    JobId(u128::MAX),
                )),
            usize::MAX,
        )
        .await
        .expect("the store reads the jobs");
    let mut jobs = HashMap::new();
    let mut visited = HashSet::new();
    let mut unvisited = Vec::new();
    for (key, _) in stored_jobs {
        let node = match storekey::decode(&key[..]) {
            Ok(TreeKey::Dereference(_, Referenced::Node(node), _)) => node,
            Ok(TreeKey::Dereference(_, Referenced::Content(_), _)) => continue,
            _ => panic!("a stored job key is valid"),
        };
        *jobs.entry(node.0.get()).or_insert(0) += 1;
        if visited.insert(node.0.get()) {
            unvisited.push(node);
        }
    }
    let mut parents = HashMap::new();
    let mut point_at = |child: NodeId, unvisited: &mut Vec<NodeId>| {
        *parents.entry(child.0.get()).or_insert(0) += 1;
        if visited.insert(child.0.get()) {
            unvisited.push(child);
        }
    };
    for (_, header) in headers {
        let header = VolumeHeader::view(&header).expect("a stored header is valid");
        point_at(header.root, &mut unvisited);
    }
    while let Some(id) = unvisited.pop() {
        let stored = transaction
            .get(TreeKey::Node(id).into())
            .await
            .expect("the store reads the node")
            .expect("a node a parent or a job points at is stored");
        let (_, node) = Node::<Bytes, R>::read(stored).expect("a stored node is valid");
        if let Node::Internal(entries) = node {
            for index in 0..entries.len() {
                let entry = entries.get(index).expect("the index is in the node");
                point_at(
                    entry.expect("a stored entry is valid").child,
                    &mut unvisited,
                );
            }
        }
    }
    let visited_nodes = visited.len();
    let mut visited_counts = 0;
    for id in visited {
        let stored = transaction
            .get(TreeKey::RefCount(Referenced::Node(NodeId(U128::new(id)))).into())
            .await
            .expect("the store reads the count");
        visited_counts += usize::from(stored.is_some());
        let count = stored.map_or(1, |stored| {
            U64::read_from_bytes(&stored)
                .expect("a stored count is a U64")
                .get()
        });
        let parents = parents.get(&id).copied().unwrap_or(0);
        let jobs = jobs.get(&id).copied().unwrap_or(0);
        if count != parents + jobs {
            return false;
        }
    }
    let stored_counts = transaction
        .get_range(
            Bytes::from(TreeKey::RefCount(Referenced::Node(NodeId(U128::new(0)))))
                ..=Bytes::from(TreeKey::RefCount(Referenced::Node(NodeId(U128::new(
                    u128::MAX,
                ))))),
            usize::MAX,
        )
        .await
        .expect("the store reads the counts")
        .count();
    let stored_nodes = transaction
        .get_range(
            Bytes::from(TreeKey::Node(NodeId(U128::new(0))))
                ..=Bytes::from(TreeKey::Node(NodeId(U128::new(u128::MAX)))),
            usize::MAX,
        )
        .await
        .expect("the store reads the nodes")
        .count();
    stored_nodes == visited_nodes && stored_counts == visited_counts
}

// content_ref_counts_match_leaves checks that each blob's stored count, where a missing count is 1, equals the entries
// of every stored leaf that reference it plus its pending jobs, that no count is stored for a blob with neither, and
// that a blob waiting to be deleted has none of the three.
#[cfg(test)]
pub(crate) async fn content_ref_counts_match_leaves<T, C>(transaction: &T) -> bool
where
    T: stratafs_store::kv::ReadKeyValueTransaction,
    C: crate::tree::TreeConfig,
{
    use std::collections::HashMap;

    use bytes::Bytes;
    use zerocopy::FromBytes;
    use zerocopy::little_endian::{U64, U128};

    use crate::header::{JobId, NodeId, TreeContentId, TreeVolumeId};
    use crate::key::{Referenced, TreeKey};
    use crate::node::Node;

    let mut references: HashMap<u128, u64> = HashMap::new();
    let nodes = transaction
        .get_range(
            Bytes::from(TreeKey::Node(NodeId(U128::new(0))))
                ..=Bytes::from(TreeKey::Node(NodeId(U128::new(u128::MAX)))),
            usize::MAX,
        )
        .await
        .expect("the store reads the nodes");
    for (_, stored) in nodes {
        let (_, node) = Node::<Bytes, C::Range>::read(stored).expect("a stored node is valid");
        if let Node::Leaf(entries) = node {
            for index in 0..entries.len() {
                let entry = entries.get(index).expect("the index is in the node");
                let (key, value) = entry
                    .and_then(|entry| entry.key_and_value())
                    .expect("a stored entry is valid");
                if let Some(reference) = C::referenced_content(key, value) {
                    *references.entry(reference.id().0).or_insert(0) += 1;
                }
            }
        }
    }
    let jobs = transaction
        .get_range(
            Bytes::from(TreeKey::Dereference(
                TreeVolumeId(0),
                Referenced::Node(NodeId(U128::new(0))),
                JobId(0),
            ))
                ..=Bytes::from(TreeKey::Dereference(
                    TreeVolumeId(u128::MAX),
                    Referenced::Content(TreeContentId(u128::MAX)),
                    JobId(u128::MAX),
                )),
            usize::MAX,
        )
        .await
        .expect("the store reads the content jobs");
    for (key, _) in jobs {
        let id = match storekey::decode(&key[..]) {
            Ok(TreeKey::Dereference(_, Referenced::Content(id), _)) => id,
            Ok(TreeKey::Dereference(_, Referenced::Node(_), _)) => continue,
            _ => panic!("a stored job key is valid"),
        };
        *references.entry(id.0).or_insert(0) += 1;
    }
    let stored_counts = transaction
        .get_range(
            Bytes::from(TreeKey::RefCount(Referenced::Content(TreeContentId(0))))
                ..=Bytes::from(TreeKey::RefCount(Referenced::Content(TreeContentId(
                    u128::MAX,
                )))),
            usize::MAX,
        )
        .await
        .expect("the store reads the content counts");
    let mut counts = HashMap::new();
    for (key, count) in stored_counts {
        let Ok(TreeKey::RefCount(Referenced::Content(id))) = storekey::decode(&key[..]) else {
            panic!("a stored content count key is valid");
        };
        let count = U64::read_from_bytes(&count).expect("a stored count is a U64");
        counts.insert(id.0, count.get());
    }
    let deletions = transaction
        .get_range(
            Bytes::from(TreeKey::ContentDeletion(TreeVolumeId(0), TreeContentId(0)))
                ..=Bytes::from(TreeKey::ContentDeletion(
                    TreeVolumeId(u128::MAX),
                    TreeContentId(u128::MAX),
                )),
            usize::MAX,
        )
        .await
        .expect("the store reads the content deletions");
    for (key, _) in deletions {
        let Ok(TreeKey::ContentDeletion(_, id)) = storekey::decode(&key[..]) else {
            panic!("a stored content deletion key is valid");
        };
        if references.contains_key(&id.0) || counts.contains_key(&id.0) {
            return false;
        }
    }
    counts.keys().all(|id| references.contains_key(id))
        && references
            .iter()
            .all(|(id, references)| counts.get(id).copied().unwrap_or(1) == *references)
}

#[cfg(test)]
mod tests;
