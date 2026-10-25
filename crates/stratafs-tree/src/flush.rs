use std::cmp::Ordering;
use std::mem;
use std::ops::Bound::{Excluded, Included, Unbounded};

use bytes::{Bytes, BytesMut};
use equivalent::{Comparable, Equivalent};
use smallvec::{SmallVec, smallvec};
use stratafs_range_bounds_map::RangeBoundsMap;
use stratafs_store::error::StoreError;
use stratafs_store::kv::{KeyValueStore, KeyValueTransaction, SetRejection};

use crate::descent::DescentPath;
use crate::error::TreeFailure;
use crate::node::{LeafEntry, NodeKind};
use crate::slotted::{AddressableRange, SlottedList};
use crate::tree::TreeConfig;
use crate::volume::TreeTransaction;
use crate::write_over_layer::packing::PackingStrategy;
use crate::write_over_layer::{Layer, LayerNode, LayerSpan};

// FlushTarget is a point to descend to while flushing: a key, or the end of the key space,
// which is where a clear with no end stops.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum FlushTarget {
    Key(Bytes),
    AfterEveryKey,
}

impl Equivalent<FlushTarget> for [u8] {
    fn equivalent(&self, target: &FlushTarget) -> bool {
        self.compare(target) == Ordering::Equal
    }
}

impl Comparable<FlushTarget> for [u8] {
    fn compare(&self, target: &FlushTarget) -> Ordering {
        match target {
            FlushTarget::Key(key) => self.cmp(&key[..]),
            FlushTarget::AfterEveryKey => Ordering::Less,
        }
    }
}

// FlushSpan is a run of leaves the buffered writes touch, and the keys set in them.
// Leaves skipped between two of its leaves lie inside a cleared range.
struct FlushSpan<'flush, R: AddressableRange> {
    leaves: SmallVec<[&'flush DescentPath<R>; 2]>,
    sets: SmallVec<[(&'flush Bytes, &'flush Bytes); 4]>,
}

impl<T, S, A, C> TreeTransaction<'_, T, S, A, C>
where
    T: KeyValueTransaction<Rejection = S::Rejection>,
    S: KeyValueStore,
    A: Fn() -> u128 + Send + Sync + 'static,
    C: TreeConfig,
{
    // flush writes the buffered writes into the tree, it costs only one descent pass (by parallelizing reads) and one ascent pass at runtime.
    pub(crate) async fn flush(
        &mut self,
    ) -> Result<(), StoreError<SetRejection<S::Rejection>, TreeFailure<T::Failure, S::Rejection>>>
    {
        // Phase 1: take the buffer, leaving the transaction with an empty one.
        let buffered = mem::take(&mut self.buffered_delta);
        if buffered.is_empty() {
            return Ok(());
        }
        // Phase 2: collect the targets, in buffer order, which is sorted.
        // Each buffered range descends to the leaf of its start and the leaf of its end.
        // A set is a range of one key, so both of its targets are its key.
        let targets: SmallVec<[FlushTarget; 2]> = buffered
            .iter()
            .flat_map(|((start, end), _)| {
                let first = match start {
                    Included(key) | Excluded(key) => FlushTarget::Key(key.clone()),
                    Unbounded => FlushTarget::Key(Bytes::new()),
                };
                let last: FlushTarget = match end {
                    Included(key) | Excluded(key) => FlushTarget::Key(key.clone()),
                    Unbounded => FlushTarget::AfterEveryKey,
                };
                [first, last]
            })
            .collect();
        // Phase 3: descend to every target at once, reading each level's nodes in parallel.
        // The paths come back in target order, two per buffered range.
        let paths = self
            .descend_many(&targets)
            .await
            .map_err(StoreError::Failed)?;
        let volume = paths.first().expect("a buffered range has paths").volume;

        // Phase 4: group the touched leaves into spans.
        // A range joins the current span when it starts in the span's last leaf, otherwise it starts a new one.
        // A range ending in a later leaf adds that leaf, and the leaves skipped on the way are the hole it clears.
        let mut flush_spans: SmallVec<[FlushSpan<C::Range>; 1]> = SmallVec::new();
        let (ends, _) = paths.as_chunks::<2>();
        for (((start, _), write), [first, last]) in buffered.iter().zip(ends) {
            match flush_spans.last_mut() {
                Some(span) if same_leaf(span.leaves.last(), first) => {}
                _ => flush_spans.push(FlushSpan {
                    leaves: smallvec![first],
                    sets: SmallVec::new(),
                }),
            }
            let span = flush_spans.last_mut().expect("a span was just ensured");
            if !same_leaf(span.leaves.last(), last) {
                span.leaves.push(last);
            }
            if let Some(value) = write {
                let Included(key) = start else {
                    unreachable!("a buffered set covers exactly its key")
                };
                span.sets.push((key, value));
            }
        }

        // Phase 5: give each span its new entries, then write the whole leaf layer
        // and every layer above it in one pass.
        let spans = flush_spans
            .iter()
            .map(|span| Self::layer_span(span, &buffered))
            .collect::<Result<_, _>>()
            .map_err(|corrupt| StoreError::Failed(TreeFailure::Corrupt(corrupt)))?;
        self.write_over_layer(
            volume,
            Layer {
                kind: NodeKind::Leaf,
                spans,
            },
            PackingStrategy::Balanced,
        )
        .await
    }

    // layer_span merges a span's stored entries with its buffered sets, in key order,
    // leaving out every stored key the buffer set or cleared.
    fn layer_span(
        span: &FlushSpan<C::Range>,
        buffered: &RangeBoundsMap<Bytes, Option<Bytes>>,
    ) -> Result<LayerSpan<C::Range>, crate::serialized::CorruptValue> {
        let mut entries = SlottedList::<BytesMut, LeafEntry<C::Range>, u32>::empty();
        let mut sets = span.sets.iter().peekable();
        for leaf in &span.leaves {
            let stored = &leaf.destination.entries;
            for index in 0..stored.len() {
                let (key, value) = stored
                    .get(index)
                    .expect("the index is in the leaf")?
                    .key_and_value()?;
                while let Some((set_key, set_value)) =
                    sets.next_if(|(set_key, _)| &set_key[..] < key)
                {
                    entries.insert_entry(entries.len(), set_key, &set_value[..]);
                }
                if buffered.get(&stored.get_ref().slice_ref(key)).is_none() {
                    entries.insert_entry(entries.len(), key, value);
                }
            }
        }
        for (set_key, set_value) in sets {
            entries.insert_entry(entries.len(), set_key, &set_value[..]);
        }
        Ok(LayerSpan {
            nodes: span
                .leaves
                .iter()
                .map(|leaf| LayerNode {
                    node: leaf.destination.node,
                    steps: leaf.steps.clone(),
                })
                .collect(),
            entries: entries.into_untyped(),
        })
    }
}

fn same_leaf<R: AddressableRange>(leaf: Option<&&DescentPath<R>>, other: &DescentPath<R>) -> bool {
    leaf.is_some_and(|leaf| leaf.destination.node.id == other.destination.node.id)
}
