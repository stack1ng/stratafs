use std::ops::Range;

use bytes::BytesMut;
use smallvec::{SmallVec, smallvec};
use stratafs_store::error::StoreError;
use stratafs_store::kv::{KeyValueTransaction, SetRejection};
use stratafs_store::store::Store;
use zerocopy::FromBytes;
use zerocopy::little_endian::U128;

use super::span_mutation_plan::SpanMutationPlan;
use super::span_resolution::resolve_node_relative_first_separator;
use super::{LayerNode, LayerSpan};
use crate::descent::DescentSteps;
use crate::error::TreeFailure;
use crate::header::{NodeId, VolumeHeader};
use crate::key::{Referenced, TreeKey};
use crate::node::{InternalEntry, LEFTMOST_SEPARATOR, LeafEntry, Node, NodeHeader, NodeKind};
use crate::serialized::CorruptValue;
use crate::slotted::{AddressableRange, SlottedList};
use crate::tree::{MetadataRejection, TreeConfig};
use crate::volume::{TreeTransaction, with_tree_failure};
use crate::write_over_layer::LayerNodeMutation::{Create, Delete, Promote, Update};

impl<T, S, A, C> TreeTransaction<'_, T, S, A, C>
where
    T: KeyValueTransaction<Rejection = MetadataRejection<S>>,
    S: Store,
    A: Fn() -> u128 + Send + Sync + 'static,
    C: TreeConfig,
{
    pub(super) fn execute_span_mutation_plans(
        &mut self,
        volume: VolumeHeader,
        kind: NodeKind,
        plans: SmallVec<[SpanMutationPlan<C::Range>; 1]>,
    ) -> Result<
        SmallVec<[LayerSpan<C::Range>; 1]>,
        StoreError<
            SetRejection<MetadataRejection<S>>,
            TreeFailure<T::Failure, MetadataRejection<S>>,
        >,
    > {
        // group plans if they have any common parents
        let parent_of = |node: Option<&LayerNode<C::Range>>| {
            node.and_then(|node| node.steps.last())
                .map(|step| step.node.id)
        };
        let mut groups: SmallVec<[SmallVec<[SpanMutationPlan<C::Range>; 1]>; 1]> = SmallVec::new();
        for plan in plans {
            match groups.last_mut() {
                Some(group)
                    if group
                        .last()
                        .map(|last| parent_of(last.last_existing_node()))
                        == Some(parent_of(plan.first_existing_node())) =>
                {
                    group.push(plan);
                }
                _ => groups.push(smallvec![plan]),
            }
        }
        let mut parent_spans = SmallVec::new();
        for group in groups {
            if let Some(parent_span) = self.execute_plan_group(volume, kind, group)? {
                parent_spans.push(parent_span);
            }
        }
        Ok(parent_spans)
    }

    fn execute_plan_group(
        &mut self,
        volume: VolumeHeader,
        kind: NodeKind,
        group: SmallVec<[SpanMutationPlan<C::Range>; 1]>,
    ) -> Result<
        Option<LayerSpan<C::Range>>,
        StoreError<
            SetRejection<MetadataRejection<S>>,
            TreeFailure<T::Failure, MetadataRejection<S>>,
        >,
    > {
        if group[0].is_root() {
            return self.execute_root_plan_group(volume, kind, group);
        }
        // Phase 1: accumulate the parent span.
        // Take the parents of each plan's existing nodes, and put them and their
        // entries into one parent span, left to right. Each parent's first entry gets its real
        // key back, as everywhere else.
        let mut parent_span = LayerSpan {
            nodes: SmallVec::new(),
            entries: SlottedList::empty(),
        };
        let mut last_parent_offset = 0;
        let mut ranges: SmallVec<[Range<usize>; 1]> = SmallVec::with_capacity(group.len());
        for plan in &group {
            let mut slot_in_parents = |node: &LayerNode<C::Range>| -> Result<usize, CorruptValue> {
                let step = node
                    .steps
                    .last()
                    .expect("a plan below the root has a parent");
                if parent_span
                    .nodes
                    .last()
                    .is_none_or(|parent| parent.node.id != step.node.id)
                {
                    // merge the new parent ndoe into the accumulating parent_span
                    let parent_steps: DescentSteps<C::Range> =
                        node.steps[..node.steps.len() - 1].iter().cloned().collect();
                    let stored_entries = step.entries.clone().into_untyped();
                    let parent_entries = resolve_node_relative_first_separator(
                        NodeKind::Internal,
                        &stored_entries,
                        &parent_steps,
                    )?;
                    last_parent_offset = parent_span.entries.len();
                    parent_span.entries.extend_from_list(&parent_entries);
                    parent_span.nodes.push(LayerNode {
                        node: step.node,
                        steps: parent_steps,
                        stored_entries,
                    });
                }
                Ok(last_parent_offset + step.position)
            };
            // Phase 2: find each plan's range in the parent span's entries.
            // From its first existing node's entry to its last existing node's entry.
            // A parent of a node in the middle joins too, so it is rewritten rather than left pointing at that node.
            let mut nodes = plan.existing_nodes();
            let start = slot_in_parents(
                nodes
                    .next()
                    .expect("a plan below the root changes an existing node"),
            )
            .map_err(corrupt)?;
            let end = nodes
                .try_fold(start, |_, node| slot_in_parents(node))
                .map_err(corrupt)?;
            ranges.push(start..end + 1);
        }

        // Phase 3: execute each plan.
        // - write its nodes, with copy on write;
        // - collect its new parent entries, one (separator, id) per Update and Create, in order;
        // - a Delete adds nothing;
        // - a Promote adds (separator, child) without writing anything.
        let mut replacements: SmallVec<[SlottedList<BytesMut, [u8], u32>; 1]> =
            SmallVec::with_capacity(group.len());
        for plan in group {
            replacements.push(self.execute_span_mutation_plan(volume, kind, plan)?);
        }

        // Phase 4: if nothing changed, return nothing.
        // Nothing changed means only one parent was gathered, and every plan's new entries are
        // identical to its range. With more than one parent, a parent skipped between them is a
        // hole that still has to be dropped.
        let unchanged = parent_span.nodes.len() == 1
            && ranges
                .iter()
                .zip(&replacements)
                .all(|(range, replacement)| {
                    range.len() == replacement.len()
                        && range.clone().zip(0..).all(|(slot, index)| {
                            parent_span.entries.get(slot).and_then(Result::ok)
                                == replacement.get(index).and_then(Result::ok)
                        })
                });
        if unchanged {
            return Ok(None);
        }
        // Phase 5: otherwise, replace each plan's range with its new entries.
        // Work from right to left so positions stay valid.
        for (range, replacement) in ranges.into_iter().zip(replacements).rev() {
            parent_span.entries.splice(range, &replacement);
        }
        Ok(Some(parent_span))
    }

    fn execute_root_plan_group(
        &mut self,
        volume: VolumeHeader,
        kind: NodeKind,
        group: SmallVec<[SpanMutationPlan<C::Range>; 1]>,
    ) -> Result<
        Option<LayerSpan<C::Range>>,
        StoreError<
            SetRejection<MetadataRejection<S>>,
            TreeFailure<T::Failure, MetadataRejection<S>>,
        >,
    > {
        let mut roots = SlottedList::<BytesMut, [u8], u32>::empty();
        for plan in group {
            roots.extend_from_list(&self.execute_span_mutation_plan(volume, kind, plan)?);
        }
        if roots.len() == 0 {
            let empty_root = SpanMutationPlan {
                mutations: smallvec![Create {
                    separator: Box::from(LEFTMOST_SEPARATOR),
                    entries: SlottedList::empty(),
                }],
            };
            roots = self.execute_span_mutation_plan(volume, NodeKind::Leaf, empty_root)?;
        }
        if roots.len() > 1 {
            // if the current root gets planned to have multiple nodes, that means it is getting superseded by a new root, which will be created next iteration
            return Ok(Some(LayerSpan {
                nodes: SmallVec::new(),
                entries: roots,
            }));
        }
        let root = roots
            .into_typed::<InternalEntry>()
            .get(0)
            .expect("one root is left")
            .map_err(corrupt)?
            .child;
        if root != volume.root {
            self.set_volume_header(VolumeHeader {
                root,
                epoch: volume.epoch,
            })?;
            self.ref_deltas.extend([
                (Referenced::Node(root), 1),
                (Referenced::Node(volume.root), -1),
            ]);
        }
        Ok(None)
    }

    // execute_span_mutation_plan applies the plan to the current layer,
    // and returns the entries that it created/updated.
    // It records +1 per new internal pointer and -1 per pointer an owned old node held; a created or cleared node cancels its parent's change.
    fn execute_span_mutation_plan(
        &mut self,
        volume: VolumeHeader,
        kind: NodeKind,
        plan: SpanMutationPlan<C::Range>,
    ) -> Result<
        SlottedList<BytesMut, [u8], u32>,
        StoreError<
            SetRejection<MetadataRejection<S>>,
            TreeFailure<T::Failure, MetadataRejection<S>>,
        >,
    > {
        // A leaf span's old entries are compared with its new ones to count its blobs' references. A copied leaf's
        // original keeps its references.
        if kind == NodeKind::Leaf {
            let old_entries = plan
                .mutations
                .iter()
                .filter_map(|mutation| match mutation {
                    Update { node, .. } | Delete { node } => Some(node),
                    Create { .. } | Promote { .. } => None,
                })
                .flat_map(|node| {
                    let owned = node.node.epoch == volume.epoch;
                    leaf_entries(&node.stored_entries).map(move |entry| Ok((owned, entry?)))
                });
            let new_entries = plan
                .mutations
                .iter()
                .filter_map(|mutation| match mutation {
                    Create { entries, .. } | Update { entries, .. } => Some(entries),
                    Delete { .. } | Promote { .. } => None,
                })
                .flat_map(leaf_entries);
            self.push_content_ref_deltas(old_entries, new_entries)
                .map_err(corrupt)?;
        }
        let mut parent_entries = SlottedList::<BytesMut, InternalEntry, u32>::empty();
        for mutation in plan.mutations {
            match mutation {
                Create { separator, entries } => {
                    let new_node_id = NodeId(U128::new((self.shared.id_allocator)()));
                    self.inner
                        .set(
                            TreeKey::Node(new_node_id).into(),
                            Node::write(
                                &NodeHeader {
                                    kind,
                                    epoch: volume.epoch,
                                },
                                &entries,
                            ),
                        )
                        .map_err(with_tree_failure)?;
                    self.ref_deltas.push((Referenced::Node(new_node_id), -1));
                    if kind == NodeKind::Internal {
                        self.push_child_ref_deltas(&entries, 1).map_err(corrupt)?;
                    }
                    parent_entries.insert_entry(parent_entries.len(), new_node_id, &separator);
                }
                Update {
                    node,
                    separator,
                    entries,
                } => {
                    let target = if node.node.epoch == volume.epoch {
                        node.node.id
                    } else {
                        NodeId(U128::new((self.shared.id_allocator)()))
                    };
                    self.inner
                        .set(
                            TreeKey::Node(target).into(),
                            Node::write(
                                &NodeHeader {
                                    kind,
                                    epoch: volume.epoch,
                                },
                                &entries,
                            ),
                        )
                        .map_err(with_tree_failure)?;
                    if target != node.node.id {
                        self.ref_deltas.push((Referenced::Node(target), -1));
                    } else if kind == NodeKind::Internal {
                        self.push_child_ref_deltas(&node.stored_entries, -1)
                            .map_err(corrupt)?;
                    }
                    if kind == NodeKind::Internal {
                        self.push_child_ref_deltas(&entries, 1).map_err(corrupt)?;
                    }
                    parent_entries.insert_entry(parent_entries.len(), target, &separator);
                }
                Delete { node } => {
                    if node.node.epoch == volume.epoch {
                        self.inner
                            .clear(TreeKey::Node(node.node.id).into())
                            .map_err(TreeFailure::Store)
                            .map_err(StoreError::Failed)?;
                        self.ref_deltas.push((Referenced::Node(node.node.id), 1));
                        if kind == NodeKind::Internal {
                            self.push_child_ref_deltas(&node.stored_entries, -1)
                                .map_err(corrupt)?;
                        }
                    }
                }
                Promote { separator, child } => {
                    parent_entries.insert_entry(parent_entries.len(), child, &separator);
                }
            }
        }
        Ok(parent_entries.into_untyped())
    }
}

fn corrupt<F, R>(error: CorruptValue) -> StoreError<SetRejection<R>, TreeFailure<F, R>> {
    StoreError::Failed(TreeFailure::Corrupt(error))
}

// leaf_entries reads each of a leaf's untyped entries as its key and value.
fn leaf_entries<B: AsRef<[u8]>, R: AddressableRange>(
    entries: &SlottedList<B, [u8], R>,
) -> impl Iterator<Item = Result<(&[u8], &[u8]), CorruptValue>> {
    (0..entries.len()).map(|index| {
        let entry = entries.get(index).expect("the index is in the leaf")?;
        LeafEntry::<R>::ref_from_bytes(entry)
            .map_err(|_| CorruptValue::WrongLength)?
            .key_and_value()
    })
}
