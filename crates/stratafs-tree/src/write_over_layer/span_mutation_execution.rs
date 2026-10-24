use std::ops::Range;

use bytes::BytesMut;
use smallvec::{SmallVec, smallvec};
use stratafs_store::error::StoreError;
use stratafs_store::kv::{KeyValueStore, KeyValueTransaction, SetRejection};
use vec1::smallvec_v1::SmallVec1;
use zerocopy::little_endian::U128;

use super::span_mutation_plan::SpanMutationPlan;
use super::span_resolution::resolve_node_relative_first_separator;
use super::{LayerNode, LayerSpan};
use crate::descent::DescentSteps;
use crate::error::TreeFailure;
use crate::header::{NodeId, VolumeHeader};
use crate::key::TreeKey;
use crate::node::{InternalEntry, Node, NodeHeader, NodeKind};
use crate::serialized::CorruptValue;
use crate::slotted::{AddressableRange, SlottedList};
use crate::tree::TreeConfig;
use crate::volume::{TreeTransaction, with_tree_failure};
use crate::write_over_layer::LayerNodeMutation::{Create, Delete, Update};

impl<T, S, A, C> TreeTransaction<'_, T, S, A, C>
where
    T: KeyValueTransaction<Rejection = S::Rejection>,
    S: KeyValueStore,
    A: Fn() -> u128 + Send + Sync + 'static,
    C: TreeConfig,
{
    pub(super) async fn execute_span_mutation_plans(
        &mut self,
        volume: VolumeHeader,
        kind: NodeKind,
        plans: SmallVec<[SpanMutationPlan<C::Range>; 1]>,
    ) -> Result<
        SmallVec<[LayerSpan<C::Range>; 1]>,
        StoreError<SetRejection<S::Rejection>, TreeFailure<T::Failure, S::Rejection>>,
    > {
        // group plans if they have any common parents
        let parent_of = |node: &LayerNode<C::Range>| node.steps.last().map(|step| step.node.id);
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
            if let Some(parent_span) = self.execute_plan_group(volume, kind, group).await? {
                parent_spans.push(parent_span);
            }
        }
        Ok(parent_spans)
    }

    async fn execute_plan_group(
        &mut self,
        volume: VolumeHeader,
        kind: NodeKind,
        group: SmallVec<[SpanMutationPlan<C::Range>; 1]>,
    ) -> Result<
        Option<LayerSpan<C::Range>>,
        StoreError<SetRejection<S::Rejection>, TreeFailure<T::Failure, S::Rejection>>,
    > {
        if group[0].first_existing_node().steps.is_empty() {
            todo!("write the root's new children into the volume header");
        }
        // Phase 1: accumulate the parent span.
        // Take the parents of each plan's first and last existing node, and put them and their
        // entries into one parent span, left to right. Each parent's first entry gets its real
        // key back, as everywhere else.
        let mut parent_span = span_of_parent(group[0].first_existing_node()).map_err(corrupt)?;
        let mut last_parent_offset = 0;
        let mut ranges: SmallVec<[Range<usize>; 1]> = SmallVec::with_capacity(group.len());
        for plan in &group {
            let mut slot_in_parents = |node: &LayerNode<C::Range>| -> Result<usize, CorruptValue> {
                let step = node
                    .steps
                    .last()
                    .expect("a plan below the root has a parent");
                if parent_span.nodes.last().node.id != step.node.id {
                    // merge the new parent ndoe into the accumulating parent_span
                    let next_parent = span_of_parent(node)?;
                    last_parent_offset = parent_span.entries.len();
                    parent_span.entries.extend_from_list(&next_parent.entries);
                    parent_span.nodes.extend(next_parent.nodes);
                }
                Ok(last_parent_offset + step.position)
            };
            // Phase 2: find each plan's range in the parent span's entries.
            // From its first existing node's entry to its last existing node's entry.
            let start = slot_in_parents(plan.first_existing_node()).map_err(corrupt)?;
            let end = slot_in_parents(plan.last_existing_node()).map_err(corrupt)?;
            ranges.push(start..end + 1);
        }

        // Phase 3: execute each plan.
        // - write its nodes, with copy on write;
        // - collect its new parent entries, one (separator, id) per Update and Create, in order;
        // - a Delete adds nothing.
        let mut replacements: SmallVec<[SlottedList<BytesMut, [u8], C::Range>; 1]> =
            SmallVec::with_capacity(group.len());
        for plan in group {
            replacements.push(self.execute_span_mutation_plan(volume, kind, plan).await?);
        }

        // Phase 4: if nothing changed, return nothing.
        // Nothing changed means every plan's new entries are identical to its range.
        let unchanged = ranges
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

    // execute_span_mutation_plan applies the plan to the current layer,
    // and returns the entries that it created/updated
    async fn execute_span_mutation_plan(
        &mut self,
        volume: VolumeHeader,
        kind: NodeKind,
        plan: SpanMutationPlan<C::Range>,
    ) -> Result<
        SlottedList<BytesMut, [u8], C::Range>,
        StoreError<SetRejection<S::Rejection>, TreeFailure<T::Failure, S::Rejection>>,
    > {
        let mut parent_entries = SlottedList::<BytesMut, InternalEntry, C::Range>::empty();
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
                        .await
                        .map_err(with_tree_failure)?;
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
                        .await
                        .map_err(with_tree_failure)?;
                    parent_entries.insert_entry(parent_entries.len(), target, &separator);
                }
                Delete { node } => {
                    if node.node.epoch == volume.epoch {
                        self.inner
                            .clear(TreeKey::Node(node.node.id).into())
                            .await
                            .map_err(TreeFailure::Store)
                            .map_err(StoreError::Failed)?;
                    }
                }
            }
        }
        Ok(parent_entries.into_untyped())
    }
}

fn corrupt<F, R>(error: CorruptValue) -> StoreError<SetRejection<R>, TreeFailure<F, R>> {
    StoreError::Failed(TreeFailure::Corrupt(error))
}

fn span_of_parent<R: AddressableRange>(node: &LayerNode<R>) -> Result<LayerSpan<R>, CorruptValue> {
    let step = node
        .steps
        .last()
        .expect("a plan below the root has a parent");
    let steps: DescentSteps<R> = node.steps[..node.steps.len() - 1].iter().cloned().collect();
    let entries = resolve_node_relative_first_separator(
        NodeKind::Internal,
        SlottedList::from(step.entries.clone().into_untyped()),
        &steps,
    )?;
    Ok(LayerSpan {
        nodes: SmallVec1::new(LayerNode {
            node: step.node,
            steps,
        }),
        entries,
    })
}
