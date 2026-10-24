use bytes::Bytes;
use stratafs_store::kv::{KeyValueStore, KeyValueTransaction};

use super::{LayerNode, LayerSpan};
use crate::descent::{DescentStep, DescentSteps};
use crate::error::TreeFailure;
use crate::key::TreeKey;
use crate::node::{Node, ObservedNode};
use crate::serialized::CorruptValue;
use crate::slotted::SlottedList;
use crate::tree::TreeConfig;
use crate::volume::TreeTransaction;

#[derive(Clone, Copy)]
pub(super) enum Side {
    Left,
    Right,
}

pub(super) enum Neighbor<I: Future, E: Future> {
    // the neighbor sits within the same parent that we already have in memory from the descent
    Immediate(I),
    // the neighbor is either a cousin that needs to be traversed to, or does not exist
    MaybeExtended(E),
}

pub(super) struct SpanNeighbors<I: Future, E: Future> {
    pub(super) left: Neighbor<I, E>,
    pub(super) right: Neighbor<I, E>,
}

impl<T, S, A, C> TreeTransaction<'_, T, S, A, C>
where
    T: KeyValueTransaction<Rejection = S::Rejection>,
    S: KeyValueStore,
    A: Fn() -> u128 + Send + Sync + 'static,
    C: TreeConfig,
{
    pub(super) fn find_span_neighbors<'span>(
        &'span self,
        span: &'span LayerSpan<C::Range>,
    ) -> Option<
        SpanNeighbors<
            impl Future<
                Output = Result<
                    (LayerNode<C::Range>, SlottedList<Bytes, [u8], C::Range>),
                    TreeFailure<T::Failure, S::Rejection>,
                >,
            >,
            impl Future<
                Output = Result<
                    Option<(LayerNode<C::Range>, SlottedList<Bytes, [u8], C::Range>)>,
                    TreeFailure<T::Failure, S::Rejection>,
                >,
            >,
        >,
    > {
        let neighbor = |node: &'span LayerNode<C::Range>, side: Side| {
            let step = node.steps.last()?; // no steps: this is the root, so no neighbour
            let slot = match side {
                Side::Left => step.position.checked_sub(1),
                Side::Right => Some(step.position + 1).filter(|&at| at < step.entries.len()),
            };
            Some(match slot {
                Some(at) => {
                    let mut path = node.steps.clone();
                    path.last_mut()
                        .expect("the node has a parent step")
                        .position = at;
                    Neighbor::Immediate(self.read_neighbor(path))
                }
                None => Neighbor::MaybeExtended(async move {
                    match self.find_cousin_path(node, side).await? {
                        Some(path) => self.read_neighbor(path).await.map(Some),
                        None => Ok(None),
                    }
                }),
            })
        };
        Some(SpanNeighbors {
            left: neighbor(span.nodes.first(), Side::Left)?,
            right: neighbor(span.nodes.last(), Side::Right)?,
        })
    }

    async fn read_neighbor(
        &self,
        steps: DescentSteps<C::Range>,
    ) -> Result<
        (LayerNode<C::Range>, SlottedList<Bytes, [u8], C::Range>),
        TreeFailure<T::Failure, S::Rejection>,
    > {
        let id = {
            let step = steps
                .last()
                .expect("a neighbour path ends at the neighbour's parent");
            step.entries
                .get(step.position)
                .expect("the neighbour's slot is in its parent")
                .map_err(TreeFailure::Corrupt)?
                .child
        };
        let stored = self
            .inner
            .get(TreeKey::Node(id).into())
            .await
            .map_err(TreeFailure::Store)?
            .ok_or(TreeFailure::Corrupt(CorruptValue::MissingNode))?;
        let (epoch, node) = Node::<Bytes, C::Range>::read(stored).map_err(TreeFailure::Corrupt)?;
        let entries = match node {
            Node::Leaf(entries) => entries.into_untyped(),
            Node::Internal(entries) => entries.into_untyped(),
        };
        Ok((
            LayerNode {
                node: ObservedNode { id, epoch },
                steps,
            },
            entries,
        ))
    }

    async fn find_cousin_path(
        &self,
        node: &LayerNode<C::Range>,
        side: Side,
    ) -> Result<Option<DescentSteps<C::Range>>, TreeFailure<T::Failure, S::Rejection>> {
        let Some(turn) = node.steps.iter().rposition(|step| match side {
            Side::Left => step.position > 0,
            Side::Right => step.position + 1 < step.entries.len(),
        }) else {
            return Ok(None);
        };
        let mut steps: DescentSteps<C::Range> = node.steps[..=turn].iter().cloned().collect();
        let turning_step = steps.last_mut().expect("the path keeps the turning step");
        turning_step.position = match side {
            Side::Left => turning_step.position - 1,
            Side::Right => turning_step.position + 1,
        };
        let mut child = turning_step
            .entries
            .get(turning_step.position)
            .expect("the turning position is within the parent")
            .map_err(TreeFailure::Corrupt)?
            .child;
        while steps.len() < node.steps.len() {
            let id = child;
            let stored = self
                .inner
                .get(TreeKey::Node(id).into())
                .await
                .map_err(TreeFailure::Store)?
                .ok_or(TreeFailure::Corrupt(CorruptValue::MissingNode))?;
            let (epoch, read) =
                Node::<Bytes, C::Range>::read(stored).map_err(TreeFailure::Corrupt)?;
            let Node::Internal(entries) = read else {
                return Err(TreeFailure::Corrupt(CorruptValue::UnevenLeafDepth));
            };
            let position = match side {
                Side::Left => entries
                    .len()
                    .checked_sub(1)
                    .ok_or(TreeFailure::Corrupt(CorruptValue::EmptyInternalNode))?,
                Side::Right => 0,
            };
            child = entries
                .get(position)
                .ok_or(TreeFailure::Corrupt(CorruptValue::EmptyInternalNode))?
                .map_err(TreeFailure::Corrupt)?
                .child;
            steps.push(DescentStep {
                node: ObservedNode { id, epoch },
                entries,
                position,
            });
        }
        Ok(Some(steps))
    }
}
