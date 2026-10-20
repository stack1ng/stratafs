use crate::copy_on_write::LayerMutationPlanAction;
use crate::header::NodeId;
use crate::node::{InternalEntry, LeafEntry};
use crate::slotted::{AddressableRange, SlottedList};

pub(crate) fn plan_has_at_most_one_action_per_node<R: AddressableRange>(
    plan: &[Option<LayerMutationPlanAction<R>>],
) -> bool {
    let observed = || {
        plan.iter().flatten().filter_map(|action| match action {
            LayerMutationPlanAction::Update { node, .. }
            | LayerMutationPlanAction::Delete { node, .. } => Some(node.id),
            LayerMutationPlanAction::Create { .. } => None,
        })
    };
    observed()
        .enumerate()
        .all(|(index, id): (usize, NodeId)| observed().skip(index + 1).all(|other| other != id))
}

pub(crate) fn leaf_keys_ascend<B: AsRef<[u8]>, R: AddressableRange>(
    entries: &SlottedList<B, LeafEntry<R>, R>,
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

#[cfg(test)]
mod tests;
