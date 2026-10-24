use std::ops::Range;

use smallvec::{SmallVec, smallvec};

#[derive(Clone, Copy)]
pub(crate) enum PackingStrategy {
    Balanced,
    LeftPacked,
}

impl PackingStrategy {
    pub(super) fn boundaries(
        self,
        entry_count: usize,
        node_size: impl Fn(Range<usize>) -> usize,
        min_node_size: usize,
        max_node_size: usize,
    ) -> SmallVec<[usize; 4]> {
        let left_packed = left_packed(entry_count, &node_size, min_node_size, max_node_size);
        match self {
            PackingStrategy::LeftPacked => left_packed,
            PackingStrategy::Balanced => balanced(
                left_packed,
                entry_count,
                &node_size,
                min_node_size,
                max_node_size,
            ),
        }
    }
}

fn left_packed(
    entry_count: usize,
    node_size: &impl Fn(Range<usize>) -> usize,
    min_node_size: usize,
    max_node_size: usize,
) -> SmallVec<[usize; 4]> {
    let mut boundaries = smallvec![0];
    let mut start = 0;
    while node_size(start..entry_count) > max_node_size {
        let mut end = start + 1;
        while end < entry_count && node_size(start..end + 1) <= max_node_size {
            end += 1;
        }
        while end > start + 1 && node_size(end..entry_count) < min_node_size {
            end -= 1;
        }
        boundaries.push(end);
        start = end;
    }
    boundaries.push(entry_count);
    boundaries
}

fn right_packed(
    entry_count: usize,
    node_size: &impl Fn(Range<usize>) -> usize,
    min_node_size: usize,
    max_node_size: usize,
) -> SmallVec<[usize; 4]> {
    let mut boundaries = smallvec![entry_count];
    let mut end = entry_count;
    while node_size(0..end) > max_node_size {
        let mut start = end - 1;
        while start > 0 && node_size(start - 1..end) <= max_node_size {
            start -= 1;
        }
        while start + 1 < end && node_size(0..start) < min_node_size {
            start += 1;
        }
        boundaries.push(start);
        end = start;
    }
    boundaries.push(0);
    boundaries.reverse();
    boundaries
}

fn balanced(
    left_packed: SmallVec<[usize; 4]>,
    entry_count: usize,
    node_size: &impl Fn(Range<usize>) -> usize,
    min_node_size: usize,
    max_node_size: usize,
) -> SmallVec<[usize; 4]> {
    let right_packed = right_packed(entry_count, node_size, min_node_size, max_node_size);
    if right_packed.len() != left_packed.len() {
        return left_packed;
    }
    let node_count = left_packed.len() - 1;
    let mut boundaries = SmallVec::with_capacity(left_packed.len());
    boundaries.push(0);
    let mut previous = 0;
    for index in 1..node_count {
        let remaining_nodes = node_count - (index - 1);
        let remaining_size = node_size(previous..entry_count);
        let mut closest: Option<(usize, usize)> = None;
        for cut in right_packed[index].max(previous + 1)..=left_packed[index] {
            let size = node_size(previous..cut);
            if size > max_node_size {
                break;
            }
            if size < min_node_size {
                continue;
            }
            let distance = (remaining_nodes * size).abs_diff(remaining_size);
            if let Some((closest_distance, _)) = closest
                && distance >= closest_distance
            {
                break;
            }
            closest = Some((distance, cut));
        }
        let Some((_, cut)) = closest else {
            return left_packed;
        };
        boundaries.push(cut);
        previous = cut;
    }
    if node_count > 1
        && !(min_node_size..=max_node_size).contains(&node_size(previous..entry_count))
    {
        return left_packed;
    }
    boundaries.push(entry_count);
    boundaries
}

#[cfg(test)]
mod tests;
