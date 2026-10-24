use super::*;

const STRATEGIES: [PackingStrategy; 2] = [PackingStrategy::Balanced, PackingStrategy::LeftPacked];

fn summed(sizes: &[usize]) -> impl Fn(Range<usize>) -> usize + '_ {
    |range| sizes[range].iter().sum()
}

fn node_sizes(boundaries: &[usize], node_size: &impl Fn(Range<usize>) -> usize) -> Vec<usize> {
    boundaries
        .windows(2)
        .map(|pair| node_size(pair[0]..pair[1]))
        .collect()
}

fn fewest_nodes(
    entry_count: usize,
    node_size: &impl Fn(Range<usize>) -> usize,
    min_node_size: usize,
    max_node_size: usize,
) -> usize {
    if node_size(0..entry_count) <= max_node_size {
        return 1;
    }
    let mut fewest = vec![usize::MAX; entry_count + 1];
    fewest[0] = 0;
    for end in 1..=entry_count {
        for start in 0..end {
            if fewest[start] != usize::MAX
                && (min_node_size..=max_node_size).contains(&node_size(start..end))
            {
                fewest[end] = fewest[end].min(fewest[start] + 1);
            }
        }
    }
    fewest[entry_count]
}

fn assert_conforms(
    strategy: PackingStrategy,
    entry_count: usize,
    node_size: &impl Fn(Range<usize>) -> usize,
    min_node_size: usize,
    max_node_size: usize,
) {
    let boundaries = strategy.boundaries(entry_count, node_size, min_node_size, max_node_size);
    assert_eq!(boundaries.first(), Some(&0));
    assert_eq!(boundaries.last(), Some(&entry_count));
    assert!(boundaries.windows(2).all(|pair| pair[0] < pair[1]));
    let sizes = node_sizes(&boundaries, node_size);
    assert!(sizes.iter().all(|&size| size <= max_node_size), "{sizes:?}");
    if sizes.len() > 1 {
        assert!(sizes.iter().all(|&size| size >= min_node_size), "{sizes:?}");
    }
    assert_eq!(
        sizes.len(),
        fewest_nodes(entry_count, node_size, min_node_size, max_node_size)
    );
}

fn every_sequence(max_length: usize, entry_sizes: Range<usize>) -> Vec<Vec<usize>> {
    let mut sequences = vec![Vec::new()];
    let mut all = Vec::new();
    for _ in 0..max_length {
        sequences = sequences
            .iter()
            .flat_map(|sequence| {
                entry_sizes.clone().map(move |size| {
                    let mut longer = sequence.clone();
                    longer.push(size);
                    longer
                })
            })
            .collect();
        all.extend(sequences.iter().cloned());
    }
    all
}

#[test]
fn every_short_sequence_packs_into_the_fewest_nodes_within_the_limits() {
    for (max_length, entry_sizes, min_node_size, max_node_size) in [
        (8, 1..4, 3, 9),
        (10, 1..3, 2, 6),
        (7, 1..4, 3, 10),
        (7, 2..4, 3, 9),
    ] {
        for sizes in every_sequence(max_length, entry_sizes) {
            for strategy in STRATEGIES {
                assert_conforms(
                    strategy,
                    sizes.len(),
                    &summed(&sizes),
                    min_node_size,
                    max_node_size,
                );
            }
        }
    }
}

#[test]
fn sequences_that_break_an_even_split_still_conform() {
    for strategy in STRATEGIES {
        let sizes = [8, 5, 9, 9, 10, 6, 6];
        assert_conforms(strategy, sizes.len(), &summed(&sizes), 10, 30);
        let sizes = [46, 100, 100, 98, 100, 100, 46];
        assert_conforms(strategy, sizes.len(), &summed(&sizes), 100, 300);
    }
}

#[test]
fn node_sizes_that_depend_on_the_first_entry_still_conform() {
    let sizes = [12, 30, 25, 30, 18, 30, 30, 22, 9];
    let first_entry_savings = [4, 9, 2, 9, 0, 9, 9, 5, 1];
    let node_size =
        |range: Range<usize>| summed(&sizes)(range.clone()) - first_entry_savings[range.start];
    for strategy in STRATEGIES {
        assert_conforms(strategy, sizes.len(), &node_size, 30, 90);
    }
}

#[test]
fn entries_that_fit_one_node_stay_in_one_node_even_below_the_minimum() {
    for strategy in STRATEGIES {
        assert_eq!(strategy.boundaries(2, summed(&[1, 1]), 3, 9)[..], [0, 2]);
    }
}

#[test]
fn left_packed_fills_each_node_before_starting_the_next() {
    assert_eq!(
        PackingStrategy::LeftPacked.boundaries(9, summed(&[2; 9]), 3, 9)[..],
        [0, 4, 7, 9]
    );
}

#[test]
fn left_packed_never_leaves_its_last_node_below_the_minimum() {
    let sizes = [3, 3, 3, 3, 3, 3, 1];
    assert_eq!(
        PackingStrategy::LeftPacked.boundaries(sizes.len(), summed(&sizes), 3, 9)[..],
        [0, 3, 5, 7]
    );
}

#[test]
fn balanced_spreads_entries_evenly_across_the_fewest_nodes() {
    assert_eq!(
        PackingStrategy::Balanced.boundaries(9, summed(&[2; 9]), 3, 9)[..],
        [0, 3, 6, 9]
    );
    let sizes = [1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1];
    let boundaries = PackingStrategy::Balanced.boundaries(sizes.len(), summed(&sizes), 2, 9);
    assert_eq!(node_sizes(&boundaries, &summed(&sizes)), vec![6, 6]);
}
