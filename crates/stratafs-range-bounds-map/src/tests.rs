use std::ops::Bound::{Excluded, Included, Unbounded};
use std::panic::{AssertUnwindSafe, catch_unwind};

use proptest::collection::vec;
use proptest::prelude::*;

use super::*;

type Bounds = (Bound<u8>, Bound<u8>);

const KEYS: std::ops::RangeInclusive<u8> = 0..=12;
const ENDS: [Bound<u8>; 3] = [Included(4), Excluded(4), Unbounded];

fn owned((start, end): (Bound<&u8>, Bound<&u8>)) -> Bounds {
    (start.cloned(), end.cloned())
}

fn entries(map: &RangeBoundsMap<u8, char>) -> Vec<(Bounds, char)> {
    map.iter()
        .map(|(bounds, value)| (owned(bounds), *value))
        .collect()
}

fn map_of(inserts: &[(Bounds, char)]) -> RangeBoundsMap<u8, char> {
    let mut map = RangeBoundsMap::new();
    for (bounds, value) in inserts {
        map.insert(*bounds, *value);
    }
    map
}

fn unstored(map: &RangeBoundsMap<u8, char>, bounds: Bounds) -> Vec<Bounds> {
    map.matching_ranges(bounds, |value| value.is_none())
        .collect()
}

fn ends_before_it_starts((start, end): &Bounds) -> bool {
    match (start, end) {
        (Included(start) | Excluded(start), Included(end) | Excluded(end)) if start > end => true,
        (Excluded(start), Excluded(end)) => start == end,
        _ => false,
    }
}

fn in_order(earlier: &Bounds, later: &Bounds) -> bool {
    match (points_of(earlier), points_of(later)) {
        (Some(earlier), Some(later)) => earlier.end <= later.start,
        _ => false,
    }
}

fn apart(earlier: &Bounds, later: &Bounds) -> bool {
    match (points_of(earlier), points_of(later)) {
        (Some(earlier), Some(later)) => earlier.end < later.start,
        _ => false,
    }
}

fn within(part: &Bounds, query: &Bounds) -> bool {
    match (points_of(part), points_of(query)) {
        (Some(part), Some(query)) => query.start <= part.start && part.end <= query.end,
        _ => false,
    }
}

fn share_a_point(left: &Bounds, right: &Bounds) -> bool {
    match (points_of(left), points_of(right)) {
        (Some(left), Some(right)) => left.start < right.end && right.start < left.end,
        _ => false,
    }
}

#[test]
fn an_empty_map_holds_nothing_so_every_query_is_unstored() {
    let map = RangeBoundsMap::<u8, char>::new();
    assert!(map.is_empty());
    assert_eq!(map.get(&5), None);
    assert_eq!(entries(&map), []);
    assert_eq!(map.overlapping(..).count(), 0);
    assert_eq!(
        unstored(&map, (Included(2), Included(7))),
        [(Included(2), Included(7))]
    );
    assert_eq!(
        unstored(&map, (Unbounded, Unbounded)),
        [(Unbounded, Unbounded)]
    );
}

#[test]
fn every_kind_of_range_comes_back_as_it_was_inserted() {
    for start in [Included(2), Excluded(2), Unbounded] {
        for end in ENDS {
            let map = map_of(&[((start, end), 'a')]);
            assert_eq!(entries(&map), [((start, end), 'a')]);
        }
    }
}

#[test]
fn a_range_covers_exactly_the_keys_its_bounds_include() {
    for start in [Included(2), Excluded(2), Unbounded] {
        for end in ENDS {
            let map = map_of(&[((start, end), 'a')]);
            for key in KEYS {
                assert_eq!(
                    map.get(&key).is_some(),
                    (start, end).contains(&key),
                    "{start:?} to {end:?} at {key}"
                );
            }
        }
    }
}

#[test]
fn a_single_key_range_covers_only_that_key() {
    let map = map_of(&[((Included(3), Included(3)), 'a')]);
    assert_eq!(map.get(&2), None);
    assert_eq!(map.get(&3), Some(&'a'));
    assert_eq!(map.get(&4), None);
}

#[test]
fn empty_ranges_are_ignored() {
    let map = map_of(&[
        ((Included(3), Excluded(3)), 'a'),
        ((Excluded(3), Included(3)), 'a'),
    ]);
    assert!(map.is_empty());
    assert_eq!(map.overlapping((Included(3), Excluded(3))).count(), 0);
    assert_eq!(
        map.matching_ranges((Excluded(3), Included(3)), |_| true)
            .count(),
        0
    );
}

#[test]
fn ranges_that_end_before_they_start_panic() {
    for bounds in [
        (Excluded(3), Excluded(3)),
        (Included(4), Included(3)),
        (Excluded(4), Excluded(3)),
    ] {
        let mut map = map_of(&[((Unbounded, Unbounded), 'a')]);
        assert!(catch_unwind(AssertUnwindSafe(|| map.insert(bounds, 'b'))).is_err());
        assert!(catch_unwind(AssertUnwindSafe(|| map.overlapping(bounds).count())).is_err());
        assert!(
            catch_unwind(AssertUnwindSafe(|| map
                .matching_ranges(bounds, |_| true)
                .count()))
            .is_err()
        );
    }
}

#[test]
fn a_later_range_overwrites_the_part_it_overlaps() {
    let map = map_of(&[
        ((Unbounded, Unbounded), 'a'),
        ((Included(3), Included(5)), 'b'),
    ]);
    assert_eq!(
        entries(&map),
        [
            ((Unbounded, Excluded(3)), 'a'),
            ((Included(3), Included(5)), 'b'),
            ((Excluded(5), Unbounded), 'a'),
        ]
    );
    let map = map_of(&[
        ((Included(2), Included(6)), 'a'),
        ((Excluded(4), Unbounded), 'b'),
        ((Unbounded, Excluded(3)), 'c'),
    ]);
    assert_eq!(
        entries(&map),
        [
            ((Unbounded, Excluded(3)), 'c'),
            ((Included(3), Included(4)), 'a'),
            ((Excluded(4), Unbounded), 'b'),
        ]
    );
}

#[test]
fn a_range_over_earlier_ranges_replaces_them_whole() {
    let map = map_of(&[
        ((Included(2), Included(3)), 'a'),
        ((Included(5), Included(6)), 'b'),
        ((Included(1), Excluded(8)), 'c'),
    ]);
    assert_eq!(entries(&map), [((Included(1), Excluded(8)), 'c')]);
}

#[test]
fn touching_ranges_with_equal_values_merge() {
    let map = map_of(&[
        ((Included(1), Excluded(3)), 'a'),
        ((Included(3), Excluded(5)), 'a'),
    ]);
    assert_eq!(entries(&map), [((Included(1), Excluded(5)), 'a')]);
    let map = map_of(&[
        ((Unbounded, Included(3)), 'a'),
        ((Excluded(3), Unbounded), 'a'),
    ]);
    assert_eq!(entries(&map), [((Unbounded, Unbounded), 'a')]);
    let map = map_of(&[
        ((Included(1), Excluded(3)), 'a'),
        ((Included(3), Excluded(5)), 'b'),
    ]);
    assert_eq!(
        entries(&map),
        [
            ((Included(1), Excluded(3)), 'a'),
            ((Included(3), Excluded(5)), 'b')
        ]
    );
}

#[test]
fn ranges_with_no_key_between_them_stay_separate_but_cover_every_key() {
    let map = map_of(&[
        ((Unbounded, Included(3)), 'a'),
        ((Included(4), Unbounded), 'a'),
    ]);
    assert_eq!(entries(&map).len(), 2);
    assert!(KEYS.into_iter().all(|key| map.get(&key) == Some(&'a')));

    let mut bytes = RangeBoundsMap::<Vec<u8>, char>::new();
    bytes.insert(..=b"a".to_vec(), 'a');
    bytes.insert(b"a\0".to_vec().., 'a');
    assert_eq!(bytes.iter().count(), 2);
    for key in [&b""[..], b"a", b"a\0", b"a\0\0", b"b"] {
        assert_eq!(bytes.get(&key.to_vec()), Some(&'a'), "{key:?}");
    }
}

#[test]
fn overlapping_returns_the_stored_ranges_the_query_shares_keys_with() {
    let map = map_of(&[
        ((Included(1), Included(2)), 'a'),
        ((Included(4), Included(5)), 'b'),
        ((Included(7), Included(8)), 'c'),
    ]);
    let overlapping = |bounds: Bounds| -> Vec<(Bounds, char)> {
        map.overlapping(bounds)
            .map(|(bounds, value)| (owned(bounds), *value))
            .collect()
    };
    assert_eq!(
        overlapping((Included(2), Included(4))),
        [
            ((Included(1), Included(2)), 'a'),
            ((Included(4), Included(5)), 'b')
        ]
    );
    assert_eq!(overlapping((Excluded(2), Excluded(4))), []);
    assert_eq!(overlapping((Unbounded, Unbounded)).len(), 3);
}

#[test]
fn matching_nothing_stored_finds_the_parts_no_range_covers() {
    let map = map_of(&[
        ((Included(2), Included(3)), 'a'),
        ((Excluded(5), Excluded(7)), 'b'),
    ]);
    assert_eq!(
        unstored(&map, (Unbounded, Unbounded)),
        [
            (Unbounded, Excluded(2)),
            (Excluded(3), Included(5)),
            (Included(7), Unbounded),
        ]
    );
    assert_eq!(
        unstored(&map, (Included(3), Included(6))),
        [(Excluded(3), Included(5))]
    );
    assert_eq!(unstored(&map, (Included(2), Included(3))), []);
    assert_eq!(
        unstored(&map, (Excluded(0), Included(1))),
        [(Excluded(0), Included(1))]
    );
}

#[test]
fn neighbouring_parts_that_match_merge() {
    let map = map_of(&[
        ((Included(2), Included(3)), 'a'),
        ((Included(5), Included(6)), 'b'),
        ((Included(8), Included(9)), 'a'),
    ]);
    let not_b: Vec<Bounds> = map
        .matching_ranges(.., |value| value != Some(&'b'))
        .collect();
    assert_eq!(not_b, [(Unbounded, Excluded(5)), (Excluded(6), Unbounded)]);
}

#[test]
fn matching_ranges_are_clipped_to_the_query() {
    let map = map_of(&[((Unbounded, Unbounded), 'a')]);
    let matching: Vec<Bounds> = map
        .matching_ranges(3..=5, |value| value == Some(&'a'))
        .collect();
    assert_eq!(matching, [(Included(3), Included(5))]);
}

#[test]
fn matching_everything_returns_the_query_and_matching_nothing_returns_nothing() {
    let map = map_of(&[
        ((Included(2), Included(3)), 'a'),
        ((Included(5), Included(6)), 'b'),
    ]);
    let everything: Vec<Bounds> = map.matching_ranges(1..=8, |_| true).collect();
    assert_eq!(everything, [(Included(1), Included(8))]);
    assert_eq!(map.matching_ranges(1..=8, |_| false).count(), 0);
}

#[test]
fn the_parts_not_cleared_include_a_key_set_inside_a_cleared_range() {
    let map = map_of(&[
        ((Included(3), Included(6)), 'c'),
        ((Included(4), Included(4)), 's'),
    ]);
    let not_cleared: Vec<Bounds> = map
        .matching_ranges(1..=8, |value| value != Some(&'c'))
        .collect();
    assert_eq!(
        not_cleared,
        [
            (Included(1), Excluded(3)),
            (Included(4), Included(4)),
            (Excluded(6), Included(8)),
        ]
    );
}

fn bound() -> impl Strategy<Value = Bound<u8>> {
    prop_oneof![
        (1u8..=11).prop_map(Included),
        (1u8..=11).prop_map(Excluded),
        Just(Unbounded),
    ]
}

fn valid_range() -> impl Strategy<Value = Bounds> {
    (bound(), bound()).prop_filter("the range ends before it starts", |bounds| {
        !ends_before_it_starts(bounds)
    })
}

proptest! {
    #[test]
    fn matches_applying_every_insert_to_each_key_in_turn(
        inserts in vec((valid_range(), prop_oneof![Just('a'), Just('b'), Just('c')]), 0..12),
        queries in vec(valid_range(), 1..6),
        matches_unstored in any::<bool>(),
        matching_values in proptest::sample::subsequence(vec!['a', 'b', 'c'], 0..=3),
    ) {
        let matches = |value: Option<&char>| match value {
            Some(value) => matching_values.contains(value),
            None => matches_unstored,
        };
        let map = map_of(&inserts);
        let mut model = [None; 13];
        for (bounds, value) in &inserts {
            for key in KEYS.filter(|key| bounds.contains(key)) {
                model[usize::from(key)] = Some(*value);
            }
        }
        let stored = entries(&map);
        for pair in stored.windows(2) {
            prop_assert!(in_order(&pair[0].0, &pair[1].0));
        }
        for key in KEYS {
            prop_assert_eq!(map.get(&key).copied(), model[usize::from(key)]);
            let covering: Vec<char> = stored
                .iter()
                .filter(|(bounds, _)| bounds.contains(&key))
                .map(|(_, value)| *value)
                .collect();
            prop_assert!(covering.len() <= 1);
            prop_assert_eq!(covering.first().copied(), model[usize::from(key)]);
        }
        for query in queries {
            let overlapping: Vec<(Bounds, char)> = map
                .overlapping(query)
                .map(|(bounds, value)| (owned(bounds), *value))
                .collect();
            let expected: Vec<(Bounds, char)> = stored
                .iter()
                .filter(|(bounds, _)| share_a_point(bounds, &query))
                .copied()
                .collect();
            prop_assert_eq!(overlapping, expected);

            let matching: Vec<Bounds> = map.matching_ranges(query, matches).collect();
            for key in KEYS {
                let in_a_part = matching.iter().filter(|part| part.contains(&key)).count();
                let expected = query.contains(&key) && matches(model[usize::from(key)].as_ref());
                prop_assert_eq!(in_a_part, usize::from(expected), "key {}", key);
            }
            for part in &matching {
                prop_assert!(within(part, &query));
                for (bounds, value) in &stored {
                    prop_assert!(matches(Some(value)) || !share_a_point(part, bounds));
                }
            }
            for pair in matching.windows(2) {
                prop_assert!(apart(&pair[0], &pair[1]));
            }
        }
    }
}
