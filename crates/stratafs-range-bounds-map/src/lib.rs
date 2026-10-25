use std::cmp::Ordering;
use std::iter;
use std::ops::{Bound, Range, RangeBounds};

use rangemap::RangeMap;

// RangeBoundsMap maps ranges of keys to values, where each end of a range can be included,
// excluded or unbounded. Inserting a range overwrites whatever part of earlier ranges it overlaps,
// and touching ranges with equal values merge.
// Ranges follow the same rules as BTreeMap::range: a range that ends before it starts panics,
// and an empty one, like (Included(k), Excluded(k)), holds nothing.
// Internally each end is a point between keys, so rangemap can store every kind of range half open.
pub struct RangeBoundsMap<K, V> {
    map: RangeMap<Point<K>, V>,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum Point<K> {
    BeforeEverything,
    Inside(K, Side),
    AfterEverything,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum Side {
    At,
    JustAfter,
}

impl<K, V> Default for RangeBoundsMap<K, V> {
    fn default() -> Self {
        Self {
            map: RangeMap::default(),
        }
    }
}

impl<K, V> RangeBoundsMap<K, V> {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    pub fn iter(&self) -> impl Iterator<Item = ((Bound<&K>, Bound<&K>), &V)> {
        self.map
            .iter()
            .map(|(range, value)| (bounds_of(range), value))
    }
}

impl<K: Ord + Clone, V> RangeBoundsMap<K, V> {
    pub fn get(&self, key: &K) -> Option<&V> {
        self.map.get(&Point::Inside(key.clone(), Side::At))
    }

    // overlapping returns every stored range that shares at least one point with the given range, in order.
    pub fn overlapping(
        &self,
        range: impl RangeBounds<K>,
    ) -> impl Iterator<Item = ((Bound<&K>, Bound<&K>), &V)> {
        points_of(&range)
            .into_iter()
            .flat_map(|points| self.map.overlapping(points))
            .map(|(range, value)| (bounds_of(range), value))
    }

    // gaps returns the parts of the given range that no stored range covers, in order.
    pub fn gaps(&self, range: impl RangeBounds<K>) -> impl Iterator<Item = (Bound<K>, Bound<K>)> {
        let points = points_of(&range);
        let end = points.as_ref().map(|points| points.end.clone());
        let mut cursor = points.as_ref().map(|points| points.start.clone());
        let mut stored = points
            .into_iter()
            .flat_map(|points| self.map.overlapping(points))
            .map(|(range, _)| range);
        iter::from_fn(move || {
            let end = end.as_ref()?;
            let at = cursor.as_mut()?;
            for range in stored.by_ref() {
                let gap = (*at < range.start).then(|| (at.clone(), range.start.clone()));
                *at = range.end.clone();
                if gap.is_some() {
                    return gap;
                }
            }
            let last = cursor.take()?;
            (last < *end).then(|| (last, end.clone()))
        })
        .map(|(start, end)| (into_start_bound(start), into_end_bound(end)))
    }
}

impl<K: Ord + Clone, V: PartialEq + Clone> RangeBoundsMap<K, V> {
    pub fn insert(&mut self, range: impl RangeBounds<K>, value: V) {
        if let Some(points) = points_of(&range) {
            self.map.insert(points, value);
        }
    }
}

impl<K> Point<K> {
    fn as_ref(&self) -> Point<&K> {
        match self {
            Point::BeforeEverything => Point::BeforeEverything,
            Point::Inside(key, side) => Point::Inside(key, *side),
            Point::AfterEverything => Point::AfterEverything,
        }
    }
}

fn points_of<K: Ord + Clone>(range: &impl RangeBounds<K>) -> Option<Range<Point<K>>> {
    let start = match range.start_bound() {
        Bound::Included(key) => Point::Inside(key.clone(), Side::At),
        Bound::Excluded(key) => Point::Inside(key.clone(), Side::JustAfter),
        Bound::Unbounded => Point::BeforeEverything,
    };
    let end = match range.end_bound() {
        Bound::Included(key) => Point::Inside(key.clone(), Side::JustAfter),
        Bound::Excluded(key) => Point::Inside(key.clone(), Side::At),
        Bound::Unbounded => Point::AfterEverything,
    };
    match start.cmp(&end) {
        Ordering::Less => Some(start..end),
        Ordering::Equal => None,
        Ordering::Greater => panic!("the range ends before it starts"),
    }
}

fn bounds_of<K>(range: &Range<Point<K>>) -> (Bound<&K>, Bound<&K>) {
    (
        into_start_bound(range.start.as_ref()),
        into_end_bound(range.end.as_ref()),
    )
}

fn into_start_bound<K>(point: Point<K>) -> Bound<K> {
    match point {
        Point::Inside(key, Side::At) => Bound::Included(key),
        Point::Inside(key, Side::JustAfter) => Bound::Excluded(key),
        Point::BeforeEverything | Point::AfterEverything => Bound::Unbounded,
    }
}

fn into_end_bound<K>(point: Point<K>) -> Bound<K> {
    match point {
        Point::Inside(key, Side::At) => Bound::Excluded(key),
        Point::Inside(key, Side::JustAfter) => Bound::Included(key),
        Point::BeforeEverything | Point::AfterEverything => Bound::Unbounded,
    }
}

#[cfg(test)]
mod tests;
