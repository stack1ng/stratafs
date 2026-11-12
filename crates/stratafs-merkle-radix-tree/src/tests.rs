use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use bytes::Buf;
use futures_lite::future::{block_on, yield_now};
use proptest::prelude::*;
use proptest::sample::Index;
use sha2::Sha256;
use stratafs_store::kv::assert_valid_range;

use super::*;

type Hash = Output<Sha256>;
type Tree<const N: usize> = MerkleRadixTree<Sha256, N>;

const PARTITION: &[u8] = b"tree/";

// Store is a transaction over entries kept in memory, which notes how it was read and written.
#[derive(Default)]
struct Store {
    entries: BTreeMap<Bytes, Bytes>,
    reads: AtomicUsize,
    finished: AtomicUsize,
    // read_after_another says whether a read began after another had finished.
    read_after_another: AtomicBool,
    writes: usize,
}

impl Store {
    // watch forgets the reads and writes made so far.
    fn watch(&mut self) {
        *self = Store {
            entries: std::mem::take(&mut self.entries),
            ..Store::default()
        };
    }

    fn reads(&self) -> usize {
        self.reads.load(Ordering::Relaxed)
    }
}

impl ReadKeyValueTransaction for Store {
    type Failure = Infallible;

    async fn get_range<R: RangeBounds<Bytes> + Send>(
        &self,
        range: R,
        limit: usize,
    ) -> Result<impl Iterator<Item = (Bytes, Bytes)> + Send + use<R>, Self::Failure> {
        assert_valid_range(&range);
        self.reads.fetch_add(1, Ordering::Relaxed);
        if self.finished.load(Ordering::Relaxed) > 0 {
            self.read_after_another.store(true, Ordering::Relaxed);
        }
        let bounds = (range.start_bound().cloned(), range.end_bound().cloned());
        drop(range);
        let found: Vec<_> = self
            .entries
            .range(bounds)
            .take(limit)
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect();
        // A read takes a moment, so reads made one after another are told from reads made together.
        yield_now().await;
        self.finished.fetch_add(1, Ordering::Relaxed);
        Ok(found.into_iter())
    }
}

impl KeyValueTransaction for Store {
    type Rejection = Infallible;

    fn set(
        &mut self,
        key: Bytes,
        mut value: impl Buf,
    ) -> Result<(), StoreError<SetRejection<Infallible>, Infallible>> {
        self.entries
            .insert(key, value.copy_to_bytes(value.remaining()));
        self.writes += 1;
        Ok(())
    }

    fn clear_range(&mut self, range: impl RangeBounds<Bytes>) -> Result<(), Infallible> {
        assert_valid_range(&range);
        let bounds = (range.start_bound().cloned(), range.end_bound().cloned());
        let cleared: Vec<Bytes> = self
            .entries
            .range(bounds)
            .map(|(key, _)| key.clone())
            .collect();
        for key in cleared {
            self.entries.remove(&key);
        }
        // A clear is a write even when it finds nothing to remove.
        self.writes += 1;
        Ok(())
    }
}

fn tree<const N: usize>() -> Tree<N> {
    Tree::new(Bytes::from_static(PARTITION))
}

// verify verifies as the tree does, and checks it read the store at most once.
fn verify<const N: usize>(
    store: &mut Store,
    root: &Hash,
    range: impl RangeBounds<[u8; N]>,
    records: &[Record<Sha256, N>],
) -> Result<(), Error<Infallible>> {
    store.watch();
    let verified = block_on(tree::<N>().verify(store, root, range, records));
    assert!(!store.read_after_another.load(Ordering::Relaxed));
    verified
}

// update updates as the tree does, and checks it read the store at most once.
fn update<const N: usize>(
    store: &mut Store,
    root: &Hash,
    changes: &[Change<Sha256, N>],
) -> Result<Hash, UpdateError<Store>> {
    store.watch();
    let updated = block_on(tree::<N>().update(store, root, changes));
    assert!(!store.read_after_another.load(Ordering::Relaxed));
    updated
}

fn content(seed: u8) -> Hash {
    Sha256::digest([seed])
}

fn key(digits: &[u8]) -> Bytes {
    Bytes::from([PARTITION, &[digits.len() as u8], digits].concat())
}

// Model is the records a tree holds, which is all there is to know about it.
type Model<const N: usize> = BTreeMap<[u8; N], Hash>;

// node_hash is the hash of the node with digits in the model's tree, worked out from the records alone, or None when
// no record lies below it.
fn node_hash<const N: usize>(model: &Model<N>, digits: &[u8]) -> Option<Hash> {
    if digits.len() == N {
        return model.get(digits).cloned();
    }
    let mut next: Vec<u8> = model
        .keys()
        .filter(|position| position.starts_with(digits))
        .map(|position| position[digits.len()])
        .collect();
    next.dedup();
    let mut hasher = None;
    for digit in next {
        let child = [digits, &[digit]].concat();
        let hash = node_hash(model, &child).expect("a record lies below the child");
        let hasher = hasher.get_or_insert_with(Sha256::new);
        hasher.update(&child);
        hasher.update(hash);
    }
    hasher.map(Sha256::finalize)
}

fn root_of<const N: usize>(model: &Model<N>) -> Hash {
    node_hash(model, &[]).unwrap_or_else(|| Sha256::digest([]))
}

// entries_of is every entry the store holds for the model's tree.
fn entries_of<const N: usize>(model: &Model<N>) -> BTreeMap<Bytes, Bytes> {
    let nodes = model
        .keys()
        .flat_map(|position| (1..=N).map(move |length| &position[..length]));
    nodes
        .map(|digits| {
            let hash = node_hash(model, digits).expect("a record lies below the node");
            (key(digits), Bytes::copy_from_slice(&hash))
        })
        .collect()
}

// built is a store holding the model's tree, made by one update, and the tree's root.
fn built<const N: usize>(model: &Model<N>) -> (Store, Hash) {
    let mut store = Store::default();
    let changes: Vec<_> = model
        .iter()
        .map(|(position, hash)| (*position, Some(*hash)))
        .collect();
    let root = update(&mut store, &Tree::<N>::initial_root(), &changes).unwrap();
    (store, root)
}

fn records_of<const N: usize>(model: &Model<N>) -> Vec<Record<Sha256, N>> {
    model
        .iter()
        .map(|(position, hash)| (*position, *hash))
        .collect()
}

// A digit is mostly one of a few, so positions often share their leading digits.
fn digit() -> impl Strategy<Value = u8> {
    prop_oneof![
        4 => prop::sample::select(vec![0, 1, 2, 255]),
        1 => any::<u8>(),
    ]
}

fn position<const N: usize>() -> impl Strategy<Value = [u8; N]> {
    prop::collection::vec(digit(), N)
        .prop_map(|digits| <[u8; N]>::try_from(digits).expect("the vector has N digits"))
}

fn model<const N: usize>(records: std::ops::Range<usize>) -> impl Strategy<Value = Model<N>> {
    prop::collection::btree_map(position(), any::<u8>().prop_map(content), records)
}

fn bound<const N: usize>() -> impl Strategy<Value = Bound<[u8; N]>> {
    prop_oneof![
        Just(Bound::Unbounded),
        position().prop_map(Bound::Included),
        position().prop_map(Bound::Excluded),
    ]
}

// Step is the changes of one update, then the ranges to verify, each with a position no record may be at.
type Step<const N: usize> = (
    BTreeMap<[u8; N], Option<u8>>,
    Vec<((Bound<[u8; N]>, Bound<[u8; N]>), [u8; N])>,
);

fn steps<const N: usize>() -> impl Strategy<Value = Vec<Step<N>>> {
    let changes =
        prop::collection::btree_map(position(), prop::option::weighted(0.7, any::<u8>()), 0..6);
    let ranges = prop::collection::vec(((bound(), bound()), position()), 0..3);
    prop::collection::vec((changes, ranges), 1..8)
}

fn unverified<T, F: std::fmt::Debug, R: std::fmt::Debug>(result: Result<T, Error<F, R>>) -> bool {
    matches!(result, Err(Error::Unverified))
}

// agrees_with_the_model runs updates, and checks after each that the tree is the one the records alone give, that it
// verifies exactly those records in any range, and that it verifies nothing else.
fn agrees_with_the_model<const N: usize>(steps: Vec<Step<N>>) -> Result<(), TestCaseError> {
    let everything = (Bound::Unbounded, Bound::Unbounded);
    let mut store = Store::default();
    let mut model = Model::<N>::new();
    let mut root = Tree::<N>::initial_root();
    for (changes, ranges) in steps {
        let changes: Vec<Change<Sha256, N>> = changes
            .into_iter()
            .map(|(position, seed)| (position, seed.map(content)))
            .collect();
        let before = root;
        root = update(&mut store, &root, &changes).unwrap();
        for (position, hash) in changes {
            match hash {
                Some(hash) => model.insert(position, hash),
                None => model.remove(&position),
            };
        }
        prop_assert_eq!(&root, &root_of(&model));
        prop_assert_eq!(&store.entries, &entries_of(&model));

        for (range, stray) in ranges.into_iter().chain([(everything, [7; N])]) {
            let records: Vec<_> = records_of(&model)
                .into_iter()
                .filter(|(position, _)| range.contains(position))
                .collect();
            prop_assert!(verify(&mut store, &root, range, &records).is_ok());
            // A range with no positions asks nothing of the tree, so any root verifies it.
            if before != root && inclusive(range).is_some() {
                prop_assert!(unverified(verify(&mut store, &before, range, &records)));
            }
            if let Some((_, others)) = records.split_last() {
                prop_assert!(unverified(verify(&mut store, &root, range, others)));
                let mut altered = records.clone();
                altered.last_mut().expect("there is a last record").1[0] ^= 1;
                prop_assert!(unverified(verify(&mut store, &root, range, &altered)));
            }
            if range.contains(&stray) && !model.contains_key(&stray) {
                let mut added = records.clone();
                added.push((stray, content(0)));
                added.sort();
                prop_assert!(unverified(verify(&mut store, &root, range, &added)));
            }
        }
    }
    Ok(())
}

// Tampering is a change to one entry of a store.
#[derive(Debug, Clone)]
enum Tampering {
    // Altered changes one byte of the entry's hash.
    Altered(Index),
    Removed,
}

fn tampering() -> impl Strategy<Value = Tampering> {
    prop_oneof![
        any::<Index>().prop_map(Tampering::Altered),
        Just(Tampering::Removed)
    ]
}

fn tamper(store: &mut Store, key: &Bytes, tampering: &Tampering) {
    match tampering {
        Tampering::Altered(byte) => {
            let mut hash = store.entries[key].to_vec();
            let byte = byte.index(hash.len());
            hash[byte] ^= 1;
            store.entries.insert(key.clone(), Bytes::from(hash));
        }
        Tampering::Removed => {
            store.entries.remove(key);
        }
    }
}

proptest! {
    #[test]
    fn a_tree_of_one_digit_positions_agrees_with_the_model(steps in steps::<1>()) {
        agrees_with_the_model(steps)?;
    }

    #[test]
    fn a_tree_of_three_digit_positions_agrees_with_the_model(steps in steps::<3>()) {
        agrees_with_the_model(steps)?;
    }

    #[test]
    fn a_tree_of_nine_digit_positions_agrees_with_the_model(steps in steps::<9>()) {
        agrees_with_the_model(steps)?;
    }

    #[test]
    fn verify_refuses_a_tree_with_any_entry_tampered_with(
        model in model::<3>(1..12),
        entry in any::<Index>(),
        tampering in tampering(),
    ) {
        let (mut store, root) = built(&model);
        let keys: Vec<Bytes> = store.entries.keys().cloned().collect();
        tamper(&mut store, entry.get(&keys), &tampering);
        prop_assert!(unverified(verify(&mut store, &root, .., &records_of(&model))));
    }

    #[test]
    fn verify_refuses_a_tree_with_an_entry_added(
        model in model::<3>(0..12),
        digits in prop::collection::vec(digit(), 1..=3),
        hash in any::<u8>(),
    ) {
        let (mut store, root) = built(&model);
        prop_assume!(!store.entries.contains_key(&key(&digits)));
        store.entries.insert(key(&digits), Bytes::copy_from_slice(&content(hash)));
        prop_assert!(unverified(verify(&mut store, &root, .., &records_of(&model))));
    }

    // An update reads the children of every node above the record it changes, so tampering with any of them stops it.
    #[test]
    fn update_refuses_a_tree_with_an_entry_it_reads_tampered_with(
        model in model::<3>(1..12),
        record in any::<Index>(),
        entry in any::<Index>(),
        tampering in tampering(),
        change in prop::option::of(any::<u8>()),
    ) {
        let (mut store, root) = built(&model);
        let positions: Vec<[u8; 3]> = model.keys().copied().collect();
        let position = *record.get(&positions);
        let read: Vec<Bytes> = store
            .entries
            .keys()
            .filter(|stored| {
                let digits = &stored[PARTITION.len() + 1..];
                digits[..digits.len() - 1] == position[..digits.len() - 1]
            })
            .cloned()
            .collect();
        tamper(&mut store, entry.get(&read), &tampering);
        let changes = [(position, change.map(content))];
        prop_assert!(unverified(update(&mut store, &root, &changes)));
    }

    #[test]
    fn update_refuses_a_tree_with_an_entry_added_where_it_reads(
        model in model::<3>(1..12),
        record in any::<Index>(),
        length in 1usize..=3,
        digit in any::<u8>(),
        change in prop::option::of(any::<u8>()),
    ) {
        let (mut store, root) = built(&model);
        let positions: Vec<[u8; 3]> = model.keys().copied().collect();
        let position = *record.get(&positions);
        let added = [&position[..length - 1], &[digit]].concat();
        prop_assume!(!store.entries.contains_key(&key(&added)));
        store.entries.insert(key(&added), Bytes::copy_from_slice(&content(0)));
        let changes = [(position, change.map(content))];
        prop_assert!(unverified(update(&mut store, &root, &changes)));
    }

    #[test]
    fn the_order_of_updates_does_not_change_the_tree(model in model::<3>(0..12), split in any::<Index>()) {
        let (store, root) = built(&model);
        let records = records_of(&model);
        let (early, late) = records.split_at(split.index(records.len() + 1));
        let changes = |records: &[Record<Sha256, 3>]| -> Vec<Change<Sha256, 3>> {
            records.iter().map(|(position, hash)| (*position, Some(*hash))).collect()
        };
        let mut other = Store::default();
        // The later records go in first.
        let first = update(&mut other, &Tree::<3>::initial_root(), &changes(late)).unwrap();
        let second = update(&mut other, &first, &changes(early)).unwrap();
        prop_assert_eq!(second, root);
        prop_assert_eq!(other.entries, store.entries);
    }
}

fn hex(hex: &str) -> Bytes {
    Bytes::from(hex::decode(hex).unwrap())
}

// The hashes here were worked out apart from this crate, with Python's hashlib.
#[test]
fn a_tree_has_the_hashes_worked_out_by_hand() {
    let mut store = Store::default();
    let empty = Tree::<2>::initial_root();
    assert_eq!(
        empty[..],
        hex("e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855")
    );
    let (a, b, c) = (
        Sha256::digest(b"a"),
        Sha256::digest(b"b"),
        Sha256::digest(b"c"),
    );

    let one = update(&mut store, &empty, &[([1, 2], Some(a))]).unwrap();
    assert_eq!(
        one[..],
        hex("19d6f42dcc515a04df0f28dfeb390476c57e08bbb2c8f1755796d7006bc422a1")
    );
    let entries = [
        (
            b"tree/\x01\x01".as_slice(),
            "ffd5d5cd8c3db73c2acd0afc973556eb159555aa9f4646943c413dcbfab3f4fd",
        ),
        (
            b"tree/\x02\x01\x02".as_slice(),
            "ca978112ca1bbdcafac231b39a23dc4da786eff8147c4e72b9807785afee48bb",
        ),
    ];
    let entries: BTreeMap<_, _> = entries
        .into_iter()
        .map(|(key, hash)| (Bytes::from_static(key), hex(hash)))
        .collect();
    assert_eq!(store.entries, entries);

    let three = update(&mut store, &one, &[([1, 3], Some(b)), ([255, 0], Some(c))]).unwrap();
    assert_eq!(
        three[..],
        hex("2e507abf9298b4a86e7304299fc53ade36933be45a3459869fe5da796f32b009")
    );
    let entries = [
        (
            b"tree/\x01\x01".as_slice(),
            "e4652ed91e2c439ef6ada279ae5253635d4a89c9874f40c5365e2c09d6e387ec",
        ),
        (
            b"tree/\x01\xff".as_slice(),
            "fcbd50e8164c0ccc79dc10d73920e8531928124ed387a8ab4685f7f897448554",
        ),
        (
            b"tree/\x02\x01\x02".as_slice(),
            "ca978112ca1bbdcafac231b39a23dc4da786eff8147c4e72b9807785afee48bb",
        ),
        (
            b"tree/\x02\x01\x03".as_slice(),
            "3e23e8160039594a33894f6564e1b1348bbd7a0088d42c4acb73eeaed59c009d",
        ),
        (
            b"tree/\x02\xff\x00".as_slice(),
            "2e7d2c03a9507ae265ecf5b5356885a53393a2029d241394997265a1a25aefc6",
        ),
    ];
    let entries: BTreeMap<_, _> = entries
        .into_iter()
        .map(|(key, hash)| (Bytes::from_static(key), hex(hash)))
        .collect();
    assert_eq!(store.entries, entries);
}

// sample is a tree of a few records that share some of their digits, and the tree's root.
fn sample() -> (Store, Hash, Model<3>) {
    let positions = [[0, 0, 5], [0, 5, 0], [0, 5, 1], [5, 0, 0]];
    let model: Model<3> = positions.into_iter().zip((1..).map(content)).collect();
    let (store, root) = built(&model);
    (store, root, model)
}

#[test]
fn a_tree_with_no_records_verifies_no_records() {
    let mut store = Store::default();
    let root = Tree::<3>::initial_root();
    assert!(verify::<3>(&mut store, &root, .., &[]).is_ok());
    assert!(unverified(verify(
        &mut store,
        &root,
        ..,
        &[([0, 0, 5], content(1))]
    )));
    assert!(unverified(verify::<3>(&mut store, &content(1), .., &[])));
}

#[test]
fn verify_tells_a_record_that_is_there_from_one_that_is_not() {
    let (mut store, root, model) = sample();
    let there = [0, 5, 0];
    let record = [(there, model[&there])];
    assert!(verify(&mut store, &root, there..=there, &record).is_ok());
    assert!(unverified(verify(&mut store, &root, there..=there, &[])));
    let missing = [0, 5, 2];
    assert!(verify(&mut store, &root, missing..=missing, &[]).is_ok());
    assert!(unverified(verify(
        &mut store,
        &root,
        missing..=missing,
        &[(missing, content(9))]
    )));
}

#[test]
fn verify_takes_every_kind_of_range() {
    let (mut store, root, model) = sample();
    let records = records_of(&model);
    assert!(verify(&mut store, &root, .., &records).is_ok());
    assert!(verify(&mut store, &root, [0, 5, 0].., &records[1..]).is_ok());
    assert!(verify(&mut store, &root, ..[0, 5, 1], &records[..2]).is_ok());
    assert!(verify(&mut store, &root, ..=[0, 5, 1], &records[..3]).is_ok());
    assert!(verify(&mut store, &root, [0, 0, 6]..[5, 0, 0], &records[1..3]).is_ok());
    let after = (Bound::Excluded([0, 5, 0]), Bound::Unbounded);
    assert!(verify(&mut store, &root, after, &records[2..]).is_ok());
    let ends = (Bound::Excluded([255; 3]), Bound::Excluded([0; 3]));
    assert!(verify(&mut store, &root, (ends.0, Bound::Unbounded), &[]).is_ok());
    assert!(verify(&mut store, &root, (Bound::Unbounded, ends.1), &[]).is_ok());
}

#[test]
fn verify_of_a_range_with_no_positions_reads_nothing() {
    let (mut store, root, model) = sample();
    for range in [[0, 5, 0]..[0, 5, 0], [5, 0, 0]..[0, 0, 5]] {
        assert!(verify(&mut store, &root, range.clone(), &[]).is_ok());
        assert_eq!(store.reads(), 0);
        assert!(unverified(verify(
            &mut store,
            &root,
            range,
            &records_of(&model)[..1]
        )));
    }
}

#[test]
fn verify_refuses_records_out_of_order_or_outside_the_range() {
    let (mut store, root, model) = sample();
    let mut records = records_of(&model);
    assert!(unverified(verify(&mut store, &root, ..[5, 0, 0], &records)));
    records.swap(0, 1);
    assert!(unverified(verify(&mut store, &root, .., &records)));
}

#[test]
fn verify_reads_once_for_each_digit_of_a_position() {
    let (mut store, root, model) = sample();
    verify(&mut store, &root, .., &records_of(&model)).unwrap();
    assert_eq!(store.reads(), 3);
    verify(&mut store, &root, [0, 5, 2]..=[0, 5, 2], &[]).unwrap();
    assert_eq!(store.reads(), 3);
}

#[test]
fn update_reads_the_children_of_each_node_above_a_change_once() {
    let (mut store, root, _) = sample();
    let root = update(&mut store, &root, &[([0, 5, 2], Some(content(9)))]).unwrap();
    assert_eq!(store.reads(), 3);
    // The second change shares its first two digits with the first, and the third only its first.
    let changes = [
        ([0, 5, 3], None),
        ([0, 5, 4], Some(content(9))),
        ([0, 7, 0], None),
    ];
    update(&mut store, &root, &changes).unwrap();
    assert_eq!(store.reads(), 4);
}

#[test]
fn update_with_no_changes_returns_the_root_and_reads_nothing() {
    let (mut store, root, _) = sample();
    assert_eq!(update::<3>(&mut store, &root, &[]).unwrap(), root);
    assert_eq!(store.reads(), 0);
    let empty = Tree::<3>::initial_root();
    assert_eq!(
        update::<3>(&mut Store::default(), &empty, &[]).unwrap(),
        empty
    );
}

#[test]
fn update_that_changes_nothing_writes_nothing() {
    let (mut store, root, model) = sample();
    let entries = store.entries.clone();
    let there = [0, 5, 0];
    let same = [([0, 5, 2], None), (there, Some(model[&there]))];
    let mut same = same.to_vec();
    same.sort();
    assert_eq!(update(&mut store, &root, &same).unwrap(), root);
    assert_eq!(store.writes, 0);
    assert_eq!(store.entries, entries);
}

#[test]
fn removing_every_record_leaves_no_entries() {
    let (mut store, root, model) = sample();
    let removals: Vec<Change<Sha256, 3>> = model.keys().map(|position| (*position, None)).collect();
    let (some, rest) = removals.split_at(2);
    let root = update(&mut store, &root, some).unwrap();
    let root = update(&mut store, &root, rest).unwrap();
    assert_eq!(root, Tree::<3>::initial_root());
    assert!(store.entries.is_empty());
}

#[test]
fn update_refuses_a_root_that_is_not_the_trees() {
    let (mut store, root, _) = sample();
    let changes = [([0, 5, 2], Some(content(9)))];
    let next = update(&mut store, &root, &changes).unwrap();
    assert!(unverified(update(&mut store, &root, &[([0, 5, 2], None)])));
    assert!(unverified(update(&mut Store::default(), &next, &changes)));
}

#[test]
fn a_hash_of_the_wrong_length_is_corrupt() {
    let (mut store, root, model) = sample();
    let short = Bytes::copy_from_slice(&content(1)[..31]);
    store.entries.insert(key(&[0, 5]), short);
    let verified = verify(&mut store, &root, .., &records_of(&model));
    assert!(matches!(verified, Err(Error::Corrupt)));
    let updated = update(&mut store, &root, &[([0, 5, 0], None)]);
    assert!(matches!(updated, Err(Error::Corrupt)));
}

#[test]
fn a_key_that_is_not_a_nodes_is_corrupt() {
    let (mut store, root, model) = sample();
    // The key says one digit and holds two.
    let stray = Bytes::from([PARTITION, &[1, 0, 9]].concat());
    store
        .entries
        .insert(stray, Bytes::copy_from_slice(&content(1)));
    let verified = verify(&mut store, &root, .., &records_of(&model));
    assert!(matches!(verified, Err(Error::Corrupt)));
    let updated = update(&mut store, &root, &[([0, 5, 0], None)]);
    assert!(matches!(updated, Err(Error::Corrupt)));
}

#[test]
fn trees_with_different_partition_prefixes_share_a_store() {
    let (mut store, root, model) = sample();
    let other = MerkleRadixTree::<Sha256, 3>::new(Bytes::from_static(b"other/"));
    let empty = Tree::<3>::initial_root();
    let changes = [([0, 5, 0], Some(content(9)))];
    let other_root = block_on(other.update(&mut store, &empty, &changes)).unwrap();
    assert!(verify(&mut store, &root, .., &records_of(&model)).is_ok());
    let records = [([0, 5, 0], content(9))];
    assert!(block_on(other.verify(&store, &other_root, .., &records)).is_ok());
    assert!(
        store
            .entries
            .keys()
            .all(|key| key.starts_with(b"tree/") || key.starts_with(b"other/"))
    );
}

#[test]
#[should_panic(expected = "in order of position")]
fn update_panics_on_changes_out_of_order() {
    let changes = [([0, 5, 1], None), ([0, 5, 0], None)];
    let _ = update(&mut Store::default(), &Tree::<3>::initial_root(), &changes);
}

#[test]
#[should_panic(expected = "one for each")]
fn update_panics_on_two_changes_to_one_position() {
    let changes = [([0, 5, 0], None), ([0, 5, 0], Some(content(1)))];
    let _ = update(&mut Store::default(), &Tree::<3>::initial_root(), &changes);
}

#[test]
fn verify_and_update_can_run_on_any_thread() {
    fn sendable(_: &impl Send) {}
    let mut store = Store::default();
    let root = Tree::<3>::initial_root();
    let tree = tree::<3>();
    sendable(&tree.verify(&store, &root, .., &[]));
    sendable(&tree.update(&mut store, &root, &[]));
}
