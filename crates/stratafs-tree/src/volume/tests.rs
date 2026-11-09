use std::collections::{HashMap, HashSet};
use std::convert::Infallible;
use std::ops::Range;
use std::sync::atomic::{AtomicU64, Ordering};

use futures_lite::future::block_on;
use proptest::prelude::RngExt;
use proptest::test_runner::{RngAlgorithm, TestRng};
use redb::backends::InMemoryBackend;
use stratafs_redb::{RedbFailure, RedbStore};
use stratafs_store::forkable::ForkableKeyValueStore;
use stratafs_store::spawn::Inline;
use zerocopy::little_endian::{U64, U128};
use zerocopy::{FromBytes, IntoBytes};

use super::*;
use crate::header::JobId;
use crate::invariants;
use crate::node::Node;
use crate::tree::Tree;

struct SmallNodeConfig;

impl TreeConfig for SmallNodeConfig {
    type Range = u8;
    const NODE_CAPACITY: usize = 128;
    const MERGE_BELOW_PERCENT: u8 = 25;
}

type TestTree = Tree<RedbStore<Inline>, fn() -> u128, SmallNodeConfig>;
type TestVolume = TreeVolume<RedbStore<Inline>, fn() -> u128, SmallNodeConfig>;
type Failure = TreeFailure<RedbFailure, Infallible>;

const KEY_COUNT: usize = 400;

fn allocate_id() -> u128 {
    static NEXT_ID: AtomicU64 = AtomicU64::new(1);
    u128::from(NEXT_ID.fetch_add(1, Ordering::Relaxed))
}

fn new_tree() -> TestTree {
    let database = redb::Database::builder()
        .create_with_backend(InMemoryBackend::new())
        .unwrap();
    block_on(Tree::new(
        RedbStore::new(database, Inline).unwrap(),
        allocate_id as fn() -> u128,
    ))
    .unwrap()
}

fn new_volume(tree: &TestTree) -> TestVolume {
    block_on(async {
        let id = tree.create().await.unwrap();
        tree.open(id).await.unwrap()
    })
}

// key spreads the indexes over the key space, so consecutive indexes land in different leaves.
fn key(index: usize) -> Bytes {
    Bytes::from(format!("key/{:04}", index * 37 % KEY_COUNT))
}

fn set_keys(volume: &TestVolume, indexes: Range<usize>) {
    block_on(volume.transact(|mut transaction| {
        let indexes = indexes.clone();
        Box::pin(async move {
            for index in indexes {
                transaction.set(key(index), &b"value"[..])?;
            }
            Ok::<_, StoreError<SetRejection<Infallible>, Failure>>(((), transaction))
        })
    }))
    .unwrap();
}

fn clear_keys(volume: &TestVolume, indexes: Range<usize>) {
    block_on(volume.transact(|mut transaction| {
        let indexes = indexes.clone();
        Box::pin(async move {
            for index in indexes {
                transaction.clear(key(index))?;
            }
            Ok::<_, Failure>(((), transaction))
        })
    }))
    .unwrap();
}

fn clear_range(volume: &TestVolume, range: Range<Bytes>) {
    block_on(volume.transact(|mut transaction| {
        let range = range.clone();
        Box::pin(async move {
            transaction.clear_range(range)?;
            Ok::<_, Failure>(((), transaction))
        })
    }))
    .unwrap();
}

fn fork<V>(volume: &V) -> TreeVolumeId
where
    V: KeyValueStore,
    for<'txn> V::Transaction<'txn>: ForkingTransaction<VolumeId = TreeVolumeId>,
{
    block_on(volume.transact(|mut transaction| {
        Box::pin(async move {
            let id = transaction.fork().await?;
            Ok::<_, <V::Transaction<'_> as ReadKeyValueTransaction>::Failure>((id, transaction))
        })
    }))
    .unwrap()
}

// set_keys_and_fork sets every key and forks in one transaction, so the fork flushes the growth first.
fn set_keys_and_fork<V>(volume: &V) -> TreeVolumeId
where
    V: KeyValueStore,
    for<'txn> V::Transaction<'txn>: ForkingTransaction<VolumeId = TreeVolumeId>,
{
    block_on(volume.transact(|mut transaction| {
        Box::pin(async move {
            for index in 0..KEY_COUNT {
                transaction.set(key(index), &b"value"[..])?;
            }
            let id = transaction.fork().await?;
            Ok::<_, StoreError<SetRejection<V::Rejection>, V::Failure>>((id, transaction))
        })
    }))
    .unwrap()
}

// depth is the number of internal layers above the leaves.
fn depth<S: KeyValueStore, A: Fn() -> u128 + Send + Sync + 'static>(
    volume: &TreeVolume<S, A, SmallNodeConfig>,
) -> usize {
    block_on(volume.read_transact(|transaction| {
        Box::pin(async move {
            let path = transaction.descend(&Bytes::new()).await?;
            Ok::<_, TreeFailure<S::Failure, S::Rejection>>(path.steps.len())
        })
    }))
    .unwrap()
}

fn root<S: KeyValueStore, A: Fn() -> u128 + Send + Sync + 'static>(
    volume: &TreeVolume<S, A, SmallNodeConfig>,
) -> NodeId {
    block_on(volume.read_transact(|transaction| {
        Box::pin(async move {
            let header = transaction.get_volume_header().await?;
            Ok::<_, TreeFailure<S::Failure, S::Rejection>>(header.root)
        })
    }))
    .unwrap()
}

fn counts_match_parents<S: KeyValueStore, A>(volume: &TreeVolume<S, A, SmallNodeConfig>) -> bool {
    block_on(volume.shared.store.read_transact(|transaction| {
        Box::pin(async move {
            Ok::<_, Infallible>(
                invariants::node_ref_counts_match_parents::<_, u8>(&*transaction).await,
            )
        })
    }))
    .unwrap()
}

// stored_counts are every node reference count the store holds.
fn stored_counts<S: KeyValueStore, A>(volume: &TreeVolume<S, A, SmallNodeConfig>) -> Vec<u64> {
    block_on(volume.shared.store.read_transact(|transaction| {
        Box::pin(async move {
            let counts: Vec<_> = transaction
                .get_range(
                    Bytes::from(TreeKey::NodeRefCount(NodeId(U128::new(0))))
                        ..=Bytes::from(TreeKey::NodeRefCount(NodeId(U128::new(u128::MAX)))),
                    usize::MAX,
                )
                .await?
                .map(|(_, count)| U64::read_from_bytes(&count).unwrap().get())
                .collect();
            Ok::<_, <S::ReadTransaction<'_> as ReadKeyValueTransaction>::Failure>(counts)
        })
    }))
    .unwrap()
}

// jobs are every pending job's volume and node, in key order.
fn jobs<S: KeyValueStore, A>(
    volume: &TreeVolume<S, A, SmallNodeConfig>,
) -> Vec<(TreeVolumeId, NodeId)> {
    block_on(volume.shared.store.read_transact(|transaction| {
        Box::pin(async move {
            let jobs: Vec<_> = transaction
                .get_range(
                    Bytes::from(TreeKey::Dereference(
                        TreeVolumeId(0),
                        NodeId(U128::new(0)),
                        JobId(0),
                    ))
                        ..=Bytes::from(TreeKey::Dereference(
                            TreeVolumeId(u128::MAX),
                            NodeId(U128::new(u128::MAX)),
                            JobId(u128::MAX),
                        )),
                    usize::MAX,
                )
                .await?
                .map(|(key, value)| {
                    assert!(value.is_empty());
                    match storekey::decode(&key[..]).unwrap() {
                        TreeKey::Dereference(volume, node, _) => (volume, node),
                        _ => unreachable!("the range holds only jobs"),
                    }
                })
                .collect();
            Ok::<_, <S::ReadTransaction<'_> as ReadKeyValueTransaction>::Failure>(jobs)
        })
    }))
    .unwrap()
}

// tree_parents maps every node in the volume's tree to its parent, where the root has none.
fn tree_parents<S: KeyValueStore, A>(
    volume: &TreeVolume<S, A, SmallNodeConfig>,
) -> HashMap<u128, Option<u128>> {
    let volume_id = volume.id;
    block_on(volume.shared.store.read_transact(|transaction| {
        Box::pin(async move {
            let header = transaction
                .get(TreeKey::VolumeHeader(volume_id).into())
                .await?
                .unwrap();
            let root = VolumeHeader::view(&header).unwrap().root;
            let mut parents = HashMap::from([(root.0.get(), None)]);
            let mut unvisited = vec![root];
            while let Some(id) = unvisited.pop() {
                let stored = transaction.get(TreeKey::Node(id).into()).await?.unwrap();
                if let (_, Node::Internal(entries)) = Node::<Bytes, u8>::read(stored).unwrap() {
                    for index in 0..entries.len() {
                        let child = entries.get(index).unwrap().unwrap().child;
                        parents.insert(child.0.get(), Some(id.0.get()));
                        unvisited.push(child);
                    }
                }
            }
            Ok::<_, <S::ReadTransaction<'_> as ReadKeyValueTransaction>::Failure>(parents)
        })
    }))
    .unwrap()
}

// stored_nodes are the ids of every node the store holds.
fn stored_nodes<S: KeyValueStore, A>(volume: &TreeVolume<S, A, SmallNodeConfig>) -> HashSet<u128> {
    block_on(volume.shared.store.read_transact(|transaction| {
        Box::pin(async move {
            let nodes: HashSet<_> = transaction
                .get_range(
                    Bytes::from(TreeKey::Node(NodeId(U128::new(0))))
                        ..=Bytes::from(TreeKey::Node(NodeId(U128::new(u128::MAX)))),
                    usize::MAX,
                )
                .await?
                .map(|(key, _)| match storekey::decode(&key[..]).unwrap() {
                    TreeKey::Node(id) => id.0.get(),
                    _ => unreachable!("the range holds only nodes"),
                })
                .collect();
            Ok::<_, <S::ReadTransaction<'_> as ReadKeyValueTransaction>::Failure>(nodes)
        })
    }))
    .unwrap()
}

#[test]
fn growing_and_shrinking_the_tree_only_moves_pointers() {
    let tree = new_tree();
    let volume = new_volume(&tree);
    let mut deepest = 0;
    for start in (0..KEY_COUNT).step_by(10) {
        set_keys(&volume, start..start + 10);
        assert!(counts_match_parents(&volume));
        deepest = deepest.max(depth(&volume));
    }
    assert!(deepest >= 3, "the tree grew {deepest} internal layers");
    for start in (0..KEY_COUNT).step_by(10) {
        clear_keys(&volume, start..start + 10);
        assert!(counts_match_parents(&volume));
    }
    assert_eq!(depth(&volume), 0);
    assert!(stored_counts(&volume).is_empty());
}

#[test]
fn growing_and_shrinking_the_tree_in_one_write_each_only_moves_pointers() {
    let tree = new_tree();
    let volume = new_volume(&tree);
    set_keys(&volume, 0..KEY_COUNT);
    assert!(depth(&volume) >= 3);
    assert!(counts_match_parents(&volume));
    clear_keys(&volume, 0..KEY_COUNT);
    assert_eq!(depth(&volume), 0);
    assert!(counts_match_parents(&volume));
    assert!(stored_counts(&volume).is_empty());
}

#[test]
fn a_fork_counts_both_headers_on_the_root() {
    let tree = new_tree();
    let volume = new_volume(&tree);
    set_keys(&volume, 0..KEY_COUNT);
    assert!(depth(&volume) >= 2);
    fork(&volume);
    assert_eq!(stored_counts(&volume), [2]);
    assert!(counts_match_parents(&volume));
}

#[test]
fn forks_of_forks_before_any_write_count_every_header_on_the_root() {
    let tree = new_tree();
    let volume = new_volume(&tree);
    set_keys(&volume, 0..KEY_COUNT);
    let forked = block_on(tree.open(fork(&volume))).unwrap();
    fork(&forked);
    fork(&volume);
    assert_eq!(root(&forked), root(&volume));
    assert_eq!(stored_counts(&volume), [4]);
    assert!(counts_match_parents(&volume));
}

#[test]
fn a_fork_after_a_write_in_the_same_transaction_counts_both_headers_on_the_new_root() {
    let tree = new_tree();
    let volume = new_volume(&tree);
    let forked = block_on(tree.open(set_keys_and_fork(&volume))).unwrap();
    assert!(depth(&volume) >= 2);
    assert_eq!(root(&forked), root(&volume));
    assert_eq!(stored_counts(&volume), [2]);
    assert!(counts_match_parents(&volume));
}

#[test]
fn a_fork_of_an_empty_volume_counts_both_headers_on_the_root() {
    let tree = new_tree();
    let volume = new_volume(&tree);
    fork(&volume);
    assert_eq!(stored_counts(&volume), [2]);
    assert!(counts_match_parents(&volume));
}

#[test]
fn the_check_catches_a_count_that_misses_a_header() {
    let tree = new_tree();
    let volume = new_volume(&tree);
    fork(&volume);
    let root = root(&volume);
    block_on(volume.shared.store.transact(|mut transaction| {
        Box::pin(async move {
            transaction.clear(TreeKey::NodeRefCount(root).into())?;
            Ok::<_, RedbFailure>(((), transaction))
        })
    }))
    .unwrap();
    assert!(!counts_match_parents(&volume));
}

#[test]
fn the_check_catches_a_count_on_a_node_no_tree_reaches() {
    let tree = new_tree();
    let volume = new_volume(&tree);
    block_on(volume.shared.store.transact(|mut transaction| {
        Box::pin(async move {
            transaction.set(
                TreeKey::NodeRefCount(NodeId(U128::new(u128::MAX))).into(),
                U64::new(2).as_bytes(),
            )?;
            Ok::<_, StoreError<SetRejection<Infallible>, RedbFailure>>(((), transaction))
        })
    }))
    .unwrap();
    assert!(!counts_match_parents(&volume));
}

#[test]
fn a_range_clear_writes_one_job_for_each_subtree_it_cuts_off() {
    let tree = new_tree();
    let volume = new_volume(&tree);
    set_keys(&volume, 0..KEY_COUNT);
    assert!(depth(&volume) >= 3);
    let before = tree_parents(&volume);
    clear_range(&volume, Bytes::from("key/0040")..Bytes::from("key/0360"));
    let after = tree_parents(&volume);
    let stored = stored_nodes(&volume);
    // a cut off node left the tree but is still stored, and only the top of each cut off subtree lost its parent
    let cut_off = |id: &u128| !after.contains_key(id) && stored.contains(id);
    let mut expected: Vec<_> = before
        .iter()
        .filter(|(id, parent)| cut_off(id) && !parent.is_some_and(|parent| cut_off(&parent)))
        .map(|(id, _)| (volume.id, NodeId(U128::new(*id))))
        .collect();
    expected.sort_by_key(|(_, id)| id.0.get());
    assert!(!expected.is_empty());
    assert_eq!(jobs(&volume), expected);
    assert!(counts_match_parents(&volume));
}

#[test]
fn a_write_after_a_fork_writes_one_job_for_the_shared_root_in_the_writing_volume() {
    let tree = new_tree();
    let volume = new_volume(&tree);
    set_keys(&volume, 0..KEY_COUNT);
    assert!(depth(&volume) >= 2);
    let shared_root = root(&volume);
    let forked = block_on(tree.open(fork(&volume))).unwrap();
    set_keys(&forked, 0..1);
    assert_eq!(jobs(&volume), [(forked.id, shared_root)]);
    assert!(counts_match_parents(&volume));
    set_keys(&volume, 0..1);
    assert_eq!(
        jobs(&volume),
        [(volume.id, shared_root), (forked.id, shared_root)]
    );
    assert!(counts_match_parents(&volume));
}

#[test]
fn a_root_move_in_a_fork_writes_one_job_for_the_old_root() {
    let tree = new_tree();
    let shrinking = new_volume(&tree);
    set_keys(&shrinking, 0..KEY_COUNT);
    assert!(depth(&shrinking) >= 3);
    let tall_root = root(&shrinking);
    let shrunk = block_on(tree.open(fork(&shrinking))).unwrap();
    clear_keys(&shrunk, 0..KEY_COUNT);
    assert_eq!(depth(&shrunk), 0);
    assert_eq!(jobs(&shrunk), [(shrunk.id, tall_root)]);
    assert!(counts_match_parents(&shrunk));

    let growing = new_volume(&tree);
    let leaf_root = root(&growing);
    let grown = block_on(tree.open(fork(&growing))).unwrap();
    set_keys(&grown, 0..KEY_COUNT);
    assert!(depth(&grown) >= 3);
    assert_eq!(
        jobs(&grown),
        [(shrunk.id, tall_root), (grown.id, leaf_root)]
    );
    assert!(counts_match_parents(&grown));
}

#[test]
fn counts_match_parents_with_pending_jobs_after_random_forks_writes_and_clears() {
    let tree = new_tree();
    let mut volumes = vec![new_volume(&tree)];
    let mut rng = TestRng::deterministic_rng(RngAlgorithm::ChaCha);
    for _ in 0..300 {
        let index = rng.random_range(0..volumes.len());
        let volume = &volumes[index];
        let start = rng.random_range(0..KEY_COUNT);
        let end = rng.random_range(start + 1..=KEY_COUNT.min(start + 80));
        match rng.random_range(0..4) {
            0 if volumes.len() < 8 => {
                let forked = block_on(tree.open(fork(volume))).unwrap();
                volumes.push(forked);
            }
            0 | 1 => set_keys(volume, start..end),
            2 => clear_keys(volume, start..end),
            _ => clear_range(
                volume,
                Bytes::from(format!("key/{start:04}"))..Bytes::from(format!("key/{end:04}")),
            ),
        }
        assert!(counts_match_parents(&volumes[index]));
    }
    assert!(!jobs(&volumes[0]).is_empty());
}

#[test]
fn the_check_catches_a_job_on_a_node_that_lost_no_parent() {
    let tree = new_tree();
    let volume = new_volume(&tree);
    fork(&volume);
    let root = root(&volume);
    let volume_id = volume.id;
    block_on(volume.shared.store.transact(|mut transaction| {
        Box::pin(async move {
            transaction.set(
                TreeKey::Dereference(volume_id, root, JobId(u128::MAX)).into(),
                Bytes::new(),
            )?;
            Ok::<_, StoreError<SetRejection<Infallible>, RedbFailure>>(((), transaction))
        })
    }))
    .unwrap();
    assert!(!counts_match_parents(&volume));
}

#[test]
fn the_check_catches_a_missing_job_on_a_cut_off_subtree() {
    let tree = new_tree();
    let volume = new_volume(&tree);
    set_keys(&volume, 0..KEY_COUNT);
    clear_range(&volume, Bytes::from("key/0040")..Bytes::from("key/0360"));
    assert!(counts_match_parents(&volume));
    block_on(volume.shared.store.transact(|mut transaction| {
        Box::pin(async move {
            let (first, _) = transaction
                .get_range(
                    Bytes::from(TreeKey::Dereference(
                        TreeVolumeId(0),
                        NodeId(U128::new(0)),
                        JobId(0),
                    ))
                        ..=Bytes::from(TreeKey::Dereference(
                            TreeVolumeId(u128::MAX),
                            NodeId(U128::new(u128::MAX)),
                            JobId(u128::MAX),
                        )),
                    1,
                )
                .await?
                .next()
                .unwrap();
            transaction.clear(first)?;
            Ok::<_, RedbFailure>(((), transaction))
        })
    }))
    .unwrap();
    assert!(!counts_match_parents(&volume));
}

#[test]
fn the_check_catches_a_lost_header() {
    let tree = new_tree();
    let volume = new_volume(&tree);
    set_keys(&volume, 0..KEY_COUNT);
    let volume_id = volume.id;
    block_on(volume.shared.store.transact(|mut transaction| {
        Box::pin(async move {
            transaction.clear(TreeKey::VolumeHeader(volume_id).into())?;
            Ok::<_, RedbFailure>(((), transaction))
        })
    }))
    .unwrap();
    assert!(!counts_match_parents(&volume));
}
