use std::convert::Infallible;
use std::ops::Range;
use std::sync::atomic::{AtomicU64, Ordering};

use futures_lite::future::block_on;
use redb::backends::InMemoryBackend;
use stratafs_redb::{RedbFailure, RedbStore};
use stratafs_store::forkable::ForkableKeyValueStore;
use stratafs_store::spawn::Inline;
use zerocopy::little_endian::{U64, U128};
use zerocopy::{FromBytes, IntoBytes};

use super::*;
use crate::invariants;
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
