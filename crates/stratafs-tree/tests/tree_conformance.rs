use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;
use futures_lite::future::block_on;
use redb::backends::InMemoryBackend;
use stratafs_kv_conformance::runtime::{exhaustive_dfs, seeded};
use stratafs_redb::RedbStore;
use stratafs_store::error::StoreError;
use stratafs_store::forkable::{ForkableKeyValueStore, UnknownVolume};
use stratafs_store::spawn::Inline;
use stratafs_tree::header::TreeVolumeId;
use stratafs_tree::serialized::CorruptValue;
use stratafs_tree::tree::{Tree, TreeConfig};
use stratafs_tree::volume::TreeVolume;

struct SingleLeafConfig;

impl TreeConfig for SingleLeafConfig {
    type Range = u16;
    const NODE_CAPACITY: usize = 4096;
    const MERGE_BELOW_PERCENT: u8 = 25;
}

struct SmallNodeConfig;

impl TreeConfig for SmallNodeConfig {
    type Range = u8;
    const NODE_CAPACITY: usize = 128;
    const MERGE_BELOW_PERCENT: u8 = 25;
}

fn new_tree<C: TreeConfig>() -> Tree<RedbStore<Inline>, impl Fn() -> u128, C> {
    let database = redb::Database::builder()
        .create_with_backend(InMemoryBackend::new())
        .unwrap();
    let next_id = AtomicU64::new(1);
    let allocate_id = move || u128::from(next_id.fetch_add(1, Ordering::Relaxed));
    block_on(Tree::new(
        RedbStore::new(database, Inline).unwrap(),
        allocate_id,
    ))
    .unwrap()
}

fn open_volume<C: TreeConfig>() -> TreeVolume<RedbStore<Inline>, impl Fn() -> u128, C> {
    let tree = new_tree::<C>();
    block_on(async {
        let id = tree.create().await.unwrap();
        tree.open(id).await.unwrap()
    })
}

#[test]
fn opening_a_volume_from_another_tree_is_rejected() {
    let id = block_on(new_tree::<SingleLeafConfig>().create()).unwrap();
    assert!(matches!(
        block_on(new_tree::<SingleLeafConfig>().open(id)),
        Err(StoreError::Rejected(UnknownVolume))
    ));
}

#[test]
fn a_volume_id_round_trips_through_bytes() {
    let id = block_on(new_tree::<SingleLeafConfig>().create()).unwrap();
    assert_eq!(TreeVolumeId::try_from(Bytes::from(id)).unwrap(), id);
    assert!(matches!(
        TreeVolumeId::try_from(Bytes::from_static(b"not a tree volume id")),
        Err(CorruptValue::WrongLength)
    ));
}

#[test]
fn panics_on_invalid_ranges() {
    stratafs_kv_conformance::panics_on_invalid_ranges(open_volume::<SingleLeafConfig>);
}

#[test]
fn is_strictly_serializable_in_a_single_leaf() {
    stratafs_kv_conformance::is_strictly_serializable(seeded(), open_volume::<SingleLeafConfig>);
}

#[test]
fn is_strictly_serializable_across_small_nodes() {
    stratafs_kv_conformance::is_strictly_serializable(seeded(), open_volume::<SmallNodeConfig>);
}

#[test]
fn observation_lifecycle_in_a_single_leaf() {
    stratafs_kv_conformance::observation_lifecycle(
        exhaustive_dfs(),
        open_volume::<SingleLeafConfig>,
    );
}

#[test]
fn observation_lifecycle_across_small_nodes() {
    stratafs_kv_conformance::observation_lifecycle(
        exhaustive_dfs(),
        open_volume::<SmallNodeConfig>,
    );
}

#[test]
fn forks_stay_isolated_in_a_single_leaf() {
    stratafs_kv_conformance::forks_stay_isolated(seeded(), new_tree::<SingleLeafConfig>);
}

#[test]
fn forks_stay_isolated_across_small_nodes() {
    stratafs_kv_conformance::forks_stay_isolated(seeded(), new_tree::<SmallNodeConfig>);
}
