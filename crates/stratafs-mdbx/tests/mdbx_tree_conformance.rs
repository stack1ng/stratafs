mod configuration;

use std::sync::atomic::{AtomicU64, Ordering};

use futures_lite::future::block_on;
use stratafs_kv_conformance::runtime::seeded;
use stratafs_mdbx::MdbxMetadata;
use stratafs_store::spawn::Inline;
use stratafs_tree::tree::{Tree, TreeConfig};

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

// new_tree is a tree over a store in a fresh directory, which it removes at once:
// the store keeps its files open, and they go when it closes.
fn new_tree<C: TreeConfig>() -> Tree<MdbxMetadata<Inline>, impl Fn() -> u128, C> {
    let directory = tempfile::tempdir().unwrap();
    let store = configuration::open(directory.path().join("store.mdbx"), Inline).unwrap();
    let next_id = AtomicU64::new(1);
    let allocate_id = move || u128::from(next_id.fetch_add(1, Ordering::Relaxed));
    block_on(Tree::new(store, allocate_id)).unwrap()
}

#[test]
fn forks_stay_isolated_in_a_single_leaf() {
    stratafs_kv_conformance::forks_stay_isolated(seeded(), new_tree::<SingleLeafConfig>);
}

#[test]
fn forks_stay_isolated_across_small_nodes() {
    stratafs_kv_conformance::forks_stay_isolated(seeded(), new_tree::<SmallNodeConfig>);
}
