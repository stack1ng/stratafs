mod configuration;

use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use futures_lite::future::block_on;
use stratafs_kv_conformance::runtime::seeded;
use stratafs_mdbx::MdbxMetadata;
use stratafs_runtime::TaskDroppingRuntime;
use stratafs_store::spawn::Inline;
use stratafs_tree::tree::{NoContent, Tree, TreeConfig};

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

// new_tree is a tree over a store in a fresh directory in stores, which lasts as long as stores, since a store whose
// change log is removed reads Gaps, which wake its watches from outside the simulation.
fn new_tree<C: TreeConfig>(
    stores: &Path,
) -> Tree<MdbxMetadata<Inline>, impl Fn() -> u128 + use<C>, C> {
    let directory = tempfile::tempdir_in(stores).unwrap().keep();
    let store = configuration::open(directory.join("store.mdbx"), Inline).unwrap();
    let next_id = AtomicU64::new(1);
    let allocate_id = move || u128::from(next_id.fetch_add(1, Ordering::Relaxed));
    block_on(Tree::new(
        store,
        allocate_id,
        NoContent,
        Arc::new(TaskDroppingRuntime),
    ))
    .unwrap()
}

#[test]
fn forks_stay_isolated_in_a_single_leaf() {
    let stores = tempfile::tempdir().unwrap();
    stratafs_kv_conformance::forks_stay_isolated(seeded(), move || {
        new_tree::<SingleLeafConfig>(stores.path())
    });
}

#[test]
fn forks_stay_isolated_across_small_nodes() {
    let stores = tempfile::tempdir().unwrap();
    stratafs_kv_conformance::forks_stay_isolated(seeded(), move || {
        new_tree::<SmallNodeConfig>(stores.path())
    });
}
