mod common;
mod stores;

use std::sync::Arc;

use stores::Stores;
use stratafs_kv_conformance::runtime::{exhaustive_dfs, seeded};

#[test]
fn is_strictly_serializable() {
    let stores = Arc::new(Stores::new());
    stratafs_kv_conformance::is_strictly_serializable(
        // the search space here is unbounded, so we must bound it with a seed
        seeded(),
        move || stores.open(),
    );
}

#[test]
fn observation_lifecycle() {
    let stores = Arc::new(Stores::new());
    stratafs_kv_conformance::observation_lifecycle(
        // the search space here is bounded, so we should be exhaustive
        exhaustive_dfs(),
        move || stores.open(),
    );
}
