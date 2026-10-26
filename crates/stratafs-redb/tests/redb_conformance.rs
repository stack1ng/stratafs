mod common;

use redb::backends::InMemoryBackend;
use stratafs_kv_conformance::runtime::{exhaustive_dfs, seeded};

#[test]
fn is_strictly_serializable() {
    stratafs_kv_conformance::is_strictly_serializable(
        // the search space here is unbounded, so we must bound it with a seed
        seeded(),
        || common::open_store(InMemoryBackend::new()),
    );
}

#[test]
fn observation_lifecycle() {
    stratafs_kv_conformance::observation_lifecycle(
        // the search space here is bounded, so we should be exhaustive
        exhaustive_dfs(),
        || common::open_store(InMemoryBackend::new()),
    );
}
