mod common;

use stratafs_kv_conformance::runtime::{exhaustive_dfs, seeded};

#[test]
fn is_strictly_serializable() {
    stratafs_kv_conformance::is_strictly_serializable(
        // the search space here is large, so we must bound it with a seed
        seeded(),
        common::new_store,
    );
}

#[test]
fn changed_waits_for_the_observing_transaction() {
    stratafs_kv_conformance::changed_waits_for_the_observing_transaction(
        // the search space here is small, so we should be exhaustive
        exhaustive_dfs(),
        common::new_store,
    );
}
