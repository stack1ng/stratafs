mod common;
mod stores;

use std::sync::Arc;

use stores::Stores;
use stratafs_kv_conformance::runtime::{exhaustive_dfs, seeded};
use stratafs_sqlite::SqliteContent;

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

#[test]
fn stores_content_under_its_hash() {
    let stores = Stores::new();
    stratafs_content_conformance::stores_content_under_its_hash(|| {
        SqliteContent::of(&stores.open())
    });
}

#[test]
fn reads_ranges() {
    let stores = Stores::new();
    stratafs_content_conformance::reads_ranges(|| SqliteContent::of(&stores.open()));
}

#[test]
fn rejects_ranges_beyond_the_content() {
    let stores = Stores::new();
    stratafs_content_conformance::rejects_ranges_beyond_the_content(|| {
        SqliteContent::of(&stores.open())
    });
}

#[test]
fn rejects_unknown_content() {
    let stores = Stores::new();
    stratafs_content_conformance::rejects_unknown_content(|| SqliteContent::of(&stores.open()));
}
