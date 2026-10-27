mod common;
mod stores;

use stores::Stores;

#[test]
fn panics_on_invalid_ranges() {
    let stores = Stores::new();
    stratafs_kv_conformance::panics_on_invalid_ranges(|| stores.open());
}
