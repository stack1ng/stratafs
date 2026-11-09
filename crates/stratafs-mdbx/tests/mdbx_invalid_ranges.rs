mod common;
mod configuration;

#[test]
fn panics_on_invalid_ranges() {
    let stores = common::Stores::new();
    stratafs_kv_conformance::panics_on_invalid_ranges(|| stores.open());
}
