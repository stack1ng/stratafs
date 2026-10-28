mod common;
mod configuration;

#[test]
fn panics_on_invalid_ranges() {
    stratafs_kv_conformance::panics_on_invalid_ranges(common::open_store);
}
