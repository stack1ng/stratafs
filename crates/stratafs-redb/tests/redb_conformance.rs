mod common;

#[test]
fn is_strictly_serializable() {
    stratafs_kv_conformance::is_strictly_serializable(common::new_store);
}
