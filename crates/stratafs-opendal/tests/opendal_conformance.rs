use opendal::Operator;
use stratafs_opendal::OpendalContentStore;

fn memory_store() -> OpendalContentStore {
    OpendalContentStore::new(Operator::new(opendal::services::Memory::default()).unwrap())
}

#[test]
fn stores_content_under_its_hash() {
    stratafs_content_conformance::stores_content_under_its_hash(memory_store);
}

#[test]
fn reads_ranges() {
    stratafs_content_conformance::reads_ranges(memory_store);
}

#[test]
fn rejects_ranges_beyond_the_content() {
    stratafs_content_conformance::rejects_ranges_beyond_the_content(memory_store);
}

#[test]
fn rejects_unknown_content() {
    stratafs_content_conformance::rejects_unknown_content(memory_store);
}
