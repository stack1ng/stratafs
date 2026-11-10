use object_store::memory::InMemory;
use stratafs_object_store::ObjectStoreContentStore;

fn memory_store() -> ObjectStoreContentStore {
    ObjectStoreContentStore::new(InMemory::new())
}

#[test]
fn stores_content_under_its_id() {
    stratafs_content_conformance::stores_content_under_its_id(memory_store);
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

#[test]
fn keeps_the_same_content_under_each_id_apart() {
    stratafs_content_conformance::keeps_the_same_content_under_each_id_apart(memory_store);
}

#[test]
fn deletes_content() {
    stratafs_content_conformance::deletes_content(memory_store);
}

#[test]
fn deletes_content_that_is_already_gone() {
    stratafs_content_conformance::deletes_content_that_is_already_gone(memory_store);
}
