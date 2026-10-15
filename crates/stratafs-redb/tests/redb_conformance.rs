use redb::Database;
use redb::backends::InMemoryBackend;
use stratafs_redb::{Inline, RedbStore};

#[test]
fn is_strictly_serializable() {
    stratafs_kv_conformance::is_strictly_serializable(new_store);
}

#[test]
fn panics_on_invalid_ranges() {
    stratafs_kv_conformance::panics_on_invalid_ranges(new_store);
}

fn new_store() -> RedbStore<Inline> {
    RedbStore::new(
        Database::builder()
            .create_with_backend(InMemoryBackend::new())
            .unwrap(),
        Inline,
    )
    .unwrap()
}
