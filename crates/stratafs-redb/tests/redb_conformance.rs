use redb::Database;
use redb::backends::InMemoryBackend;
use stratafs_redb::RedbStore;

#[test]
fn is_strictly_serializable() {
    stratafs_kv_conformance::is_strictly_serializable(|| {
        RedbStore::new(
            Database::builder()
                .create_with_backend(InMemoryBackend::new())
                .unwrap(),
        )
        .unwrap()
    });
}
